//! Shared test helpers for the anti-pattern integration tests.
//!
//! Each `tests/*.rs` file is its own crate; declaring `mod common;`
//! re-includes this file in that crate's build. Helpers here:
//!  * `TestProxy` — wraps a `ProxyHandle` + correlator.
//!  * `WireClient` — minimal Kafka wire driver (no librdkafka dep).
//!  * `env_or_skip` / `upstream_or_skip` — gate tests on env vars set
//!    by CI / dev when a real cluster is available.
//!  * `negotiate` — round-trip `ApiVersions` to confirm reachability.
//!  * `wait_for_kind` — poll the detector snapshot until a kind
//!    appears, with a bounded grace window.
//!  * `record_batch_one` — build a 1-record `RecordBatch` v2.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::useless_vec,
    clippy::large_stack_arrays,
    clippy::must_use_candidate,
    clippy::future_not_send,
    dead_code
)]

use std::io;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    api_versions_request::ApiVersionsRequest, ApiKey, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use kapture_lib::example_api::{
    CapturedMessage, ProtoCorrelator, ProxyConfig, ProxyHandle, RecordSink,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const ENV_BOOTSTRAP: &str = "KAPTURE_KAFKA_BOOTSTRAP";
pub const ENV_MB_BROKERS: &str = "KAPTURE_KAFKA_MB_BROKERS";
pub const ENV_LEGACY: &str = "KAPTURE_KAFKA_LEGACY";
pub const ENV_SASL: &str = "KAPTURE_KAFKA_SASL";

pub fn upstream_or_skip(test_name: &str) -> Option<String> {
    env_or_skip(test_name, ENV_BOOTSTRAP)
}

pub fn env_or_skip(test_name: &str, key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping {test_name}: ${key} not set");
            None
        }
    }
}

pub struct TestProxy {
    pub handle: ProxyHandle,
    pub correlator: Arc<ProtoCorrelator>,
}

impl TestProxy {
    pub async fn start(upstream: String) -> io::Result<Self> {
        Self::start_with_correlator(upstream, Arc::new(ProtoCorrelator::new())).await
    }
    pub async fn start_with_correlator(
        upstream: String,
        correlator: Arc<ProtoCorrelator>,
    ) -> io::Result<Self> {
        let cfg = ProxyConfig::new(upstream, 0).with_bind(IpAddr::from_str("127.0.0.1").unwrap());
        let sink: RecordSink = Arc::new(|_: CapturedMessage| {});
        let handle = ProxyHandle::start(cfg, Arc::clone(&correlator), sink)
            .await
            .map_err(io::Error::other)?;
        Ok(Self { handle, correlator })
    }
    pub fn listen_port(&self) -> u16 {
        self.handle.local_addr().port()
    }
    pub fn detections(&self) -> Vec<kapture_lib::example_api::Detection> {
        self.correlator.anti_patterns().detections
    }
    pub async fn stop(self) {
        self.handle.stop().await;
    }
}

/// Minimal Kafka wire client. Length-prefixed frames; `kafka-protocol`
/// encodes/decodes headers + bodies.
pub struct WireClient {
    pub stream: TcpStream,
    pub next_corr: i32,
    pub client_id: StrBytes,
}

impl WireClient {
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            next_corr: 1,
            client_id: StrBytes::from_static_str("kapture-it"),
        })
    }
    pub fn alloc_corr(&mut self) -> i32 {
        let c = self.next_corr;
        self.next_corr += 1;
        c
    }
    pub async fn send<R>(&mut self, api: ApiKey, version: i16, body: &R) -> io::Result<i32>
    where
        R: Encodable,
    {
        let corr = self.alloc_corr();
        let header_version = api.request_header_version(version);
        let header = RequestHeader::default()
            .with_request_api_key(api as i16)
            .with_request_api_version(version)
            .with_correlation_id(corr)
            .with_client_id(Some(self.client_id.clone()));
        let mut body_buf = BytesMut::with_capacity(256);
        header
            .encode(&mut body_buf, header_version)
            .map_err(io::Error::other)?;
        body.encode(&mut body_buf, version)
            .map_err(io::Error::other)?;
        let payload = body_buf.freeze();
        let mut framed = BytesMut::with_capacity(4 + payload.len());
        framed.put_i32(i32::try_from(payload.len()).map_err(io::Error::other)?);
        framed.extend_from_slice(&payload);
        self.stream.write_all(&framed).await?;
        Ok(corr)
    }
    pub async fn recv_raw(&mut self) -> io::Result<Bytes> {
        let mut len_buf = [0_u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0_u8; len];
        self.stream.read_exact(&mut body).await?;
        Ok(Bytes::from(body))
    }
    pub async fn recv_response<R>(&mut self, api: ApiKey, version: i16) -> io::Result<R>
    where
        R: Decodable,
    {
        let mut buf = self.recv_raw().await?;
        let header_version = api.response_header_version(version);
        ResponseHeader::decode(&mut buf, header_version).map_err(io::Error::other)?;
        R::decode(&mut buf, version).map_err(io::Error::other)
    }
}

pub async fn negotiate(client: &mut WireClient) -> io::Result<i16> {
    let req = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("kapture-it"))
        .with_client_software_version(StrBytes::from_static_str("0"));
    client.send(ApiKey::ApiVersions, 3, &req).await?;
    let resp: kafka_protocol::messages::ApiVersionsResponse =
        client.recv_response(ApiKey::ApiVersions, 3).await?;
    let produce_max = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::Produce as i16)
        .map_or(7, |k| k.max_version);
    Ok(produce_max)
}

pub async fn wait_for_kind<F>(proxy: &TestProxy, mut pred: F, label: &str) -> bool
where
    F: FnMut(&kapture_lib::example_api::Detection) -> bool,
{
    for _ in 0..50 {
        if proxy.detections().iter().any(&mut pred) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!(
        "no '{label}' detection — current detections: {:#?}",
        proxy.detections()
    );
    false
}

pub fn record_batch_one(payload: &[u8]) -> Bytes {
    let records = [Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        timestamp: 0,
        sequence: -1,
        offset: 0,
        key: None,
        value: Some(Bytes::copy_from_slice(payload)),
        headers: Default::default(),
    }];
    let opts = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    let mut buf = BytesMut::with_capacity(128);
    RecordBatchEncoder::encode(&mut buf, records.iter(), &opts).expect("encode batch");
    buf.freeze()
}
