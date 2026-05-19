//! Drive the 4 client-side anti-patterns through whatever Kafka proxy
//! is listening on `127.0.0.1:9092` — typically the Kapture desktop
//! app's local listener. Use this to demo the Expert tab live:
//!
//! ```text
//!   docker compose --profile redpanda up -d
//!   pnpm tauri dev            # start Kapture, point upstream at localhost:19092
//!   cargo run --manifest-path src-tauri/Cargo.toml --example drive_anti_patterns
//! ```
//!
//! The 3 broker-symptom patterns (stale-leader, mixed-api-version,
//! SASL-too-short) need specific broker shapes (multi-broker /
//! mixed-version / SASL-enabled) and aren't exercised here — they're
//! covered by the integration test crate.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::default_trait_access,
    clippy::useless_vec,
    clippy::future_not_send,
    clippy::doc_markdown,
    clippy::must_use_candidate
)]

use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    api_versions_request::ApiVersionsRequest,
    init_producer_id_request::InitProducerIdRequest,
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    metadata_request::{MetadataRequest, MetadataRequestTopic},
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    ApiKey, GroupId, ProducerId, RequestHeader, ResponseHeader, TopicName, TransactionalId,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

const TARGET: &str = "127.0.0.1:9092";

struct WireClient {
    stream: TcpStream,
    next_corr: i32,
    client_id: StrBytes,
}

impl WireClient {
    async fn connect() -> std::io::Result<Self> {
        let stream = TcpStream::connect(TARGET).await?;
        Ok(Self {
            stream,
            next_corr: 1,
            client_id: StrBytes::from_static_str("kapture-demo"),
        })
    }
    fn alloc_corr(&mut self) -> i32 {
        let c = self.next_corr;
        self.next_corr += 1;
        c
    }
    async fn send<R: Encodable>(
        &mut self,
        api: ApiKey,
        version: i16,
        body: &R,
    ) -> std::io::Result<()> {
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
            .map_err(std::io::Error::other)?;
        body.encode(&mut body_buf, version)
            .map_err(std::io::Error::other)?;
        let payload = body_buf.freeze();
        let mut framed = BytesMut::with_capacity(4 + payload.len());
        framed.put_i32(i32::try_from(payload.len()).map_err(std::io::Error::other)?);
        framed.extend_from_slice(&payload);
        self.stream.write_all(&framed).await
    }
    async fn recv_raw(&mut self) -> std::io::Result<Bytes> {
        let mut len_buf = [0_u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0_u8; len];
        self.stream.read_exact(&mut body).await?;
        Ok(Bytes::from(body))
    }
    async fn recv_response<R: Decodable>(
        &mut self,
        api: ApiKey,
        version: i16,
    ) -> std::io::Result<R> {
        let mut buf = self.recv_raw().await?;
        let header_version = api.response_header_version(version);
        ResponseHeader::decode(&mut buf, header_version).map_err(std::io::Error::other)?;
        R::decode(&mut buf, version).map_err(std::io::Error::other)
    }
}

async fn negotiate(client: &mut WireClient) -> std::io::Result<i16> {
    let req = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("kapture-demo"))
        .with_client_software_version(StrBytes::from_static_str("0"));
    client.send(ApiKey::ApiVersions, 3, &req).await?;
    let resp: kafka_protocol::messages::ApiVersionsResponse =
        client.recv_response(ApiKey::ApiVersions, 3).await?;
    Ok(resp
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::Produce as i16)
        .map_or(7, |k| k.max_version))
}

fn record_batch_one(payload: &[u8]) -> Bytes {
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

async fn run_overcommit() -> std::io::Result<()> {
    let mut client = WireClient::connect().await?;
    negotiate(&mut client).await?;
    let group = format!("kapture-demo-overcommit-{}", Uuid::new_v4());
    println!("→ overcommit on group {group}");
    for _ in 0..30 {
        let req = OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.clone())))
            .with_generation_id_or_member_epoch(-1)
            .with_member_id(StrBytes::from_static_str(""))
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("demo-topic")))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(0)
                    .with_committed_metadata(Some(StrBytes::from_static_str("")))])]);
        client.send(ApiKey::OffsetCommit, 8, &req).await?;
        let _ = client.recv_raw().await;
    }
    Ok(())
}

async fn run_rebalance_loop() -> std::io::Result<()> {
    let mut client = WireClient::connect().await?;
    negotiate(&mut client).await?;
    let group = format!("kapture-demo-rebal-{}", Uuid::new_v4());
    println!("→ rebalance loop on group {group}");
    for _ in 0..6 {
        let req = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.clone())))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(10_000)
            .with_member_id(StrBytes::from_static_str(""))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(b""))]);
        client.send(ApiKey::JoinGroup, 5, &req).await?;
        let _ = client.recv_raw().await;
    }
    Ok(())
}

async fn run_producer_per_record() -> std::io::Result<()> {
    let mut client = WireClient::connect().await?;
    negotiate(&mut client).await?;
    println!("→ producer-per-record");
    // Blast InitProducerId frames without waiting for responses —
    // Redpanda tends to reject + close after the first frame on some
    // configs, which would reset our connection_id and starve the
    // detector. Writing them back-to-back on one TCP guarantees the
    // proxy sees them all under one connection_id before the close.
    let mut sent = 0_u32;
    for i in 0..8 {
        let txn = format!("kapture-demo-ppr-{i}-{}", Uuid::new_v4());
        let req = InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from_string(txn))))
            .with_transaction_timeout_ms(2_000)
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1);
        if client.send(ApiKey::InitProducerId, 4, &req).await.is_err() {
            break;
        }
        sent += 1;
    }
    println!("  ({sent} inits sent)");
    // Quiesce so the proxy has time to forward + fold before the next
    // pattern runs.
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(())
}

async fn run_tiny_batches() -> std::io::Result<()> {
    let mut client = WireClient::connect().await?;
    let produce_max = negotiate(&mut client).await?;
    let topic = format!(
        "kapture-demo-tiny-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    println!("→ tiny batches on topic {topic}");
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await?;
    let _ = client.recv_raw().await;
    let version = produce_max.min(9);
    for _ in 0..25 {
        let req = ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(2_000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(topic.clone())))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(record_batch_one(b"one-record")))])]);
        client.send(ApiKey::Produce, version, &req).await?;
        let _ = client.recv_raw().await;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    println!("Driving anti-pattern traffic at {TARGET}");
    println!("Open Kapture → Expert tab and watch the rows light up.\n");
    // Run sequentially so each detection is easy to spot in the UI.
    if let Err(e) = run_overcommit().await {
        eprintln!("overcommit failed: {e}");
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    if let Err(e) = run_rebalance_loop().await {
        eprintln!("rebalance loop failed: {e}");
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    if let Err(e) = run_producer_per_record().await {
        eprintln!("producer-per-record failed: {e}");
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    if let Err(e) = run_tiny_batches().await {
        eprintln!("tiny batches failed: {e}");
    }
    println!("\nDone. Expect 4 rows in the Expert tab.");
}
