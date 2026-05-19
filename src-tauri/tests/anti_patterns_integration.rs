//! Integration tests for the 7 client-side anti-pattern detectors,
//! exercised against a *real* Kafka broker via the same `ProxyHandle`
//! used by the Tauri app.
//!
//! The proxy is bound on an ephemeral local port. The client side is
//! a small hand-rolled Kafka-protocol driver built on `kafka-protocol`
//! (a regular dependency of the crate) — that lets us drive bad
//! traffic shapes without pulling `librdkafka` into the test build.
//!
//! Gating: every test reads `KAPTURE_KAFKA_BOOTSTRAP`. When the env
//! var is unset (the common case on a developer laptop with no
//! cluster running), the test prints a notice and returns early. CI
//! sets the variable after `docker compose --profile redpanda up -d`
//! makes `localhost:19092` reachable. See `.github/workflows/ci.yml`.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::useless_vec,
    clippy::large_stack_arrays,
    clippy::must_use_candidate,
    clippy::future_not_send
)]

use std::io;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    api_versions_request::ApiVersionsRequest,
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    init_producer_id_request::InitProducerIdRequest,
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    metadata_request::{MetadataRequest, MetadataRequestTopic},
    metadata_response::MetadataResponse,
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_handshake_request::SaslHandshakeRequest,
    ApiKey, GroupId, ProducerId, RequestHeader, ResponseHeader, TopicName, TransactionalId,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::RecordBatchEncoder;
use kafka_protocol::records::{Compression, Record, RecordEncodeOptions, TimestampType};
use kapture_lib::example_api::{
    AntiPatternKind, CapturedMessage, ProtoCorrelator, ProxyConfig, ProxyHandle, RecordSink,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

const ENV_BOOTSTRAP: &str = "KAPTURE_KAFKA_BOOTSTRAP";
const ENV_MB_BROKERS: &str = "KAPTURE_KAFKA_MB_BROKERS";
const ENV_LEGACY: &str = "KAPTURE_KAFKA_LEGACY";
const ENV_SASL: &str = "KAPTURE_KAFKA_SASL";

/// Look up the upstream broker (set by CI / dev). Skips the test when
/// missing so a `cargo test` on a laptop without Kafka stays green.
fn upstream_or_skip(test_name: &str) -> Option<String> {
    env_or_skip(test_name, ENV_BOOTSTRAP)
}

fn env_or_skip(test_name: &str, key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping {test_name}: ${key} not set");
            None
        }
    }
}

struct TestProxy {
    handle: ProxyHandle,
    correlator: Arc<ProtoCorrelator>,
}

impl TestProxy {
    async fn start(upstream: String) -> io::Result<Self> {
        Self::start_with_correlator(upstream, Arc::new(ProtoCorrelator::new())).await
    }
    /// Variant that shares an externally owned correlator across
    /// multiple proxies. Used by the mixed-api-version test, where
    /// two proxies (one per upstream broker) feed into the same
    /// detector fold so it can spot version divergence.
    async fn start_with_correlator(
        upstream: String,
        correlator: Arc<ProtoCorrelator>,
    ) -> io::Result<Self> {
        // listen_port=0 → OS picks an ephemeral port for us.
        let cfg = ProxyConfig::new(upstream, 0).with_bind(IpAddr::from_str("127.0.0.1").unwrap());
        let sink: RecordSink = Arc::new(|_: CapturedMessage| {});
        let handle = ProxyHandle::start(cfg, Arc::clone(&correlator), sink)
            .await
            .map_err(io::Error::other)?;
        Ok(Self { handle, correlator })
    }
    fn listen_port(&self) -> u16 {
        self.handle.local_addr().port()
    }
    fn detections(&self) -> Vec<kapture_lib::example_api::Detection> {
        self.correlator.anti_patterns().detections
    }
    async fn stop(self) {
        self.handle.stop().await;
    }
}

/// Minimal Kafka wire client. Length-prefixed frames; `kafka-protocol`
/// encodes/decodes headers + bodies. We carry our own correlation id
/// counter so the proxy + broker keep their pairings straight.
struct WireClient {
    stream: TcpStream,
    next_corr: i32,
    client_id: StrBytes,
}

impl WireClient {
    async fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            next_corr: 1,
            client_id: StrBytes::from_static_str("kapture-it"),
        })
    }
    fn alloc_corr(&mut self) -> i32 {
        let c = self.next_corr;
        self.next_corr += 1;
        c
    }
    async fn send<R>(&mut self, api: ApiKey, version: i16, body: &R) -> io::Result<i32>
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
    async fn recv_raw(&mut self) -> io::Result<Bytes> {
        let mut len_buf = [0_u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0_u8; len];
        self.stream.read_exact(&mut body).await?;
        Ok(Bytes::from(body))
    }
    async fn recv_response<R>(&mut self, api: ApiKey, version: i16) -> io::Result<R>
    where
        R: Decodable,
    {
        let mut buf = self.recv_raw().await?;
        let header_version = api.response_header_version(version);
        // Drop the response header (correlation id + tagged fields).
        ResponseHeader::decode(&mut buf, header_version).map_err(io::Error::other)?;
        R::decode(&mut buf, version).map_err(io::Error::other)
    }
}

/// Round-trip ApiVersions to ensure the broker is reachable. Returns
/// the broker-advertised `max_version` for `Produce` (used to pick a
/// version we know the broker will accept).
async fn negotiate(client: &mut WireClient) -> io::Result<i16> {
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

async fn wait_for_kind<F>(proxy: &TestProxy, mut pred: F, label: &str) -> bool
where
    F: FnMut(&kapture_lib::example_api::Detection) -> bool,
{
    // 5 × 200ms = 1s of grace for the fold to absorb the last frame.
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

fn record_batch_one(payload: &[u8]) -> Bytes {
    // Build a single-record RecordBatch v2. Goes inside ProduceRequest.
    let records = vec![Record {
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

#[tokio::test]
async fn overcommit_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("overcommit_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    negotiate(&mut client).await.expect("negotiate");

    let group = format!("kapture-it-overcommit-{}", Uuid::new_v4());
    for _ in 0..30 {
        // OffsetCommit v8 (KIP-345) accepts a group + topic + partition
        // commit triple. The detector only cares about *seeing*
        // `OffsetCommitRequest` frames, not the broker's verdict —
        // so we send a valid shape even if the offset is fictitious.
        let req = OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.clone())))
            .with_generation_id_or_member_epoch(-1)
            .with_member_id(StrBytes::from_static_str(""))
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("does-not-matter")))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(0)
                    .with_committed_metadata(Some(StrBytes::from_static_str("")))])]);
        client
            .send(ApiKey::OffsetCommit, 8, &req)
            .await
            .expect("send commit");
        let _ = client.recv_raw().await; // drain whatever the broker returns
    }

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::Overcommit) && d.scope.contains(&group),
            "Overcommit",
        )
        .await
    );
    proxy.stop().await;
}

#[tokio::test]
async fn rebalance_loop_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("rebalance_loop_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    negotiate(&mut client).await.expect("negotiate");

    let group = format!("kapture-it-rebal-{}", Uuid::new_v4());
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
        client
            .send(ApiKey::JoinGroup, 5, &req)
            .await
            .expect("send join");
        let _ = client.recv_raw().await;
    }

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::RebalanceLoop) && d.scope.contains(&group),
            "RebalanceLoop",
        )
        .await
    );
    proxy.stop().await;
}

#[tokio::test]
async fn producer_per_record_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("producer_per_record_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");

    // ApiVersions first to learn the broker's max InitProducerId
    // version. Older Redpanda builds top out at v4 but the field set
    // changes across versions — picking the broker-advertised max
    // keeps the test working against any cluster.
    let av_req = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("kapture-it"))
        .with_client_software_version(StrBytes::from_static_str("0"));
    client
        .send(ApiKey::ApiVersions, 3, &av_req)
        .await
        .expect("send av");
    let av_resp: kafka_protocol::messages::ApiVersionsResponse = client
        .recv_response(ApiKey::ApiVersions, 3)
        .await
        .expect("recv av");
    let init_max = av_resp
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::InitProducerId as i16)
        .map_or(0, |k| k.max_version)
        .min(4);

    // Six InitProducerId in quick succession — the canonical
    // producer-per-record shape: every "send" rebuilds the producer.
    // Use a transactional_id so the broker accepts the request even
    // without a producer-id allocation — the wire shape is what the
    // detector sees, not the broker verdict.
    for i in 0..6 {
        let txn = format!("kapture-it-ppr-{i}-{}", Uuid::new_v4());
        let req = InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from_string(txn))))
            .with_transaction_timeout_ms(2_000)
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1);
        client
            .send(ApiKey::InitProducerId, init_max, &req)
            .await
            .expect("send init");
        let _ = client.recv_raw().await;
    }

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::ProducerPerRecord),
            "ProducerPerRecord",
        )
        .await,
        "frames seen: {:#?}",
        proxy
            .correlator
            .summaries(50)
            .iter()
            .map(|s| (s.api_name.clone(), s.api_version))
            .collect::<Vec<_>>()
    );
    proxy.stop().await;
}

#[tokio::test]
async fn tiny_batches_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("tiny_batches_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let produce_max = negotiate(&mut client).await.expect("negotiate");

    // Pre-create a topic by issuing a Metadata-with-create — Redpanda
    // and the test cluster both auto-create. If the broker rejects
    // creation we still see the produce frames go out, which is what
    // the detector folds on.
    let topic = format!(
        "kapture-it-tiny-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;

    let version = produce_max.min(9); // we only encode up to v9 here
    for _ in 0..25 {
        let batch = record_batch_one(b"one-record");
        let req = ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(2_000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(topic.clone())))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(batch))])]);
        client
            .send(ApiKey::Produce, version, &req)
            .await
            .expect("send produce");
        let _ = client.recv_raw().await;
    }

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::TinyBatches),
            "TinyBatches",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #5 — Stale-leader producing
// =====================================================================
// Bring up a 3-broker KRaft cluster (`docker compose --profile mb`),
// create a topic with replication_factor=2, find a partition whose
// leader is broker X, then drive a `ProduceRequest` for that partition
// through the proxy pointed at broker Y. Broker Y refuses with
// `NOT_LEADER_OR_FOLLOWER` (code 6). The detector folds the response
// and flags `StaleLeaderProducing` on `topic:partition`.

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn stale_leader_detected_through_proxy() {
    let Some(brokers_csv) = env_or_skip("stale_leader_detected_through_proxy", ENV_MB_BROKERS)
    else {
        return;
    };
    let brokers: Vec<String> = brokers_csv.split(',').map(str::to_owned).collect();
    assert!(
        brokers.len() >= 2,
        "need >=2 brokers in {ENV_MB_BROKERS}, got: {brokers:?}",
    );

    // Pick the first broker as our "discovery + topic admin" endpoint.
    let admin_upstream = brokers[0].clone();
    let admin_proxy = TestProxy::start(admin_upstream.clone())
        .await
        .expect("start admin proxy");
    let admin_addr = format!("127.0.0.1:{}", admin_proxy.listen_port());
    let mut admin = WireClient::connect(&admin_addr)
        .await
        .expect("connect admin");
    negotiate(&mut admin).await.expect("admin negotiate");

    let topic = format!(
        "kapture-it-stale-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );

    // CreateTopics with num_partitions=3, replication_factor=2 so that
    // leadership is spread across brokers and at least one partition
    // has a leader != broker we'll target with the Produce.
    let ct_req = CreateTopicsRequest::default()
        .with_topics(vec![CreatableTopic::default()
            .with_name(TopicName(StrBytes::from_string(topic.clone())))
            .with_num_partitions(3)
            .with_replication_factor(2)])
        .with_timeout_ms(10_000);
    admin
        .send(ApiKey::CreateTopics, 7, &ct_req)
        .await
        .expect("send CreateTopics");
    let _ = admin.recv_raw().await;
    // Allow controller to propagate metadata.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let meta_req = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(false);
    admin
        .send(ApiKey::Metadata, 9, &meta_req)
        .await
        .expect("send Metadata");
    let meta: MetadataResponse = admin
        .recv_response(ApiKey::Metadata, 9)
        .await
        .expect("recv Metadata");

    // Identify (partition, leader_node_id) pairs.
    let topic_meta = meta
        .topics
        .iter()
        .find(|t| {
            t.name
                .as_ref()
                .is_some_and(|n| n.0.as_str() == topic.as_str())
        })
        .expect("topic in metadata");
    let mut leaders: Vec<(i32, i32)> = topic_meta
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.leader_id.0))
        .collect();
    leaders.sort_by_key(|(p, _)| *p);
    assert!(
        !leaders.is_empty(),
        "no partitions reported for topic {topic}: {topic_meta:?}",
    );
    // Map node_id → advertised host:port from the brokers list.
    let broker_map: std::collections::HashMap<i32, String> = meta
        .brokers
        .iter()
        .map(|b| (b.node_id.0, format!("{}:{}", b.host.as_str(), b.port)))
        .collect();
    // We need a (partition, non_leader_broker) where the broker is
    // reachable from outside docker. The mb profile advertises
    // `localhost:39092/93/94` for nodes 1/2/3 respectively.
    let cli_brokers: Vec<String> = brokers.clone();
    // Find a partition + a *reachable* non-leader broker.
    let (target_partition, target_upstream) = leaders
        .iter()
        .find_map(|(p, leader_id)| {
            cli_brokers
                .iter()
                .enumerate()
                .find_map(|(idx, candidate)| {
                    let node_id = i32::try_from(idx + 1).unwrap_or(0);
                    if node_id == *leader_id {
                        None
                    } else {
                        Some((*p, candidate.clone()))
                    }
                })
        })
        .unwrap_or_else(|| {
            panic!(
                "couldn't pick a (partition, non-leader) pair. leaders={leaders:?} brokers={cli_brokers:?} broker_map={broker_map:?}",
            )
        });

    admin_proxy.stop().await;

    // Now spin up a proxy that points at the non-leader broker and
    // drive a Produce against the leader-owned partition. The broker
    // we hit isn't the leader for that partition → NOT_LEADER_OR_FOLLOWER.
    let proxy = TestProxy::start(target_upstream.clone())
        .await
        .expect("start non-leader proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let produce_max = negotiate(&mut client).await.expect("negotiate");
    let version = produce_max.min(9);

    // Send a Produce for the target partition.
    let batch = record_batch_one(b"stale-leader-test");
    let req = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(2_000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(topic.clone())))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(target_partition)
                .with_records(Some(batch))])]);
    client
        .send(ApiKey::Produce, version, &req)
        .await
        .expect("send produce");
    let _ = client.recv_raw().await;

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::StaleLeaderProducing)
                && d.scope == format!("{topic}:{target_partition}"),
            "StaleLeaderProducing",
        )
        .await,
        "no StaleLeaderProducing for {topic}:{target_partition} (hit {target_upstream}, leaders={leaders:?})",
    );
    proxy.stop().await;
}

// =====================================================================
// #6 — Mixed api_version across brokers (rolling upgrade)
// =====================================================================
// Run two proxies side-by-side, each pointing at a broker of a
// different Kafka version (the `kafka:latest` and `kafka-legacy:3.7.0`
// containers). Their `ApiVersionsResponse.max_version` lists differ
// for several keys (e.g. ShareFetch only exists in 4.x). Both proxies
// feed the same `ProtoCorrelator` so the detector can compare per-port
// max_version values cross-broker.

#[tokio::test]
async fn mixed_api_version_detected_through_proxies() {
    let Some(latest) = env_or_skip("mixed_api_version_detected_through_proxies", ENV_BOOTSTRAP)
    else {
        return;
    };
    let Some(legacy) = env_or_skip("mixed_api_version_detected_through_proxies", ENV_LEGACY) else {
        return;
    };

    let correlator = Arc::new(ProtoCorrelator::new());
    let proxy_a = TestProxy::start_with_correlator(latest, Arc::clone(&correlator))
        .await
        .expect("start proxy A");
    let proxy_b = TestProxy::start_with_correlator(legacy, Arc::clone(&correlator))
        .await
        .expect("start proxy B");

    // ApiVersions on each — that's enough; the detector folds on the
    // response.
    for port in [proxy_a.listen_port(), proxy_b.listen_port()] {
        let mut client = WireClient::connect(&format!("127.0.0.1:{port}"))
            .await
            .expect("connect");
        negotiate(&mut client).await.expect("negotiate");
    }

    let detected = {
        let snapshot = || correlator.anti_patterns().detections;
        let mut fired = false;
        for _ in 0..50 {
            if snapshot()
                .iter()
                .any(|d| matches!(d.kind, AntiPatternKind::MixedApiVersion))
            {
                fired = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        fired
    };
    assert!(
        detected,
        "no MixedApiVersion detection — current: {:#?}",
        correlator.anti_patterns().detections
    );

    proxy_a.stop().await;
    proxy_b.stop().await;
}

// =====================================================================
// #7 — SASL session-too-short on re-auth
// =====================================================================
// `kafka-sasl` profile exposes a SASL_PLAINTEXT listener on 49092 with
// PLAIN credentials `alice/alice-secret`. We send the canonical
// 2-frame handshake (SaslHandshake + SaslAuthenticate) but with a
// wrong password — Kafka responds to `SaslAuthenticate` with
// `error_code=58` (SASL_AUTHENTICATION_FAILED). The detector folds
// that into a `SaslSessionTooShort` row (catch-all rule for non-zero
// error_code OR collapsed `session_lifetime_ms`).

#[tokio::test]
async fn sasl_failure_detected_through_proxy() {
    let Some(upstream) = env_or_skip("sasl_failure_detected_through_proxy", ENV_SASL) else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    // ApiVersions handshake — required before SASL on modern brokers.
    negotiate(&mut client).await.expect("negotiate");

    let hs = SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN"));
    client
        .send(ApiKey::SaslHandshake, 1, &hs)
        .await
        .expect("send handshake");
    let _ = client.recv_raw().await;

    // SASL/PLAIN auth bytes are: \0 username \0 password (RFC 4616).
    // Use a deliberately wrong password so the broker rejects.
    let auth_payload = {
        let mut v = Vec::new();
        v.push(0_u8);
        v.extend_from_slice(b"alice");
        v.push(0_u8);
        v.extend_from_slice(b"definitely-wrong");
        v
    };
    let auth_req = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(auth_payload));
    client
        .send(ApiKey::SaslAuthenticate, 2, &auth_req)
        .await
        .expect("send auth");
    let _ = client.recv_raw().await;

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::SaslSessionTooShort),
            "SaslSessionTooShort",
        )
        .await,
        "no SaslSessionTooShort detection — current: {:#?}",
        proxy.detections()
    );
    proxy.stop().await;
}
