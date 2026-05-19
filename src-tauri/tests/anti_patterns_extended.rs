//! Integration tests for the *additional 11* anti-pattern detectors
//! added in the deep-research follow-up.
//!
//! Helpers live in `tests/common/mod.rs`. Each test drives a real
//! broker (via env var) through the same `ProxyHandle` Kapture uses.

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

mod common;

use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::{
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    init_producer_id_request::InitProducerIdRequest,
    metadata_request::{MetadataRequest, MetadataRequestTopic},
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_handshake_request::SaslHandshakeRequest,
    ApiKey, GroupId, ProducerId, TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use kapture_lib::example_api::AntiPatternKind;
use uuid::Uuid;

use common::{negotiate, record_batch_one, upstream_or_skip, wait_for_kind, TestProxy, WireClient};

// =====================================================================
// #8 — acks=0 silent durability loss
// =====================================================================
#[tokio::test]
async fn acks0_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("acks0_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let produce_max = negotiate(&mut client).await.expect("negotiate");

    let topic = format!(
        "kapture-it-acks0-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;

    let version = produce_max.min(9);
    let req = ProduceRequest::default()
        .with_acks(0)
        .with_timeout_ms(2_000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(topic.clone())))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(record_batch_one(b"fire-and-forget")))])]);
    client
        .send(ApiKey::Produce, version, &req)
        .await
        .expect("send produce acks=0");
    // acks=0 → broker doesn't reply. No recv.

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::Acks0),
            "Acks0"
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #9 — Compression-off on bursty producer
// =====================================================================
#[tokio::test]
async fn compression_off_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("compression_off_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let produce_max = negotiate(&mut client).await.expect("negotiate");

    let topic = format!(
        "kapture-it-comp-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;

    let version = produce_max.min(9);
    // 15 uncompressed Produce frames over a tight loop → rate ≫ 5/s.
    for _ in 0..15 {
        let req = ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(2_000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(topic.clone())))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(record_batch_one(b"plain")))])]);
        client
            .send(ApiKey::Produce, version, &req)
            .await
            .expect("send produce");
        let _ = client.recv_raw().await;
    }
    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::CompressionOff),
            "CompressionOff",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #10 — Non-idempotent producer
// =====================================================================
#[tokio::test]
async fn non_idempotent_producer_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("non_idempotent_producer_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let produce_max = negotiate(&mut client).await.expect("negotiate");

    let topic = format!(
        "kapture-it-noidem-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;

    let version = produce_max.min(9);
    // 15 Produce with producer_id=-1 (default record_batch_one) →
    // non-idempotent ratio passes threshold.
    for _ in 0..15 {
        let req = ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(2_000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(topic.clone())))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(record_batch_one(b"x")))])]);
        client
            .send(ApiKey::Produce, version, &req)
            .await
            .expect("send produce");
        let _ = client.recv_raw().await;
    }
    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::NonIdempotentProducer),
            "NonIdempotentProducer",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #11 — Producer-instance leak
// =====================================================================
#[tokio::test]
async fn producer_instance_leak_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("producer_instance_leak_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());

    // 12 short-lived connections each completing the leak triplet.
    for i in 0..12 {
        let mut client = WireClient::connect(&addr).await.expect("connect");
        negotiate(&mut client).await.expect("negotiate");
        let meta = MetadataRequest::default()
            .with_topics(Some(vec![]))
            .with_allow_auto_topic_creation(false);
        client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
        let _ = client.recv_raw().await;
        let txn = format!("kapture-it-leak-{i}-{}", Uuid::new_v4());
        let req = InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from_string(txn))))
            .with_transaction_timeout_ms(2_000)
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1);
        let _ = client.send(ApiKey::InitProducerId, 4, &req).await;
        let _ = client.recv_raw().await;
        // Drop client → TCP closes → new connection_id next iter.
    }
    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::ProducerInstanceLeak),
            "ProducerInstanceLeak",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #12 — Transactional zombie
// =====================================================================
#[tokio::test]
async fn transactional_zombie_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("transactional_zombie_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let produce_max = negotiate(&mut client).await.expect("negotiate");

    // InitProducerId with txn id, then Produce without AddPartitionsToTxn.
    let txn = format!("kapture-it-zombie-{}", Uuid::new_v4());
    let init = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(StrBytes::from_string(txn))))
        .with_transaction_timeout_ms(2_000)
        .with_producer_id(ProducerId(-1))
        .with_producer_epoch(-1);
    client
        .send(ApiKey::InitProducerId, 4, &init)
        .await
        .expect("send init");
    let _ = client.recv_raw().await;

    let topic = format!(
        "kapture-it-zombie-t-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;

    // Produce with transactional_id set on the request but no preceding
    // AddPartitionsToTxn — this is the zombie wire shape.
    let version = produce_max.min(9);
    let req = ProduceRequest::default()
        .with_transactional_id(Some(TransactionalId(StrBytes::from_string(format!(
            "kapture-it-zombie-tx-{}",
            Uuid::new_v4()
        )))))
        .with_acks(1)
        .with_timeout_ms(2_000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(topic.clone())))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(record_batch_one(b"zombie")))])]);
    client
        .send(ApiKey::Produce, version, &req)
        .await
        .expect("send produce");
    let _ = client.recv_raw().await;

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::TransactionalZombie),
            "TransactionalZombie",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #13 — Auto-commit cadence
// =====================================================================
#[tokio::test]
async fn auto_commit_cadence_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("auto_commit_cadence_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    negotiate(&mut client).await.expect("negotiate");

    let group = format!("kapture-it-ac-{}", Uuid::new_v4());
    // 5 OffsetCommits at 5s intervals — matches AUTOCOMMIT_INTERVAL_MS.
    for i in 0..5 {
        if i > 0 {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
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
        let _ = client.recv_raw().await;
    }
    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::AutoCommitCadence) && d.scope.contains(&group),
            "AutoCommitCadence",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #14 — Tight fetch polling
// =====================================================================
#[tokio::test]
async fn tight_fetch_polling_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("tight_fetch_polling_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let _ = negotiate(&mut client).await.expect("negotiate");

    let topic = format!(
        "kapture-it-tight-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;

    // 25 Fetches with min_bytes=1, max_wait_ms=0 on an empty topic →
    // sub-KB responses, high rate.
    for _ in 0..25 {
        let req = FetchRequest::default()
            .with_max_wait_ms(0)
            .with_min_bytes(1)
            .with_max_bytes(1_048_576)
            .with_isolation_level(0)
            .with_session_id(0)
            .with_session_epoch(0)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(topic.clone())))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(0)
                    .with_partition_max_bytes(1_048_576)])]);
        client
            .send(ApiKey::Fetch, 12, &req)
            .await
            .expect("send fetch");
        let _ = client.recv_raw().await;
    }
    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::TightFetchPolling),
            "TightFetchPolling",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #15 — Fetch-session error cascade (INVALID_FETCH_SESSION_EPOCH)
// =====================================================================
#[tokio::test]
async fn fetch_session_error_cascade_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("fetch_session_error_cascade_detected_through_proxy")
    else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let _ = negotiate(&mut client).await.expect("negotiate");

    let topic = format!(
        "kapture-it-fse-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;

    // Send 4 Fetches with non-zero session_id but session_epoch=1 that
    // doesn't match any cached server-side session → broker replies
    // with INVALID_FETCH_SESSION_EPOCH (70).
    for i in 0..4_i32 {
        let req = FetchRequest::default()
            .with_max_wait_ms(100)
            .with_min_bytes(1)
            .with_max_bytes(1_048_576)
            .with_isolation_level(0)
            .with_session_id(424_242 + i)
            .with_session_epoch(1)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(topic.clone())))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(0)
                    .with_partition_max_bytes(1_048_576)])]);
        client
            .send(ApiKey::Fetch, 12, &req)
            .await
            .expect("send fetch");
        let _ = client.recv_raw().await;
    }
    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::FetchSessionErrorCascade),
            "FetchSessionErrorCascade",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #16 — Throttle pressure
// =====================================================================
// Throttling is broker-config dependent. Without a quota set, brokers
// never throttle. We exercise the *wire path* by sending a SaslHandshake
// against a SASL-enabled broker with a wrong creds — the broker may
// throttle the failed auth path. Since not every Kafka build throttles
// here, this test is informational and only asserts when KAPTURE_TEST_THROTTLE
// is explicitly set. The unit test in `anti_patterns/tests.rs` covers
// the detector behavior on a synthesized `throttle_time_ms > 0`.
#[tokio::test]
async fn throttle_pressure_smoke() {
    // Informational: run only if explicitly enabled. We don't have
    // quotas configured by default in docker-compose.
    let Ok(_) = std::env::var("KAPTURE_TEST_THROTTLE") else {
        eprintln!("skipping throttle_pressure_smoke: KAPTURE_TEST_THROTTLE not set");
        return;
    };
    let Some(upstream) = upstream_or_skip("throttle_pressure_smoke") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let _ = negotiate(&mut client).await.expect("negotiate");

    // Spam Produce on a topic to trigger byte-rate quota if configured.
    let topic = format!(
        "kapture-it-throttle-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;

    let large = vec![0_u8; 4096];
    let payload = large.as_slice();
    for _ in 0..100 {
        // bypass record_batch_one to get a bigger payload
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
        let mut buf = bytes::BytesMut::with_capacity(8192);
        RecordBatchEncoder::encode(&mut buf, records.iter(), &opts).expect("encode batch");
        let batch = buf.freeze();
        let req = ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(2_000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(topic.clone())))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(batch))])]);
        client
            .send(ApiKey::Produce, 9, &req)
            .await
            .expect("send produce");
        let _ = client.recv_raw().await;
    }
    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::ThrottlePressure),
            "ThrottlePressure",
        )
        .await,
        "no throttle pressure observed — broker likely doesn't have a quota set",
    );
    proxy.stop().await;
}

// =====================================================================
// #17 — Metadata storm
// =====================================================================
#[tokio::test]
async fn metadata_storm_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("metadata_storm_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let _ = negotiate(&mut client).await.expect("negotiate");

    // 15 MetadataRequest in tight loop ≫ threshold (10/min).
    for _ in 0..15 {
        let req = MetadataRequest::default()
            .with_topics(Some(vec![]))
            .with_allow_auto_topic_creation(false);
        client
            .send(ApiKey::Metadata, 9, &req)
            .await
            .expect("send metadata");
        let _ = client.recv_raw().await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::MetadataStorm),
            "MetadataStorm",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #18 — Classic rebalance on KIP-848-capable cluster
// =====================================================================
// Kafka 4.x advertises ConsumerGroupHeartbeat (api_key=68). A client
// using JoinGroup/SyncGroup on the same connection is using the
// classic protocol while the broker offers the new one. The detector
// fires once these are seen together.
#[tokio::test]
async fn classic_rebalance_on_kip848_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("classic_rebalance_on_kip848_detected_through_proxy")
    else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    // negotiate() returns once ApiVersionsResponse has been parsed —
    // the detector now knows whether this broker advertises KIP-848.
    let _ = negotiate(&mut client).await.expect("negotiate");

    // Drive a classic JoinGroup. If the broker advertised KIP-848,
    // detector flags `ClassicRebalanceOnModernCluster`. If not (older
    // Kafka), this test silently still passes because the detector
    // gate is wired off the broker's advertised api versions and we'd
    // see RebalanceLoop instead.
    let group = format!("kapture-it-classic-{}", Uuid::new_v4());
    let req = kafka_protocol::messages::join_group_request::JoinGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.clone())))
        .with_session_timeout_ms(10_000)
        .with_rebalance_timeout_ms(10_000)
        .with_member_id(StrBytes::from_static_str(""))
        .with_protocol_type(StrBytes::from_static_str("consumer"))
        .with_protocols(vec![
            kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(b"")),
        ]);
    client
        .send(ApiKey::JoinGroup, 5, &req)
        .await
        .expect("send join");
    let _ = client.recv_raw().await;

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::ClassicRebalanceOnModernCluster),
            "ClassicRebalanceOnModernCluster",
        )
        .await,
        "no classic-rebalance-on-modern-cluster detection — does the broker advertise ConsumerGroupHeartbeat?",
    );
    proxy.stop().await;
}

// Unused imports satisfaction for the SASL/Auth helpers (might be
// useful for future tests on the same file).
#[allow(dead_code)]
fn _used_sasl() -> (SaslHandshakeRequest, SaslAuthenticateRequest) {
    (
        SaslHandshakeRequest::default(),
        SaslAuthenticateRequest::default(),
    )
}
