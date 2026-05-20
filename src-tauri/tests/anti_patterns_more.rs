//! Integration tests for detectors #19–#25 added after the
//! practitioner-source research pass.
//!
//! Helpers in `tests/common/mod.rs`. Each test exercises a real broker
//! shape that triggers the detector. Tests gated by env vars set by
//! CI; absent → skip.

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

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    find_coordinator_request::FindCoordinatorRequest,
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    metadata_request::{MetadataRequest, MetadataRequestTopic},
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_handshake_request::SaslHandshakeRequest,
    ApiKey, GroupId, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use kapture_lib::example_api::AntiPatternKind;
use uuid::Uuid;

use common::{negotiate, upstream_or_skip, wait_for_kind, TestProxy, WireClient};

// =====================================================================
// #19 — MessageTooLargeRejected
// =====================================================================
// Default Kafka broker `message.max.bytes` is ~1 MiB. Producing a
// record value bigger than that triggers a `MESSAGE_TOO_LARGE` (10)
// in the ProduceResponse per-partition error.
#[tokio::test]
async fn message_too_large_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("message_too_large_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let produce_max = negotiate(&mut client).await.expect("negotiate");

    let topic = format!(
        "kapture-it-mtl-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;
    // Give the broker a moment to materialize the auto-created topic.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2 MiB payload > default 1 MiB broker `message.max.bytes`.
    let huge = vec![0xab_u8; 2 * 1024 * 1024];
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
        value: Some(Bytes::from(huge)),
        headers: Default::default(),
    }];
    let mut buf = BytesMut::with_capacity(3 * 1024 * 1024);
    RecordBatchEncoder::encode(
        &mut buf,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("encode batch");
    let batch = buf.freeze();
    let version = produce_max.min(9);
    let req = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(5_000)
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

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::MessageTooLargeRejected),
            "MessageTooLargeRejected",
        )
        .await,
        "no MessageTooLargeRejected; raw frames: {:?}",
        proxy
            .correlator
            .summaries(50)
            .iter()
            .map(|s| (s.api_name.clone(), s.api_version))
            .collect::<Vec<_>>()
    );
    proxy.stop().await;
}

// =====================================================================
// #20 — OffsetOutOfRangeOnFetch
// =====================================================================
// Fetch from offset 999_999_999 on a freshly auto-created (empty)
// topic → broker returns `OFFSET_OUT_OF_RANGE` (1) per partition.
#[tokio::test]
async fn offset_out_of_range_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("offset_out_of_range_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let _ = negotiate(&mut client).await.expect("negotiate");

    let topic = format!(
        "kapture-it-oor-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 3+ to clear the bug-fix rate threshold (single occurrence is
    // benign — `auto.offset.reset` legitimately triggers one).
    for _ in 0..4 {
        let req = FetchRequest::default()
            .with_max_wait_ms(100)
            .with_min_bytes(1)
            .with_max_bytes(1_048_576)
            .with_isolation_level(0)
            .with_session_id(0)
            .with_session_epoch(0)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(topic.clone())))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(999_999_999)
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
            |d| matches!(d.kind, AntiPatternKind::OffsetOutOfRangeOnFetch),
            "OffsetOutOfRangeOnFetch",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #21 — CooperativeStickyChurn
// =====================================================================
// 4 JoinGroup advertising `cooperative-sticky` → detector fires.
#[tokio::test]
async fn cooperative_sticky_churn_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("cooperative_sticky_churn_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    negotiate(&mut client).await.expect("negotiate");

    let group = format!("kapture-it-coop-{}", Uuid::new_v4());
    for _ in 0..4 {
        let req = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.clone())))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(10_000)
            .with_member_id(StrBytes::from_static_str(""))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("cooperative-sticky"))
                .with_metadata(Bytes::from_static(b""))]);
        client
            .send(ApiKey::JoinGroup, 9, &req)
            .await
            .expect("send join");
        let _ = client.recv_raw().await;
    }

    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::CooperativeStickyChurn)
                && d.scope.contains(&group),
            "CooperativeStickyChurn",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #22 — CommitDuringRebalance
// =====================================================================
// Trigger a real REBALANCE_IN_PROGRESS by sending OffsetCommit with a
// stale `member_id`/`generation_id` to a group that's mid-rebalance.
// We force the rebalance by sending a first JoinGroup as member A,
// then sending OffsetCommit before the SyncGroup completes — broker
// rejects the commit with error 27 because the group state is still
// `PreparingRebalance` from member A's perspective.
//
// In practice not every broker version returns 27 here (some return
// 22 ILLEGAL_GENERATION or 25 UNKNOWN_MEMBER_ID). To make the test
// deterministic across Kafka/Redpanda we drive the canonical wire
// sequence and treat the test as informational if 27 doesn't surface:
// the unit test covers the detector logic. Real-cluster signal:
// at minimum the OffsetCommitResponse comes back with a non-zero
// error_code, exercising the per-partition error path.
#[tokio::test]
async fn commit_during_rebalance_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("commit_during_rebalance_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    negotiate(&mut client).await.expect("negotiate");

    let group = format!("kapture-it-cdr-{}", Uuid::new_v4());

    // Auto-create a topic we'll commit against — otherwise the broker
    // returns UNKNOWN_TOPIC_OR_PARTITION (3) before checking group state.
    let topic = format!(
        "kapture-it-cdr-t-{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let meta = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.clone()))))]))
        .with_allow_auto_topic_creation(true);
    client.send(ApiKey::Metadata, 9, &meta).await.expect("meta");
    let _ = client.recv_raw().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Two-phase JoinGroup (v5+): broker first returns
    // MEMBER_ID_REQUIRED (79) with an assigned member_id. Loop once
    // to extract the assigned member_id + generation_id.
    let mut assigned_member: String = String::new();
    let mut assigned_generation: i32 = 0;
    for _ in 0..2 {
        let jg = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.clone())))
            .with_session_timeout_ms(30_000)
            .with_rebalance_timeout_ms(2_000)
            .with_member_id(StrBytes::from_string(assigned_member.clone()))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(b""))]);
        client
            .send(ApiKey::JoinGroup, 5, &jg)
            .await
            .expect("send join");
        let resp: kafka_protocol::messages::JoinGroupResponse = client
            .recv_response(ApiKey::JoinGroup, 5)
            .await
            .expect("recv join");
        if !resp.member_id.is_empty() {
            assigned_member = resp.member_id.to_string();
            assigned_generation = resp.generation_id;
        }
        if resp.error_code == 0 && !assigned_member.is_empty() {
            break;
        }
    }

    // Now commit with the real member_id but BEFORE SyncGroup — the
    // group state is `CompletingRebalance` and the broker should
    // respond with REBALANCE_IN_PROGRESS (27) per partition.
    let commit = OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.clone())))
        .with_generation_id_or_member_epoch(assigned_generation)
        .with_member_id(StrBytes::from_string(assigned_member))
        .with_topics(vec![OffsetCommitRequestTopic::default()
            .with_name(TopicName(StrBytes::from_string(topic.clone())))
            .with_partitions(vec![OffsetCommitRequestPartition::default()
                .with_partition_index(0)
                .with_committed_offset(0)
                .with_committed_metadata(Some(StrBytes::from_static_str("")))])]);
    client
        .send(ApiKey::OffsetCommit, 8, &commit)
        .await
        .expect("send commit");
    let resp: kafka_protocol::messages::OffsetCommitResponse = client
        .recv_response(ApiKey::OffsetCommit, 8)
        .await
        .expect("recv commit");
    let codes: Vec<i16> = resp
        .topics
        .iter()
        .flat_map(|t| t.partitions.iter().map(|p| p.error_code))
        .collect();
    eprintln!("OffsetCommit per-partition error codes: {codes:?}");

    // Wait a bit, but don't hard-fail if the broker returned 22/25
    // instead of 27 — the wire path is exercised either way. The unit
    // test asserts the detector logic for error_code 27.
    let detected = wait_for_kind(
        &proxy,
        |d| matches!(d.kind, AntiPatternKind::CommitDuringRebalance),
        "CommitDuringRebalance",
    )
    .await;
    if !detected {
        eprintln!(
            "commit_during_rebalance: broker didn't return error 27 (returned a different code or no error). Frames seen: {:#?}",
            proxy
                .correlator
                .summaries(50)
                .iter()
                .map(|s| (s.api_name.clone(), s.api_version))
                .collect::<Vec<_>>()
        );
    }
    assert!(
        detected,
        "expected CommitDuringRebalance but broker did not return REBALANCE_IN_PROGRESS"
    );
    proxy.stop().await;
}

// =====================================================================
// #23 — AclDeny
// =====================================================================
// Requires a broker with ACLs enabled AND no grants for the
// authenticated user. Gated behind `KAPTURE_KAFKA_ACL` env var pointing
// at such a broker (e.g. a profile that sets
// `allow.everyone.if.no.acl.found=false`). Without that, the test
// skips and the unit test covers the detector.
#[tokio::test]
async fn acl_deny_detected_through_proxy() {
    let Ok(upstream) = std::env::var("KAPTURE_KAFKA_ACL") else {
        eprintln!("skipping acl_deny_detected_through_proxy: KAPTURE_KAFKA_ACL not set");
        return;
    };
    if upstream.is_empty() {
        return;
    }
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let _ = negotiate(&mut client).await.expect("negotiate");

    // SASL PLAIN auth as alice — broker config has no ACL grants for
    // alice, so any subsequent operation returns *_AUTHORIZATION_FAILED.
    let hs = SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN"));
    client
        .send(ApiKey::SaslHandshake, 1, &hs)
        .await
        .expect("send sasl handshake");
    let _ = client.recv_raw().await;
    let mut auth_bytes = Vec::new();
    auth_bytes.push(0_u8);
    auth_bytes.extend_from_slice(b"alice");
    auth_bytes.push(0_u8);
    auth_bytes.extend_from_slice(b"alice-secret");
    let auth = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(auth_bytes));
    client
        .send(ApiKey::SaslAuthenticate, 2, &auth)
        .await
        .expect("send sasl auth");
    let _ = client.recv_raw().await;

    // Send 3 Fetches on a topic with no ACL grant → broker returns
    // TOPIC_AUTHORIZATION_FAILED (29) per partition. Threshold = 3.
    let topic = "kapture-it-acl-denied";
    for _ in 0..3 {
        let req = FetchRequest::default()
            .with_max_wait_ms(100)
            .with_min_bytes(1)
            .with_max_bytes(1_048_576)
            .with_isolation_level(0)
            .with_session_id(0)
            .with_session_epoch(0)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_static_str(topic)))
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
            |d| matches!(d.kind, AntiPatternKind::AclDeny),
            "AclDeny",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #24 — UnknownTopicPollLoop
// =====================================================================
// 3 Fetches for a non-existent topic with `allow_auto_topic_creation=false`
// → broker returns UNKNOWN_TOPIC_OR_PARTITION (3) per partition.
#[tokio::test]
async fn unknown_topic_poll_loop_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("unknown_topic_poll_loop_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let _ = negotiate(&mut client).await.expect("negotiate");

    let topic = format!("kapture-it-ghost-{}", Uuid::new_v4().simple());
    // Don't pre-create or refresh metadata — the broker will respond
    // with UNKNOWN_TOPIC_OR_PARTITION because Fetch doesn't auto-create.
    for _ in 0..4 {
        let req = FetchRequest::default()
            .with_max_wait_ms(100)
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
            |d| matches!(d.kind, AntiPatternKind::UnknownTopicPollLoop) && d.scope.contains(&topic),
            "UnknownTopicPollLoop",
        )
        .await
    );
    proxy.stop().await;
}

// =====================================================================
// #25 — CoordinatorChurn
// =====================================================================
// 4 FindCoordinator for the same group_id → detector fires.
#[tokio::test]
async fn coordinator_churn_detected_through_proxy() {
    let Some(upstream) = upstream_or_skip("coordinator_churn_detected_through_proxy") else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let _ = negotiate(&mut client).await.expect("negotiate");

    let group = format!("kapture-it-coord-{}", Uuid::new_v4());
    for _ in 0..4 {
        let req = FindCoordinatorRequest::default()
            .with_key(StrBytes::from_string(group.clone()))
            .with_key_type(0); // 0 = group
        client
            .send(ApiKey::FindCoordinator, 2, &req)
            .await
            .expect("send find-coord");
        let _ = client.recv_raw().await;
    }
    assert!(
        wait_for_kind(
            &proxy,
            |d| matches!(d.kind, AntiPatternKind::CoordinatorChurn) && d.scope.contains(&group),
            "CoordinatorChurn",
        )
        .await
    );
    proxy.stop().await;
}
