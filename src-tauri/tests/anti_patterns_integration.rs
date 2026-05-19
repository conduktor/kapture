//! Integration tests for the *original 7* client-side anti-pattern
//! detectors, exercised against a real Kafka broker via the same
//! `ProxyHandle` used by the Tauri app.
//!
//! Shared helpers live in `tests/common/mod.rs`. Tests for the
//! *additional 11* detectors live in `anti_patterns_extended.rs`.
//!
//! Gating: every test reads `KAPTURE_KAFKA_BOOTSTRAP` (or one of the
//! profile-specific env vars). When unset, the test prints a notice
//! and returns early.

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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
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
    ApiKey, GroupId, ProducerId, TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use kapture_lib::example_api::{AntiPatternKind, ProtoCorrelator};
use uuid::Uuid;

use common::{
    env_or_skip, negotiate, record_batch_one, upstream_or_skip, wait_for_kind, TestProxy,
    WireClient, ENV_BOOTSTRAP, ENV_LEGACY, ENV_MB_BROKERS, ENV_SASL,
};

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
        .await
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

    let version = produce_max.min(9);
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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn stale_leader_detected_through_proxy() {
    let Some(brokers_csv) = env_or_skip("stale_leader_detected_through_proxy", ENV_MB_BROKERS)
    else {
        return;
    };
    let brokers: Vec<String> = brokers_csv.split(',').map(str::to_owned).collect();
    assert!(brokers.len() >= 2, "need >=2 brokers in {ENV_MB_BROKERS}");

    let admin_proxy = TestProxy::start(brokers[0].clone())
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
    assert!(!leaders.is_empty(), "no partitions");

    let (target_partition, target_upstream) = leaders
        .iter()
        .find_map(|(p, leader_id)| {
            brokers.iter().enumerate().find_map(|(idx, candidate)| {
                let node_id = i32::try_from(idx + 1).unwrap_or(0);
                if node_id == *leader_id {
                    None
                } else {
                    Some((*p, candidate.clone()))
                }
            })
        })
        .expect("non-leader pair");

    admin_proxy.stop().await;

    let proxy = TestProxy::start(target_upstream.clone())
        .await
        .expect("start non-leader proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    let produce_max = negotiate(&mut client).await.expect("negotiate");
    let version = produce_max.min(9);

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
        .await
    );
    proxy.stop().await;
}

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

    for port in [proxy_a.listen_port(), proxy_b.listen_port()] {
        let mut client = WireClient::connect(&format!("127.0.0.1:{port}"))
            .await
            .expect("connect");
        negotiate(&mut client).await.expect("negotiate");
    }

    let mut fired = false;
    for _ in 0..50 {
        if correlator
            .anti_patterns()
            .detections
            .iter()
            .any(|d| matches!(d.kind, AntiPatternKind::MixedApiVersion))
        {
            fired = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(fired, "no MixedApiVersion");
    proxy_a.stop().await;
    proxy_b.stop().await;
}

#[tokio::test]
async fn sasl_failure_detected_through_proxy() {
    let Some(upstream) = env_or_skip("sasl_failure_detected_through_proxy", ENV_SASL) else {
        return;
    };
    let proxy = TestProxy::start(upstream).await.expect("start proxy");
    let addr = format!("127.0.0.1:{}", proxy.listen_port());
    let mut client = WireClient::connect(&addr).await.expect("connect");
    negotiate(&mut client).await.expect("negotiate");

    let hs = SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN"));
    client
        .send(ApiKey::SaslHandshake, 1, &hs)
        .await
        .expect("send handshake");
    let _ = client.recv_raw().await;

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
        .await
    );
    proxy.stop().await;
}
