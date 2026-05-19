//! Unit tests for the anti-pattern detectors. Each test mocks
//! `FrameSummary` values and runs them through `AntiPatternsFold`, then
//! asserts that the expected detection appears in the snapshot.
//!
//! Integration tests in `tests/anti_patterns_integration.rs` drive
//! these same detectors through a real broker.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::anti_patterns::fold::AntiPatternsFold;
use crate::anti_patterns::{AntiPatternKind, Severity};
use crate::correlator::ProtoFrame;
use crate::proto_event::ProtoDirection;
use crate::proto_summary::{FrameSummary, ProducePartitionError, TopicPartitionError};

fn frame(id: &str, ts: &str, connection_id: i32, local_port: u16) -> ProtoFrame {
    ProtoFrame {
        id: id.to_string(),
        timestamp: ts.to_string(),
        direction: ProtoDirection::Send,
        api_key: 0,
        api_name: "ProduceRequest".into(),
        api_version: 9,
        connection_id,
        local_port,
        corr_id: 0,
        size: 0,
        captured: 0,
        rtt_ms: 0.0,
        frame_error: None,
        payload_hex: String::new(),
        decoded_json: None,
        summary: None,
    }
}

fn produce(
    record_count: u32,
    acks: i16,
    compression: Option<u8>,
    producer_id: Option<i64>,
    transactional: bool,
) -> FrameSummary {
    FrameSummary::ProduceRequest {
        topics: vec!["t".into()],
        partitions: vec![],
        record_count,
        batch_bytes: 100,
        batch_count: 1,
        transactional,
        acks,
        first_batch_compression: compression,
        first_batch_producer_id: producer_id,
    }
}

#[test]
fn overcommit_flags_after_high_rate() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..40 {
        let f = frame(&format!("f{i}"), "2026-05-19T10:00:00Z", 1, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::OffsetCommitRequest {
                group_id: "g1".into(),
                member_id: "m".into(),
                topics: vec!["t".into()],
            }),
        );
    }
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::Overcommit)
        .expect("overcommit should fire");
    assert_eq!(det.scope, "group=g1");
}

#[test]
fn producer_per_record_ratio() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..6 {
        let f = frame(&format!("i{i}"), "t", 7, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::InitProducerIdRequest {
                transactional: false,
                transactional_id: None,
            }),
        );
    }
    for i in 0..4 {
        let f = frame(&format!("p{i}"), "t", 7, 9092);
        fold.absorb(&f, Some(&produce(1, 1, Some(0), Some(-1), false)));
    }
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::ProducerPerRecord)
        .expect("producer-per-record should fire");
    assert_eq!(det.scope, "conn=7");
}

#[test]
fn tiny_batches_when_records_per_produce_close_to_one() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..30 {
        let f = frame(&format!("p{i}"), "t", 8, 9092);
        fold.absorb(&f, Some(&produce(1, 1, Some(0), Some(-1), false)));
    }
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::TinyBatches)
        .expect("tiny-batches should fire");
    assert_eq!(det.scope, "conn=8");
}

#[test]
fn rebalance_loop_after_five_joins() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..5 {
        let f = frame(&format!("j{i}"), "t", 1, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::JoinGroupRequest {
                group_id: "g1".into(),
                member_id: "m".into(),
                protocols: vec!["range".into()],
            }),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::RebalanceLoop && d.scope == "group=g1"));
}

#[test]
fn stale_leader_on_not_leader_response() {
    let mut fold = AntiPatternsFold::default();
    let f = frame("r1", "t", 1, 9092);
    fold.absorb(
        &f,
        Some(&FrameSummary::ProduceResponse {
            errors: vec![ProducePartitionError {
                topic: "orders".into(),
                partition: 3,
                error_code: 6,
                current_leader_id: Some(50),
            }],
            throttle_time_ms: 0,
        }),
    );
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::StaleLeaderProducing)
        .expect("stale-leader should fire");
    assert_eq!(det.scope, "orders:3");
    assert!(det.detail.contains("current leader is broker 50"));
}

#[test]
fn mixed_api_version_across_brokers() {
    let mut fold = AntiPatternsFold::default();
    let f1 = frame("av1", "t", 1, 9001);
    fold.absorb(
        &f1,
        Some(&FrameSummary::ApiVersionsResponse {
            error_code: 0,
            max_versions: vec![(0, 11)],
        }),
    );
    let f2 = frame("av2", "t", 2, 9002);
    fold.absorb(
        &f2,
        Some(&FrameSummary::ApiVersionsResponse {
            error_code: 0,
            max_versions: vec![(0, 10)],
        }),
    );
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::MixedApiVersion));
}

#[test]
fn sasl_session_too_short_on_reauth() {
    let mut fold = AntiPatternsFold::default();
    let f1 = frame("s1", "t1", 1, 9092);
    fold.absorb(
        &f1,
        Some(&FrameSummary::SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            session_lifetime_ms: 3_600_000,
        }),
    );
    let f2 = frame("s2", "t2", 1, 9092);
    fold.absorb(
        &f2,
        Some(&FrameSummary::SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            session_lifetime_ms: 5_000,
        }),
    );
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::SaslSessionTooShort));
}

// ---------- New detectors ----------

#[test]
fn acks0_fires_on_any_acks_zero_produce() {
    let mut fold = AntiPatternsFold::default();
    let f = frame("a0", "t", 1, 9092);
    fold.absorb(&f, Some(&produce(1, 0, Some(0), Some(-1), false)));
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::Acks0)
        .expect("acks=0 should fire");
    assert_eq!(det.severity, Severity::Warn);
    assert_eq!(det.scope, "conn=1");
}

#[test]
fn compression_off_fires_after_min_samples_and_rate() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..30 {
        let f = frame(&format!("c{i}"), "t", 1, 9092);
        // compression=0 (none), pid=-1 (non-idempotent), high rate via
        // tight loop (Instant::now()).
        fold.absorb(&f, Some(&produce(1, 1, Some(0), Some(-1), false)));
    }
    let snap = fold.snapshot();
    assert!(snap
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::CompressionOff));
}

#[test]
fn non_idempotent_producer_fires() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..30 {
        let f = frame(&format!("ni{i}"), "t", 2, 9092);
        fold.absorb(&f, Some(&produce(1, 1, Some(1), Some(-1), false)));
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::NonIdempotentProducer && d.scope == "conn=2"));
}

#[test]
fn producer_instance_leak_fires_on_many_handshakes() {
    let mut fold = AntiPatternsFold::default();
    // Simulate 12 distinct connections, each completing the
    // ApiVersions+Metadata+InitProducerId triplet.
    for conn in 0..12 {
        let f = frame(&format!("c{conn}-av"), "t", conn, 7777);
        fold.mark_handshake_api_versions(&f);
        let f = frame(&format!("c{conn}-md"), "t", conn, 7777);
        fold.absorb(
            &f,
            Some(&FrameSummary::MetadataRequest {
                topics: vec![],
                allow_auto_topic_creation: false,
            }),
        );
        let f = frame(&format!("c{conn}-ip"), "t", conn, 7777);
        fold.absorb(
            &f,
            Some(&FrameSummary::InitProducerIdRequest {
                transactional: false,
                transactional_id: None,
            }),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::ProducerInstanceLeak && d.scope == "port=7777"));
}

#[test]
fn transactional_zombie_fires_on_produce_without_add_partitions() {
    let mut fold = AntiPatternsFold::default();
    // 1) init producer with a txn id
    let f = frame("zi", "t", 3, 9092);
    fold.absorb(
        &f,
        Some(&FrameSummary::InitProducerIdRequest {
            transactional: true,
            transactional_id: Some("txn-1".into()),
        }),
    );
    // 2) produce without AddPartitionsToTxn → zombie shape
    let f = frame("zp", "t", 3, 9092);
    fold.absorb(&f, Some(&produce(1, 1, Some(0), Some(42), true)));
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::TransactionalZombie && d.scope == "conn=3"));
}

#[test]
fn auto_commit_cadence_fires_on_near_5s_intervals() {
    let mut fold = AntiPatternsFold::default();
    // Drive 5 commits at exactly 5000ms apart by seeding the
    // intervals window directly — using real sleeps would make this
    // a ~25s test. The detector compares mean to
    // `AUTOCOMMIT_INTERVAL_MS` = 5000ms.
    let now = Instant::now();
    let intervals = fold.autocommit_intervals.entry("g-ac".into()).or_default();
    intervals.push(now);
    intervals.push(now + Duration::from_secs(5));
    intervals.push(now + Duration::from_secs(10));
    intervals.push(now + Duration::from_secs(15));
    intervals.push(now + Duration::from_secs(20));
    // Now drive one more OffsetCommit which will rerun the check.
    let f = frame("oc", "t", 1, 9092);
    fold.on_offset_commit(&f, "g-ac", now + Duration::from_secs(25));
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::AutoCommitCadence && d.scope == "group=g-ac"));
}

#[test]
fn tight_fetch_polling_fires() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..30 {
        let f = frame(&format!("ft{i}"), "t", 5, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::FetchResponse {
                error_code: 0,
                session_id: 1,
                throttle_time_ms: 0,
                response_size: 100,
                errors: vec![],
            }),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::TightFetchPolling && d.scope == "conn=5"));
}

#[test]
fn fetch_session_error_cascade_fires_after_3() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..4 {
        let f = frame(&format!("fse{i}"), "t", 6, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::FetchResponse {
                error_code: 70,
                session_id: 1,
                throttle_time_ms: 0,
                response_size: 0,
                errors: vec![],
            }),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::FetchSessionErrorCascade && d.scope == "conn=6"));
}

#[test]
fn throttle_pressure_fires_on_nonzero_throttle() {
    let mut fold = AntiPatternsFold::default();
    let f = frame("th", "t", 1, 9092);
    fold.absorb(
        &f,
        Some(&FrameSummary::ProduceResponse {
            errors: vec![],
            throttle_time_ms: 250,
        }),
    );
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::ThrottlePressure)
        .expect("throttle should fire");
    assert!(det.detail.contains("throttle_time_ms=250"));
    assert_eq!(det.scope, "conn=1|api=Produce");
}

#[test]
fn metadata_storm_fires_on_high_rate() {
    let mut fold = AntiPatternsFold::default();
    // Drive >10 metadata requests in tight loop — rate computation
    // uses Instant::now() so a burst easily exceeds threshold.
    for i in 0..30 {
        let f = frame(&format!("m{i}"), "t", 4, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::MetadataRequest {
                topics: vec![],
                allow_auto_topic_creation: false,
            }),
        );
        // Tiny pause so the rolling window has measurable span — the
        // rate detector uses .max(1.0) on the span so a 1ms gap is
        // enough to keep arithmetic valid.
        sleep(Duration::from_millis(1));
    }
    let snap = fold.snapshot();
    assert!(
        snap.detections
            .iter()
            .any(|d| d.kind == AntiPatternKind::MetadataStorm && d.scope == "conn=4"),
        "no metadata-storm in {:#?}",
        snap.detections
    );
}

#[test]
fn classic_rebalance_on_kip848_cluster_fires() {
    let mut fold = AntiPatternsFold::default();
    // Broker advertises ConsumerGroupHeartbeat (api_key=68).
    let f = frame("av", "t", 1, 9092);
    fold.absorb(
        &f,
        Some(&FrameSummary::ApiVersionsResponse {
            error_code: 0,
            max_versions: vec![(68, 1)],
        }),
    );
    // Client then uses classic JoinGroup.
    let f = frame("jg", "t", 1, 9092);
    fold.absorb(
        &f,
        Some(&FrameSummary::JoinGroupRequest {
            group_id: "g-classic".into(),
            member_id: String::new(),
            protocols: vec!["range".into()],
        }),
    );
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::ClassicRebalanceOnModernCluster));
}

// ---------- Phase B+ detectors (#19–#25) ----------

#[test]
fn message_too_large_rejected_fires() {
    let mut fold = AntiPatternsFold::default();
    let f = frame("mtl", "t", 1, 9092);
    fold.absorb(
        &f,
        Some(&FrameSummary::ProduceResponse {
            errors: vec![ProducePartitionError {
                topic: "orders".into(),
                partition: 0,
                error_code: 10,
                current_leader_id: None,
            }],
            throttle_time_ms: 0,
        }),
    );
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::MessageTooLargeRejected && d.scope == "orders:0"));
}

#[test]
fn offset_out_of_range_on_fetch_fires() {
    let mut fold = AntiPatternsFold::default();
    let f = frame("oor", "t", 1, 9092);
    fold.absorb(
        &f,
        Some(&FrameSummary::FetchResponse {
            error_code: 0,
            session_id: 0,
            throttle_time_ms: 0,
            response_size: 0,
            errors: vec![TopicPartitionError {
                topic: "t1".into(),
                partition: 5,
                error_code: 1,
            }],
        }),
    );
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::OffsetOutOfRangeOnFetch && d.scope == "t1:5"));
}

#[test]
fn cooperative_sticky_churn_fires() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..4 {
        let f = frame(&format!("cs{i}"), "t", 1, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::JoinGroupRequest {
                group_id: "g-coop".into(),
                member_id: String::new(),
                protocols: vec!["cooperative-sticky".into()],
            }),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::CooperativeStickyChurn && d.scope == "group=g-coop"));
}

#[test]
fn commit_during_rebalance_fires() {
    let mut fold = AntiPatternsFold::default();
    let f = frame("cdr", "t", 1, 9092);
    fold.absorb(
        &f,
        Some(&FrameSummary::OffsetCommitResponse {
            max_error_code: 27,
            throttle_time_ms: 0,
            errors: vec![TopicPartitionError {
                topic: "t1".into(),
                partition: 0,
                error_code: 27,
            }],
        }),
    );
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::CommitDuringRebalance && d.scope == "t1:0"));
}

#[test]
fn acl_deny_fires_after_threshold() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..3 {
        let f = frame(&format!("acl{i}"), "t", 7, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::FetchResponse {
                error_code: 0,
                session_id: 0,
                throttle_time_ms: 0,
                response_size: 0,
                errors: vec![TopicPartitionError {
                    topic: "t".into(),
                    partition: 0,
                    error_code: 29,
                }],
            }),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::AclDeny && d.scope == "conn=7"));
}

#[test]
fn unknown_topic_poll_loop_fires_after_threshold() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..3 {
        let f = frame(&format!("utp{i}"), "t", 1, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::FetchResponse {
                error_code: 0,
                session_id: 0,
                throttle_time_ms: 0,
                response_size: 0,
                errors: vec![TopicPartitionError {
                    topic: "ghost".into(),
                    partition: 0,
                    error_code: 3,
                }],
            }),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::UnknownTopicPollLoop && d.scope == "ghost:0"));
}

#[test]
fn coordinator_churn_fires_after_threshold() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..4 {
        let f = frame(&format!("cc{i}"), "t", 1, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::FindCoordinatorRequest {
                keys: vec!["g-churn".into()],
            }),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::CoordinatorChurn && d.scope == "key=g-churn"));
}
