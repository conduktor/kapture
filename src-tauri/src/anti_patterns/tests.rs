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
use crate::anti_patterns::{AntiPatternKind, DetectorConfig, Severity};
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
fn offset_out_of_range_on_fetch_fires_after_threshold() {
    let mut fold = AntiPatternsFold::default();
    // Threshold = 3: a single OOR is benign (auto.offset.reset on seek);
    // ≥3 in window is the actual stuck-consumer signature.
    for i in 0..3 {
        let f = frame(&format!("oor{i}"), "t", 1, 9092);
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
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::OffsetOutOfRangeOnFetch && d.scope == "t1:5"));
}

#[test]
fn offset_out_of_range_single_occurrence_does_not_fire() {
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
    assert!(!fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::OffsetOutOfRangeOnFetch));
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
fn commit_during_rebalance_fires_with_group_in_scope() {
    let mut fold = AntiPatternsFold::default();
    // First, the request — establishes group_id for this connection
    // so the response detector can attribute its scope correctly.
    let req_frame = frame("cdr-req", "t", 1, 9092);
    fold.absorb(
        &req_frame,
        Some(&FrameSummary::OffsetCommitRequest {
            group_id: "g-cdr".into(),
            member_id: String::new(),
            topics: vec!["t1".into()],
        }),
    );
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
    // Scope must include the group so concurrent groups committing
    // to the same partition don't collide.
    assert!(fold.snapshot().detections.iter().any(|d| {
        d.kind == AntiPatternKind::CommitDuringRebalance && d.scope == "group=g-cdr|t1:0"
    }));
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

// ---------- Bug-fix regression tests ----------

#[test]
fn throttle_pressure_fires_on_join_group_response_too() {
    // Regression for bug #3: ThrottlePressure used to miss
    // JoinGroupResponse and the other group-protocol responses
    // even though they all carry `throttle_time_ms` (KIP-219).
    let mut fold = AntiPatternsFold::default();
    let f = frame("th-jg", "t", 1, 9092);
    fold.absorb(
        &f,
        Some(&FrameSummary::JoinGroupResponse {
            error_code: 0,
            generation_id: 1,
            member_id: "m".into(),
            throttle_time_ms: 200,
        }),
    );
    assert!(fold.snapshot().detections.iter().any(|d| {
        d.kind == AntiPatternKind::ThrottlePressure && d.scope == "conn=1|api=JoinGroup"
    }));
}

#[test]
fn stale_leader_does_not_invent_broker_zero_on_old_produce() {
    // Regression for bug #2: on Produce v3-v9 the `current_leader`
    // field doesn't exist on the wire and kafka-protocol fills it
    // with `BrokerId(0)`. The detector used to report
    // "current leader is broker 0" as if it were a real hint.
    //
    // The fix gates the field on api_version >= 10, so the
    // extraction code in proto_summary returns `None`. We assert
    // here that when `current_leader_id` is `None`, the detail
    // line does not falsely claim a current leader.
    let mut fold = AntiPatternsFold::default();
    let f = frame("sl-old", "t", 1, 9092);
    fold.absorb(
        &f,
        Some(&FrameSummary::ProduceResponse {
            errors: vec![ProducePartitionError {
                topic: "t1".into(),
                partition: 0,
                error_code: 6,           // NOT_LEADER_OR_FOLLOWER
                current_leader_id: None, // produce v3..=v9 path
            }],
            throttle_time_ms: 0,
        }),
    );
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::StaleLeaderProducing)
        .expect("stale leader should fire");
    assert!(
        !det.detail.contains("current leader is broker 0"),
        "regression: detail unexpectedly invented broker 0: {}",
        det.detail
    );
}

#[test]
fn slow_consumer_poll_stall_fires_after_fetch_gap() {
    // trivago shape: an established fetch stream goes silent past the
    // poll-stall threshold, then resumes. Drive `on_fetch_request`
    // directly with synthetic instants so the gap is deterministic
    // (real sleeps would make this an 11s test).
    let mut fold = AntiPatternsFold::default();
    let f = frame("fr", "t", 7, 9092);
    let t0 = Instant::now();
    // Establish an active fetch cadence (3 fetches, sub-second apart).
    fold.on_fetch_request(&f, t0);
    fold.on_fetch_request(&f, t0 + Duration::from_millis(500));
    fold.on_fetch_request(&f, t0 + Duration::from_secs(1));
    // No stall yet.
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .all(|d| d.kind != AntiPatternKind::SlowConsumerPollStall));
    // Stall then resume: 11s gap > POLL_STALL_GAP (10s) → fires on the
    // resuming fetch, scoped to the connection.
    fold.on_fetch_request(&f, t0 + Duration::from_secs(12));
    let snap = fold.snapshot();
    let det = snap
        .detections
        .iter()
        .find(|d| d.kind == AntiPatternKind::SlowConsumerPollStall)
        .expect("SlowConsumerPollStall");
    assert_eq!(det.scope, "conn=7");
    assert_eq!(det.severity, Severity::Warn);
    assert!(det.detail.contains("11.0s"), "detail: {}", det.detail);
}

#[test]
fn slow_consumer_poll_stall_ignores_short_gaps_and_startup() {
    let mut fold = AntiPatternsFold::default();
    let f = frame("fr", "t", 8, 9092);
    let t0 = Instant::now();
    // A long first inter-fetch gap before the cadence is established
    // (only 1 prior fetch) must NOT fire — that's startup, not a stall.
    fold.on_fetch_request(&f, t0);
    fold.on_fetch_request(&f, t0 + Duration::from_secs(30));
    // Then a healthy 500ms cadence, well under the threshold.
    fold.on_fetch_request(
        &f,
        t0 + Duration::from_secs(30) + Duration::from_millis(500),
    );
    fold.on_fetch_request(&f, t0 + Duration::from_secs(31));
    fold.on_fetch_request(
        &f,
        t0 + Duration::from_secs(31) + Duration::from_millis(500),
    );
    assert!(
        fold.snapshot()
            .detections
            .iter()
            .all(|d| d.kind != AntiPatternKind::SlowConsumerPollStall),
        "no stall should fire for startup gap or healthy cadence",
    );
}

#[test]
fn detector_config_override_changes_poll_stall_threshold() {
    // Same fetch pattern that fires at the 10s default must NOT fire
    // when poll_stall_gap is raised to a real max.poll.interval.ms
    // (300s) — proves the config injection path reaches the detector.
    let cfg = DetectorConfig {
        poll_stall_gap_ms: 300_000,
        ..DetectorConfig::default()
    };
    let mut fold = AntiPatternsFold::new(cfg);
    let f = frame("fr", "t", 9, 9092);
    let t0 = Instant::now();
    fold.on_fetch_request(&f, t0);
    fold.on_fetch_request(&f, t0 + Duration::from_millis(500));
    fold.on_fetch_request(&f, t0 + Duration::from_secs(1));
    fold.on_fetch_request(&f, t0 + Duration::from_secs(12)); // 11s gap < 300s
    assert!(
        fold.snapshot()
            .detections
            .iter()
            .all(|d| d.kind != AntiPatternKind::SlowConsumerPollStall),
        "raised poll_stall_gap should suppress an 11s gap",
    );
    // A 301s gap clears the raised bar.
    fold.on_fetch_request(&f, t0 + Duration::from_secs(12) + Duration::from_secs(301));
    assert!(
        fold.snapshot()
            .detections
            .iter()
            .any(|d| d.kind == AntiPatternKind::SlowConsumerPollStall),
        "a gap beyond the configured interval should fire",
    );
}

#[test]
fn detector_config_persists_and_reloads() {
    use crate::anti_patterns::DetectorConfig;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("detector_config.json");
    // Missing file → defaults.
    assert_eq!(
        DetectorConfig::load_or_default(&path),
        DetectorConfig::default()
    );
    // Save a tweaked config, reload, expect equality.
    let cfg = DetectorConfig {
        poll_stall_gap_ms: 300_000,
        overcommit_rate_per_sec: 9.0,
        ..DetectorConfig::default()
    };
    cfg.save(&path).unwrap();
    assert_eq!(DetectorConfig::load_or_default(&path), cfg);
    // Corrupt file → defaults, no panic.
    std::fs::write(&path, b"{ not json").unwrap();
    assert_eq!(
        DetectorConfig::load_or_default(&path),
        DetectorConfig::default()
    );
}

#[test]
fn gc_drops_stale_per_connection_state() {
    // Regression for bug #1: per-connection maps used to grow
    // unboundedly because there was no idle expiry.
    use std::time::Duration;
    let mut fold = AntiPatternsFold::default();
    // Seed state for connection 42 by absorbing a Produce frame.
    let f = frame("seed", "t", 42, 9092);
    fold.absorb(&f, Some(&produce(1, 1, Some(0), Some(-1), false)));
    assert!(fold.per_connection.contains_key(&42));
    assert!(fold.last_seen.contains_key(&42));
    // Backdate last-seen well past the idle expiry, then run the GC
    // sweep with a synthetic `now`.
    let now = std::time::Instant::now();
    fold.last_seen
        .insert(42, now.checked_sub(Duration::from_secs(60 * 60)).unwrap());
    fold.gc_idle_connections(now);
    assert!(!fold.per_connection.contains_key(&42));
    assert!(!fold.produce_codec.contains_key(&42));
    assert!(!fold.produce_shape.contains_key(&42));
    assert!(!fold.last_seen.contains_key(&42));
}
