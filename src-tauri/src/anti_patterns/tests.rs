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
use crate::proto_summary::{
    FrameSummary, PartitionBytes, ProducePartitionError, TopicPartitionError,
};

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
        client_id: None,
        size: 0,
        captured: 0,
        rtt_ms: 0.0,
        capture_lag_ms: 0.0,
        analysis_lag_ms: 0.0,
        frame_error: None,
        payload_hex: String::new(),
        raw_payload: Vec::new(),
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
        partition_bytes: vec![],
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
fn overcommit_detail_reports_the_real_span_of_a_burst() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..30 {
        fold.absorb(
            &frame(&format!("f{i}"), "t", 1, 9092),
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
        .unwrap();
    assert!(
        det.detail.starts_with("30 OffsetCommit in "),
        "{}",
        det.detail
    );
    assert!(det.detail.contains(" ms,"), "{}", det.detail);
    assert!(!det.detail.contains("sustained"), "{}", det.detail);
}

#[test]
fn rolling_window_observed_wording() {
    use crate::anti_patterns::state::RollingWindow;
    let t0 = Instant::now();
    let mut burst = RollingWindow::default();
    for i in 0..30 {
        burst.instants.push_back(t0 + Duration::from_millis(i));
    }
    assert_eq!(burst.observed("OffsetCommit"), "30 OffsetCommit in 29 ms");
    let mut steady = RollingWindow::default();
    for i in 0..40 {
        steady
            .instants
            .push_back(t0 + Duration::from_millis(i * 500));
    }
    assert_eq!(
        steady.observed("MetadataRequest"),
        "40 MetadataRequest in 20 s (2.1/s)"
    );
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
                protocol_type: "consumer".into(),
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

#[test]
fn sasl_failures_on_new_connections_are_one_detection_per_listener() {
    // A client with a rotated password: every attempt is a new
    // connection that ends in SASL_AUTHENTICATION_FAILED.
    let mut fold = AntiPatternsFold::default();
    for conn in 1..=25 {
        fold.absorb(
            &frame(&format!("sa{conn}"), "t", conn, 9092),
            Some(&FrameSummary::SaslAuthenticateResponse {
                error_code: 58,
                error_message: Some(
                    "Authentication failed during authentication due to invalid credentials".into(),
                ),
                session_lifetime_ms: 0,
            }),
        );
    }
    let dets = fold.snapshot().detections;
    let sasl: Vec<_> = dets
        .iter()
        .filter(|d| d.kind == AntiPatternKind::SaslAuthFailed)
        .collect();
    assert_eq!(sasl.len(), 1, "{dets:?}");
    assert_eq!(sasl[0].scope, "port=9092");
    assert_eq!(sasl[0].occurrences, 25);
    assert!(
        sasl[0].detail.contains("25 failed SASL authentications"),
        "{}",
        sasl[0].detail
    );
    assert!(!dets
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
fn transactional_zombie_ignores_transaction_v2_produce() {
    // KIP-890 transaction v2: Produce v12+ adds the partition broker-side,
    // so a healthy transaction never sends AddPartitionsToTxn.
    let mut fold = AntiPatternsFold::default();
    fold.absorb(
        &frame("zi", "t", 3, 9092),
        Some(&FrameSummary::InitProducerIdRequest {
            transactional: true,
            transactional_id: Some("txn-1".into()),
        }),
    );
    let mut f = frame("zp", "t", 3, 9092);
    f.api_version = 13;
    fold.absorb(&f, Some(&produce(1, -1, Some(0), Some(42), true)));
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .all(|d| d.kind != AntiPatternKind::TransactionalZombie));
}

#[test]
fn transactional_zombie_still_fires_on_v1_produce_v11() {
    let mut fold = AntiPatternsFold::default();
    fold.absorb(
        &frame("zi", "t", 3, 9092),
        Some(&FrameSummary::InitProducerIdRequest {
            transactional: true,
            transactional_id: Some("txn-1".into()),
        }),
    );
    let mut f = frame("zp", "t", 3, 9092);
    f.api_version = 11;
    fold.absorb(&f, Some(&produce(1, -1, Some(0), Some(42), true)));
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::TransactionalZombie));
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

fn advertise(fold: &mut AntiPatternsFold, api_keys: &[i16]) {
    let mut f = frame("av", "t", 1, 9092);
    f.direction = ProtoDirection::Recv;
    fold.absorb(
        &f,
        Some(&FrameSummary::ApiVersionsResponse {
            error_code: 0,
            max_versions: api_keys.iter().map(|k| (*k, 1)).collect(),
        }),
    );
}

fn join(fold: &mut AntiPatternsFold, group: &str, protocol_type: &str, protocols: &[&str]) {
    fold.absorb(
        &frame("jg", "t", 1, 9092),
        Some(&FrameSummary::JoinGroupRequest {
            group_id: group.into(),
            member_id: String::new(),
            protocol_type: protocol_type.into(),
            protocols: protocols.iter().map(|p| (*p).to_owned()).collect(),
        }),
    );
}

fn sync(fold: &mut AntiPatternsFold, group: &str, protocol_type: &str, assignor: &str) {
    fold.absorb(
        &frame("sg", "t", 1, 9092),
        Some(&FrameSummary::SyncGroupRequest {
            group_id: group.into(),
            member_id: "m".into(),
            generation_id: 1,
            protocol_type: Some(protocol_type.into()),
            protocol_name: Some(assignor.into()),
        }),
    );
}

fn classic(fold: &AntiPatternsFold) -> Option<crate::anti_patterns::Detection> {
    fold.snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::ClassicRebalanceOnModernCluster)
}

#[test]
fn classic_rebalance_on_kip848_cluster_fires() {
    let mut fold = AntiPatternsFold::default();
    advertise(&mut fold, &[68]);
    join(&mut fold, "g-classic", "consumer", &["range"]);
    let det = classic(&fold).expect("classic JoinGroup on a KIP-848 broker");
    assert_eq!(det.scope, "group=g-classic");
    assert!(
        det.detail.contains("'range' assignor offered first"),
        "{}",
        det.detail
    );
    assert!(det.detail.contains("eager"), "{}", det.detail);
}

#[test]
fn classic_rebalance_needs_kip848_broker() {
    let mut fold = AntiPatternsFold::default();
    join(&mut fold, "g", "consumer", &["range"]);
    assert!(classic(&fold).is_none());
}

#[test]
fn classic_rebalance_reports_the_assignor_syncgroup_chose() {
    // Java default: offers [range, cooperative-sticky]; the coordinator
    // may still settle on cooperative-sticky when every member agrees.
    let mut fold = AntiPatternsFold::default();
    advertise(&mut fold, &[68]);
    join(&mut fold, "g", "consumer", &["range", "cooperative-sticky"]);
    sync(&mut fold, "g", "consumer", "cooperative-sticky");
    let det = classic(&fold).unwrap();
    assert!(
        det.detail
            .contains("with the 'cooperative-sticky' assignor"),
        "{}",
        det.detail
    );
    assert!(det.detail.contains("incremental"), "{}", det.detail);
    assert!(
        !det.detail.contains("revokes all partitions"),
        "{}",
        det.detail
    );
}

#[test]
fn classic_rebalance_eager_assignor_says_stop_the_world() {
    let mut fold = AntiPatternsFold::default();
    advertise(&mut fold, &[68]);
    sync(&mut fold, "g", "consumer", "range");
    let det = classic(&fold).unwrap();
    assert!(
        det.detail.contains("revokes all partitions"),
        "{}",
        det.detail
    );
    assert!(
        det.detail.contains("group.protocol=consumer"),
        "{}",
        det.detail
    );
}

#[test]
fn classic_rebalance_custom_assignor_does_not_promise_a_drop_in() {
    let mut fold = AntiPatternsFold::default();
    advertise(&mut fold, &[68]);
    sync(&mut fold, "g", "consumer", "rack-aware-custom");
    let det = classic(&fold).unwrap();
    assert!(det.detail.contains("does not carry over"), "{}", det.detail);
}

#[test]
fn classic_rebalance_ignores_connect_workers() {
    let mut fold = AntiPatternsFold::default();
    advertise(&mut fold, &[68]);
    join(&mut fold, "connect-cluster", "connect", &["sessioned"]);
    sync(&mut fold, "connect-cluster", "connect", "sessioned");
    assert!(classic(&fold).is_none());
}

#[test]
fn classic_rebalance_streams_needs_kip1071() {
    // KIP-848 alone doesn't apply to Kafka Streams.
    let mut fold = AntiPatternsFold::default();
    advertise(&mut fold, &[68]);
    join(&mut fold, "app", "consumer", &["stream"]);
    sync(&mut fold, "app", "consumer", "stream");
    assert!(classic(&fold).is_none());

    let mut fold = AntiPatternsFold::default();
    advertise(&mut fold, &[68, 88]);
    sync(&mut fold, "app", "consumer", "stream");
    let det = classic(&fold).expect("Streams group on a KIP-1071 broker");
    assert!(det.title.contains("Streams"), "{}", det.title);
    assert!(
        det.detail.contains("group.protocol=streams"),
        "{}",
        det.detail
    );
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
fn offset_out_of_range_single_occurrence_is_a_note() {
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
    // One is enough for auto.offset.reset to skip or re-read records: a
    // Note until the reset (or a repeat) says more.
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::OffsetOutOfRangeOnFetch)
        .expect("single OFFSET_OUT_OF_RANGE");
    assert_eq!(det.severity, Severity::Note);
    assert_eq!(det.scope, "t1:5");
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
                protocol_type: "consumer".into(),
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
fn init_producer_id_authorization_loop_is_an_acl_deny_not_a_producer_per_record() {
    // One idempotent producer with no WRITE anywhere: InitProducerId comes
    // back CLUSTER_AUTHORIZATION_FAILED and is asked again, on the same
    // connection, with no Produce in between.
    let mut fold = AntiPatternsFold::default();
    for i in 0..50 {
        fold.absorb(
            &frame(&format!("ipr{i}"), "t", 9, 9092),
            Some(&FrameSummary::InitProducerIdRequest {
                transactional: false,
                transactional_id: None,
            }),
        );
        fold.absorb(
            &frame(&format!("ips{i}"), "t", 9, 9092),
            Some(&FrameSummary::InitProducerIdResponse { error_code: 31 }),
        );
    }
    let dets = fold.snapshot().detections;
    assert!(
        !dets
            .iter()
            .any(|d| d.kind == AntiPatternKind::ProducerPerRecord),
        "retries of a failed InitProducerId are one producer: {dets:?}",
    );
    let acl = dets
        .iter()
        .find(|d| d.kind == AntiPatternKind::AclDeny && d.scope == "conn=9")
        .expect("the CLUSTER_AUTHORIZATION_FAILED loop should raise an ACL deny");
    assert!(acl.detail.contains("50 denials"), "{}", acl.detail);
    assert!(acl.detail.contains("IdempotentWrite"), "{}", acl.detail);
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

#[test]
fn hung_request_appears_at_timeout_and_response_clears_it() {
    let cfg = DetectorConfig {
        hung_request_timeout_ms: 0,
        ..DetectorConfig::default()
    };
    let mut fold = AntiPatternsFold::new(cfg);
    let mut request = frame("req", "2026-05-19T10:00:00Z", 77, 9092);
    request.corr_id = 123;
    fold.absorb(&request, Some(&produce(1, 1, Some(1), Some(9), false)));
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::HungRequests));

    let mut response = frame("resp", "2026-05-19T10:00:01Z", 77, 9092);
    response.direction = ProtoDirection::Recv;
    response.corr_id = 123;
    fold.absorb(&response, None);
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .all(|d| d.kind != AntiPatternKind::HungRequests));
}

#[test]
fn in_flight_thresholds_have_a_clean_boundary() {
    let cfg = DetectorConfig {
        in_flight_saturation_threshold: 3,
        idempotent_produce_in_flight_threshold: 2,
        hung_request_timeout_ms: u64::MAX,
        ..DetectorConfig::default()
    };
    let mut fold = AntiPatternsFold::new(cfg);
    for corr_id in 1..=2 {
        let mut request = frame(&format!("req-{corr_id}"), "t", 5, 9092);
        request.corr_id = corr_id;
        fold.absorb(&request, Some(&produce(1, 1, Some(1), Some(42), false)));
    }
    assert!(fold.snapshot().detections.iter().all(|d| {
        !matches!(
            d.kind,
            AntiPatternKind::InFlightSaturation | AntiPatternKind::ExcessiveIdempotentInFlight
        )
    }));
    let mut third = frame("req-3", "t", 5, 9092);
    third.corr_id = 3;
    fold.absorb(&third, Some(&produce(1, 1, Some(1), Some(42), false)));
    let snapshot = fold.snapshot();
    assert!(snapshot
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::InFlightSaturation));
    assert!(snapshot
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::ExcessiveIdempotentInFlight));
}

#[test]
fn read_uncommitted_requires_transactional_traffic() {
    let mut fold = AntiPatternsFold::default();
    let fetch = FrameSummary::FetchRequest {
        topics: vec!["orders".into()],
        min_bytes: 1,
        max_wait_ms: 500,
        isolation_level: 0,
        session_id: 0,
        session_epoch: 0,
    };
    let consumer = frame("fetch", "t", 1, 9092);
    fold.absorb(&consumer, Some(&fetch));
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .all(|d| d.kind != AntiPatternKind::ReadUncommittedTransactional));
    let producer = frame("produce", "t", 2, 9092);
    fold.absorb(&producer, Some(&produce(1, -1, Some(1), Some(42), true)));
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::ReadUncommittedTransactional));
}

#[test]
fn partition_skew_fires_only_after_sample_and_ratio_thresholds() {
    let cfg = DetectorConfig {
        partition_skew_min_bytes: 100,
        partition_skew_min_samples: 4,
        partition_skew_ratio: 0.80,
        ..DetectorConfig::default()
    };
    let mut fold = AntiPatternsFold::new(cfg);
    let mut summary = produce(1, 1, Some(1), Some(42), false);
    let FrameSummary::ProduceRequest {
        ref mut partition_bytes,
        ..
    } = summary
    else {
        unreachable!()
    };
    *partition_bytes = vec![
        PartitionBytes {
            topic: "orders".into(),
            partition: 0,
            bytes: 90,
        },
        PartitionBytes {
            topic: "orders".into(),
            partition: 1,
            bytes: 10,
        },
    ];
    let f = frame("skew", "t", 3, 9092);
    fold.absorb(&f, Some(&summary));
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .all(|d| d.kind != AntiPatternKind::PartitionSkew));
    fold.absorb(&f, Some(&summary));
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::PartitionSkew));
}

#[test]
fn retry_storm_fires_on_threshold_not_single_error() {
    let cfg = DetectorConfig {
        retry_storm_threshold: 3,
        ..DetectorConfig::default()
    };
    let mut fold = AntiPatternsFold::new(cfg);
    let response = FrameSummary::ProduceResponse {
        errors: vec![ProducePartitionError {
            topic: "orders".into(),
            partition: 0,
            error_code: 6,
            current_leader_id: None,
        }],
        throttle_time_ms: 0,
    };
    let mut f = frame("retry", "t", 4, 9092);
    f.direction = ProtoDirection::Recv;
    for _ in 0..2 {
        fold.absorb(&f, Some(&response));
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .all(|d| d.kind != AntiPatternKind::RetryStorm));
    fold.absorb(&f, Some(&response));
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::RetryStorm));
}

#[test]
fn detections_name_the_client_ids_of_their_connections() {
    let mut fold = AntiPatternsFold::default();
    let mut req = frame("p", "t", 5, 9092);
    req.client_id = Some("orders-svc".into());
    fold.absorb(&req, Some(&produce(50, 0, Some(1), Some(7), false)));
    // A response carries no client.id; the connection does.
    let mut resp = frame("r", "t", 5, 9092);
    resp.direction = ProtoDirection::Recv;
    fold.absorb(
        &resp,
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
    let snap = fold.snapshot();
    for kind in [
        AntiPatternKind::Acks0,
        AntiPatternKind::MessageTooLargeRejected,
    ] {
        let det = snap.detections.iter().find(|d| d.kind == kind).unwrap();
        assert_eq!(det.client_ids, vec!["orders-svc".to_owned()], "{kind:?}");
    }
}

#[test]
fn group_detection_lists_distinct_client_ids_capped() {
    let mut fold = AntiPatternsFold::new(DetectorConfig {
        overcommit_min_samples: 1,
        ..DetectorConfig::default()
    });
    for conn in 0..12 {
        for _ in 0..2 {
            let mut f = frame("c", "t", conn, 9092);
            f.client_id = Some(format!("consumer-g-{}", conn % 10));
            fold.absorb(
                &f,
                Some(&FrameSummary::OffsetCommitRequest {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    topics: vec!["t".into()],
                }),
            );
        }
    }
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::Overcommit)
        .unwrap();
    assert_eq!(det.client_ids.len(), 8);
    let unique: std::collections::HashSet<&String> = det.client_ids.iter().collect();
    assert_eq!(unique.len(), 8);
}

fn join_req(group: &str, member: &str, protocols: &[&str]) -> FrameSummary {
    FrameSummary::JoinGroupRequest {
        group_id: group.into(),
        member_id: member.into(),
        protocol_type: "consumer".into(),
        protocols: protocols.iter().map(|p| (*p).to_owned()).collect(),
    }
}

#[test]
fn leave_group_poll_timeout_flags_the_slow_consumer() {
    let mut fold = AntiPatternsFold::default();
    let leave = |reason: &str| FrameSummary::LeaveGroupRequest {
        group_id: "orders".into(),
        reasons: vec![reason.into()],
    };
    fold.absorb(
        &frame("l1", "t", 4, 9092),
        Some(&leave("the consumer is being closed")),
    );
    assert!(fold.snapshot().detections.is_empty());
    fold.absorb(
        &frame("l2", "t", 4, 9092),
        Some(&leave("consumer poll timeout has expired.")),
    );
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::SlowConsumerPollStall)
        .expect("poll timeout");
    assert_eq!(det.scope, "group=orders");
    assert_eq!(det.severity, Severity::Warn);
    assert!(
        det.detail.contains("max.poll.interval.ms"),
        "{}",
        det.detail
    );
}

#[test]
fn a_join_with_its_member_id_handshake_counts_once() {
    // Three rebalances of one v4+ member: six JoinGroup requests.
    let mut fold = AntiPatternsFold::default();
    for i in 0..3 {
        fold.absorb(
            &frame(&format!("a{i}"), "t", 1, 9092),
            Some(&join_req("g", "", &["range"])),
        );
        fold.absorb(
            &frame(&format!("b{i}"), "t", 1, 9092),
            Some(&join_req("g", "m-1", &["range"])),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .all(|d| d.kind != AntiPatternKind::RebalanceLoop));
    // Two more rebalances reach the threshold of five.
    for i in 3..5 {
        fold.absorb(
            &frame(&format!("a{i}"), "t", 1, 9092),
            Some(&join_req("g", "", &["range"])),
        );
        fold.absorb(
            &frame(&format!("b{i}"), "t", 1, 9092),
            Some(&join_req("g", "m-1", &["range"])),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .any(|d| d.kind == AntiPatternKind::RebalanceLoop));
}

#[test]
fn cooperative_sticky_churn_ignores_the_default_range_first_list() {
    let mut fold = AntiPatternsFold::default();
    for i in 0..6 {
        fold.absorb(
            &frame(&format!("j{i}"), "t", 1, 9092),
            Some(&join_req("g", "m", &["range", "cooperative-sticky"])),
        );
    }
    assert!(fold
        .snapshot()
        .detections
        .iter()
        .all(|d| d.kind != AntiPatternKind::CooperativeStickyChurn));
}

fn oor_fetch(topic: &str) -> FrameSummary {
    FrameSummary::FetchResponse {
        error_code: 0,
        session_id: 0,
        throttle_time_ms: 0,
        response_size: 0,
        errors: vec![TopicPartitionError {
            topic: topic.into(),
            partition: 0,
            error_code: 1,
        }],
    }
}

fn recv(id: &str, conn: i32) -> ProtoFrame {
    let mut f = frame(id, "t", conn, 9092);
    f.direction = ProtoDirection::Recv;
    f
}

fn reset(fold: &mut AntiPatternsFold, timestamp: i64, offset: i64) {
    fold.absorb(
        &frame("lo", "t", 9, 9092),
        Some(&FrameSummary::ListOffsetsRequest {
            partitions: vec![crate::proto_summary::ListOffsetsQuery {
                topic: "orders".into(),
                partition: 0,
                timestamp,
            }],
        }),
    );
    fold.absorb(
        &recv("lor", 9),
        Some(&FrameSummary::ListOffsetsResponse {
            partitions: vec![crate::proto_summary::ListOffsetsAnswer {
                topic: "orders".into(),
                partition: 0,
                error_code: 0,
                offset,
            }],
        }),
    );
}

#[test]
fn a_single_offset_out_of_range_is_noted_and_its_reset_to_latest_flagged() {
    let mut fold = AntiPatternsFold::default();
    // Fetch v13+ names the topic by id; Metadata maps it back.
    fold.absorb(
        &recv("md", 9),
        Some(&FrameSummary::MetadataResponse {
            topics: vec!["orders".into()],
            brokers: 1,
            leaders: vec![],
            brokers_map: vec![],
            throttle_time_ms: 0,
            topic_ids: vec![("8f1c-orders-id".into(), "orders".into())],
        }),
    );
    fold.absorb(&recv("f", 9), Some(&oor_fetch("8f1c-orders-id")));
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::OffsetOutOfRangeOnFetch)
        .unwrap();
    assert_eq!(det.scope, "orders:0");
    assert_eq!(det.severity, Severity::Note);

    reset(&mut fold, -1, 61);
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::OffsetOutOfRangeOnFetch)
        .unwrap();
    assert_eq!(det.severity, Severity::Warn);
    assert!(det.title.contains("latest"), "{}", det.title);
    assert!(det.detail.contains("offset 61"), "{}", det.detail);
}

#[test]
fn a_reset_to_earliest_stays_a_note() {
    let mut fold = AntiPatternsFold::default();
    fold.absorb(&recv("f", 9), Some(&oor_fetch("orders")));
    reset(&mut fold, -2, 51);
    let det = fold
        .snapshot()
        .detections
        .into_iter()
        .find(|d| d.kind == AntiPatternKind::OffsetOutOfRangeOnFetch)
        .unwrap();
    assert_eq!(det.severity, Severity::Note);
    assert!(det.title.contains("earliest"), "{}", det.title);
}
