#![allow(clippy::doc_markdown)]
//! Per-pattern detector methods. Each `on_*` is invoked from the
//! `dispatch` table in `fold.rs`; each `check_*` is shared logic.
//!
//! All methods are `impl AntiPatternsFold`; they update `self.*` state
//! and call `self.upsert(...)` when the pattern fires. Methods are
//! deliberately *short* — heuristics, not algorithms — and grouped by
//! category (producer, consumer, cluster).

use std::time::Instant;

use crate::anti_patterns::fold::AntiPatternsFold;
use crate::anti_patterns::kafka_errors as kerr;
use crate::anti_patterns::state::{
    ACL_DENY_THRESHOLD, AUTOCOMMIT_INTERVAL_MS, AUTOCOMMIT_INTERVAL_TOLERANCE,
    AUTOCOMMIT_MIN_SAMPLES, COMPRESSION_OFF_MIN_RATE, COMPRESSION_OFF_MIN_SAMPLES,
    COOPERATIVE_STICKY_CHURN_THRESHOLD, COORDINATOR_CHURN_THRESHOLD,
    FETCH_SESSION_ERRORS_THRESHOLD, METADATA_STORM_MIN_SAMPLES, METADATA_STORM_RATE_PER_SEC,
    NON_IDEMPOTENT_MIN_SAMPLES, OFFSET_OUT_OF_RANGE_THRESHOLD, OVERCOMMIT_MIN_SAMPLES,
    OVERCOMMIT_RATE_PER_SEC, PRODUCER_INSTANCE_LEAK_MIN_SAMPLES, PRODUCER_INSTANCE_LEAK_PER_SEC,
    PRODUCER_PER_RECORD_INIT_RATIO, PRODUCER_PER_RECORD_MIN_INITS, RATE_QUEUE_CAP, RATE_WINDOW,
    REBALANCE_JOINS_IN_WINDOW, SASL_SHORT_SESSION_MS, TIGHT_FETCH_AVG_RESPONSE_BYTES,
    TIGHT_FETCH_MIN_RATE, TIGHT_FETCH_MIN_SAMPLES, TINY_BATCH_MIN_PRODUCE_RATE,
    TINY_BATCH_MIN_SAMPLES, TINY_BATCH_RECORDS_PER_PRODUCE, UNKNOWN_TOPIC_POLL_THRESHOLD,
};
use crate::anti_patterns::{AntiPatternKind, Severity};
use crate::correlator::ProtoFrame;
use crate::proto_summary::{FrameSummary, ProducePartitionError, TopicPartitionError};

/// `ConsumerGroupHeartbeat` API key — used to gate KIP-848 detection.
const API_KEY_CONSUMER_GROUP_HEARTBEAT: i16 = 68;

impl AntiPatternsFold {
    // ---------- Producer-side detectors ----------

    pub(super) fn on_init_producer_id(
        &mut self,
        frame: &ProtoFrame,
        transactional_id: Option<&str>,
    ) {
        // Counters for the producer-per-record ratio.
        {
            let c = self.per_connection.entry(frame.connection_id).or_default();
            c.init_producer_id = c.init_producer_id.saturating_add(1);
        }
        self.check_producer_per_record(frame);
        // Handshake fingerprint for the producer-instance leak.
        self.mark_handshake_init_producer_id(frame);
        // Txn lifecycle: a non-empty transactional_id opens a txn.
        if let Some(tid) = transactional_id {
            let st = self.txn_state.entry(frame.connection_id).or_default();
            st.transactional_id = Some(tid.to_owned());
            st.produced_in_txn = false;
            st.add_partitions = false;
            st.ended = false;
        }
    }

    pub(super) fn on_add_partitions_to_txn(&mut self, frame: &ProtoFrame, _txn_id: &str) {
        let st = self.txn_state.entry(frame.connection_id).or_default();
        st.add_partitions = true;
    }

    pub(super) fn on_end_txn(&mut self, frame: &ProtoFrame, _txn_id: &str, _committed: bool) {
        let st = self.txn_state.entry(frame.connection_id).or_default();
        st.ended = true;
        // EndTxn closes the txn — reset the produced-in-txn flag so a
        // subsequent Produce on the same connection doesn't fire stale
        // zombie detections.
        st.produced_in_txn = false;
        st.transactional_id = None;
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn on_produce_request(
        &mut self,
        frame: &ProtoFrame,
        record_count: u32,
        topics: &[String],
        acks: i16,
        first_batch_compression: Option<u8>,
        first_batch_producer_id: Option<i64>,
        transactional: bool,
        now: Instant,
    ) {
        // Counters for the producer-per-record ratio.
        {
            let c = self.per_connection.entry(frame.connection_id).or_default();
            c.produce_requests = c.produce_requests.saturating_add(1);
        }
        self.check_producer_per_record(frame);

        // Tiny-batch shape tracking.
        self.update_produce_shape(frame, record_count, topics, now);

        // Codec + idempotence stats.
        self.update_produce_codec(frame, first_batch_compression, first_batch_producer_id, now);

        // acks=0 — silent durability loss.
        if acks == 0 {
            self.upsert(
                AntiPatternKind::Acks0,
                format!("conn={}", frame.connection_id),
                Severity::Warn,
                format!("acks=0 producer (conn {})", frame.connection_id),
                "ProduceRequest sent with acks=0 — broker crash between socket buffer and log write loses records silently.".into(),
                frame,
            );
        }

        // Txn tracking: this Produce arrived on a connection with an
        // in-flight transactional id but no preceding AddPartitionsToTxn.
        if transactional || self.in_open_txn(frame.connection_id) {
            let conn = frame.connection_id;
            let (open_txn, no_add_partitions, txn_id) = {
                let st = self.txn_state.entry(conn).or_default();
                st.produced_in_txn = true;
                (
                    st.transactional_id.is_some(),
                    !st.add_partitions,
                    st.transactional_id.clone(),
                )
            };
            if open_txn && no_add_partitions {
                self.upsert(
                    AntiPatternKind::TransactionalZombie,
                    format!("conn={conn}"),
                    Severity::Warn,
                    format!(
                        "Produce without AddPartitionsToTxn (conn {conn}, txn '{}')",
                        txn_id.as_deref().unwrap_or("?"),
                    ),
                    "ProduceRequest with transactional_id but no preceding AddPartitionsToTxn — orphans the txn in ProducerStateManager and blocks LastStableOffset.".into(),
                    frame,
                );
            }
        }
    }

    fn update_produce_shape(
        &mut self,
        frame: &ProtoFrame,
        record_count: u32,
        topics: &[String],
        now: Instant,
    ) {
        let (avg_records, rate, samples) = {
            let shape = self.produce_shape.entry(frame.connection_id).or_default();
            shape.samples = shape.samples.saturating_add(1);
            shape.total_records = shape.total_records.saturating_add(u64::from(record_count));
            shape.instants.push_back(now);
            while shape.instants.len() > RATE_QUEUE_CAP {
                shape.instants.pop_front();
            }
            while let Some(front) = shape.instants.front() {
                if now.duration_since(*front) > RATE_WINDOW {
                    shape.instants.pop_front();
                } else {
                    break;
                }
            }
            let avg_records = shape.total_records as f64 / f64::from(shape.samples);
            let span = match (shape.instants.front(), shape.instants.back()) {
                (Some(a), Some(b)) => b.duration_since(*a).as_secs_f64().max(1.0),
                _ => 1.0,
            };
            let rate = shape.instants.len() as f64 / span;
            (avg_records, rate, shape.samples)
        };
        if samples >= TINY_BATCH_MIN_SAMPLES
            && avg_records <= TINY_BATCH_RECORDS_PER_PRODUCE
            && rate >= TINY_BATCH_MIN_PRODUCE_RATE
        {
            let scope_topic = topics.first().cloned().unwrap_or_else(|| "—".into());
            self.upsert(
                AntiPatternKind::TinyBatches,
                format!("conn={}", frame.connection_id),
                Severity::Warn,
                format!("Tiny Produce batches (conn {})", frame.connection_id),
                format!(
                    "{avg_records:.2} records / Produce, {rate:.1} Produce/s — first topic '{scope_topic}'"
                ),
                frame,
            );
        }
    }

    fn update_produce_codec(
        &mut self,
        frame: &ProtoFrame,
        first_batch_compression: Option<u8>,
        first_batch_producer_id: Option<i64>,
        now: Instant,
    ) {
        let (samples, uncompressed, non_idempotent, rate) = {
            let stats = self.produce_codec.entry(frame.connection_id).or_default();
            stats.samples = stats.samples.saturating_add(1);
            if matches!(first_batch_compression, Some(0)) {
                stats.uncompressed = stats.uncompressed.saturating_add(1);
            }
            if matches!(first_batch_producer_id, Some(-1)) {
                stats.non_idempotent = stats.non_idempotent.saturating_add(1);
            }
            stats.instants.push_back(now);
            while stats.instants.len() > RATE_QUEUE_CAP {
                stats.instants.pop_front();
            }
            while let Some(front) = stats.instants.front() {
                if now.duration_since(*front) > RATE_WINDOW {
                    stats.instants.pop_front();
                } else {
                    break;
                }
            }
            let span = match (stats.instants.front(), stats.instants.back()) {
                (Some(a), Some(b)) => b.duration_since(*a).as_secs_f64().max(1.0),
                _ => 1.0,
            };
            let rate = stats.instants.len() as f64 / span;
            (
                stats.samples,
                stats.uncompressed,
                stats.non_idempotent,
                rate,
            )
        };
        if samples >= COMPRESSION_OFF_MIN_SAMPLES
            && uncompressed * 2 >= samples
            && rate >= COMPRESSION_OFF_MIN_RATE
        {
            self.upsert(
                AntiPatternKind::CompressionOff,
                format!("conn={}", frame.connection_id),
                Severity::Note,
                format!("Compression off (conn {})", frame.connection_id),
                format!(
                    "{uncompressed}/{samples} Produce batches with no compression at {rate:.1} req/s — bandwidth + broker disk left on the table.",
                ),
                frame,
            );
        }
        if samples >= NON_IDEMPOTENT_MIN_SAMPLES && non_idempotent * 2 >= samples {
            self.upsert(
                AntiPatternKind::NonIdempotentProducer,
                format!("conn={}", frame.connection_id),
                Severity::Note,
                format!("Non-idempotent producer (conn {})", frame.connection_id),
                format!(
                    "{non_idempotent}/{samples} Produce batches with producerId=-1 — retries can reorder + duplicate. Enable enable.idempotence=true.",
                ),
                frame,
            );
        }
    }

    fn in_open_txn(&self, conn: i32) -> bool {
        self.txn_state
            .get(&conn)
            .is_some_and(|st| st.transactional_id.is_some() && !st.ended)
    }

    pub(super) fn check_producer_per_record(&mut self, frame: &ProtoFrame) {
        let Some(c) = self.per_connection.get(&frame.connection_id) else {
            return;
        };
        if c.init_producer_id < PRODUCER_PER_RECORD_MIN_INITS {
            return;
        }
        let total = c.init_producer_id.saturating_add(c.produce_requests).max(1);
        let ratio = f64::from(c.init_producer_id) / f64::from(total);
        if ratio < PRODUCER_PER_RECORD_INIT_RATIO {
            return;
        }
        let detail = format!(
            "{} InitProducerId vs {} Produce (ratio {:.0}%) — a fresh producer per record",
            c.init_producer_id,
            c.produce_requests,
            ratio * 100.0
        );
        let scope = format!("conn={}", frame.connection_id);
        self.upsert(
            AntiPatternKind::ProducerPerRecord,
            scope,
            Severity::Warn,
            format!("Producer per record (conn {})", frame.connection_id),
            detail,
            frame,
        );
    }

    fn mark_handshake_init_producer_id(&mut self, frame: &ProtoFrame) {
        let st = self
            .in_flight_handshakes
            .entry(frame.connection_id)
            .or_default();
        st.saw_init_producer_id = true;
        if st.saw_api_versions && st.saw_metadata && !st.counted {
            st.counted = true;
            let now = Instant::now();
            let (samples, rate) = {
                let w = self.leak_handshakes.entry(frame.local_port).or_default();
                w.instants.push_back(now);
                while w.instants.len() > RATE_QUEUE_CAP {
                    w.instants.pop_front();
                }
                while let Some(front) = w.instants.front() {
                    if now.duration_since(*front) > RATE_WINDOW {
                        w.instants.pop_front();
                    } else {
                        break;
                    }
                }
                let span = match (w.instants.front(), w.instants.back()) {
                    (Some(a), Some(b)) => b.duration_since(*a).as_secs_f64().max(1.0),
                    _ => 1.0,
                };
                let rate = w.instants.len() as f64 / span;
                (w.instants.len(), rate)
            };
            if samples >= PRODUCER_INSTANCE_LEAK_MIN_SAMPLES
                && rate >= PRODUCER_INSTANCE_LEAK_PER_SEC
            {
                self.upsert(
                    AntiPatternKind::ProducerInstanceLeak,
                    format!("port={}", frame.local_port),
                    Severity::Warn,
                    format!("Producer-instance leak (port {})", frame.local_port),
                    format!(
                        "{samples} fresh producer handshakes (ApiVersions+Metadata+InitProducerId) in last {}s at {rate:.1}/s on this listener — the PagerDuty leak shape.",
                        RATE_WINDOW.as_secs(),
                    ),
                    frame,
                );
            }
        }
    }

    pub(super) fn on_produce_response(
        &mut self,
        frame: &ProtoFrame,
        errors: &[ProducePartitionError],
    ) {
        let now = Instant::now();
        for e in errors {
            // MessageTooLargeRejected (#19): broker rejected the
            // produce because it exceeded `message.max.bytes`.
            if e.error_code == kerr::MESSAGE_TOO_LARGE {
                self.upsert(
                    AntiPatternKind::MessageTooLargeRejected,
                    format!("{}:{}", e.topic, e.partition),
                    Severity::Warn,
                    format!("Message too large on {}:{}", e.topic, e.partition),
                    "ProduceResponse partition error_code=10 (MESSAGE_TOO_LARGE) — producer's max.request.size cleared but broker's message.max.bytes (or topic-level max.message.bytes) rejected it. Align both sides.".into(),
                    frame,
                );
                continue;
            }
            if kerr::is_auth_error(e.error_code) {
                self.fire_acl_deny(frame, "Produce", e.error_code, now);
                continue;
            }
            if !kerr::is_stale_leader_error(e.error_code) {
                continue;
            }
            let leader_hint = match e.current_leader_id {
                Some(id) => format!(" — current leader is broker {id}"),
                None => String::new(),
            };
            let err_name = kerr::name(e.error_code);
            self.upsert(
                AntiPatternKind::StaleLeaderProducing,
                format!("{}:{}", e.topic, e.partition),
                Severity::Warn,
                format!("Stale leader on {}:{}", e.topic, e.partition),
                format!("Produce returned {err_name}{leader_hint}"),
                frame,
            );
        }
    }

    // ---------- Consumer-side detectors ----------

    pub(super) fn on_offset_commit(&mut self, frame: &ProtoFrame, group_id: &str, now: Instant) {
        // Remember the group_id for this connection so the
        // CommitDuringRebalance detector (which only sees the
        // *response*) can attribute its scope to the right group.
        self.last_commit_group
            .insert(frame.connection_id, group_id.to_owned());
        // Overcommit rate detector.
        let (samples, rate) = {
            let w = self
                .commits_per_group
                .entry(group_id.to_owned())
                .or_default();
            w.push(now);
            w.trim(now);
            (w.len(), w.rate_per_sec())
        };
        if samples >= OVERCOMMIT_MIN_SAMPLES && rate >= OVERCOMMIT_RATE_PER_SEC {
            self.upsert(
                AntiPatternKind::Overcommit,
                format!("group={group_id}"),
                Severity::Warn,
                format!("Overcommit on '{group_id}'"),
                format!(
                    "{:.1} OffsetCommit/s sustained over the last {}s ({samples} commits in window)",
                    rate,
                    RATE_WINDOW.as_secs(),
                ),
                frame,
            );
        }
        // Auto-commit cadence detector — inter-arrival close to ~5s ± tolerance.
        let intervals = self
            .autocommit_intervals
            .entry(group_id.to_owned())
            .or_default();
        intervals.push(now);
        if intervals.len() > AUTOCOMMIT_MIN_SAMPLES * 2 {
            intervals.remove(0);
        }
        if intervals.len() >= AUTOCOMMIT_MIN_SAMPLES {
            let mut diffs_ms: Vec<f64> = intervals
                .windows(2)
                .map(|w| (w[1].duration_since(w[0])).as_secs_f64() * 1000.0)
                .collect();
            diffs_ms.retain(|d| *d > 0.0);
            if diffs_ms.len() >= AUTOCOMMIT_MIN_SAMPLES - 1 {
                let mean: f64 = diffs_ms.iter().copied().sum::<f64>() / diffs_ms.len() as f64;
                let max_dev = diffs_ms
                    .iter()
                    .map(|d| (d - mean).abs() / mean.max(1.0))
                    .fold(0.0_f64, f64::max);
                let near_5s = (mean - AUTOCOMMIT_INTERVAL_MS).abs() / AUTOCOMMIT_INTERVAL_MS
                    <= AUTOCOMMIT_INTERVAL_TOLERANCE;
                if near_5s && max_dev <= AUTOCOMMIT_INTERVAL_TOLERANCE {
                    self.upsert(
                        AntiPatternKind::AutoCommitCadence,
                        format!("group={group_id}"),
                        Severity::Note,
                        format!("Auto-commit on '{group_id}'"),
                        format!(
                            "{} OffsetCommits at {mean:.0}ms interval — enable.auto.commit=true. At-least-once with a ~{:.0}s duplicate window on crash.",
                            diffs_ms.len() + 1,
                            mean / 1000.0,
                        ),
                        frame,
                    );
                }
            }
        }
    }

    pub(super) fn on_join_group_request(
        &mut self,
        frame: &ProtoFrame,
        group_id: &str,
        protocols: &[String],
        now: Instant,
    ) {
        // Cooperative-sticky churn (#21): client uses cooperative-sticky
        // and fires JoinGroup repeatedly. KAFKA-12896. Distinct from
        // the generic rebalance loop in that it specifically flags
        // the incremental-rebalance loop variant.
        if protocols
            .iter()
            .any(|p| p.eq_ignore_ascii_case("cooperative-sticky"))
        {
            let count = self
                .cooperative_sticky_joins
                .entry(group_id.to_owned())
                .or_default()
                .push_and_count(now);
            if count >= COOPERATIVE_STICKY_CHURN_THRESHOLD {
                self.upsert(
                    AntiPatternKind::CooperativeStickyChurn,
                    format!("group={group_id}"),
                    Severity::Warn,
                    format!("Cooperative-sticky churn on '{group_id}'"),
                    format!(
                        "{count} JoinGroup with cooperative-sticky in the rolling window — KAFKA-12896 leader-retrigger loop or unstable assignor.",
                    ),
                    frame,
                );
            }
        }
        self.on_join_group(frame, group_id, now);
    }

    pub(super) fn on_join_group(&mut self, frame: &ProtoFrame, group_id: &str, now: Instant) {
        // Classic-protocol-on-modern-cluster detector — fires once if the
        // listener has seen ConsumerGroupHeartbeat advertised.
        if self.kip848_ports.contains(&frame.local_port) {
            self.upsert(
                AntiPatternKind::ClassicRebalanceOnModernCluster,
                format!("group={group_id}"),
                Severity::Note,
                format!("Classic rebalance on KIP-848 cluster — group '{group_id}'"),
                "Broker advertises ConsumerGroupHeartbeat (KIP-848) but client uses JoinGroup/SyncGroup. Stop-the-world rebalances chosen over incremental. Set group.protocol=consumer.".into(),
                frame,
            );
        }
        // Existing rebalance-loop detector.
        let samples = self
            .joins_per_group
            .entry(group_id.to_owned())
            .or_default()
            .push_and_count(now);
        if u32::try_from(samples).unwrap_or(u32::MAX) >= REBALANCE_JOINS_IN_WINDOW {
            self.upsert(
                AntiPatternKind::RebalanceLoop,
                format!("group={group_id}"),
                Severity::Warn,
                format!("Rebalance loop on '{group_id}'"),
                format!(
                    "{samples} JoinGroup in last {}s — heartbeat / session timeout likely misconfigured",
                    RATE_WINDOW.as_secs()
                ),
                frame,
            );
        }
    }

    #[allow(clippy::unused_self)]
    pub(super) const fn on_fetch_request(
        &self,
        _frame: &ProtoFrame,
        _min_bytes: i32,
        _session_epoch: i32,
    ) {
        // No-op for now. Kept as an explicit dispatch target so we can
        // add `min_bytes`-based heuristics later without changing the
        // dispatch table.
    }

    #[allow(clippy::too_many_lines)]
    pub(super) fn on_fetch_response(
        &mut self,
        frame: &ProtoFrame,
        error_code: i16,
        response_size: u64,
        errors: &[TopicPartitionError],
        now: Instant,
    ) {
        // Per-partition errors → OffsetOutOfRange (1),
        // UnknownTopicPollLoop (3), AclDeny (29/30/31).
        for e in errors {
            match e.error_code {
                kerr::OFFSET_OUT_OF_RANGE => {
                    // Rate-threshold to avoid flagging benign single-seek
                    // cases (auto.offset.reset triggers one OFFSET_OUT_OF_RANGE
                    // on a healthy consumer). The bug is repeated polling
                    // at an out-of-range offset.
                    let key = (e.topic.clone(), e.partition);
                    let count = self
                        .oor_per_partition
                        .entry(key)
                        .or_default()
                        .push_and_count(now);
                    if count >= OFFSET_OUT_OF_RANGE_THRESHOLD {
                        self.upsert(
                            AntiPatternKind::OffsetOutOfRangeOnFetch,
                            format!("{}:{}", e.topic, e.partition),
                            Severity::Warn,
                            format!("Offset out of range on {}:{}", e.topic, e.partition),
                            format!(
                                "{count} FetchResponse partition errors OFFSET_OUT_OF_RANGE in the rolling window — consumer position past the broker's log end.",
                            ),
                            frame,
                        );
                    }
                }
                kerr::UNKNOWN_TOPIC_OR_PARTITION => {
                    let key = (frame.connection_id, e.topic.clone(), e.partition);
                    let count = self
                        .utop_per_partition
                        .entry(key)
                        .or_default()
                        .push_and_count(now);
                    if count >= UNKNOWN_TOPIC_POLL_THRESHOLD {
                        self.upsert(
                            AntiPatternKind::UnknownTopicPollLoop,
                            format!("{}:{}", e.topic, e.partition),
                            Severity::Warn,
                            format!("Unknown-topic poll loop on {}:{}", e.topic, e.partition),
                            format!(
                                "{count} FetchResponse partition errors UNKNOWN_TOPIC_OR_PARTITION on the same partition. Consumer pointed at a non-existent or pending topic.",
                            ),
                            frame,
                        );
                    }
                }
                c if kerr::is_auth_error(c) => self.fire_acl_deny(frame, "Fetch", c, now),
                _ => {}
            }
        }

        // Fetch-session error cascade.
        if kerr::is_fetch_session_error(error_code) {
            let count = self
                .fetch_session_errors
                .entry(frame.connection_id)
                .or_default()
                .push_and_count(now);
            if count >= FETCH_SESSION_ERRORS_THRESHOLD {
                self.upsert(
                    AntiPatternKind::FetchSessionErrorCascade,
                    format!("conn={}", frame.connection_id),
                    Severity::Warn,
                    format!(
                        "Fetch-session reset cascade (conn {})",
                        frame.connection_id
                    ),
                    format!(
                        "{count} INVALID_FETCH_SESSION_* errors in the rolling window — client repeatedly forced into full fetches.",
                    ),
                    frame,
                );
            }
        }
        // Tight-fetch polling shape.
        let (samples, avg_size, rate) = {
            let shape = self.fetch_shape.entry(frame.connection_id).or_default();
            shape.samples = shape.samples.saturating_add(1);
            shape.total_response_bytes = shape.total_response_bytes.saturating_add(response_size);
            shape.instants.push_back(now);
            while shape.instants.len() > RATE_QUEUE_CAP {
                shape.instants.pop_front();
            }
            while let Some(front) = shape.instants.front() {
                if now.duration_since(*front) > RATE_WINDOW {
                    shape.instants.pop_front();
                } else {
                    break;
                }
            }
            let avg_size = shape.total_response_bytes as f64 / f64::from(shape.samples.max(1));
            let span = match (shape.instants.front(), shape.instants.back()) {
                (Some(a), Some(b)) => b.duration_since(*a).as_secs_f64().max(1.0),
                _ => 1.0,
            };
            let rate = shape.instants.len() as f64 / span;
            (shape.samples, avg_size, rate)
        };
        if samples >= TIGHT_FETCH_MIN_SAMPLES
            && avg_size <= TIGHT_FETCH_AVG_RESPONSE_BYTES
            && rate >= TIGHT_FETCH_MIN_RATE
        {
            self.upsert(
                AntiPatternKind::TightFetchPolling,
                format!("conn={}", frame.connection_id),
                Severity::Note,
                format!("Tight fetch polling (conn {})", frame.connection_id),
                format!(
                    "{rate:.1} Fetch/s averaging {avg_size:.0} response bytes over {samples} samples — raise fetch.min.bytes / fetch.max.wait.ms.",
                ),
                frame,
            );
        }
    }

    // ---------- Cluster-wide detectors ----------

    pub(super) fn on_api_versions_response(
        &mut self,
        frame: &ProtoFrame,
        max_versions: &[(i16, i16)],
    ) {
        // Mark the first leg of the leak-handshake triplet — every
        // fresh connection always sends ApiVersions before anything
        // else, so the response landing on this `connection_id` is the
        // anchor.
        {
            let st = self
                .in_flight_handshakes
                .entry(frame.connection_id)
                .or_default();
            st.saw_api_versions = true;
        }
        let port = frame.local_port;
        let mut mixed_keys: Vec<(i16, i16, i16, u16, u16)> = Vec::new();
        for (api_key, max_v) in max_versions {
            // KIP-848 readiness: presence of ConsumerGroupHeartbeat.
            if *api_key == API_KEY_CONSUMER_GROUP_HEARTBEAT && *max_v >= 0 {
                self.kip848_ports.insert(port);
            }
            let mut conflict: Option<(u16, i16)> = None;
            for ((other_port, other_key), other_v) in &self.api_versions {
                if *other_key == *api_key && *other_port != port && *other_v != *max_v {
                    conflict = Some((*other_port, *other_v));
                    break;
                }
            }
            if let Some((other_port, other_v)) = conflict {
                mixed_keys.push((*api_key, *max_v, other_v, port, other_port));
            }
            self.api_versions.insert((port, *api_key), *max_v);
        }
        for (api_key, this_v, other_v, this_port, other_port) in mixed_keys {
            let scope = format!("ports={this_port}|{other_port}");
            self.upsert(
                AntiPatternKind::MixedApiVersion,
                scope,
                Severity::Warn,
                format!(
                    "Mixed api_version (key {api_key}) across ports {this_port} / {other_port}"
                ),
                format!(
                    "max_version diverges: port {this_port} → {this_v}, port {other_port} → {other_v}",
                ),
                frame,
            );
        }
    }

    pub(super) fn on_metadata_request(&mut self, frame: &ProtoFrame, now: Instant) {
        // ApiVersions+Metadata+InitProducerId triplet tracking.
        {
            let st = self
                .in_flight_handshakes
                .entry(frame.connection_id)
                .or_default();
            st.saw_metadata = true;
        }
        // Metadata storm rate.
        let (samples, rate) = {
            let w = self
                .metadata_requests
                .entry(frame.connection_id)
                .or_default();
            w.push(now);
            w.trim(now);
            (w.len(), w.rate_per_sec())
        };
        if samples >= METADATA_STORM_MIN_SAMPLES && rate >= METADATA_STORM_RATE_PER_SEC {
            self.upsert(
                AntiPatternKind::MetadataStorm,
                format!("conn={}", frame.connection_id),
                Severity::Warn,
                format!("Metadata storm (conn {})", frame.connection_id),
                format!(
                    "{rate:.2} MetadataRequest/s sustained ({samples} in the window) — healthy clients refresh every metadata.max.age.ms (~5min). Likely broken refresh or topic-not-found loop.",
                ),
                frame,
            );
        }
    }

    pub(super) fn on_metadata_response(&mut self, frame: &ProtoFrame) {
        // Part of the ApiVersions+Metadata+InitProducerId triplet — we
        // also mark the response side so a triplet whose request frame
        // didn't decode (e.g. truncated payload) still tracks.
        let st = self
            .in_flight_handshakes
            .entry(frame.connection_id)
            .or_default();
        st.saw_metadata = true;
    }

    pub(super) fn on_sasl_authenticate_response(
        &mut self,
        frame: &ProtoFrame,
        error_code: i16,
        error_message: Option<&str>,
        session_lifetime_ms: i64,
    ) {
        let (count, prev) = {
            let st = self.sasl_state.entry(frame.connection_id).or_default();
            st.count = st.count.saturating_add(1);
            let prev = st.last_lifetime_ms;
            st.last_lifetime_ms = session_lifetime_ms;
            (st.count, prev)
        };
        if error_code != 0 {
            let msg = error_message.unwrap_or("(no message)");
            self.upsert(
                AntiPatternKind::SaslSessionTooShort,
                format!("conn={}", frame.connection_id),
                Severity::Warn,
                format!("SASL auth failed (conn {})", frame.connection_id),
                format!("error_code={error_code} message='{msg}'"),
                frame,
            );
        } else if count >= 2
            && session_lifetime_ms > 0
            && session_lifetime_ms < SASL_SHORT_SESSION_MS
        {
            self.upsert(
                AntiPatternKind::SaslSessionTooShort,
                format!("conn={}", frame.connection_id),
                Severity::Warn,
                format!(
                    "SASL re-auth lifetime collapsed (conn {})",
                    frame.connection_id
                ),
                format!(
                    "session_lifetime_ms dropped from {prev} to {session_lifetime_ms} on re-auth #{count}",
                ),
                frame,
            );
        }
    }

    pub(super) fn on_offset_commit_response(
        &mut self,
        frame: &ProtoFrame,
        errors: &[TopicPartitionError],
        now: Instant,
    ) {
        // Scope: include the group_id we captured from the matching
        // request — different groups committing to the same partition
        // should NOT collide on the same detection row.
        let group_id = self
            .last_commit_group
            .get(&frame.connection_id)
            .cloned()
            .unwrap_or_else(|| "?".into());
        for e in errors {
            match e.error_code {
                kerr::REBALANCE_IN_PROGRESS => self.upsert(
                    AntiPatternKind::CommitDuringRebalance,
                    format!("group={group_id}|{}:{}", e.topic, e.partition),
                    Severity::Warn,
                    format!(
                        "Commit during rebalance — group '{group_id}' on {}:{}",
                        e.topic, e.partition
                    ),
                    "OffsetCommitResponse partition error_code=27 (REBALANCE_IN_PROGRESS) — commit dropped. Consumer can re-process the records after the rebalance settles (at-least-once duplicate window).".into(),
                    frame,
                ),
                c if kerr::is_auth_error(c) => self.fire_acl_deny(frame, "OffsetCommit", c, now),
                _ => {}
            }
        }
    }

    pub(super) fn on_find_coordinator_request(
        &mut self,
        frame: &ProtoFrame,
        keys: &[String],
        now: Instant,
    ) {
        for key in keys {
            let count = self
                .coordinator_requests
                .entry(key.clone())
                .or_default()
                .push_and_count(now);
            if count >= COORDINATOR_CHURN_THRESHOLD {
                self.upsert(
                    AntiPatternKind::CoordinatorChurn,
                    format!("key={key}"),
                    Severity::Warn,
                    format!("Coordinator churn for '{key}'"),
                    format!(
                        "{count} FindCoordinatorRequest for the same key in the rolling window — coordinator unstable, broker GC pause, or client churning connections.",
                    ),
                    frame,
                );
            }
        }
    }

    /// Catch-all auth-error funnel for response variants that only
    /// carry a top-level `error_code`. ACL denies (29/30/31) flow
    /// through `fire_acl_deny`; other codes are ignored here.
    pub(super) fn on_auth_error_response(
        &mut self,
        frame: &ProtoFrame,
        api_name: &str,
        error_code: i16,
        now: Instant,
    ) {
        if kerr::is_auth_error(error_code) {
            self.fire_acl_deny(frame, api_name, error_code, now);
        }
    }

    fn fire_acl_deny(&mut self, frame: &ProtoFrame, api_name: &str, error_code: i16, now: Instant) {
        let count = self
            .acl_deny_window
            .entry(frame.connection_id)
            .or_default()
            .push_and_count(now);
        if count >= ACL_DENY_THRESHOLD {
            let name = kerr::name(error_code);
            self.upsert(
                AntiPatternKind::AclDeny,
                format!("conn={}", frame.connection_id),
                Severity::Warn,
                format!("ACL deny (conn {})", frame.connection_id),
                format!(
                    "{count} {name} errors on this connection (latest from {api_name}). Principal lacks the required ACL — fix grants or stop retrying blindly.",
                ),
                frame,
            );
        }
    }

    /// Throttle pressure: scan any response carrying `throttle_time_ms`
    /// for a non-zero value. Fires immediately — quota throttling is a
    /// strong wire signal that doesn't need a rolling window.
    pub(super) fn check_throttle(&mut self, frame: &ProtoFrame, s: &FrameSummary) {
        let (api_name, throttle_ms) = match s {
            FrameSummary::ProduceResponse {
                throttle_time_ms, ..
            } => ("Produce", *throttle_time_ms),
            FrameSummary::MetadataResponse {
                throttle_time_ms, ..
            } => ("Metadata", *throttle_time_ms),
            FrameSummary::FetchResponse {
                throttle_time_ms, ..
            } => ("Fetch", *throttle_time_ms),
            FrameSummary::OffsetCommitResponse {
                throttle_time_ms, ..
            } => ("OffsetCommit", *throttle_time_ms),
            FrameSummary::FindCoordinatorResponse {
                throttle_time_ms, ..
            } => ("FindCoordinator", *throttle_time_ms),
            FrameSummary::JoinGroupResponse {
                throttle_time_ms, ..
            } => ("JoinGroup", *throttle_time_ms),
            FrameSummary::SyncGroupResponse {
                throttle_time_ms, ..
            } => ("SyncGroup", *throttle_time_ms),
            FrameSummary::HeartbeatResponse {
                throttle_time_ms, ..
            } => ("Heartbeat", *throttle_time_ms),
            FrameSummary::LeaveGroupResponse {
                throttle_time_ms, ..
            } => ("LeaveGroup", *throttle_time_ms),
            _ => return,
        };
        if throttle_ms <= 0 {
            return;
        }
        // ApiVersions request *response* tracking: also note the handshake.
        self.upsert(
            AntiPatternKind::ThrottlePressure,
            format!("conn={}|api={api_name}", frame.connection_id),
            Severity::Warn,
            format!("Throttled on {api_name} (conn {})", frame.connection_id),
            format!(
                "{api_name}Response throttle_time_ms={throttle_ms} — broker is delaying this client (KIP-219). Likely byte-rate / request-time quota exceeded.",
            ),
            frame,
        );
    }
}

impl AntiPatternsFold {
    /// Called from the ApiVersions *request* path — only the request
    /// (not the response) signals the handshake on the wire. Marks the
    /// first leg of the leak-handshake triplet.
    #[allow(dead_code)]
    pub(super) fn mark_handshake_api_versions(&mut self, frame: &ProtoFrame) {
        let st = self
            .in_flight_handshakes
            .entry(frame.connection_id)
            .or_default();
        st.saw_api_versions = true;
    }
}
