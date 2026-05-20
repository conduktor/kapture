#![allow(clippy::doc_markdown)]
//! `AntiPatternsFold` — the incremental detector. One per
//! `ProtoCorrelator`. `absorb` dispatches each typed `FrameSummary`
//! variant to the relevant detector method (in `detectors.rs`).

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::anti_patterns::state::{
    severity_rank, ConnectionCounters, DetectionKey, DetectionState, FetchShape, HandshakeState,
    LeakWindow, ProduceCodecStats, ProduceShape, RollingWindow, SaslState, TxnState,
    CONNECTION_IDLE_EXPIRY, GC_SWEEP_EVERY,
};
use crate::anti_patterns::{AntiPatternKind, AntiPatternsSnapshot, Detection, Severity};
use crate::correlator::ProtoFrame;
use crate::proto_summary::FrameSummary;

/// Incremental detector fold. One per `ProtoCorrelator`.
#[derive(Debug, Default)]
pub struct AntiPatternsFold {
    pub(super) detections: HashMap<DetectionKey, DetectionState>,
    /// Per-group commit timestamps in the rolling window.
    pub(super) commits_per_group: HashMap<String, RollingWindow>,
    /// Per-group join timestamps.
    pub(super) joins_per_group: HashMap<String, RollingWindow>,
    /// Per-connection `InitProducerId` + Produce counters
    /// (producer-per-record detector).
    pub(super) per_connection: HashMap<i32, ConnectionCounters>,
    /// Per-connection `ProduceRequest` shape (tiny-batches detector).
    pub(super) produce_shape: HashMap<i32, ProduceShape>,
    /// Per-`(local_port, api_key)` last-seen `max_version`.
    pub(super) api_versions: HashMap<(u16, i16), i16>,
    /// Per-connection SASL re-auth state.
    pub(super) sasl_state: HashMap<i32, SaslState>,
    /// Per-connection Produce codec + idempotence stats.
    pub(super) produce_codec: HashMap<i32, ProduceCodecStats>,
    /// Per-`local_port` rolling-window of leak-handshake completions.
    pub(super) leak_handshakes: HashMap<u16, LeakWindow>,
    /// Per-connection partial handshake tracking.
    pub(super) in_flight_handshakes: HashMap<i32, HandshakeState>,
    /// Per-connection in-flight transactional state.
    pub(super) txn_state: HashMap<i32, TxnState>,
    /// Per-group OffsetCommit inter-arrival timestamps.
    pub(super) autocommit_intervals: HashMap<String, Vec<Instant>>,
    /// Per-connection fetch response shape + rate.
    pub(super) fetch_shape: HashMap<i32, FetchShape>,
    /// Per-connection fetch-session error window.
    pub(super) fetch_session_errors: HashMap<i32, RollingWindow>,
    /// Per-connection MetadataRequest timestamps.
    pub(super) metadata_requests: HashMap<i32, RollingWindow>,
    /// Ports where the broker advertised KIP-848 (api_key=68).
    pub(super) kip848_ports: HashSet<u16>,
    /// Per-group `FindCoordinator` request timestamps (coordinator
    /// churn).
    pub(super) coordinator_requests: HashMap<String, RollingWindow>,
    /// Per-(connection, topic, partition) timestamps of
    /// `UNKNOWN_TOPIC_OR_PARTITION` fetch errors (UTOP poll loop).
    pub(super) utop_per_partition: HashMap<(i32, String, i32), RollingWindow>,
    /// Per-scope (principal stand-in: `conn=N`) ACL deny timestamps.
    pub(super) acl_deny_window: HashMap<i32, RollingWindow>,
    /// Per-group `JoinGroup` events that advertised `cooperative-sticky`.
    pub(super) cooperative_sticky_joins: HashMap<String, RollingWindow>,
    /// Per-(topic, partition) timestamps of `OFFSET_OUT_OF_RANGE` fetch
    /// errors. Rate-thresholded to avoid flagging benign single-seek.
    pub(super) oor_per_partition: HashMap<(String, i32), RollingWindow>,
    /// Per-connection last seen `OffsetCommitRequest.group_id`. Lets
    /// the commit-during-rebalance detector attribute its scope to a
    /// group (the response itself doesn't carry the id).
    pub(super) last_commit_group: HashMap<i32, String>,
    /// Per-connection last-touched timestamp. Drives GC of stale
    /// per-connection maps so memory doesn't grow unboundedly with
    /// short-lived TCP connections — the exact shape of the
    /// producer-instance leak this crate is meant to detect.
    pub(super) last_seen: HashMap<i32, Instant>,
    /// Frame counter for the GC sweep cadence.
    pub(super) frames_absorbed: u64,
}

impl AntiPatternsFold {
    /// Absorb one frame. Mirrors `SessionFold::absorb`. Side-effect:
    /// may upsert one or more detections.
    pub fn absorb(&mut self, frame: &ProtoFrame, summary: Option<&FrameSummary>) {
        let Some(s) = summary else { return };
        let now = Instant::now();
        // Touch the per-connection last-seen so the GC knows this
        // connection is still active. Done before dispatch so even
        // frames that fall through the dispatch (variants we ignore)
        // keep the connection alive.
        self.last_seen.insert(frame.connection_id, now);
        self.frames_absorbed = self.frames_absorbed.wrapping_add(1);
        if self.frames_absorbed % GC_SWEEP_EVERY == 0 {
            self.gc_idle_connections(now);
        }
        self.dispatch(frame, s, now);
    }

    /// Drop per-connection state for connections that haven't been
    /// touched in `CONNECTION_IDLE_EXPIRY`. Caps memory so the
    /// producer-instance-leak shape can't make Kapture itself leak.
    pub(super) fn gc_idle_connections(&mut self, now: Instant) {
        let mut stale: Vec<i32> = Vec::new();
        for (conn, last) in &self.last_seen {
            if now.duration_since(*last) > CONNECTION_IDLE_EXPIRY {
                stale.push(*conn);
            }
        }
        for conn in &stale {
            self.last_seen.remove(conn);
            self.per_connection.remove(conn);
            self.produce_codec.remove(conn);
            self.produce_shape.remove(conn);
            self.in_flight_handshakes.remove(conn);
            self.txn_state.remove(conn);
            self.sasl_state.remove(conn);
            self.fetch_shape.remove(conn);
            self.fetch_session_errors.remove(conn);
            self.metadata_requests.remove(conn);
            self.acl_deny_window.remove(conn);
            self.last_commit_group.remove(conn);
        }
        // utop_per_partition is keyed on (conn, topic, partition) —
        // drop any entry whose conn just expired.
        if !stale.is_empty() {
            let drop_set: std::collections::HashSet<i32> = stale.into_iter().collect();
            self.utop_per_partition
                .retain(|(c, _, _), _| !drop_set.contains(c));
        }
    }

    fn dispatch(&mut self, frame: &ProtoFrame, s: &FrameSummary, now: Instant) {
        // Throttle pressure detector runs on every response that carries
        // the field — kept out of the per-variant match for brevity.
        self.check_throttle(frame, s);
        // Each `dispatch_*` only matches the variants in its category
        // and returns silently otherwise. Splitting this way keeps
        // each function under the clippy::too_many_lines threshold and
        // makes the per-category surface obvious.
        self.dispatch_producer(frame, s, now);
        self.dispatch_consumer(frame, s, now);
        self.dispatch_cluster(frame, s, now);
        self.dispatch_sasl(frame, s);
    }

    fn dispatch_producer(&mut self, frame: &ProtoFrame, s: &FrameSummary, now: Instant) {
        match s {
            FrameSummary::InitProducerIdRequest {
                transactional_id, ..
            } => {
                self.on_init_producer_id(frame, transactional_id.as_deref());
            }
            FrameSummary::AddPartitionsToTxnRequest { transactional_id } => {
                self.on_add_partitions_to_txn(frame, transactional_id);
            }
            FrameSummary::EndTxnRequest {
                transactional_id,
                committed,
            } => {
                self.on_end_txn(frame, transactional_id, *committed);
            }
            FrameSummary::ProduceRequest {
                record_count,
                topics,
                acks,
                first_batch_compression,
                first_batch_producer_id,
                transactional,
                ..
            } => {
                self.on_produce_request(
                    frame,
                    *record_count,
                    topics,
                    *acks,
                    *first_batch_compression,
                    *first_batch_producer_id,
                    *transactional,
                    now,
                );
            }
            FrameSummary::ProduceResponse { errors, .. } => {
                self.on_produce_response(frame, errors);
            }
            _ => {}
        }
    }

    fn dispatch_consumer(&mut self, frame: &ProtoFrame, s: &FrameSummary, now: Instant) {
        match s {
            FrameSummary::OffsetCommitRequest { group_id, .. } => {
                self.on_offset_commit(frame, group_id, now);
            }
            FrameSummary::JoinGroupRequest {
                group_id,
                protocols,
                ..
            } => {
                self.on_join_group_request(frame, group_id, protocols, now);
            }
            FrameSummary::OffsetCommitResponse { errors, .. } => {
                self.on_offset_commit_response(frame, errors, now);
            }
            FrameSummary::FindCoordinatorRequest { keys } => {
                self.on_find_coordinator_request(frame, keys, now);
            }
            FrameSummary::FindCoordinatorResponse { error_code, .. } => {
                self.on_auth_error_response(frame, "FindCoordinator", *error_code, now);
            }
            FrameSummary::JoinGroupResponse { error_code, .. } => {
                self.on_auth_error_response(frame, "JoinGroup", *error_code, now);
            }
            FrameSummary::SyncGroupResponse { error_code, .. } => {
                self.on_auth_error_response(frame, "SyncGroup", *error_code, now);
            }
            FrameSummary::HeartbeatResponse { error_code, .. } => {
                self.on_auth_error_response(frame, "Heartbeat", *error_code, now);
            }
            FrameSummary::LeaveGroupResponse { error_code, .. } => {
                self.on_auth_error_response(frame, "LeaveGroup", *error_code, now);
            }
            FrameSummary::FetchRequest {
                min_bytes,
                session_epoch,
                ..
            } => {
                self.on_fetch_request(frame, *min_bytes, *session_epoch);
            }
            FrameSummary::FetchResponse {
                error_code,
                response_size,
                errors,
                ..
            } => {
                self.on_fetch_response(frame, *error_code, *response_size, errors, now);
            }
            _ => {}
        }
    }

    fn dispatch_cluster(&mut self, frame: &ProtoFrame, s: &FrameSummary, now: Instant) {
        match s {
            FrameSummary::ApiVersionsResponse { max_versions, .. } => {
                self.on_api_versions_response(frame, max_versions);
            }
            FrameSummary::MetadataRequest { .. } => {
                self.on_metadata_request(frame, now);
            }
            FrameSummary::MetadataResponse { .. } => {
                self.on_metadata_response(frame);
            }
            _ => {}
        }
    }

    fn dispatch_sasl(&mut self, frame: &ProtoFrame, s: &FrameSummary) {
        if let FrameSummary::SaslAuthenticateResponse {
            error_code,
            error_message,
            session_lifetime_ms,
        } = s
        {
            self.on_sasl_authenticate_response(
                frame,
                *error_code,
                error_message.as_deref(),
                *session_lifetime_ms,
            );
        }
    }

    pub(super) fn upsert(
        &mut self,
        kind: AntiPatternKind,
        scope: String,
        severity: Severity,
        title: String,
        detail: String,
        frame: &ProtoFrame,
    ) {
        let key = DetectionKey { kind, scope };
        match self.detections.get_mut(&key) {
            Some(state) => {
                state.severity = severity;
                state.title = title;
                state.detail = detail;
                state.last_seen.clone_from(&frame.timestamp);
                state.occurrences = state.occurrences.saturating_add(1);
                state.frame_id = Some(frame.id.clone());
            }
            None => {
                self.detections.insert(
                    key,
                    DetectionState {
                        severity,
                        title,
                        detail,
                        first_seen: frame.timestamp.clone(),
                        last_seen: frame.timestamp.clone(),
                        occurrences: 1,
                        frame_id: Some(frame.id.clone()),
                    },
                );
            }
        }
    }

    /// Cheap snapshot for the Tauri command — clones state into the
    /// public `Detection` shape, sorted by severity then most-recent.
    #[must_use]
    pub fn snapshot(&self) -> AntiPatternsSnapshot {
        let mut detections: Vec<Detection> = self
            .detections
            .iter()
            .map(|(k, v)| Detection {
                kind: k.kind,
                severity: v.severity,
                title: v.title.clone(),
                detail: v.detail.clone(),
                scope: k.scope.clone(),
                first_seen: v.first_seen.clone(),
                last_seen: v.last_seen.clone(),
                occurrences: v.occurrences,
                frame_id: v.frame_id.clone(),
            })
            .collect();
        detections.sort_by(|a, b| {
            let sev = severity_rank(a.severity).cmp(&severity_rank(b.severity));
            if sev != std::cmp::Ordering::Equal {
                return sev;
            }
            b.last_seen.cmp(&a.last_seen)
        });
        AntiPatternsSnapshot { detections }
    }

    pub fn clear(&mut self) {
        self.detections.clear();
        self.commits_per_group.clear();
        self.joins_per_group.clear();
        self.per_connection.clear();
        self.produce_shape.clear();
        self.api_versions.clear();
        self.sasl_state.clear();
        self.produce_codec.clear();
        self.leak_handshakes.clear();
        self.in_flight_handshakes.clear();
        self.txn_state.clear();
        self.autocommit_intervals.clear();
        self.fetch_shape.clear();
        self.fetch_session_errors.clear();
        self.metadata_requests.clear();
        self.kip848_ports.clear();
        self.coordinator_requests.clear();
        self.utop_per_partition.clear();
        self.acl_deny_window.clear();
        self.cooperative_sticky_joins.clear();
        self.oor_per_partition.clear();
        self.last_commit_group.clear();
        self.last_seen.clear();
        self.frames_absorbed = 0;
    }
}
