//! Detection of the 7 client-side anti-patterns called out in the
//! Kapture README. Each detector folds incrementally per captured
//! frame and exposes a stable `Detection` row to the Expert tab.
//!
//! The 7 patterns:
//!  1. `Overcommit` — `OffsetCommit` after every single record.
//!  2. `ProducerPerRecord` — fresh producer (full `ApiVersions` +
//!     `Metadata` + `InitProducerId` handshake) per record.
//!  3. `TinyBatches` — `linger.ms=0` + tiny `batch.size` (high
//!     Produce rate × ~1 record per batch).
//!  4. `RebalanceLoop` — consumer group re-joining every few seconds.
//!  5. `StaleLeaderProducing` — `Produce` targeting a partition whose
//!     leader the broker just told us moved (`NOT_LEADER_OR_FOLLOWER` /
//!     `FENCED_LEADER_EPOCH`).
//!  6. `MixedApiVersion` — different brokers advertising different
//!     `max_version` for the same api key, while a rolling upgrade is
//!     in progress.
//!  7. `SaslSessionTooShort` — re-auth that dies on a clock-like
//!     cadence (non-zero `error_code` or `session_lifetime_ms` collapse).
//!
//! Each detector keeps a tiny rolling window of timestamps so we can
//! compute rates without unbounded growth. Snapshot is a cheap clone
//! of the current detections; the GUI polls it from the Expert tab.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use schemars::JsonSchema;
use serde::Serialize;

use crate::correlator::ProtoFrame;
use crate::proto_summary::FrameSummary;

/// Sliding-window length for rate-based detectors. 60s strikes a
/// balance between "react in time for a live dev session" and "don't
/// alert on a 5-second blip".
const RATE_WINDOW: Duration = Duration::from_secs(60);
/// Cap on per-window timestamp queues to bound memory under storms.
const RATE_QUEUE_CAP: usize = 10_000;

/// Overcommit: ≥ this many `OffsetCommit` per second sustained over the
/// rolling window. 5/s is well above any reasonable healthy app — the
/// canonical bug rolls 1 commit per record consumed at hundreds of
/// records/s.
const OVERCOMMIT_RATE_PER_SEC: f64 = 5.0;
/// Need at least this many samples in the window before firing — keeps
/// us from flagging the very first burst of frames on a fresh capture.
const OVERCOMMIT_MIN_SAMPLES: usize = 20;

/// Producer-per-record: `InitProducerId` count vs `Produce` count
/// ratio. A healthy producer issues *one* `InitProducerId` and then
/// many `Produce`. The anti-pattern flips that.
const PRODUCER_PER_RECORD_INIT_RATIO: f64 = 0.5;
const PRODUCER_PER_RECORD_MIN_INITS: u32 = 5;

/// Tiny batches: `records/Produce` close to 1, while Produce rate is
/// high. The KIP-1030 motivation is "lots of tiny batches at a high
/// rate"; <= 2 records per batch with >= 10 Produce/s is the canonical
/// shape.
const TINY_BATCH_RECORDS_PER_PRODUCE: f64 = 2.0;
const TINY_BATCH_MIN_PRODUCE_RATE: f64 = 10.0;
const TINY_BATCH_MIN_SAMPLES: u32 = 20;

/// Rebalance loop: a healthy consumer group re-joins on membership
/// change. Five `JoinGroups` inside a minute is the threshold — that's
/// well past "occasional reshuffle".
const REBALANCE_JOINS_IN_WINDOW: u32 = 5;

/// SASL: a session lifetime below this on a *re-auth* (second or
/// later) is the "Session too short" symptom from `aws-msk-iam-auth#176`.
/// 30s is shorter than any realistic IAM session.
const SASL_SHORT_SESSION_MS: i64 = 30_000;

/// Severity. The frontend renders icons + tone from this.
#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Warn,
    Note,
}

/// The kind of anti-pattern detected. The frontend keys row rendering
/// off this, the detector dedupe key uses (kind, scope).
#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum AntiPatternKind {
    Overcommit,
    ProducerPerRecord,
    TinyBatches,
    RebalanceLoop,
    StaleLeaderProducing,
    MixedApiVersion,
    SaslSessionTooShort,
}

impl AntiPatternKind {
    #[must_use]
    pub const fn title(self) -> &'static str {
        match self {
            Self::Overcommit => "Overcommit",
            Self::ProducerPerRecord => "Producer per record",
            Self::TinyBatches => "Tiny Produce batches",
            Self::RebalanceLoop => "Rebalance loop",
            Self::StaleLeaderProducing => "Stale-leader producing",
            Self::MixedApiVersion => "Mixed api_version across brokers",
            Self::SaslSessionTooShort => "SASL session too short on re-auth",
        }
    }
}

/// One row in the Expert tab.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Detection {
    pub kind: AntiPatternKind,
    pub severity: Severity,
    /// Human-readable header like "Overcommit" or
    /// "Producer per record (conn 4)".
    pub title: String,
    /// One-sentence detail line: rates, counts, partition, etc.
    pub detail: String,
    /// Scope label — group id, connection id, broker pair, … — feeds
    /// the table's "Where" column. Empty when the pattern is
    /// cluster-wide.
    pub scope: String,
    /// RFC 3339 timestamp of the first frame that triggered the
    /// pattern. Lets the UI sort by recency.
    pub first_seen: String,
    /// RFC 3339 timestamp of the most recent frame.
    pub last_seen: String,
    /// Count of contributing frames seen so far.
    pub occurrences: u32,
    /// Latest frame id involved. Lets the UI's "jump to frame" button
    /// land on a row that demonstrates the pattern.
    pub frame_id: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, JsonSchema)]
pub struct AntiPatternsSnapshot {
    pub detections: Vec<Detection>,
}

/// Incremental detector fold. One per `ProtoCorrelator`.
#[derive(Debug, Default)]
pub struct AntiPatternsFold {
    /// Detections keyed by `(kind, scope)` so successive contributing
    /// frames update the same row instead of fanning out.
    detections: HashMap<DetectionKey, DetectionState>,
    /// Per-group commit timestamps in the rolling window.
    commits_per_group: HashMap<String, RollingWindow>,
    /// Per-group join timestamps.
    joins_per_group: HashMap<String, RollingWindow>,
    /// Per-connection `InitProducerId` count + Produce count, used by
    /// the producer-per-record detector. We don't time-window these
    /// because the ratio over a session is the signal.
    per_connection: HashMap<i32, ConnectionCounters>,
    /// Per-connection `ProduceRequest` batch-shape rolling samples for
    /// the tiny-batch detector.
    produce_shape: HashMap<i32, ProduceShape>,
    /// Per-`(local_port, api_key)` last-seen `max_version` from
    /// `ApiVersionsResponse`. When two ports report different values
    /// for the same key we flag mixed versions.
    api_versions: HashMap<(u16, i16), i16>,
    /// Per-connection SASL re-auth state — last `session_lifetime_ms`
    /// seen so we can flag a drop on the *second* hop.
    sasl_state: HashMap<i32, SaslState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DetectionKey {
    kind: AntiPatternKind,
    scope: String,
}

#[derive(Debug)]
struct DetectionState {
    severity: Severity,
    title: String,
    detail: String,
    first_seen: String,
    last_seen: String,
    occurrences: u32,
    frame_id: Option<String>,
}

#[derive(Debug, Default)]
struct ConnectionCounters {
    init_producer_id: u32,
    produce_requests: u32,
}

#[derive(Debug, Default)]
struct ProduceShape {
    samples: u32,
    total_records: u64,
    /// `Instant`s of recent Produce requests. Bounded by `RATE_QUEUE_CAP`.
    instants: VecDeque<Instant>,
}

#[derive(Debug, Default)]
struct SaslState {
    /// Number of `SaslAuthenticateResponse` already seen on this
    /// connection. The "Session too short" symptom only ever triggers
    /// on a re-auth — `count >= 2`.
    count: u32,
    /// Last broker-granted lifetime. We track the drop, not the
    /// absolute value, since the legitimate-but-short case is rare.
    last_lifetime_ms: i64,
}

/// Tiny `VecDeque` of `Instant` timestamps + drift-bounded cap.
#[derive(Debug, Default)]
struct RollingWindow {
    instants: VecDeque<Instant>,
}

impl RollingWindow {
    fn push(&mut self, now: Instant) {
        self.instants.push_back(now);
        while self.instants.len() > RATE_QUEUE_CAP {
            self.instants.pop_front();
        }
    }
    fn trim(&mut self, now: Instant) {
        while let Some(front) = self.instants.front() {
            if now.duration_since(*front) > RATE_WINDOW {
                self.instants.pop_front();
            } else {
                break;
            }
        }
    }
    fn rate_per_sec(&self) -> f64 {
        if self.instants.is_empty() {
            return 0.0;
        }
        let span = match (self.instants.front(), self.instants.back()) {
            (Some(a), Some(b)) => b.duration_since(*a),
            _ => Duration::ZERO,
        };
        let secs = span.as_secs_f64().max(1.0);
        self.instants.len() as f64 / secs
    }
    fn len(&self) -> usize {
        self.instants.len()
    }
}

impl AntiPatternsFold {
    /// Absorb one frame. Mirrors `SessionFold::absorb`. Side-effect:
    /// may upsert one or more detections.
    #[allow(clippy::too_many_lines)]
    pub fn absorb(&mut self, frame: &ProtoFrame, summary: Option<&FrameSummary>) {
        let Some(s) = summary else { return };
        let now = Instant::now();
        match s {
            FrameSummary::OffsetCommitRequest { group_id, .. } => {
                let (samples, rate) = {
                    let w = self.commits_per_group.entry(group_id.clone()).or_default();
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
            }
            FrameSummary::JoinGroupRequest { group_id, .. } => {
                let samples = {
                    let w = self.joins_per_group.entry(group_id.clone()).or_default();
                    w.push(now);
                    w.trim(now);
                    w.len()
                };
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
            FrameSummary::InitProducerIdRequest { .. } => {
                let c = self.per_connection.entry(frame.connection_id).or_default();
                c.init_producer_id = c.init_producer_id.saturating_add(1);
                self.check_producer_per_record(frame);
            }
            FrameSummary::ProduceRequest {
                record_count,
                batch_count,
                topics,
                ..
            } => {
                // Producer-per-record counter — needed for the ratio.
                {
                    let c = self.per_connection.entry(frame.connection_id).or_default();
                    c.produce_requests = c.produce_requests.saturating_add(1);
                }
                self.check_producer_per_record(frame);

                // Tiny-batch shape tracking — bounded queue per connection.
                let _ = batch_count; // batch_count is captured for future per-partition expert info
                let (avg_records, rate, samples) = {
                    let shape = self.produce_shape.entry(frame.connection_id).or_default();
                    shape.samples = shape.samples.saturating_add(1);
                    shape.total_records =
                        shape.total_records.saturating_add(u64::from(*record_count));
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
            FrameSummary::ProduceResponse { errors } => {
                // NOT_LEADER_OR_FOLLOWER (6) or FENCED_LEADER_EPOCH (47):
                // the broker is telling us this client wrote to the
                // wrong leader for this (topic, partition). Exact
                // signal for #5.
                for e in errors {
                    if !matches!(e.error_code, 6 | 47) {
                        continue;
                    }
                    let leader_hint = match e.current_leader_id {
                        Some(id) => format!(" — current leader is broker {id}"),
                        None => String::new(),
                    };
                    let err_name = match e.error_code {
                        6 => "NOT_LEADER_OR_FOLLOWER",
                        47 => "FENCED_LEADER_EPOCH",
                        _ => "STALE_LEADER",
                    };
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
            FrameSummary::ApiVersionsResponse { max_versions, .. } => {
                let port = frame.local_port;
                let mut mixed_keys: Vec<(i16, i16, i16, u16, u16)> = Vec::new();
                for (api_key, max_v) in max_versions {
                    // Compare to every previously seen (port, key=*api_key).
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
                if !mixed_keys.is_empty() {
                    // Aggregate across all conflicting keys in one row
                    // per broker pair so the Expert tab stays readable.
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
            }
            FrameSummary::SaslAuthenticateResponse {
                error_code,
                error_message,
                session_lifetime_ms,
            } => {
                let (count, prev) = {
                    let st = self.sasl_state.entry(frame.connection_id).or_default();
                    st.count = st.count.saturating_add(1);
                    let prev = st.last_lifetime_ms;
                    st.last_lifetime_ms = *session_lifetime_ms;
                    (st.count, prev)
                };
                if *error_code != 0 {
                    let msg = error_message
                        .as_ref()
                        .map_or("(no message)", String::as_str);
                    self.upsert(
                        AntiPatternKind::SaslSessionTooShort,
                        format!("conn={}", frame.connection_id),
                        Severity::Warn,
                        format!("SASL auth failed (conn {})", frame.connection_id),
                        format!("error_code={error_code} message='{msg}'"),
                        frame,
                    );
                } else if count >= 2
                    && *session_lifetime_ms > 0
                    && *session_lifetime_ms < SASL_SHORT_SESSION_MS
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
            _ => {}
        }
    }

    fn check_producer_per_record(&mut self, frame: &ProtoFrame) {
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

    fn upsert(
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
    }
}

const fn severity_rank(s: Severity) -> u8 {
    match s {
        Severity::Warn => 0,
        Severity::Note => 1,
    }
}

// Implement Ord/PartialOrd for AntiPatternKind so BTreeMap above
// (and any external consumer) gets a stable ordering. The variant
// order itself is what we want.
impl PartialOrd for AntiPatternKind {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for AntiPatternKind {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as u8).cmp(&(*other as u8))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::proto_event::ProtoDirection;
    use crate::proto_summary::{
        BrokerEndpoint, ProducePartitionError, TopicLeaders, TopicPartition,
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
            size: 0,
            captured: 0,
            rtt_ms: 0.0,
            frame_error: None,
            payload_hex: String::new(),
            decoded_json: None,
            summary: None,
        }
    }

    #[test]
    fn overcommit_flags_after_high_rate() {
        let mut fold = AntiPatternsFold::default();
        // Need OVERCOMMIT_MIN_SAMPLES commits *and* rate >= 5/s. The
        // detector uses `Instant::now()` so we just fire a tight loop —
        // the burst will fit inside a second.
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
        let snap = fold.snapshot();
        let det = snap
            .detections
            .iter()
            .find(|d| d.kind == AntiPatternKind::Overcommit)
            .expect("overcommit should fire");
        assert_eq!(det.scope, "group=g1");
    }

    #[test]
    fn producer_per_record_ratio() {
        let mut fold = AntiPatternsFold::default();
        // 6 InitProducerId vs 4 Produce → ratio 60% on conn 7.
        for i in 0..6 {
            let f = frame(&format!("i{i}"), "2026-05-19T10:00:00Z", 7, 9092);
            fold.absorb(
                &f,
                Some(&FrameSummary::InitProducerIdRequest {
                    transactional: false,
                }),
            );
        }
        for i in 0..4 {
            let f = frame(&format!("p{i}"), "2026-05-19T10:00:00Z", 7, 9092);
            fold.absorb(
                &f,
                Some(&FrameSummary::ProduceRequest {
                    topics: vec!["t".into()],
                    partitions: vec![],
                    record_count: 1,
                    batch_bytes: 100,
                    batch_count: 1,
                    transactional: false,
                }),
            );
        }
        let snap = fold.snapshot();
        let det = snap
            .detections
            .iter()
            .find(|d| d.kind == AntiPatternKind::ProducerPerRecord)
            .expect("producer-per-record should fire");
        assert_eq!(det.scope, "conn=7");
    }

    #[test]
    fn tiny_batches_when_records_per_produce_close_to_one() {
        let mut fold = AntiPatternsFold::default();
        for i in 0..30 {
            let f = frame(&format!("p{i}"), "2026-05-19T10:00:00Z", 8, 9092);
            fold.absorb(
                &f,
                Some(&FrameSummary::ProduceRequest {
                    topics: vec!["orders".into()],
                    partitions: vec![],
                    record_count: 1,
                    batch_bytes: 80,
                    batch_count: 1,
                    transactional: false,
                }),
            );
        }
        let snap = fold.snapshot();
        let det = snap
            .detections
            .iter()
            .find(|d| d.kind == AntiPatternKind::TinyBatches)
            .expect("tiny-batches should fire");
        assert_eq!(det.scope, "conn=8");
    }

    #[test]
    fn rebalance_loop_after_five_joins() {
        let mut fold = AntiPatternsFold::default();
        for i in 0..5 {
            let f = frame(&format!("j{i}"), "2026-05-19T10:00:00Z", 1, 9092);
            fold.absorb(
                &f,
                Some(&FrameSummary::JoinGroupRequest {
                    group_id: "g1".into(),
                    member_id: "m".into(),
                }),
            );
        }
        let snap = fold.snapshot();
        assert!(snap
            .detections
            .iter()
            .any(|d| d.kind == AntiPatternKind::RebalanceLoop && d.scope == "group=g1"));
    }

    #[test]
    fn stale_leader_on_not_leader_response() {
        let mut fold = AntiPatternsFold::default();
        let f = frame("r1", "2026-05-19T10:00:00Z", 1, 9092);
        fold.absorb(
            &f,
            Some(&FrameSummary::ProduceResponse {
                errors: vec![ProducePartitionError {
                    topic: "orders".into(),
                    partition: 3,
                    error_code: 6,
                    current_leader_id: Some(50),
                }],
            }),
        );
        let snap = fold.snapshot();
        let det = snap
            .detections
            .iter()
            .find(|d| d.kind == AntiPatternKind::StaleLeaderProducing)
            .expect("stale-leader should fire");
        assert_eq!(det.scope, "orders:3");
        assert!(det.detail.contains("current leader is broker 50"));
    }

    #[test]
    fn mixed_api_version_across_brokers() {
        let mut fold = AntiPatternsFold::default();
        // Broker 1 advertises Produce max=11; broker 2 advertises max=10.
        let f1 = frame("av1", "2026-05-19T10:00:00Z", 1, 9001);
        fold.absorb(
            &f1,
            Some(&FrameSummary::ApiVersionsResponse {
                error_code: 0,
                max_versions: vec![(0, 11)],
            }),
        );
        let f2 = frame("av2", "2026-05-19T10:00:01Z", 2, 9002);
        fold.absorb(
            &f2,
            Some(&FrameSummary::ApiVersionsResponse {
                error_code: 0,
                max_versions: vec![(0, 10)],
            }),
        );
        let snap = fold.snapshot();
        assert!(snap
            .detections
            .iter()
            .any(|d| d.kind == AntiPatternKind::MixedApiVersion));
    }

    #[test]
    fn sasl_session_too_short_on_reauth() {
        let mut fold = AntiPatternsFold::default();
        let f1 = frame("s1", "2026-05-19T10:00:00Z", 1, 9092);
        fold.absorb(
            &f1,
            Some(&FrameSummary::SaslAuthenticateResponse {
                error_code: 0,
                error_message: None,
                session_lifetime_ms: 3_600_000,
            }),
        );
        let f2 = frame("s2", "2026-05-19T11:00:00Z", 1, 9092);
        fold.absorb(
            &f2,
            Some(&FrameSummary::SaslAuthenticateResponse {
                error_code: 0,
                error_message: None,
                session_lifetime_ms: 5_000,
            }),
        );
        let snap = fold.snapshot();
        assert!(snap
            .detections
            .iter()
            .any(|d| d.kind == AntiPatternKind::SaslSessionTooShort));
    }

    // Suppress unused-import warning in non-test cfg.
    #[allow(dead_code)]
    fn _used(_: TopicLeaders, _: BrokerEndpoint, _: TopicPartition) {}
}
