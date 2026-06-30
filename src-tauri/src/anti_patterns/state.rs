//! Private state structs + constants for the anti-pattern fold.
//!
//! All state lives behind `AntiPatternsFold` (in `fold.rs`). The
//! constants here are detector tuning knobs; the structs are the
//! per-scope counters and rolling windows.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::anti_patterns::{AntiPatternKind, Severity};

/// Sliding-window length for rate-based detectors. 60s strikes a
/// balance between "react in time for a live dev session" and "don't
/// alert on a 5-second blip".
pub(super) const RATE_WINDOW: Duration = Duration::from_secs(60);
/// Cap on per-window timestamp queues to bound memory under storms.
pub(super) const RATE_QUEUE_CAP: usize = 10_000;

/// Overcommit: ≥ this many `OffsetCommit` per second sustained.
pub(super) const OVERCOMMIT_RATE_PER_SEC: f64 = 5.0;
pub(super) const OVERCOMMIT_MIN_SAMPLES: usize = 20;

/// Producer-per-record: ratio of `InitProducerId` over total
/// (Init+Produce) on a connection.
pub(super) const PRODUCER_PER_RECORD_INIT_RATIO: f64 = 0.5;
pub(super) const PRODUCER_PER_RECORD_MIN_INITS: u32 = 5;

/// Tiny batches: `records/Produce` close to 1, while Produce rate is
/// high.
pub(super) const TINY_BATCH_RECORDS_PER_PRODUCE: f64 = 2.0;
pub(super) const TINY_BATCH_MIN_PRODUCE_RATE: f64 = 10.0;
pub(super) const TINY_BATCH_MIN_SAMPLES: u32 = 20;

/// Rebalance loop: `JoinGroup` count in rolling window per group.
pub(super) const REBALANCE_JOINS_IN_WINDOW: u32 = 5;

/// SASL: session lifetime below this on a re-auth is "Session too short".
pub(super) const SASL_SHORT_SESSION_MS: i64 = 30_000;

/// Compression-off: minimum number of Produce samples before flagging.
pub(super) const COMPRESSION_OFF_MIN_SAMPLES: u32 = 10;
/// Compression-off: minimum Produce rate (req/s) to flag as bandwidth
/// concern (a one-shot send doesn't warrant alerting).
pub(super) const COMPRESSION_OFF_MIN_RATE: f64 = 5.0;

/// Non-idempotent producer: minimum Produce samples to flag.
pub(super) const NON_IDEMPOTENT_MIN_SAMPLES: u32 = 10;

/// Producer-instance leak: new TCP connections completing the
/// `ApiVersions + Metadata + InitProducerId` triplet at this rate per
/// second sustained over `RATE_WINDOW` count as a leak.
pub(super) const PRODUCER_INSTANCE_LEAK_PER_SEC: f64 = 1.0;
pub(super) const PRODUCER_INSTANCE_LEAK_MIN_SAMPLES: usize = 10;

/// Auto-commit: inter-arrival close to ~5000ms ± tolerance.
pub(super) const AUTOCOMMIT_INTERVAL_MS: f64 = 5000.0;
pub(super) const AUTOCOMMIT_INTERVAL_TOLERANCE: f64 = 0.10;
pub(super) const AUTOCOMMIT_MIN_SAMPLES: usize = 4;

/// Tight fetch polling thresholds.
pub(super) const TIGHT_FETCH_AVG_RESPONSE_BYTES: f64 = 1024.0;
pub(super) const TIGHT_FETCH_MIN_RATE: f64 = 5.0;
pub(super) const TIGHT_FETCH_MIN_SAMPLES: u32 = 20;

/// Fetch-session error cascade threshold.
pub(super) const FETCH_SESSION_ERRORS_THRESHOLD: usize = 3;

/// Metadata storm: >10 MetadataRequest/min sustained.
pub(super) const METADATA_STORM_RATE_PER_SEC: f64 = 10.0 / 60.0;
pub(super) const METADATA_STORM_MIN_SAMPLES: usize = 10;

/// Cooperative-sticky churn: at least this many `JoinGroup` frames
/// using `cooperative-sticky` in the rolling window.
pub(super) const COOPERATIVE_STICKY_CHURN_THRESHOLD: usize = 4;

/// Coordinator churn: at least this many `FindCoordinatorRequest`
/// frames for the same key in the rolling window.
pub(super) const COORDINATOR_CHURN_THRESHOLD: usize = 4;

/// Unknown-topic poll loop: at least this many `UNKNOWN_TOPIC_OR_PARTITION`
/// errors on the same partition in the rolling window.
pub(super) const UNKNOWN_TOPIC_POLL_THRESHOLD: usize = 3;

/// Offset out of range: rate-threshold to avoid flagging benign
/// auto.offset.reset single-seek cases. The actual bug is when a
/// consumer keeps polling an out-of-range offset.
pub(super) const OFFSET_OUT_OF_RANGE_THRESHOLD: usize = 3;

/// Per-connection state idle expiry. Connections that haven't been
/// observed for this long are GCed from every per-connection map
/// (counters, rolling windows, handshake state, …). Bounds memory
/// against the producer-instance leak we're meant to detect — without
/// it Kapture itself leaks state proportionally to the bug.
pub(super) const CONNECTION_IDLE_EXPIRY: Duration = Duration::from_secs(10 * 60);

/// How often (in absorb calls) we run the GC sweep. A small constant
/// keeps the amortised cost negligible while still catching leaks
/// promptly under realistic frame rates.
pub(super) const GC_SWEEP_EVERY: u64 = 1_000;

/// ACL deny storm: at least this many auth errors (29/30/31) in the
/// rolling window on the same scope.
pub(super) const ACL_DENY_THRESHOLD: usize = 3;

/// Slow consumer poll stall: a gap between two consecutive
/// `FetchRequest`s on the same connection that's long enough to risk
/// breaching `max.poll.interval.ms` (default 300_000ms). A healthy
/// consumer re-polls within `fetch.max.wait.ms` (default 500ms) even on
/// an idle topic, so a multi-second gap means the application thread
/// blocked between polls — the trivago slow-processing shape. 10s is
/// conservative: well above the healthy re-poll cadence, low enough to
/// catch a stall before the (invisible-on-the-wire) poll interval fires.
pub(super) const POLL_STALL_GAP: Duration = Duration::from_secs(10);
/// Require this many prior `FetchRequest`s on the connection before a
/// gap counts — establishes an active fetch cadence so we don't flag a
/// slow first fetch at startup or a one-off probe.
pub(super) const POLL_STALL_MIN_FETCHES: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct DetectionKey {
    pub kind: AntiPatternKind,
    pub scope: String,
}

#[derive(Debug)]
pub(super) struct DetectionState {
    pub severity: Severity,
    pub title: String,
    pub detail: String,
    pub first_seen: String,
    pub last_seen: String,
    pub occurrences: u32,
    pub frame_id: Option<String>,
}

#[derive(Debug, Default)]
pub(super) struct ConnectionCounters {
    pub init_producer_id: u32,
    pub produce_requests: u32,
}

#[derive(Debug, Default)]
pub(super) struct ProduceShape {
    pub samples: u32,
    pub total_records: u64,
    pub instants: VecDeque<Instant>,
}

#[derive(Debug, Default)]
pub(super) struct ProduceCodecStats {
    /// Count of Produce requests seen on this connection.
    pub samples: u32,
    /// Count that used compression `none` (first batch attributes & 0b111 == 0).
    pub uncompressed: u32,
    /// Count that had `producerId == -1` in first batch (non-idempotent).
    pub non_idempotent: u32,
    /// Rolling-window instants for rate computation.
    pub instants: VecDeque<Instant>,
}

#[derive(Debug, Default)]
pub(super) struct LeakWindow {
    pub instants: VecDeque<Instant>,
}

#[derive(Debug, Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct HandshakeState {
    pub saw_api_versions: bool,
    pub saw_metadata: bool,
    pub saw_init_producer_id: bool,
    pub counted: bool,
}

#[derive(Debug, Default)]
pub(super) struct TxnState {
    pub transactional_id: Option<String>,
    pub produced_in_txn: bool,
    pub add_partitions: bool,
    pub ended: bool,
}

#[derive(Debug, Default)]
pub(super) struct FetchShape {
    pub samples: u32,
    pub total_response_bytes: u64,
    pub instants: VecDeque<Instant>,
}

#[derive(Debug, Default)]
pub(super) struct SaslState {
    pub count: u32,
    pub last_lifetime_ms: i64,
}

/// Per-connection `FetchRequest` cadence tracker for the slow-poll-stall
/// detector. `last` is the instant of the previous fetch; `count` the
/// number of fetches seen so far (gates on `POLL_STALL_MIN_FETCHES`).
#[derive(Debug, Default)]
pub(super) struct FetchPollState {
    pub last: Option<Instant>,
    pub count: u32,
}

/// Tiny `VecDeque` of `Instant` timestamps + drift-bounded cap.
#[derive(Debug, Default)]
pub(super) struct RollingWindow {
    pub instants: VecDeque<Instant>,
}

impl RollingWindow {
    pub fn push(&mut self, now: Instant) {
        self.instants.push_back(now);
        while self.instants.len() > RATE_QUEUE_CAP {
            self.instants.pop_front();
        }
    }
    pub fn trim(&mut self, now: Instant) {
        while let Some(front) = self.instants.front() {
            if now.duration_since(*front) > RATE_WINDOW {
                self.instants.pop_front();
            } else {
                break;
            }
        }
    }
    /// One-shot helper that combines the three steps every detector
    /// did inline: push `now`, trim the window, return the resulting
    /// length. Cuts 5 LOC × 8 call sites of duplication and centralises
    /// the ordering contract (push *then* trim — the other way around
    /// drops the just-added sample).
    pub fn push_and_count(&mut self, now: Instant) -> usize {
        self.push(now);
        self.trim(now);
        self.len()
    }
    pub fn rate_per_sec(&self) -> f64 {
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
    pub fn len(&self) -> usize {
        self.instants.len()
    }
}

pub(super) const fn severity_rank(s: Severity) -> u8 {
    match s {
        Severity::Warn => 0,
        Severity::Note => 1,
    }
}
