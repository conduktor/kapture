#![allow(clippy::doc_markdown)]
//! Detection of the client-side anti-patterns called out in the
//! Kapture README. Each detector folds incrementally per captured
//! frame and exposes a stable `Detection` row to the Expert tab.
//!
//! The original 7 patterns:
//!  1. `Overcommit` — `OffsetCommit` after every single record.
//!  2. `ProducerPerRecord` — fresh producer (full `ApiVersions` +
//!     `Metadata` + `InitProducerId` handshake) per record.
//!  3. `TinyBatches` — `linger.ms=0` + tiny `batch.size`.
//!  4. `RebalanceLoop` — consumer group re-joining every few seconds.
//!  5. `StaleLeaderProducing` — `Produce` returns
//!     `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH`.
//!  6. `MixedApiVersion` — brokers advertising different `max_version`.
//!  7. `SaslSessionTooShort` — re-auth that dies on a clock-like cadence.
//!
//! Eleven additional patterns added in the deep-research pass:
//!  8. `Acks0` — silent durability loss on broker crash.
//!  9. `CompressionOff` — bandwidth waste on a bursty producer.
//! 10. `NonIdempotentProducer` — `producerId == -1`, retries can reorder.
//! 11. `ProducerInstanceLeak` — PagerDuty 2025 shape.
//! 12. `TransactionalZombie` — txn Produce without `EndTxn`.
//! 13. `AutoCommitCadence` — `OffsetCommit` at ~5s intervals.
//! 14. `TightFetchPolling` — `fetch.min.bytes=1` × low traffic.
//! 15. `FetchSessionErrorCascade` — `INVALID_FETCH_SESSION_*` recurring.
//! 16. `ThrottlePressure` — `throttle_time_ms > 0` in responses.
//! 17. `MetadataStorm` — `MetadataRequest` rate too high.
//! 18. `ClassicRebalanceOnModernCluster` — KIP-848 ignored.
//!
//! The detector keeps tiny rolling windows of timestamps so rates can
//! be computed without unbounded growth. Snapshot is a cheap clone of
//! the current detections; the GUI polls it from the Expert tab.

mod detectors;
mod fold;
mod state;

#[cfg(test)]
mod tests;

pub use fold::AntiPatternsFold;

use schemars::JsonSchema;
use serde::Serialize;

/// Severity. The frontend renders icons + tone from this.
#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Warn,
    Note,
}

/// The kind of anti-pattern detected. The frontend keys row rendering
/// off this; the detector dedupe key uses `(kind, scope)`.
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
    /// `acks=0` on a sustained producer — silent durability loss on any
    /// broker crash between socket buffer and log write.
    Acks0,
    /// RecordBatch v2 attributes byte signals no compression on a
    /// producer with high throughput — bandwidth waste.
    CompressionOff,
    /// `producerId == -1` in RecordBatch headers — non-idempotent
    /// producer; retries can cause reorder + duplicates.
    NonIdempotentProducer,
    /// New TCP + full handshake triplet (ApiVersions + Metadata +
    /// InitProducerId) per record — the PagerDuty 2025 leak shape.
    ProducerInstanceLeak,
    /// `InitProducerId` with a transactional_id then `Produce` without
    /// `EndTxn` — abandoned txn blocks LSO.
    TransactionalZombie,
    /// `OffsetCommitRequest` at a regular ~5s cadence — auto-commit
    /// pattern (at-least-once with crash duplicate window).
    AutoCommitCadence,
    /// `FetchRequest.min_bytes=1` on a low-traffic topic — broker
    /// CPU/network burned for sub-KB responses.
    TightFetchPolling,
    /// `INVALID_FETCH_SESSION_EPOCH` / `INVALID_SESSION_ID` recurring
    /// in `FetchResponse` — client repeatedly resetting session,
    /// forcing full fetches.
    FetchSessionErrorCascade,
    /// Any response with `throttle_time_ms > 0` sustained — client
    /// exceeds quota (byte-rate / request-time / mutation).
    ThrottlePressure,
    /// `MetadataRequest` rate sustained above the healthy refresh
    /// cadence — broken refresh or topic-not-found loop.
    MetadataStorm,
    /// Client uses classic `JoinGroup`/`SyncGroup` while broker
    /// advertises `ConsumerGroupHeartbeat` (api_key 68) — KIP-848
    /// incremental rebalance available, stop-the-world chosen instead.
    ClassicRebalanceOnModernCluster,
    /// Broker rejected a `ProduceRequest` with `MESSAGE_TOO_LARGE` (10)
    /// — producer + broker size-limit configs out of sync.
    MessageTooLargeRejected,
    /// `FetchResponse` per-partition `OFFSET_OUT_OF_RANGE` (1) — the
    /// consumer is past the broker's log end. Auto-reset will mask it
    /// but the underlying drift is real.
    OffsetOutOfRangeOnFetch,
    /// Repeated `JoinGroup` advertising `cooperative-sticky` — the
    /// KAFKA-12896 leader-retrigger loop or a misconfigured incremental
    /// rebalance. Distinct from the generic `RebalanceLoop`.
    CooperativeStickyChurn,
    /// `OffsetCommitResponse` per-partition `REBALANCE_IN_PROGRESS` (27)
    /// — client committed mid-rebalance. Commit dropped; can cause
    /// duplicate processing.
    CommitDuringRebalance,
    /// Any response with `TOPIC_AUTHORIZATION_FAILED` (29),
    /// `GROUP_AUTHORIZATION_FAILED` (30), or
    /// `CLUSTER_AUTHORIZATION_FAILED` (31). ACL deny — common
    /// multi-tenant pain.
    AclDeny,
    /// Repeated `FetchResponse` per-partition `UNKNOWN_TOPIC_OR_PARTITION`
    /// (3) on the same partition — consumer pointed at non-existent
    /// or pending topic (KAFKA-3727).
    UnknownTopicPollLoop,
    /// Repeated `FindCoordinatorRequest` for the same group within the
    /// rolling window — coordinator unstable or client churning
    /// connections.
    CoordinatorChurn,
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
            Self::Acks0 => "acks=0 (silent durability loss)",
            Self::CompressionOff => "Compression off on bursty producer",
            Self::NonIdempotentProducer => "Non-idempotent producer",
            Self::ProducerInstanceLeak => "Producer-instance leak",
            Self::TransactionalZombie => "Transactional zombie",
            Self::AutoCommitCadence => "Auto-commit cadence",
            Self::TightFetchPolling => "Tight fetch polling",
            Self::FetchSessionErrorCascade => "Fetch-session error cascade",
            Self::ThrottlePressure => "Throttle pressure",
            Self::MetadataStorm => "Metadata storm",
            Self::ClassicRebalanceOnModernCluster => "Classic rebalance on KIP-848 cluster",
            Self::MessageTooLargeRejected => "Message too large rejected by broker",
            Self::OffsetOutOfRangeOnFetch => "Offset out of range on Fetch",
            Self::CooperativeStickyChurn => "Cooperative-sticky rebalance churn",
            Self::CommitDuringRebalance => "Offset commit during rebalance",
            Self::AclDeny => "ACL deny",
            Self::UnknownTopicPollLoop => "Unknown-topic poll loop",
            Self::CoordinatorChurn => "Coordinator churn",
        }
    }
}

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

/// One row in the Expert tab.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Detection {
    pub kind: AntiPatternKind,
    pub severity: Severity,
    pub title: String,
    pub detail: String,
    pub scope: String,
    pub first_seen: String,
    pub last_seen: String,
    pub occurrences: u32,
    pub frame_id: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, JsonSchema)]
pub struct AntiPatternsSnapshot {
    pub detections: Vec<Detection>,
}
