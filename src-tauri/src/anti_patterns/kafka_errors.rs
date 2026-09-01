//! Symbolic constants for the Kafka error codes the detectors look at.
//!
//! Source of truth: <https://kafka.apache.org/protocol#protocol_error_codes>.
//! Only the codes Kapture's detection layer actually pattern-matches
//! on are listed — the full table lives in `src/lib/sessionStats.ts`
//! for the UI rendering side.
//!
//! Why a dedicated module: detectors used to scatter raw integer
//! literals (`error_code == 10`, `27`, `29..=31`, …) across multiple
//! files. A typo would silently break a detector. Centralising the
//! codes here lets us name them once and grep / refactor safely.

#![allow(dead_code)]

// Replication / leadership ------------------------------------------------
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const MESSAGE_TOO_LARGE: i16 = 10;
pub const FENCED_LEADER_EPOCH: i16 = 47;

// Consumer group coordination --------------------------------------------
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const NOT_COORDINATOR: i16 = 16;
pub const ILLEGAL_GENERATION: i16 = 22;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const REBALANCE_IN_PROGRESS: i16 = 27;

// Authorization ----------------------------------------------------------
pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;

// Fetch session ----------------------------------------------------------
pub const INVALID_FETCH_SESSION_EPOCH: i16 = 70;
pub const INVALID_SESSION_ID: i16 = 71;

// Predicates --------------------------------------------------------------

/// `true` for `NOT_LEADER_OR_FOLLOWER` or `FENCED_LEADER_EPOCH` — the
/// "you wrote to the wrong broker" signals on `ProduceResponse`.
#[must_use]
pub const fn is_stale_leader_error(code: i16) -> bool {
    matches!(code, NOT_LEADER_OR_FOLLOWER | FENCED_LEADER_EPOCH)
}

/// `true` for TOPIC / GROUP / CLUSTER authorization failures.
#[must_use]
pub const fn is_auth_error(code: i16) -> bool {
    matches!(
        code,
        TOPIC_AUTHORIZATION_FAILED | GROUP_AUTHORIZATION_FAILED | CLUSTER_AUTHORIZATION_FAILED
    )
}

/// `true` for `INVALID_FETCH_SESSION_EPOCH` or `INVALID_SESSION_ID`.
#[must_use]
pub const fn is_fetch_session_error(code: i16) -> bool {
    matches!(code, INVALID_FETCH_SESSION_EPOCH | INVALID_SESSION_ID)
}

/// Transient errors for which healthy clients normally retry. A high
/// rolling rate is a retry storm even when each individual retry is
/// correct.
#[must_use]
pub const fn is_retriable(code: i16) -> bool {
    matches!(
        code,
        3 | 5 | 6 | 7 | 13 | 14 | 15 | 16 | 19 | 20 | 47 | 56 | 70 | 71
    )
}

/// Short name used in detection messages.
#[must_use]
pub const fn name(code: i16) -> &'static str {
    match code {
        OFFSET_OUT_OF_RANGE => "OFFSET_OUT_OF_RANGE",
        UNKNOWN_TOPIC_OR_PARTITION => "UNKNOWN_TOPIC_OR_PARTITION",
        NOT_LEADER_OR_FOLLOWER => "NOT_LEADER_OR_FOLLOWER",
        MESSAGE_TOO_LARGE => "MESSAGE_TOO_LARGE",
        FENCED_LEADER_EPOCH => "FENCED_LEADER_EPOCH",
        REBALANCE_IN_PROGRESS => "REBALANCE_IN_PROGRESS",
        TOPIC_AUTHORIZATION_FAILED => "TOPIC_AUTHORIZATION_FAILED",
        GROUP_AUTHORIZATION_FAILED => "GROUP_AUTHORIZATION_FAILED",
        CLUSTER_AUTHORIZATION_FAILED => "CLUSTER_AUTHORIZATION_FAILED",
        INVALID_FETCH_SESSION_EPOCH => "INVALID_FETCH_SESSION_EPOCH",
        INVALID_SESSION_ID => "INVALID_SESSION_ID",
        _ => "UNKNOWN_ERROR",
    }
}
