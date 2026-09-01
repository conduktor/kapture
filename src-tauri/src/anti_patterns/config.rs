//! User-tunable detector thresholds.
//!
//! Every detector folds against a `DetectorConfig` instead of reading
//! module constants directly, so a profile can override the judgment
//! calls without recompiling. `Default` reproduces the constants in
//! `state.rs` verbatim — a fold built with the default config behaves
//! exactly as it did before this struct existed.
//!
//! Two classes of field live here:
//!
//!  * **Sensitivity knobs** (rates, counts, ratios) — how aggressive an
//!    alarm is; tune to your false-positive tolerance.
//!  * **Values the wire can't reveal** — `poll_stall_gap` (your
//!    `max.poll.interval.ms`), `autocommit_interval_ms` (your
//!    `auto.commit.interval.ms`), `sasl_short_session_ms` (your
//!    `connections.max.reauth.ms`). The *correct* value objectively
//!    exists in the client config and is invisible on the wire, so a
//!    user-supplied value is the only way to make these precise.
//!
//! Structural invariants (`RATE_WINDOW`, `RATE_QUEUE_CAP`,
//! `CONNECTION_IDLE_EXPIRY`, `GC_SWEEP_EVERY`) stay as constants — they
//! govern memory + the fold's mechanics, not detection judgment.

use std::path::Path;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::anti_patterns::state::{
    ACL_DENY_THRESHOLD, AUTOCOMMIT_INTERVAL_MS, AUTOCOMMIT_INTERVAL_TOLERANCE,
    AUTOCOMMIT_MIN_SAMPLES, COMPRESSION_OFF_MIN_RATE, COMPRESSION_OFF_MIN_SAMPLES,
    COOPERATIVE_STICKY_CHURN_THRESHOLD, COORDINATOR_CHURN_THRESHOLD,
    FETCH_SESSION_ERRORS_THRESHOLD, HUNG_REQUEST_TIMEOUT, IDEMPOTENT_PRODUCE_IN_FLIGHT_THRESHOLD,
    IN_FLIGHT_SATURATION_THRESHOLD, METADATA_STORM_MIN_SAMPLES, METADATA_STORM_RATE_PER_SEC,
    NON_IDEMPOTENT_MIN_SAMPLES, OFFSET_OUT_OF_RANGE_THRESHOLD, OVERCOMMIT_MIN_SAMPLES,
    OVERCOMMIT_RATE_PER_SEC, PARTITION_SKEW_MIN_BYTES, PARTITION_SKEW_MIN_SAMPLES,
    PARTITION_SKEW_RATIO, POLL_STALL_GAP, POLL_STALL_MIN_FETCHES,
    PRODUCER_INSTANCE_LEAK_MIN_SAMPLES, PRODUCER_INSTANCE_LEAK_PER_SEC,
    PRODUCER_PER_RECORD_INIT_RATIO, PRODUCER_PER_RECORD_MIN_INITS, REBALANCE_JOINS_IN_WINDOW,
    RETRY_STORM_THRESHOLD, SASL_SHORT_SESSION_MS, TIGHT_FETCH_AVG_RESPONSE_BYTES,
    TIGHT_FETCH_MIN_RATE, TIGHT_FETCH_MIN_SAMPLES, TINY_BATCH_MIN_PRODUCE_RATE,
    TINY_BATCH_MIN_SAMPLES, TINY_BATCH_RECORDS_PER_PRODUCE, UNKNOWN_TOPIC_POLL_THRESHOLD,
};

/// Tunable thresholds for the anti-pattern detectors. Serialized into a
/// profile; `Default` reproduces the historical constants exactly.
///
/// `poll_stall_gap_ms` is stored as milliseconds (not `Duration`) so it
/// round-trips cleanly through JSON and the Tauri IPC boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct DetectorConfig {
    // ---- Class B: values the wire can't reveal ----
    /// Slow-poll-stall trip gap. Set to *your* consumer's
    /// `max.poll.interval.ms` (default 300_000) for a precise eviction
    /// signal; the historical default is a conservative 10_000 early
    /// warning.
    pub poll_stall_gap_ms: u64,
    /// Prior fetches required before a gap counts (cadence gate).
    pub poll_stall_min_fetches: u32,
    /// Expected auto-commit interval. Set to *your*
    /// `auto.commit.interval.ms` (default 5_000).
    pub autocommit_interval_ms: f64,
    /// Allowed relative deviation around `autocommit_interval_ms`.
    pub autocommit_interval_tolerance: f64,
    /// Re-auth session lifetime below this (ms) is "too short". Relate
    /// to *your* `connections.max.reauth.ms`.
    pub sasl_short_session_ms: i64,

    // ---- Class A: sensitivity knobs ----
    pub overcommit_rate_per_sec: f64,
    pub overcommit_min_samples: usize,
    pub producer_per_record_init_ratio: f64,
    pub producer_per_record_min_inits: u32,
    pub tiny_batch_records_per_produce: f64,
    pub tiny_batch_min_produce_rate: f64,
    pub tiny_batch_min_samples: u32,
    pub rebalance_joins_in_window: u32,
    pub compression_off_min_samples: u32,
    pub compression_off_min_rate: f64,
    pub non_idempotent_min_samples: u32,
    pub producer_instance_leak_per_sec: f64,
    pub producer_instance_leak_min_samples: usize,
    pub autocommit_min_samples: usize,
    pub tight_fetch_avg_response_bytes: f64,
    pub tight_fetch_min_rate: f64,
    pub tight_fetch_min_samples: u32,
    pub fetch_session_errors_threshold: usize,
    pub metadata_storm_rate_per_sec: f64,
    pub metadata_storm_min_samples: usize,
    pub cooperative_sticky_churn_threshold: usize,
    pub coordinator_churn_threshold: usize,
    pub unknown_topic_poll_threshold: usize,
    pub offset_out_of_range_threshold: usize,
    pub acl_deny_threshold: usize,
    pub hung_request_timeout_ms: u64,
    pub in_flight_saturation_threshold: usize,
    pub idempotent_produce_in_flight_threshold: usize,
    pub retry_storm_threshold: usize,
    pub partition_skew_min_bytes: u64,
    pub partition_skew_min_samples: u32,
    pub partition_skew_ratio: f64,
}

impl Default for DetectorConfig {
    fn default() -> Self {
        Self {
            poll_stall_gap_ms: u64::try_from(POLL_STALL_GAP.as_millis()).unwrap_or(10_000),
            poll_stall_min_fetches: POLL_STALL_MIN_FETCHES,
            autocommit_interval_ms: AUTOCOMMIT_INTERVAL_MS,
            autocommit_interval_tolerance: AUTOCOMMIT_INTERVAL_TOLERANCE,
            sasl_short_session_ms: SASL_SHORT_SESSION_MS,
            overcommit_rate_per_sec: OVERCOMMIT_RATE_PER_SEC,
            overcommit_min_samples: OVERCOMMIT_MIN_SAMPLES,
            producer_per_record_init_ratio: PRODUCER_PER_RECORD_INIT_RATIO,
            producer_per_record_min_inits: PRODUCER_PER_RECORD_MIN_INITS,
            tiny_batch_records_per_produce: TINY_BATCH_RECORDS_PER_PRODUCE,
            tiny_batch_min_produce_rate: TINY_BATCH_MIN_PRODUCE_RATE,
            tiny_batch_min_samples: TINY_BATCH_MIN_SAMPLES,
            rebalance_joins_in_window: REBALANCE_JOINS_IN_WINDOW,
            compression_off_min_samples: COMPRESSION_OFF_MIN_SAMPLES,
            compression_off_min_rate: COMPRESSION_OFF_MIN_RATE,
            non_idempotent_min_samples: NON_IDEMPOTENT_MIN_SAMPLES,
            producer_instance_leak_per_sec: PRODUCER_INSTANCE_LEAK_PER_SEC,
            producer_instance_leak_min_samples: PRODUCER_INSTANCE_LEAK_MIN_SAMPLES,
            autocommit_min_samples: AUTOCOMMIT_MIN_SAMPLES,
            tight_fetch_avg_response_bytes: TIGHT_FETCH_AVG_RESPONSE_BYTES,
            tight_fetch_min_rate: TIGHT_FETCH_MIN_RATE,
            tight_fetch_min_samples: TIGHT_FETCH_MIN_SAMPLES,
            fetch_session_errors_threshold: FETCH_SESSION_ERRORS_THRESHOLD,
            metadata_storm_rate_per_sec: METADATA_STORM_RATE_PER_SEC,
            metadata_storm_min_samples: METADATA_STORM_MIN_SAMPLES,
            cooperative_sticky_churn_threshold: COOPERATIVE_STICKY_CHURN_THRESHOLD,
            coordinator_churn_threshold: COORDINATOR_CHURN_THRESHOLD,
            unknown_topic_poll_threshold: UNKNOWN_TOPIC_POLL_THRESHOLD,
            offset_out_of_range_threshold: OFFSET_OUT_OF_RANGE_THRESHOLD,
            acl_deny_threshold: ACL_DENY_THRESHOLD,
            hung_request_timeout_ms: u64::try_from(HUNG_REQUEST_TIMEOUT.as_millis())
                .unwrap_or(30_000),
            in_flight_saturation_threshold: IN_FLIGHT_SATURATION_THRESHOLD,
            idempotent_produce_in_flight_threshold: IDEMPOTENT_PRODUCE_IN_FLIGHT_THRESHOLD,
            retry_storm_threshold: RETRY_STORM_THRESHOLD,
            partition_skew_min_bytes: PARTITION_SKEW_MIN_BYTES,
            partition_skew_min_samples: PARTITION_SKEW_MIN_SAMPLES,
            partition_skew_ratio: PARTITION_SKEW_RATIO,
        }
    }
}

impl DetectorConfig {
    /// `poll_stall_gap_ms` as a `Duration` for the detector comparison.
    #[must_use]
    pub const fn poll_stall_gap(&self) -> Duration {
        Duration::from_millis(self.poll_stall_gap_ms)
    }

    /// Load from a JSON file. A missing file or a parse error both fall
    /// back to `Default` (with a warning for the latter) — a corrupt
    /// settings file must never stop the app from capturing.
    #[must_use]
    pub fn load_or_default(path: &Path) -> Self {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                warn!(
                    "detector config at {} is invalid ({e}); using defaults",
                    path.display()
                );
                Self::default()
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(e) => {
                warn!(
                    "detector config at {} unreadable ({e}); using defaults",
                    path.display()
                );
                Self::default()
            }
        }
    }

    /// Persist as pretty JSON via a temp-file rename (atomic on the same
    /// filesystem), creating the parent directory if needed.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_vec_pretty(self).map_err(std::io::Error::other)?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, path)
    }
}
