//! Correlate raw `ProtoEvent`s emitted by the patched librdkafka with
//! the application-level messages they bring.
//!
//! The current implementation keeps the latest `Fetch` response seen on
//! each broker and a single "global latest" pointer. When a
//! `CapturedMessage` is built, we look up the global latest as an
//! approximation — sufficient for single-broker dev clusters and a
//! useful first pass for multi-broker production. Sharper
//! partition→broker mapping (via `Metadata` responses or rdkafka's
//! metadata API) is future work.
//!
//! Thread model: the trampoline in `proto_hook` runs on the librdkafka
//! broker thread and calls [`ProtoCorrelator::record_event`]
//! **synchronously** before returning. Because the same broker thread
//! then continues parsing the Fetch response and dispatching messages
//! to the consumer queue, by the time `consumer.recv()` returns a
//! message the correlator has already absorbed the corresponding RECV
//! event — there is no async race.

use std::collections::{HashMap, VecDeque};

use chrono::Utc;
use parking_lot::{Mutex, RwLock};
use schemars::JsonSchema;
use serde::Serialize;
use uuid::Uuid;

use crate::proto_decode;
use crate::proto_hook::{ProtoDirection, ProtoEvent};

const FETCH_API_KEY: i32 = 1;
/// Cap on the protocol frames ring buffer. ~2 KB of memory per frame in
/// the serialized JSON; 4000 entries ≈ 8 MB at the high water mark.
const PROTO_FRAMES_CAPACITY: usize = 4000;
/// We trim in chunks to avoid per-event O(n) shift cost on the `VecDeque`.
const PROTO_FRAMES_TRIM_HEADROOM: usize = 256;

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FetchMetadata {
    /// API key — always `1` (Fetch) today, kept for forward-compat.
    pub api_key: i32,
    pub api_name: &'static str,
    pub api_version: i32,
    pub broker_id: i32,
    pub corr_id: i32,
    pub response_size: usize,
    pub rtt_ms: f64,
}

/// Lightweight projection of `ProtoFrame`.
///
/// Everything needed to draw the Protocol list row. Excludes
/// `payload_hex` and `decoded` so the 1 Hz poll doesn't ship MB of data
/// to the renderer when the ring buffer is full of large Fetch
/// responses.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtoFrameSummary {
    pub id: String,
    pub timestamp: String,
    pub direction: ProtoDirection,
    pub api_key: i32,
    pub api_name: &'static str,
    pub api_version: i32,
    pub broker_id: i32,
    pub corr_id: i32,
    pub size: usize,
    pub captured: usize,
    pub rtt_ms: f64,
}

impl From<&ProtoFrame> for ProtoFrameSummary {
    fn from(f: &ProtoFrame) -> Self {
        Self {
            id: f.id.clone(),
            timestamp: f.timestamp.clone(),
            direction: f.direction,
            api_key: f.api_key,
            api_name: f.api_name,
            api_version: f.api_version,
            broker_id: f.broker_id,
            corr_id: f.corr_id,
            size: f.size,
            captured: f.captured,
            rtt_ms: f.rtt_ms,
        }
    }
}

/// One observed Kafka protocol frame. Either a request (Send) or a
/// response (Recv); pairing happens at view time on the frontend
/// (group by `corrId`+`brokerId`). RTT is broker-side measurement,
/// only meaningful on Recv.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtoFrame {
    pub id: String,
    /// RFC 3339 / ISO 8601 (UTC). Microsecond precision.
    pub timestamp: String,
    pub direction: ProtoDirection,
    pub api_key: i32,
    pub api_name: &'static str,
    pub api_version: i32,
    pub broker_id: i32,
    pub corr_id: i32,
    /// True wire size (request size on Send, response size on Recv).
    pub size: usize,
    /// Bytes captured for inspection — this is `≤ size` because the
    /// proto-hook caps the per-frame capture at 64 KiB to bound memory
    /// pressure on the broker thread.
    pub captured: usize,
    /// Round-trip time in milliseconds. Only meaningful on `Recv`.
    pub rtt_ms: f64,
    /// Lowercase hex of the captured prefix. Empty when `captured == 0`.
    /// At ~64 KiB cap → ~128 KiB of hex per frame in the worst case.
    pub payload_hex: String,
    /// Pretty-printed `Debug` of the decoded request/response body, when
    /// the `api_key` is in our supported set. `None` for APIs we don't
    /// have a `kafka-protocol` decode arm for, when the bytes are
    /// truncated past the body, or when the header parse fails.
    pub decoded: Option<String>,
}

#[derive(Debug, Default)]
pub struct ProtoCorrelator {
    state: RwLock<CorrelatorState>,
    /// Separately locked so the wire-frame ring buffer doesn't contend
    /// with the (much hotter) `FetchMetadata` correlator state.
    frames: Mutex<VecDeque<ProtoFrame>>,
}

#[derive(Debug, Default)]
struct CorrelatorState {
    by_broker: HashMap<i32, FetchMetadata>,
    latest: Option<FetchMetadata>,
}

impl ProtoCorrelator {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a protocol event. Called synchronously from the
    /// librdkafka broker thread (via the proto-hook trampoline).
    ///
    /// Two side effects:
    ///  1. Update the `FetchMetadata` correlator (latest Fetch RECV per
    ///     broker) — used to enrich the next captured message.
    ///  2. Append a `ProtoFrame` to the ring buffer — used by the
    ///     "Protocol" view in the GUI.
    pub fn record_event(&self, event: &ProtoEvent) {
        // (1) Frames ring buffer: every event, both directions.
        {
            let captured = event.payload.len();
            // Decode synchronously here. proto_decode::decode_frame is
            // bounded by the captured prefix (≤ 64 KiB) so even Fetch
            // responses stay sub-ms. Anything we don't have a decoder
            // arm for returns None and the UI falls back to the hex
            // view.
            let decoded = if event.payload.is_empty() {
                None
            } else {
                let api_version = i16::try_from(event.api_version).unwrap_or(0);
                proto_decode::decode_frame(
                    event.api_key,
                    api_version,
                    event.direction,
                    &event.payload,
                )
            };
            let frame = ProtoFrame {
                id: Uuid::new_v4().simple().to_string(),
                timestamp: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                direction: event.direction,
                api_key: event.api_key,
                api_name: ProtoEvent::api_name(event.api_key),
                api_version: event.api_version,
                broker_id: event.broker_id,
                corr_id: event.corr_id,
                size: event.payload_size,
                captured,
                rtt_ms: event.rtt_ms,
                payload_hex: hex::encode(&event.payload),
                decoded,
            };
            let mut frames = self.frames.lock();
            frames.push_back(frame);
            // Trim in a single batch when we exceed the cap by the
            // headroom amount, to keep amortised O(1) on push.
            if frames.len() > PROTO_FRAMES_CAPACITY + PROTO_FRAMES_TRIM_HEADROOM {
                let drop_n = frames.len() - PROTO_FRAMES_CAPACITY;
                drop(frames.drain(..drop_n));
            }
        }

        // (2) FetchMetadata correlator: only Fetch RECV is meaningful.
        if event.api_key != FETCH_API_KEY {
            return;
        }
        if !matches!(event.direction, ProtoDirection::Recv) {
            return;
        }
        let meta = FetchMetadata {
            api_key: event.api_key,
            api_name: ProtoEvent::api_name(event.api_key),
            api_version: event.api_version,
            broker_id: event.broker_id,
            corr_id: event.corr_id,
            response_size: event.payload_size,
            rtt_ms: event.rtt_ms,
        };
        let mut state = self.state.write();
        state.by_broker.insert(event.broker_id, meta.clone());
        state.latest = Some(meta);
    }

    /// Snapshot of the most recent `limit` frames (clamped to the
    /// buffer cap), oldest first, projected to summaries (no payload
    /// bytes, no decoded body — they're heavy and the list view doesn't
    /// need them). Use `frame_detail` for the full frame on selection.
    #[must_use]
    pub fn summaries(&self, limit: usize) -> Vec<ProtoFrameSummary> {
        let frames = self.frames.lock();
        let n = limit.min(frames.len());
        frames
            .iter()
            .skip(frames.len() - n)
            .map(ProtoFrameSummary::from)
            .collect()
    }

    /// Full `ProtoFrame` by id — including the captured bytes and
    /// decoded debug string. Returns `None` if the frame has aged out
    /// of the ring buffer or never existed.
    #[must_use]
    pub fn frame_detail(&self, id: &str) -> Option<ProtoFrame> {
        self.frames.lock().iter().find(|f| f.id == id).cloned()
    }

    /// Total number of frames currently in the ring buffer.
    #[must_use]
    #[allow(dead_code)] // surfaced via stats in a follow-up
    pub fn frame_count(&self) -> usize {
        self.frames.lock().len()
    }

    /// Approximation: returns the most-recent `Fetch` response across
    /// all brokers. The (topic, partition) inputs are accepted for
    /// future per-leader correlation but are unused today.
    #[must_use]
    pub fn lookup(&self, _topic: &str, _partition: i32) -> Option<FetchMetadata> {
        self.state.read().latest.clone()
    }

    /// Snapshot of the per-broker map. Useful for diagnostics; not yet
    /// surfaced to the UI.
    #[must_use]
    #[allow(dead_code)]
    pub fn per_broker(&self) -> HashMap<i32, FetchMetadata> {
        self.state.read().by_broker.clone()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::proto_hook::ProtoDirection;

    fn ev(direction: ProtoDirection, api_key: i32, broker_id: i32, rtt_ms: f64) -> ProtoEvent {
        ProtoEvent {
            direction,
            api_key,
            api_version: 11,
            corr_id: 42,
            broker_id,
            payload_size: 1024,
            rtt_ms,
            payload: Vec::new(),
        }
    }

    #[test]
    fn records_fetch_recv_only() {
        let c = ProtoCorrelator::new();
        c.record_event(&ev(ProtoDirection::Send, 1, 0, 0.0));
        assert!(c.lookup("orders.raw", 0).is_none());

        c.record_event(&ev(ProtoDirection::Recv, 3, 0, 1.0)); // Metadata, not Fetch
        assert!(c.lookup("orders.raw", 0).is_none());

        c.record_event(&ev(ProtoDirection::Recv, 1, 0, 2.5));
        let meta = c.lookup("orders.raw", 0).unwrap();
        assert_eq!(meta.broker_id, 0);
        assert!((meta.rtt_ms - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn updates_per_broker_and_latest() {
        let c = ProtoCorrelator::new();
        c.record_event(&ev(ProtoDirection::Recv, 1, 0, 1.0));
        c.record_event(&ev(ProtoDirection::Recv, 1, 1, 2.0));
        let map = c.per_broker();
        assert_eq!(map.len(), 2);
        let latest = c.lookup("any", 0).unwrap();
        assert_eq!(latest.broker_id, 1);
    }
}
