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
use crate::proto_event::{ProtoDirection, ProtoEvent};
use crate::proto_summary::{self, FrameSummary};
use crate::session_stats::{SessionFold, SessionStats};

const FETCH_API_KEY: i32 = 1;
/// Cap on the protocol frames ring buffer. ~2 KB of memory per frame in
/// the serialized JSON; 5000 entries ≈ 10 MB at the high water mark.
/// Aligned with the Messages tab UI cap so both tabs surface a
/// consistent recent-history depth.
const PROTO_FRAMES_CAPACITY: usize = 5000;
/// We trim in chunks to avoid per-event O(n) shift cost on the `VecDeque`.
const PROTO_FRAMES_TRIM_HEADROOM: usize = 256;

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FetchMetadata {
    /// API key — always `1` (Fetch) today, kept for forward-compat.
    pub api_key: i32,
    pub api_name: String,
    pub api_version: i32,
    pub connection_id: i32,
    pub corr_id: i32,
    pub response_size: usize,
    pub rtt_ms: f64,
}

/// Lightweight projection of `ProtoFrame`.
///
/// Everything needed to draw the Protocol list row, plus the typed
/// `summary` projection used by the Session Activity tab. Excludes
/// `payload_hex` and `decoded` so the 1 Hz poll doesn't ship MB of
/// data to the renderer when the ring buffer is full of large Fetch
/// responses. The summary is bounded (a few hundred bytes worst case
/// — list of topic names) and well worth shipping eagerly: it's what
/// lets the frontend aggregate session-level stats without a second
/// round-trip per frame.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtoFrameSummary {
    pub id: String,
    pub timestamp: String,
    pub direction: ProtoDirection,
    pub api_key: i32,
    pub api_name: String,
    pub api_version: i32,
    pub connection_id: i32,
    /// Local listener port that owned the pump emitting this frame.
    /// Lets the frontend aggregate send/recv counters per broker.
    pub local_port: u16,
    pub corr_id: i32,
    pub size: usize,
    pub captured: usize,
    pub rtt_ms: f64,
    /// Typed structured projection of the decoded body for the APIs
    /// the Session Activity tab cares about. `None` when the api isn't
    /// projected, when the bytes were truncated past the body, or when
    /// the body decode failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<FrameSummary>,
}

impl From<&ProtoFrame> for ProtoFrameSummary {
    fn from(f: &ProtoFrame) -> Self {
        Self {
            id: f.id.clone(),
            timestamp: f.timestamp.clone(),
            direction: f.direction,
            api_key: f.api_key,
            api_name: f.api_name.clone(),
            api_version: f.api_version,
            connection_id: f.connection_id,
            local_port: f.local_port,
            corr_id: f.corr_id,
            size: f.size,
            captured: f.captured,
            rtt_ms: f.rtt_ms,
            summary: f.summary.clone(),
        }
    }
}

/// One observed Kafka protocol frame. Either a request (Send) or a
/// response (Recv); pairing happens at view time on the frontend
/// (group by `corrId`+`connectionId`). RTT is broker-side measurement,
/// only meaningful on Recv.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtoFrame {
    pub id: String,
    /// RFC 3339 / ISO 8601 (UTC). Microsecond precision.
    pub timestamp: String,
    pub direction: ProtoDirection,
    pub api_key: i32,
    pub api_name: String,
    pub api_version: i32,
    pub connection_id: i32,
    /// Local TCP listener port that owned the pump that recorded this
    /// frame. Stamped at emission time so that closed-connection
    /// frames retain their broker attribution in the ring buffer.
    pub local_port: u16,
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
    /// Typed JSON of the decoded request/response body. The Kapture
    /// fork of `kafka-protocol` derives `serde::Serialize` on every
    /// message struct; this is the result of `serde_json::to_value`.
    /// Newtype wrappers like `GroupId` and `TopicName` flatten
    /// transparently to strings; `unknown_tagged_fields` surface as
    /// JSON objects keyed by tag id. `None` for APIs we don't decode
    /// yet, when the bytes are truncated past the body, or when the
    /// header parse fails.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decoded_json: Option<serde_json::Value>,
    /// Narrow typed projection driving the Session Activity tab —
    /// see [`crate::proto_summary`]. Subset of `decoded_json` for the
    /// few APIs we aggregate; lets the frontend fold without walking
    /// the full body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<FrameSummary>,
}

#[derive(Debug, Default)]
pub struct ProtoCorrelator {
    state: RwLock<CorrelatorState>,
    /// Separately locked so the wire-frame ring buffer doesn't contend
    /// with the (much hotter) `FetchMetadata` correlator state.
    frames: Mutex<VecDeque<ProtoFrame>>,
    /// Incremental session aggregate. Folded once per event so it
    /// survives ring eviction — the user gets a stable view of "this
    /// client is librdkafka 2.3.0; topics seen are X, Y; groups
    /// active are A, B" even after the originating frames scrolled
    /// out of `frames`.
    session: Mutex<SessionFold>,
}

#[derive(Debug, Default)]
struct CorrelatorState {
    by_connection: HashMap<i32, FetchMetadata>,
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
            let (decoded_json, summary) = if event.payload.is_empty() {
                (None, None)
            } else {
                let api_version = i16::try_from(event.api_version).unwrap_or(0);
                // Two passes over the same captured prefix:
                //  * `decode_frame` walks the body once and emits a
                //    typed `serde_json::Value` (full inspector tree
                //    + decodedContains/decodedField filtering);
                //  * `extract_summary` runs a *narrow* second decode
                //    only for the few APIs that drive Session
                //    Activity aggregates — keeps the IPC summary
                //    payload small without forcing the frontend to
                //    walk full bodies.
                // Both are bounded by the ≤ 64 KiB capture cap.
                let json = proto_decode::decode_frame(
                    event.api_key,
                    api_version,
                    event.direction,
                    &event.payload,
                );
                let summary = proto_summary::extract_summary(
                    event.api_key,
                    api_version,
                    event.direction,
                    &event.payload,
                );
                (json, summary)
            };
            let frame = ProtoFrame {
                id: Uuid::new_v4().simple().to_string(),
                timestamp: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                direction: event.direction,
                api_key: event.api_key,
                api_name: ProtoEvent::api_name_with_direction(event.api_key, event.direction),
                api_version: event.api_version,
                connection_id: event.connection_id,
                local_port: event.local_port,
                corr_id: event.corr_id,
                size: event.payload_size,
                captured,
                rtt_ms: event.rtt_ms,
                payload_hex: hex::encode(&event.payload),
                decoded_json,
                summary,
            };
            // Fold into the persistent session aggregate before
            // pushing — the frame may evict before the user opens
            // the Session Activity tab, but the aggregate persists.
            self.session.lock().absorb(&frame, frame.summary.as_ref());
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
            api_name: ProtoEvent::api_name_with_direction(event.api_key, event.direction),
            api_version: event.api_version,
            connection_id: event.connection_id,
            corr_id: event.corr_id,
            response_size: event.payload_size,
            rtt_ms: event.rtt_ms,
        };
        let mut state = self.state.write();
        state
            .by_connection
            .insert(event.connection_id, meta.clone());
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

    /// Clone every `ProtoFrame` currently in the ring buffer, including
    /// payload bytes and decoded body. Used by the pause-pinning path
    /// in `AppState` so the frontend can still resolve a selected row
    /// after the live ring evicts it.
    #[must_use]
    pub fn frames_snapshot(&self) -> Vec<ProtoFrame> {
        self.frames.lock().iter().cloned().collect()
    }

    /// Drain the entire frame ring buffer + reset the per-connection
    /// fetch-metadata map. Used by the GUI's "Clear" button so the
    /// user can start a fresh capture session for a new test scenario
    /// without restarting the proxy.
    pub fn clear(&self) {
        self.frames.lock().clear();
        self.session.lock().clear();
        let mut state = self.state.write();
        state.by_connection.clear();
        state.latest = None;
    }

    /// Snapshot of the persistent session aggregate. Cheap clone of
    /// the fold state, returned to the GUI by the `session_stats`
    /// Tauri command.
    #[must_use]
    pub fn session_stats(&self) -> SessionStats {
        self.session.lock().snapshot()
    }

    /// Approximation: returns the most-recent `Fetch` response across
    /// all brokers. The (topic, partition) inputs are accepted for
    /// future per-leader correlation but are unused today.
    #[must_use]
    pub fn lookup(&self, _topic: &str, _partition: i32) -> Option<FetchMetadata> {
        self.state.read().latest.clone()
    }

    /// Snapshot of the per-connection map. Useful for diagnostics; not yet
    /// surfaced to the UI.
    #[must_use]
    #[allow(dead_code)]
    pub fn per_connection(&self) -> HashMap<i32, FetchMetadata> {
        self.state.read().by_connection.clone()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::proto_event::ProtoDirection;

    fn ev(direction: ProtoDirection, api_key: i32, connection_id: i32, rtt_ms: f64) -> ProtoEvent {
        ProtoEvent {
            direction,
            api_key,
            api_version: 11,
            corr_id: 42,
            connection_id,
            local_port: 0,
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
        assert_eq!(meta.connection_id, 0);
        assert!((meta.rtt_ms - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn updates_per_connection_and_latest() {
        let c = ProtoCorrelator::new();
        c.record_event(&ev(ProtoDirection::Recv, 1, 0, 1.0));
        c.record_event(&ev(ProtoDirection::Recv, 1, 1, 2.0));
        let map = c.per_connection();
        assert_eq!(map.len(), 2);
        let latest = c.lookup("any", 0).unwrap();
        assert_eq!(latest.connection_id, 1);
    }
}
