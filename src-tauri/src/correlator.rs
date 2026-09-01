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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::{Mutex, RwLock};
use schemars::JsonSchema;
use serde::Serialize;
use uuid::Uuid;

use crate::anti_patterns::{AntiPatternsFold, AntiPatternsSnapshot, DetectorConfig};
use crate::proto_decode;
use crate::proto_event::{ProtoDirection, ProtoEvent};
use crate::proto_summary::{self, FrameSummary};
use crate::session_stats::{SessionFold, SessionStats};

const FETCH_API_KEY: i32 = 1;
/// Count and retained-heap caps for protocol history. The byte cap is
/// essential: even though captured payloads are retained only once and
/// expanded lazily, decoded summaries and per-frame metadata still have
/// a real retained cost.
const PROTO_FRAMES_CAPACITY: usize = 5000;
const PROTO_FRAMES_BYTE_CAPACITY: usize = 128 * 1024 * 1024;
const ANALYZER_QUEUE_CAPACITY: usize = 1024;

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
    pub capture_lag_ms: f64,
    /// Observation-to-analyzer delay, including external-agent queueing.
    pub analysis_lag_ms: f64,
    /// Typed structured projection of the decoded body for the APIs
    /// the Session Activity tab cares about. `None` when the api isn't
    /// projected, when the bytes were truncated past the body, or when
    /// the body decode failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<FrameSummary>,
    /// Set when the proxy accepted the client TCP but couldn't reach
    /// upstream — the frame was decoded from the client's send but
    /// never forwarded. UI renders the row in error state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtoFramesDelta {
    pub frames: Vec<ProtoFrameSummary>,
    /// True when the cursor was absent/evicted and the frontend must
    /// replace its list rather than append.
    pub reset: bool,
    /// Last frame returned (or the unchanged input cursor when there
    /// was no delta). Feed this into the next poll.
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DecodedBodyResult {
    pub id: String,
    pub decoded_json: Option<serde_json::Value>,
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
            capture_lag_ms: f.capture_lag_ms,
            analysis_lag_ms: f.analysis_lag_ms,
            summary: f.summary.clone(),
            frame_error: f.frame_error.clone(),
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
    /// Capture-to-agent-writer delay. Zero outside external tap modes.
    pub capture_lag_ms: f64,
    /// Capture-to-analysis delay. Includes bounded analyzer queueing and,
    /// for external taps, the agent-writer delay above.
    pub analysis_lag_ms: f64,
    /// Set when the proxy accepted the client TCP but couldn't reach
    /// upstream — the frame was read off the client side but never
    /// forwarded. The string is the upstream-connect error reason
    /// (`Connection refused`, `dns lookup failed`, …). The frontend
    /// renders the row + detail in error state so the user can still
    /// see what the client was trying to send and how it retried.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame_error: Option<String>,
    /// Lowercase hex of the captured prefix. Empty when `captured == 0`.
    /// Materialized only on detail/MCP inspection.
    pub payload_hex: String,
    /// Authoritative captured prefix retained once in the backend ring.
    /// It is deliberately excluded from IPC; `payload_hex` is the
    /// serialized detail representation.
    #[serde(skip)]
    #[schemars(skip)]
    pub raw_payload: Vec<u8>,
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

impl ProtoFrame {
    /// Materialize the heavyweight inspector fields on a detached
    /// frame. The ring itself keeps only `raw_payload` + the bounded
    /// typed summary, so polling and long captures never retain both
    /// bytes, hex and a full decoded tree for every frame.
    pub fn materialize_detail(&mut self) {
        if self.payload_hex.is_empty() && !self.raw_payload.is_empty() {
            self.payload_hex = hex::encode(&self.raw_payload);
        }
        if self.decoded_json.is_none() && !self.raw_payload.is_empty() {
            let api_version = i16::try_from(self.api_version).unwrap_or(0);
            self.decoded_json = proto_decode::decode_frame(
                self.api_key,
                api_version,
                self.direction,
                &self.raw_payload,
            );
        }
    }

    pub(crate) fn decoded_body(&self) -> Option<serde_json::Value> {
        if self.raw_payload.is_empty() {
            return None;
        }
        let api_version = i16::try_from(self.api_version).unwrap_or(0);
        proto_decode::decode_frame(self.api_key, api_version, self.direction, &self.raw_payload)
    }

    #[must_use]
    pub fn estimated_storage_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.id.capacity())
            .saturating_add(self.timestamp.capacity())
            .saturating_add(self.api_name.capacity())
            .saturating_add(self.frame_error.as_ref().map_or(0, String::capacity))
            .saturating_add(self.payload_hex.capacity())
            .saturating_add(self.raw_payload.capacity())
            .saturating_add(
                self.decoded_json
                    .as_ref()
                    .map_or(0, estimated_json_heap_bytes),
            )
            .saturating_add(self.summary.as_ref().map_or(0, serialized_size))
    }
}

fn estimated_json_heap_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
        serde_json::Value::String(value) => value.capacity(),
        serde_json::Value::Array(values) => values
            .capacity()
            .saturating_mul(std::mem::size_of::<serde_json::Value>())
            .saturating_add(values.iter().fold(0usize, |total, value| {
                total.saturating_add(estimated_json_heap_bytes(value))
            })),
        serde_json::Value::Object(values) => values.iter().fold(0usize, |total, (key, value)| {
            total
                .saturating_add(std::mem::size_of::<String>())
                .saturating_add(std::mem::size_of::<serde_json::Value>())
                .saturating_add(key.capacity())
                .saturating_add(estimated_json_heap_bytes(value))
        }),
    }
}

fn serialized_size<T: Serialize>(value: &T) -> usize {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut counter = Counter(0);
    let _ = serde_json::to_writer(&mut counter, value);
    counter.0
}

#[derive(Debug, Default)]
struct FrameRing {
    items: VecDeque<ProtoFrame>,
    bytes: usize,
    evictions: u64,
    oversized_drops: u64,
}

impl FrameRing {
    fn push(&mut self, frame: ProtoFrame) {
        let incoming = frame.estimated_storage_bytes();
        if incoming > PROTO_FRAMES_BYTE_CAPACITY {
            self.oversized_drops = self.oversized_drops.saturating_add(1);
            return;
        }
        while self.items.len() >= PROTO_FRAMES_CAPACITY
            || self.bytes.saturating_add(incoming) > PROTO_FRAMES_BYTE_CAPACITY
        {
            let Some(victim) = self.items.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(victim.estimated_storage_bytes());
            self.evictions = self.evictions.saturating_add(1);
        }
        self.bytes = self.bytes.saturating_add(incoming);
        self.items.push_back(frame);
    }

    fn clear(&mut self) {
        self.items.clear();
        self.bytes = 0;
    }
}

#[derive(Debug, Default)]
pub struct ProtoCorrelator {
    state: RwLock<CorrelatorState>,
    /// Separately locked so the wire-frame ring buffer doesn't contend
    /// with the (much hotter) `FetchMetadata` correlator state.
    frames: Mutex<FrameRing>,
    /// Incremental session aggregate. Folded once per event so it
    /// survives ring eviction — the user gets a stable view of "this
    /// client is librdkafka 2.3.0; topics seen are X, Y; groups
    /// active are A, B" even after the originating frames scrolled
    /// out of `frames`.
    session: Mutex<SessionFold>,
    /// Anti-pattern detectors — folded next to `session` and
    /// surfaced to the Expert tab. Same eviction-survival logic.
    anti_patterns: Mutex<AntiPatternsFold>,
    /// Lazily created single-consumer analyzer queue. Proxy/JVM I/O
    /// tasks only copy the bounded prefix and `try_send`; JSON decode,
    /// summary extraction, hex rendering, and folds happen here.
    analyzer_tx: OnceLock<tokio::sync::mpsc::Sender<ProtoEvent>>,
    analyzer_drops: AtomicU64,
    analyzer_pending: AtomicU64,
    record_extraction_drops: AtomicU64,
    agent_drops: AtomicU64,
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

    /// Build a correlator whose detector fold uses explicit thresholds.
    /// Everything else (frame ring, session fold, fetch-metadata state)
    /// is default.
    #[must_use]
    pub fn with_config(config: DetectorConfig) -> Self {
        Self {
            anti_patterns: Mutex::new(AntiPatternsFold::new(config)),
            ..Self::default()
        }
    }

    /// Enqueue analysis without ever blocking Kafka forwarding. The
    /// queue is bounded; overload is explicit through `analyzer_drops`.
    pub fn enqueue_event(self: &Arc<Self>, event: ProtoEvent) {
        let tx = self.analyzer_tx.get_or_init(|| {
            let (tx, mut rx) = tokio::sync::mpsc::channel(ANALYZER_QUEUE_CAPACITY);
            let correlator = Arc::downgrade(self);
            tauri::async_runtime::spawn(async move {
                while let Some(event) = rx.recv().await {
                    let Some(correlator) = correlator.upgrade() else {
                        break;
                    };
                    correlator.record_event_owned(event);
                    correlator.analyzer_pending.fetch_sub(1, Ordering::Release);
                }
            });
            tx
        });
        self.analyzer_pending.fetch_add(1, Ordering::AcqRel);
        if tx.try_send(event).is_err() {
            self.analyzer_pending.fetch_sub(1, Ordering::Release);
            self.analyzer_drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[must_use]
    pub fn analyzer_drops(&self) -> u64 {
        self.analyzer_drops.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub async fn flush_analysis(&self) {
        while self.analyzer_pending.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
    }

    pub fn record_extraction_drop(&self, count: usize) {
        self.record_extraction_drops
            .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    #[must_use]
    pub fn record_extraction_drops(&self) -> u64 {
        self.record_extraction_drops.load(Ordering::Relaxed)
    }

    pub fn record_agent_drops(&self, count: u64) {
        self.agent_drops.fetch_add(count, Ordering::Relaxed);
    }

    #[must_use]
    pub fn agent_drops(&self) -> u64 {
        self.agent_drops.load(Ordering::Relaxed)
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
        self.record_event_inner(event, event.payload.clone());
    }

    fn record_event_owned(&self, mut event: ProtoEvent) {
        let raw_payload = std::mem::take(&mut event.payload);
        self.record_event_inner(&event, raw_payload);
    }

    fn record_event_inner(&self, event: &ProtoEvent, raw_payload: Vec<u8>) {
        // (1) Frames ring buffer: every event, both directions.
        {
            let captured = raw_payload.len();
            // The compact summary drives incremental folds and must be
            // available now. Full JSON and hex are intentionally left
            // lazy until a detail/batch inspection asks for them.
            let summary = if raw_payload.is_empty() {
                None
            } else {
                let api_version = i16::try_from(event.api_version).unwrap_or(0);
                proto_summary::extract_summary(
                    event.api_key,
                    api_version,
                    event.direction,
                    &raw_payload,
                )
            };
            let frame = ProtoFrame {
                id: Uuid::new_v4().simple().to_string(),
                timestamp: event.observed_at.clone(),
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
                capture_lag_ms: event.capture_lag_ms,
                analysis_lag_ms: event
                    .queued_at
                    .elapsed()
                    .as_secs_f64()
                    .mul_add(1_000.0, event.capture_lag_ms),
                frame_error: event.frame_error.clone(),
                payload_hex: String::new(),
                raw_payload,
                decoded_json: None,
                summary,
            };
            // Fold into the persistent session aggregate before
            // pushing — the frame may evict before the user opens
            // the Session Activity tab, but the aggregate persists.
            self.session.lock().absorb(&frame, frame.summary.as_ref());
            self.anti_patterns
                .lock()
                .absorb(&frame, frame.summary.as_ref());
            self.frames.lock().push(frame);
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
        let n = limit.min(frames.items.len());
        frames
            .items
            .iter()
            .skip(frames.items.len() - n)
            .map(ProtoFrameSummary::from)
            .collect()
    }

    /// Return only summaries newer than `after_id`. If the cursor has
    /// aged out, return a bounded replacement snapshot with `reset`.
    #[must_use]
    pub fn summaries_delta(&self, after_id: Option<&str>, limit: usize) -> ProtoFramesDelta {
        let frames = self.frames.lock();
        let limit = limit.min(PROTO_FRAMES_CAPACITY);
        let cursor_index = after_id.and_then(|id| frames.items.iter().rposition(|f| f.id == id));
        let reset = after_id.is_none() || cursor_index.is_none();
        let start = if reset {
            frames.items.len().saturating_sub(limit)
        } else {
            cursor_index.unwrap_or(0).saturating_add(1)
        };
        let summaries: Vec<ProtoFrameSummary> = frames
            .items
            .iter()
            .skip(start)
            .take(limit)
            .map(ProtoFrameSummary::from)
            .collect();
        drop(frames);
        let next_cursor = summaries
            .last()
            .map(|frame| frame.id.clone())
            .or_else(|| after_id.map(ToOwned::to_owned));
        ProtoFramesDelta {
            frames: summaries,
            reset,
            next_cursor,
        }
    }

    /// Fetch decoded bodies for many ids under one lock. A result is
    /// returned for every id, including `decoded_json: null`; callers
    /// use that as a negative cache entry.
    #[must_use]
    pub fn decoded_bodies(&self, ids: &[String]) -> Vec<DecodedBodyResult> {
        let wanted: HashMap<&str, usize> = ids
            .iter()
            .enumerate()
            .map(|(index, id)| (id.as_str(), index))
            .collect();
        let mut output: Vec<DecodedBodyResult> = ids
            .iter()
            .map(|id| DecodedBodyResult {
                id: id.clone(),
                decoded_json: None,
            })
            .collect();
        let mut remaining = wanted.len();
        let frames = self.frames.lock();
        for frame in frames.items.iter().rev() {
            if remaining == 0 {
                break;
            }
            let Some(&index) = wanted.get(frame.id.as_str()) else {
                continue;
            };
            if output[index].decoded_json.is_none() {
                output[index].decoded_json = frame.decoded_body();
                // Even an undecodable frame is now resolved; ids are
                // unique so no older duplicate needs to be visited.
                remaining -= 1;
            }
        }
        drop(frames);
        output
    }

    /// Full `ProtoFrame` by id — including the captured bytes and
    /// decoded debug string. Returns `None` if the frame has aged out
    /// of the ring buffer or never existed.
    #[must_use]
    pub fn frame_detail(&self, id: &str) -> Option<ProtoFrame> {
        let mut frame = self
            .frames
            .lock()
            .items
            .iter()
            .rev()
            .find(|f| f.id == id)
            .cloned()?;
        frame.materialize_detail();
        Some(frame)
    }

    /// Total number of frames currently in the ring buffer.
    #[must_use]
    #[allow(dead_code)] // surfaced via stats in a follow-up
    pub fn frame_count(&self) -> usize {
        self.frames.lock().items.len()
    }

    /// Clone recent frames for pause-pinning within an explicit retained
    /// payload budget. Newest frames win; an individual frame that does
    /// not fit is skipped without preventing smaller older frames from
    /// being retained.
    #[must_use]
    pub fn frames_snapshot_with_budget(&self, limit: usize, byte_budget: usize) -> Vec<ProtoFrame> {
        let frames = self.frames.lock();
        let mut retained = 0usize;
        let mut output = Vec::with_capacity(limit.min(frames.items.len()));
        for frame in frames.items.iter().rev() {
            if output.len() >= limit {
                break;
            }
            let incoming = frame.estimated_storage_bytes();
            if incoming > byte_budget.saturating_sub(retained) {
                continue;
            }
            retained = retained.saturating_add(incoming);
            output.push(frame.clone());
        }
        drop(frames);
        output.reverse();
        output
    }

    /// Drain the entire frame ring buffer + reset the per-connection
    /// fetch-metadata map. Used by the GUI's "Clear" button so the
    /// user can start a fresh capture session for a new test scenario
    /// without restarting the proxy.
    pub fn clear(&self) {
        self.frames.lock().clear();
        self.session.lock().clear();
        self.anti_patterns.lock().clear();
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

    /// Snapshot of the anti-pattern detector fold. Returned to the
    /// GUI by the `anti_patterns` Tauri command, polled by the
    /// Expert tab.
    #[must_use]
    pub fn anti_patterns(&self) -> AntiPatternsSnapshot {
        self.anti_patterns.lock().snapshot()
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
            observed_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            queued_at: std::time::Instant::now(),
            direction,
            api_key,
            api_version: 11,
            corr_id: 42,
            connection_id,
            local_port: 0,
            payload_size: 1024,
            rtt_ms,
            capture_lag_ms: 0.0,
            payload: Vec::new(),
            frame_error: None,
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

    #[test]
    fn protocol_hex_is_materialized_only_for_detail() {
        let c = ProtoCorrelator::new();
        let mut event = ev(ProtoDirection::Send, 99, 0, 0.0);
        event.payload = vec![0xde, 0xad, 0xbe, 0xef];
        event.payload_size = event.payload.len();
        c.record_event(&event);

        let id = c.summaries(1)[0].id.clone();
        let stored_payload = c.frames.lock().items.back().unwrap().raw_payload.clone();
        assert_eq!(stored_payload, event.payload);
        assert!(c.frames.lock().items.back().unwrap().payload_hex.is_empty());
        assert!(c.frames.lock().items.back().unwrap().decoded_json.is_none());

        let detail = c.frame_detail(&id).unwrap();
        assert_eq!(detail.payload_hex, "deadbeef");
        // Materializing the detached detail must not expand the ring.
        assert!(c.frames.lock().items.back().unwrap().payload_hex.is_empty());
    }

    #[test]
    fn protocol_pause_snapshot_respects_byte_budget() {
        let c = ProtoCorrelator::new();
        c.record_event(&ev(ProtoDirection::Send, 99, 0, 0.0));
        c.record_event(&ev(ProtoDirection::Send, 99, 0, 0.0));
        c.record_event(&ev(ProtoDirection::Send, 99, 0, 0.0));

        let one_row_budget = c
            .frames
            .lock()
            .items
            .back()
            .unwrap()
            .estimated_storage_bytes();
        let latest_id = c.summaries(1)[0].id.clone();
        let snapshot = c.frames_snapshot_with_budget(10, one_row_budget);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].id, latest_id);
    }
}
