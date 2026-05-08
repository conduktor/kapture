use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};

use crate::correlator::{ProtoCorrelator, ProtoFrame};
use crate::filter::CompiledFilter;
use crate::message::CapturedMessage;
use crate::profiles::ProfileStore;
use crate::ring_buffer::RingBuffer;
use crate::schema_registry::SchemaRegistryClient;

/// Default ring buffer capacity.
pub const DEFAULT_RING_CAPACITY: usize = 100_000;

/// Shared application state — held inside Tauri's `State`.
#[derive(Debug)]
pub struct AppState {
    pub buffer: Arc<RingBuffer>,
    pub filter: Arc<RwLock<Option<CompiledFilter>>>,
    pub profiles: Arc<ProfileStore>,
    /// Whether the user has armed MCP-initiated proxy start. Defaults
    /// to `false` so an agent cannot kick off `kapture_set_proxy_target`
    /// without an explicit human action in the GUI.
    mcp_connect_allowed: AtomicBool,
    /// Reservation flag flipped under `inner.lock()` to make the
    /// "no current proxy, install one" sequence atomic. Distinct
    /// from `inner.proxy.is_some()` because the reserve happens before
    /// the listener is bound; if bind fails the flag is released.
    proxy_pending: AtomicBool,
    /// User-driven UI pause flag. When true, `inspect_message_by_id`
    /// and `proto_frame_detail` consult the pinned snapshots below so a
    /// row the user can still see in the frozen list is fetchable even
    /// after the live ring evicts it.
    paused: AtomicBool,
    /// Snapshot of the message ring buffer at the moment of pause,
    /// keyed by message id. `None` when not paused.
    pinned_messages: Mutex<Option<HashMap<String, CapturedMessage>>>,
    /// Snapshot of the proto-frames ring buffer at the moment of pause,
    /// keyed by frame id. `None` when not paused.
    pinned_proto_frames: Mutex<Option<HashMap<String, ProtoFrame>>>,
    /// Confluent Schema Registry client, instantiated when the proxy
    /// session is started with a non-empty `schema_registry_url`.
    /// `None` when no registry is configured. Cleared on
    /// `stop_proxy` so a subsequent start with a different URL gets a
    /// fresh client (LRU cache is per-instance).
    schema_registry: Mutex<Option<Arc<SchemaRegistryClient>>>,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    proxy: Option<crate::proxy::ProxyHandle>,
    correlator: Option<Arc<ProtoCorrelator>>,
    started_at: Option<Instant>,
}

impl AppState {
    #[must_use]
    pub fn new(profiles: Arc<ProfileStore>) -> Self {
        Self {
            buffer: Arc::new(RingBuffer::new(DEFAULT_RING_CAPACITY)),
            filter: Arc::new(RwLock::new(None)),
            profiles,
            mcp_connect_allowed: AtomicBool::new(false),
            proxy_pending: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            pinned_messages: Mutex::new(None),
            pinned_proto_frames: Mutex::new(None),
            schema_registry: Mutex::new(None),
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn install_proxy(
        &self,
        handle: crate::proxy::ProxyHandle,
        correlator: Arc<ProtoCorrelator>,
    ) {
        {
            let mut guard = self.inner.lock();
            guard.proxy = Some(handle);
            guard.correlator = Some(correlator);
            guard.started_at = Some(Instant::now());
        }
        self.proxy_pending.store(false, Ordering::Release);
    }

    pub fn take_proxy(&self) -> Option<crate::proxy::ProxyHandle> {
        let taken = {
            let mut guard = self.inner.lock();
            guard.started_at = None;
            guard.correlator = None;
            guard.proxy.take()
        };
        self.proxy_pending.store(false, Ordering::Release);
        taken
    }

    #[allow(dead_code)] // exposed for future GUI/MCP consumers
    pub fn is_proxying(&self) -> bool {
        self.inner.lock().proxy.is_some()
    }

    /// Borrow the active `ProxyHandle` long enough to capture its
    /// summary, then drop the lock. `None` when no proxy is running.
    pub fn proxy_summary(&self) -> Option<crate::proxy_handle::ProxySummary> {
        self.inner
            .lock()
            .proxy
            .as_ref()
            .map(crate::proxy_handle::ProxyHandle::summary)
    }

    pub fn is_capturing(&self) -> bool {
        let has_proxy = self.inner.lock().proxy.is_some();
        has_proxy || self.proxy_pending.load(Ordering::Acquire)
    }

    /// Atomically reserve the proxy slot. Returns `true` if no proxy is
    /// currently running and no other caller has reserved the slot.
    /// The reservation MUST be cleared by `install_proxy()` on success
    /// or `release_proxy_slot()` on failure.
    pub fn try_claim_proxy_slot(&self) -> bool {
        let already_running = self.inner.lock().proxy.is_some();
        if already_running {
            return false;
        }
        // CAS only succeeds when no other caller is mid-start.
        self.proxy_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn release_proxy_slot(&self) {
        self.proxy_pending.store(false, Ordering::Release);
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.inner
            .lock()
            .started_at
            .map_or(0.0, |started| started.elapsed().as_secs_f64())
    }

    /// Cheap clone of the active proxy's `ProtoCorrelator`, if any.
    /// Returns `None` when no proxy is running.
    pub fn correlator(&self) -> Option<Arc<ProtoCorrelator>> {
        self.inner.lock().correlator.clone()
    }

    pub fn mcp_connect_allowed(&self) -> bool {
        self.mcp_connect_allowed.load(Ordering::Acquire)
    }

    pub fn set_mcp_connect_allowed(&self, allowed: bool) {
        self.mcp_connect_allowed.store(allowed, Ordering::Release);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
    }

    /// Install (or replace) the Schema Registry client for the
    /// current proxy session. Pass `None` to drop it (e.g. on
    /// `stop_proxy`).
    pub fn set_schema_registry(&self, client: Option<Arc<SchemaRegistryClient>>) {
        *self.schema_registry.lock() = client;
    }

    /// Read the current Schema Registry client. Returns `None` when
    /// the session was started without a registry URL.
    ///
    /// Wired by milestone 2 of `docs/specs/schema-registry.md` when
    /// the resolver task is added; the milestone-1 plumbing surfaces
    /// the field but doesn't yet read it.
    #[must_use]
    #[allow(dead_code)]
    pub fn schema_registry(&self) -> Option<Arc<SchemaRegistryClient>> {
        self.schema_registry.lock().clone()
    }

    /// Install pinned snapshots taken at pause time. Pass `None` to
    /// clear (used on resume).
    pub fn set_pinned_messages(&self, snapshot: Option<HashMap<String, CapturedMessage>>) {
        *self.pinned_messages.lock() = snapshot;
    }

    pub fn set_pinned_proto_frames(&self, snapshot: Option<HashMap<String, ProtoFrame>>) {
        *self.pinned_proto_frames.lock() = snapshot;
    }

    /// Look up a pinned message by id. Returns `None` when not paused
    /// or when the id is not in the snapshot.
    #[must_use]
    pub fn pinned_message(&self, id: &str) -> Option<CapturedMessage> {
        self.pinned_messages
            .lock()
            .as_ref()
            .and_then(|m| m.get(id).cloned())
    }

    #[must_use]
    pub fn pinned_proto_frame(&self, id: &str) -> Option<ProtoFrame> {
        self.pinned_proto_frames
            .lock()
            .as_ref()
            .and_then(|m| m.get(id).cloned())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::decode::DecodedValue;
    use crate::proto_event::ProtoDirection;
    use tempfile::TempDir;

    fn fresh_state() -> (AppState, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(ProfileStore::open(dir.path().to_path_buf()).unwrap());
        (AppState::new(store), dir)
    }

    fn make_msg(id: &str) -> CapturedMessage {
        CapturedMessage {
            id: id.to_owned(),
            timestamp: "2026-05-05T12:00:00Z".to_owned(),
            topic: "t".to_owned(),
            topic_id: None,
            partition: 0,
            offset: 0,
            key: None,
            schema_name: None,
            schema_id: None,
            schema_guid: None,
            schema_kind: None,
            size_bytes: 0,
            key_size: 0,
            value_size: 0,
            headers: vec![],
            payload: DecodedValue::Bytes {
                hex: String::new(),
                length: 0,
            },
            raw_hex: String::new(),
            fetch: None,
            connection_id: None,
        }
    }

    fn make_frame(id: &str) -> ProtoFrame {
        ProtoFrame {
            id: id.to_owned(),
            timestamp: "2026-05-05T12:00:00Z".to_owned(),
            direction: ProtoDirection::Send,
            api_key: 1,
            api_name: "Fetch",
            api_version: 11,
            connection_id: 0,
            local_port: 0,
            corr_id: 0,
            size: 0,
            captured: 0,
            rtt_ms: 0.0,
            payload_hex: String::new(),
            decoded_json: None,
            summary: None,
        }
    }

    #[test]
    fn paused_defaults_false() {
        let (state, _dir) = fresh_state();
        assert!(!state.is_paused());
    }

    #[test]
    fn set_paused_round_trips() {
        let (state, _dir) = fresh_state();
        state.set_paused(true);
        assert!(state.is_paused());
        state.set_paused(false);
        assert!(!state.is_paused());
    }

    #[test]
    fn pinned_message_returns_some_only_when_pinned() {
        let (state, _dir) = fresh_state();
        let msg = make_msg("m1");
        // Not paused, no snapshot installed -> None.
        assert!(state.pinned_message("m1").is_none());

        let mut map = HashMap::new();
        map.insert(msg.id.clone(), msg);
        state.set_pinned_messages(Some(map));
        assert!(state.pinned_message("m1").is_some());
        assert!(state.pinned_message("missing").is_none());

        // Resume clears.
        state.set_pinned_messages(None);
        assert!(state.pinned_message("m1").is_none());
    }

    #[test]
    fn pinned_proto_frame_returns_some_only_when_pinned() {
        let (state, _dir) = fresh_state();
        let frame = make_frame("f1");
        assert!(state.pinned_proto_frame("f1").is_none());

        let mut map = HashMap::new();
        map.insert(frame.id.clone(), frame);
        state.set_pinned_proto_frames(Some(map));
        assert!(state.pinned_proto_frame("f1").is_some());

        state.set_pinned_proto_frames(None);
        assert!(state.pinned_proto_frame("f1").is_none());
    }
}
