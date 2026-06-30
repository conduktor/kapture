use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use std::path::{Path, PathBuf};

use parking_lot::{Mutex, RwLock};

use crate::anti_patterns::DetectorConfig;
use crate::correlator::{ProtoCorrelator, ProtoFrame};
use crate::filter::CompiledFilter;
#[cfg(unix)]
use crate::jvm_tap::JvmTapHandle;
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
    /// User-tunable detector thresholds applied to every new capture
    /// session's correlator. Mutated via the settings UI; persisted to
    /// `detector_config_path`.
    detector_config: RwLock<DetectorConfig>,
    /// Where `detector_config` is persisted (`<config_dir>/detector_config.json`).
    /// `None` until `init_detector_config` runs (e.g. some test harnesses).
    detector_config_path: Mutex<Option<PathBuf>>,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    proxy: Option<crate::proxy::ProxyHandle>,
    /// Mutually exclusive with `proxy`: at most one capture source
    /// feeds `correlator` at a time. The JVM tap path installs this
    /// in place of `proxy`; the proxy path installs `proxy` in place
    /// of this. Unix-only — the tap transport is a Unix domain socket.
    #[cfg(unix)]
    jvm_tap: Option<JvmTapHandle>,
    correlator: Option<Arc<ProtoCorrelator>>,
    started_at: Option<Instant>,
}

impl Inner {
    /// Whether a JVM tap session is active. Always `false` on non-Unix
    /// where the tap feature is compiled out.
    #[cfg(unix)]
    const fn tap_active(&self) -> bool {
        self.jvm_tap.is_some()
    }
    #[cfg(not(unix))]
    #[allow(clippy::unused_self)]
    const fn tap_active(&self) -> bool {
        false
    }
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
            detector_config: RwLock::new(DetectorConfig::default()),
            detector_config_path: Mutex::new(None),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Point the state at `<config_dir>/detector_config.json` and load
    /// it (falling back to defaults if absent/corrupt). Call once during
    /// app setup, before `manage`.
    pub fn init_detector_config(&self, config_dir: &Path) {
        let path = config_dir.join("detector_config.json");
        *self.detector_config.write() = DetectorConfig::load_or_default(&path);
        *self.detector_config_path.lock() = Some(path);
    }

    /// Snapshot of the active detector thresholds.
    #[must_use]
    pub fn detector_config(&self) -> DetectorConfig {
        self.detector_config.read().clone()
    }

    /// Replace the detector thresholds and persist them. The new config
    /// applies to the *next* capture session (the running correlator is
    /// not rebuilt mid-flight). Returns the persistence result; the
    /// in-memory value is updated regardless.
    pub fn set_detector_config(&self, config: DetectorConfig) -> std::io::Result<()> {
        *self.detector_config.write() = config.clone();
        let path = self.detector_config_path.lock().clone();
        match path {
            Some(path) => config.save(&path),
            None => Ok(()),
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

    /// Install (or replace) the active JVM tap session. Caller must
    /// have verified via `try_claim_proxy_slot` that the capture slot
    /// is free — the tap is mutually exclusive with a running proxy
    /// because both share the single `correlator` field.
    #[cfg(unix)]
    pub fn install_jvm_tap(&self, handle: JvmTapHandle, correlator: Arc<ProtoCorrelator>) {
        {
            let mut guard = self.inner.lock();
            guard.jvm_tap = Some(handle);
            guard.correlator = Some(correlator);
            guard.started_at = Some(Instant::now());
        }
        self.proxy_pending.store(false, Ordering::Release);
    }

    /// Take ownership of the running JVM tap, if any, and clear the
    /// associated correlator + start time. Mirrors `take_proxy()`.
    #[cfg(unix)]
    pub fn take_jvm_tap(&self) -> Option<JvmTapHandle> {
        let taken = {
            let mut guard = self.inner.lock();
            guard.started_at = None;
            guard.correlator = None;
            guard.jvm_tap.take()
        };
        self.proxy_pending.store(false, Ordering::Release);
        taken
    }

    #[allow(dead_code)] // exposed for future GUI/MCP consumers
    pub fn is_proxying(&self) -> bool {
        self.inner.lock().proxy.is_some()
    }

    /// `true` when a JVM tap session is active. Used by command
    /// handlers to refuse a `start_proxy` while a tap is running (and
    /// vice versa).
    #[allow(dead_code)] // exposed for future GUI/MCP consumers
    pub fn is_jvm_tapping(&self) -> bool {
        self.inner.lock().tap_active()
    }

    /// Read the path the active JVM tap listener is bound to.
    /// `None` when no tap is running. Used by `attach_jvm_tap_agent`
    /// to feed the right socket path into the target JVM via
    /// `vm.loadAgent(jar, "kapture.tap.socket=...")`.
    #[cfg(unix)]
    pub fn jvm_tap_socket_path(&self) -> Option<std::path::PathBuf> {
        self.inner
            .lock()
            .jvm_tap
            .as_ref()
            .map(|h| h.socket_path().to_path_buf())
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
        let inner = self.inner.lock();
        let has_capture = inner.proxy.is_some() || inner.tap_active();
        drop(inner);
        has_capture || self.proxy_pending.load(Ordering::Acquire)
    }

    /// Atomically reserve the capture slot. Returns `true` if no
    /// proxy AND no JVM tap is currently running and no other caller
    /// has reserved the slot. The reservation MUST be cleared by
    /// `install_proxy()` / `install_jvm_tap()` on success or
    /// `release_proxy_slot()` on failure.
    ///
    /// Despite the historical name, this gates both capture modes.
    /// Renaming would ripple into MCP / Tauri command surfaces; the
    /// shared slot is the invariant the name describes.
    pub fn try_claim_proxy_slot(&self) -> bool {
        let inner = self.inner.lock();
        let already_running = inner.proxy.is_some() || inner.tap_active();
        drop(inner);
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
            api_name: "FetchRequest".to_owned(),
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
            frame_error: None,
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
