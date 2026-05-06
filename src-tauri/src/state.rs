use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};

use crate::capture::CaptureHandle;
use crate::correlator::ProtoCorrelator;
use crate::filter::CompiledFilter;
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
    /// Whether the user has armed MCP-initiated capture connect.
    /// Defaults to `false` so an agent cannot kick off a capture
    /// without an explicit human action in the GUI.
    mcp_connect_allowed: AtomicBool,
    /// Reservation flag flipped under `inner.lock()` to make the
    /// "no current capture, install one" sequence atomic. Distinct
    /// from `inner.capture.is_some()` because the reserve happens
    /// before the rdkafka consumer is constructed; if construction
    /// fails the flag is released.
    capture_pending: AtomicBool,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    capture: Option<CaptureHandle>,
    proxy: Option<crate::proxy::ProxyHandle>,
    sr_client: Option<Arc<SchemaRegistryClient>>,
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
            capture_pending: AtomicBool::new(false),
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn install(
        &self,
        handle: CaptureHandle,
        sr_client: Option<Arc<SchemaRegistryClient>>,
        correlator: Arc<ProtoCorrelator>,
    ) {
        {
            let mut guard = self.inner.lock();
            guard.capture = Some(handle);
            guard.sr_client = sr_client;
            guard.correlator = Some(correlator);
            guard.started_at = Some(Instant::now());
        }
        // Slot is now backed by a real handle — clear the starting
        // reservation so future check-and-claim calls see
        // `capture.is_some()` instead.
        self.capture_pending.store(false, Ordering::Release);
    }

    pub fn take_capture(&self) -> Option<CaptureHandle> {
        let taken = {
            let mut guard = self.inner.lock();
            guard.started_at = None;
            guard.sr_client = None;
            guard.correlator = None;
            guard.capture.take()
        };
        self.capture_pending.store(false, Ordering::Release);
        taken
    }

    // Wired into Tauri commands in Task 8 of the proxy-mode plan.
    #[allow(dead_code)]
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
        self.capture_pending.store(false, Ordering::Release);
    }

    #[allow(dead_code)] // see note on `install_proxy`
    pub fn take_proxy(&self) -> Option<crate::proxy::ProxyHandle> {
        let taken = {
            let mut guard = self.inner.lock();
            guard.started_at = None;
            guard.correlator = None;
            guard.proxy.take()
        };
        self.capture_pending.store(false, Ordering::Release);
        taken
    }

    #[allow(dead_code)] // see note on `install_proxy`
    pub fn is_proxying(&self) -> bool {
        self.inner.lock().proxy.is_some()
    }

    pub fn is_capturing(&self) -> bool {
        let (has_capture, has_proxy) = {
            let guard = self.inner.lock();
            (guard.capture.is_some(), guard.proxy.is_some())
        };
        has_capture || has_proxy || self.capture_pending.load(Ordering::Acquire)
    }

    /// Atomically reserve the capture slot. Returns `true` if no
    /// capture is currently running and no other caller has reserved
    /// the slot. The reservation MUST be cleared by `install()` on
    /// success or `release_capture_slot()` on failure.
    pub fn try_claim_capture_slot(&self) -> bool {
        let already_running = {
            let guard = self.inner.lock();
            guard.capture.is_some() || guard.proxy.is_some()
        };
        if already_running {
            return false;
        }
        // CAS only succeeds when no other caller is mid-start.
        self.capture_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn release_capture_slot(&self) {
        self.capture_pending.store(false, Ordering::Release);
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.inner
            .lock()
            .started_at
            .map_or(0.0, |started| started.elapsed().as_secs_f64())
    }

    /// Cheap clone of the active capture's `ProtoCorrelator`, if any.
    /// Returns `None` when no capture is running.
    pub fn correlator(&self) -> Option<Arc<ProtoCorrelator>> {
        self.inner.lock().correlator.clone()
    }

    pub fn mcp_connect_allowed(&self) -> bool {
        self.mcp_connect_allowed.load(Ordering::Acquire)
    }

    pub fn set_mcp_connect_allowed(&self, allowed: bool) {
        self.mcp_connect_allowed.store(allowed, Ordering::Release);
    }
}
