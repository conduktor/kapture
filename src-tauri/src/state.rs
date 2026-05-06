use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};

use crate::correlator::ProtoCorrelator;
use crate::filter::CompiledFilter;
use crate::profiles::ProfileStore;
use crate::ring_buffer::RingBuffer;

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
}
