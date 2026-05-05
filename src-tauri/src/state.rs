use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};

use crate::capture::CaptureHandle;
use crate::correlator::ProtoCorrelator;
use crate::filter::CompiledFilter;
use crate::ring_buffer::RingBuffer;
use crate::schema_registry::SchemaRegistryClient;

/// Default ring buffer capacity.
pub const DEFAULT_RING_CAPACITY: usize = 100_000;

/// Shared application state — held inside Tauri's `State`.
#[derive(Debug)]
pub struct AppState {
    pub buffer: Arc<RingBuffer>,
    pub filter: Arc<RwLock<Option<CompiledFilter>>>,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    capture: Option<CaptureHandle>,
    sr_client: Option<Arc<SchemaRegistryClient>>,
    correlator: Option<Arc<ProtoCorrelator>>,
    started_at: Option<Instant>,
}

impl AppState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(RingBuffer::new(DEFAULT_RING_CAPACITY)),
            filter: Arc::new(RwLock::new(None)),
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn install(
        &self,
        handle: CaptureHandle,
        sr_client: Option<Arc<SchemaRegistryClient>>,
        correlator: Arc<ProtoCorrelator>,
    ) {
        let mut guard = self.inner.lock();
        guard.capture = Some(handle);
        guard.sr_client = sr_client;
        guard.correlator = Some(correlator);
        guard.started_at = Some(Instant::now());
    }

    pub fn take_capture(&self) -> Option<CaptureHandle> {
        let mut guard = self.inner.lock();
        guard.started_at = None;
        guard.sr_client = None;
        guard.correlator = None;
        guard.capture.take()
    }

    pub fn is_capturing(&self) -> bool {
        self.inner.lock().capture.is_some()
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.inner
            .lock()
            .started_at
            .map_or(0.0, |started| started.elapsed().as_secs_f64())
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
