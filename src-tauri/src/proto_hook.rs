//! FFI shim for the Kapture-patched `rd_kafka_set_proto_hook_cb`.
//!
//! The hook is installed against a live `rd_kafka_t` after the
//! consumer has been created. The C trampoline runs on the librdkafka
//! broker thread and updates the [`ProtoCorrelator`] **synchronously**
//! before returning. This guarantees that by the time
//! `rd_kafka_req_response` finishes dispatching the Fetch response —
//! which happens on the same broker thread, after our hook returns —
//! the correlator already holds the freshest `Fetch` metadata. So when
//! the high-level `consumer.recv()` later returns a message, the
//! capture loop's `correlator.lookup()` cannot observe a stale state.
//!
//! Safety contract: the underlying librdkafka must be the patched
//! build from `vendor/librdkafka` (see Kapture's docs/spec.md). Without
//! the patch the symbol `rd_kafka_set_proto_hook_cb` is absent and the
//! binary fails to link.

#![allow(unsafe_code)]

use std::ffi::c_void;
use std::os::raw::{c_double, c_int};
use std::sync::{Arc, Weak};

use schemars::JsonSchema;
use serde::Serialize;

use crate::correlator::ProtoCorrelator;

/// Direction constants matching `rdkafka.h`.
const PROTO_DIR_SEND: c_int = 0;
const PROTO_DIR_RECV: c_int = 1;

/// Mirror of the C-side function-pointer type. The patched librdkafka
/// also flattens the wire payload (request bytes on SEND, response
/// bytes on RECV) into a temp buffer and passes a (ptr, len) tuple
/// alongside the metadata. The pointer is only valid for the duration
/// of the callback — copy out before returning.
type CHookFn = unsafe extern "C" fn(
    rk: *mut c_void,
    dir: c_int,
    api_key: c_int,
    api_version: c_int,
    corr_id: i32,
    broker_id: i32,
    payload_size: usize,
    rtt_ms: c_double,
    payload_buf: *const c_void,
    payload_buf_len: usize,
    opaque: *mut c_void,
);

/// Capture cap mirrors `RD_KAFKA_PROTO_HOOK_PAYLOAD_MAX` in our patched
/// rdkafka.h. We refuse to read more than this from the C buffer just
/// in case (defence in depth — the C side already caps).
const PROTO_PAYLOAD_CAP: usize = 64 * 1024;

#[link(name = "rdkafka")]
extern "C" {
    fn rd_kafka_set_proto_hook_cb(rk: *mut c_void, cb: Option<CHookFn>, opaque: *mut c_void);
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ProtoDirection {
    Send,
    Recv,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtoEvent {
    pub direction: ProtoDirection,
    pub api_key: i32,
    pub api_version: i32,
    pub corr_id: i32,
    pub broker_id: i32,
    pub payload_size: usize,
    pub rtt_ms: f64,
    /// Captured wire-payload prefix (≤ `PROTO_PAYLOAD_CAP`). Empty if
    /// the C side couldn't allocate the temp buffer or the payload was
    /// zero-length.
    pub payload: Vec<u8>,
}

impl ProtoEvent {
    /// Human-readable name of the API key. Covers the verbs we expect to
    /// see on the consumer side; unknown keys fall through to "Unknown".
    #[must_use]
    pub const fn api_name(api_key: i32) -> &'static str {
        match api_key {
            0 => "Produce",
            1 => "Fetch",
            2 => "ListOffsets",
            3 => "Metadata",
            8 => "OffsetCommit",
            9 => "OffsetFetch",
            10 => "FindCoordinator",
            11 => "JoinGroup",
            12 => "Heartbeat",
            13 => "LeaveGroup",
            14 => "SyncGroup",
            15 => "DescribeGroups",
            16 => "ListGroups",
            17 => "SaslHandshake",
            18 => "ApiVersions",
            19 => "CreateTopics",
            22 => "InitProducerId",
            32 => "DescribeConfigs",
            36 => "SaslAuthenticate",
            60 => "DescribeCluster",
            _ => "Unknown",
        }
    }
}

/// State shared between the C callback and the Rust side. Lives in a
/// `Box` whose pointer is the C `opaque`, so the C side can locate it
/// without any extra indirection. Held by `Weak` so the underlying
/// `ProtoCorrelator` is owned by the application (e.g. `AppState`) and
/// drops naturally; the trampoline upgrades the weak ref and no-ops
/// if the correlator has been released.
///
/// IMPORTANT: this state is intentionally **leaked** when the hook is
/// detached (see [`ProtoHookHandle::Drop`]). librdkafka's setter
/// installs the new (NULL) callback non-atomically, so a broker
/// thread already inside `rd_kafka_req_response` could still observe
/// the previous callback / opaque after we've returned from the
/// setter. Freeing the box before the in-flight invocation completes
/// would be a use-after-free. Leaking is bounded — one allocation per
/// capture session (~24 B + a `Weak` count) — and the `Weak` lets the
/// real correlator memory drop normally.
struct HookState {
    correlator: Weak<ProtoCorrelator>,
}

unsafe extern "C" fn proto_hook_trampoline(
    _rk: *mut c_void,
    dir: c_int,
    api_key: c_int,
    api_version: c_int,
    corr_id: i32,
    broker_id: i32,
    payload_size: usize,
    rtt_ms: c_double,
    payload_buf: *const c_void,
    payload_buf_len: usize,
    opaque: *mut c_void,
) {
    if opaque.is_null() {
        return;
    }
    // Safety: `opaque` is a `Box::into_raw(HookState)` we passed at
    // install time. The box is intentionally leaked on detach (see
    // `Drop` below) so the pointer stays valid for the lifetime of
    // the process — this avoids the non-atomic detach race in
    // librdkafka's setter.
    let state = unsafe { &*opaque.cast::<HookState>() };
    let direction = if dir == PROTO_DIR_RECV {
        ProtoDirection::Recv
    } else {
        ProtoDirection::Send
    };
    // Copy the wire bytes out before returning — the C side frees the
    // temp buffer immediately. Cap defensively at PROTO_PAYLOAD_CAP.
    let payload = if payload_buf.is_null() || payload_buf_len == 0 {
        Vec::new()
    } else {
        let take = payload_buf_len.min(PROTO_PAYLOAD_CAP);
        // Safety: librdkafka guarantees `payload_buf` is valid for
        // `payload_buf_len` bytes for the duration of the callback.
        unsafe { std::slice::from_raw_parts(payload_buf.cast::<u8>(), take).to_vec() }
    };
    let event = ProtoEvent {
        direction,
        api_key,
        api_version,
        corr_id,
        broker_id,
        payload_size,
        rtt_ms,
        payload,
    };
    // Run the correlator update inside `catch_unwind` because a panic
    // unwinding into the C broker thread would be undefined behaviour.
    // Update is synchronous: by the time librdkafka returns from the
    // hook and continues parsing the response, the correlator is fresh.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some(correlator) = state.correlator.upgrade() {
            correlator.record_event(&event);
        }
    }));
    debug_assert!(matches!(dir, PROTO_DIR_SEND | PROTO_DIR_RECV));
}

/// Handle that owns the hook installation. Drop to detach the hook
/// (the boxed state is intentionally leaked, see [`HookState`]).
pub struct ProtoHookHandle {
    rk: *mut c_void,
    /// Held only so install / Drop can move the pointer through; the
    /// box itself is intentionally never freed by Drop.
    #[allow(dead_code)]
    state: *mut HookState,
}

// `rk` is owned by rdkafka and may be touched from any thread; we only
// pass it to `rd_kafka_set_proto_hook_cb` which is documented to be
// safe to call from any thread. The `HookState` is heap-allocated and
// only freed on Drop after we have detached the hook, so it is sound to
// move the handle across threads.
unsafe impl Send for ProtoHookHandle {}

impl ProtoHookHandle {
    /// Install a hook on `rk`. The returned handle owns the C-side
    /// state pointer; dropping it detaches the hook (and intentionally
    /// leaks the boxed state — see `HookState` for why).
    ///
    /// # Safety
    /// `rk` must point to a valid `rd_kafka_t` produced by the
    /// Kapture-patched librdkafka. The caller guarantees that `rk`
    /// outlives the returned handle (rdkafka client must not be
    /// destroyed before the handle is dropped).
    pub unsafe fn install(rk: *mut c_void, correlator: Arc<ProtoCorrelator>) -> Self {
        let state = Box::into_raw(Box::new(HookState {
            correlator: Arc::downgrade(&correlator),
        }));
        unsafe {
            rd_kafka_set_proto_hook_cb(rk, Some(proto_hook_trampoline), state.cast::<c_void>());
        }
        Self { rk, state }
    }
}

impl Drop for ProtoHookHandle {
    fn drop(&mut self) {
        // Detach the C callback. We intentionally do NOT free the
        // boxed state: librdkafka's setter is not synchronised with
        // broker-thread reads, so a hook invocation could already
        // hold the previous opaque pointer when we return. Leaking
        // bounds the cost to one small allocation per capture
        // session, and the `Weak<ProtoCorrelator>` held inside means
        // the underlying correlator memory drops normally when the
        // owning `Arc` is released elsewhere.
        unsafe {
            rd_kafka_set_proto_hook_cb(self.rk, None, std::ptr::null_mut());
        }
        // Intentionally do not free `self.state` — see the doc comment
        // on `HookState`. A use-after-free can occur if we free while
        // the broker thread is mid-callback with the previous opaque.
    }
}
