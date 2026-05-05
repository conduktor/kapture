//! FFI shim for the Kapture-patched `rd_kafka_set_proto_hook_cb`.
//!
//! The hook is installed against a live `rd_kafka_t` after the
//! consumer has been created. Each protocol frame the broker thread
//! sends or receives is captured into a `ProtoEvent` and pushed into
//! the channel given to `install`.
//!
//! Safety contract: the underlying librdkafka must be the patched
//! build from `vendor/librdkafka` (see Kapture's docs/spec.md). Without
//! the patch the symbol `rd_kafka_set_proto_hook_cb` is absent and the
//! binary fails to link.

#![allow(unsafe_code)]
// The capture pipeline does not yet consume these hooks (next round);
// the `proto_smoke` example exercises them today.
#![allow(dead_code)]

use std::ffi::c_void;
use std::os::raw::{c_double, c_int};

use serde::Serialize;
use tokio::sync::mpsc;

/// Direction constants matching `rdkafka.h`.
const PROTO_DIR_SEND: c_int = 0;
const PROTO_DIR_RECV: c_int = 1;

/// Mirror of the C-side function-pointer type.
type CHookFn = unsafe extern "C" fn(
    rk: *mut c_void,
    dir: c_int,
    api_key: c_int,
    api_version: c_int,
    corr_id: i32,
    broker_id: i32,
    payload_size: usize,
    rtt_ms: c_double,
    opaque: *mut c_void,
);

#[link(name = "rdkafka")]
extern "C" {
    fn rd_kafka_set_proto_hook_cb(rk: *mut c_void, cb: Option<CHookFn>, opaque: *mut c_void);
}

#[derive(Debug, Clone, Copy, Serialize)]
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
/// without keeping a copy of the channel sender.
struct HookState {
    sender: mpsc::UnboundedSender<ProtoEvent>,
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
    opaque: *mut c_void,
) {
    if opaque.is_null() {
        return;
    }
    let state = unsafe { &*opaque.cast::<HookState>() };
    let direction = if dir == PROTO_DIR_RECV {
        ProtoDirection::Recv
    } else {
        ProtoDirection::Send
    };
    // Best-effort send: if the receiver has been dropped (capture
    // teardown), we silently drop the event — librdkafka is on the
    // broker thread and must never block.
    let _ = state.sender.send(ProtoEvent {
        direction,
        api_key,
        api_version,
        corr_id,
        broker_id,
        payload_size,
        rtt_ms,
    });
    debug_assert!(matches!(dir, PROTO_DIR_SEND | PROTO_DIR_RECV));
}

/// Handle that owns the hook installation. Drop to detach the hook and
/// free the shared state.
pub struct ProtoHookHandle {
    rk: *mut c_void,
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
    /// state; dropping it detaches the hook.
    ///
    /// # Safety
    /// `rk` must point to a valid `rd_kafka_t` produced by the
    /// Kapture-patched librdkafka. The caller guarantees that `rk`
    /// outlives the returned handle (rdkafka client must not be
    /// destroyed before the handle is dropped).
    pub unsafe fn install(rk: *mut c_void, sender: mpsc::UnboundedSender<ProtoEvent>) -> Self {
        let state = Box::into_raw(Box::new(HookState { sender }));
        unsafe {
            rd_kafka_set_proto_hook_cb(rk, Some(proto_hook_trampoline), state.cast::<c_void>());
        }
        Self { rk, state }
    }
}

impl Drop for ProtoHookHandle {
    fn drop(&mut self) {
        unsafe {
            rd_kafka_set_proto_hook_cb(self.rk, None, std::ptr::null_mut());
            drop(Box::from_raw(self.state));
        }
    }
}
