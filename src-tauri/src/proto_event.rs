//! Wire-frame event types shared by the proxy pump and the protocol
//! correlator.
//!
//! Originally lived inside `proto_hook` — the FFI shim around the
//! Kapture-patched librdkafka — when client (rdkafka) capture mode was
//! the only path. With client mode removed the FFI module is gone, but
//! the proxy pump still emits these events from `build_proto_event`
//! and feeds them to `ProtoCorrelator::record_event`. They live here
//! as a small, dependency-free module so the proxy and the correlator
//! can share types without resurrecting any FFI surface.

use schemars::JsonSchema;
use serde::Serialize;

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
    /// Per-TCP-connection id (truncated u64 → i32) of the proxy
    /// connection that carried this frame.
    pub connection_id: i32,
    /// Local TCP listener port that owned the per-connection pump that
    /// emitted this event. Used by the `BrokersTab` to aggregate
    /// send/recv counts per upstream broker (one listener ↔ one
    /// broker mapping). `0` when the emitter is not a proxy listener
    /// (defensive default; no such code path exists today).
    pub local_port: u16,
    pub payload_size: usize,
    pub rtt_ms: f64,
    /// Captured wire-payload prefix. Empty if the proxy elected not to
    /// copy bytes (zero-length frame).
    pub payload: Vec<u8>,
}

impl ProtoEvent {
    /// Human-readable name of the API key. Covers the verbs we expect to
    /// see on the consumer + producer side; unknown keys fall through
    /// to "Unknown".
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
