//! MCP (Model Context Protocol) server: lets AI agents inspect and
//! steer the running Kapture capture via standardised tools.
//!
//! Mounted as a `tower::Service` under `/mcp` on a localhost-bound
//! axum router. Bound to `127.0.0.1` only, with the strict
//! `allowed_hosts` defaults from rmcp (loopback + DNS-rebinding
//! protection). Designed to share the same `AppState` as the GUI
//! so an agent and a human see the same capture in real time.
//!
//! ### Authentication
//!
//! Every request must carry `Authorization: Bearer <token>`. The
//! token is generated on first launch (UUID v4 hex), persisted to
//! `<config_dir>/mcp-token` with `chmod 0600`, and stays stable
//! across restarts. Rotate by deleting the file. This blocks
//! same-machine cross-user access through the loopback listener.
//!
//! ### Connect-by-profile consent
//!
//! `kafka_connect_profile` reaches into the OS keychain for the
//! profile's secrets. The user must explicitly arm MCP-initiated
//! captures via the GUI (`AppState::set_mcp_connect_allowed`)
//! before the tool is allowed to run. Default is OFF.
//!
//! ### Tool surface
//!
//!  * `kafka_stats` — current ring-buffer + throughput stats
//!  * `kafka_snapshot` — recent messages (filter applied, limit cap)
//!  * `kafka_set_filter` / `kafka_clear_filter` — Wireshark-style DSL
//!  * `kafka_follow_key` — quick-set `envelope.key == "<key>"`
//!  * `kafka_list_profiles` — saved profile metadata, no secrets
//!  * `kafka_inspect_message` — full layer details for a single id
//!  * `kafka_connect_profile` / `kafka_disconnect` — capture lifecycle
//!  * `kafka_proto_frames` — recent Kafka API protocol frames
//!    (Metadata / Fetch / Heartbeat / …) as lightweight summaries
//!  * `kafka_inspect_frame` — full `ProtoFrame` for one id (captured
//!    bytes + decoded body via the `kafka-protocol` crate)
//!  * `kapture_set_proxy_target` / `kapture_stop_proxy` /
//!    `kapture_proxy_status` — start/stop the TCP proxy and observe
//!    its listener fleet. `set_proxy_target` is gated behind
//!    `mcp_connect_allowed` (network sockets ⇒ explicit consent);
//!    `stop_proxy` and `proxy_status` are administrative and always
//!    allowed.
//!  * `kapture_test_upstream` — read-only probe of an upstream broker
//!    (TCP / TLS / SASL handshake + `ApiVersionsRequest` v3, then close).
//!    Not gated; opens no listening sockets, mutates no state.
//!
//! ### Resource surface (read-only views)
//!
//!  * `kapture://stats/current`    — same payload as `kafka_stats`
//!  * `kapture://messages/recent`  — same payload as `kafka_snapshot` capped
//!    at the server hard limit, filter applied
//!  * `kapture://protocol/recent`  — same payload as `kafka_proto_frames`
//!    capped at the server hard limit
//!
//! Resources mirror tools intentionally: agents that subscribe (clients that
//! cache or poll resources) get the same view as agents that call tools, so a
//! human and an agent never disagree about what the capture currently shows.

#![allow(clippy::too_many_lines)]

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    AnnotateAble, ErrorData, ListResourcesResult, PaginatedRequestParams, RawResource,
    ReadResourceRequestParams, ReadResourceResult, ResourceContents, ServerCapabilities,
    ServerInfo,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::tower::{
    StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::{tool, tool_handler, tool_router, Json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tauri::Manager;
use tokio::net::TcpListener;
use tracing::{info, warn};
use uuid::Uuid;

use crate::correlator::{ProtoFrame, ProtoFrameSummary};
use crate::filter::CompiledFilter;
use crate::message::CapturedMessage;
use crate::profiles::ProfileMetadata;
use crate::ring_buffer::CaptureStats;
use crate::state::AppState;

const DEFAULT_PORT: u16 = 7878;
const SNAPSHOT_HARD_LIMIT: usize = 500;
/// Reject filter expressions longer than this. Tight bound — a
/// healthy expression is well under 1 KB; anything longer is almost
/// certainly an attack on the parser or a typo.
const FILTER_EXPR_MAX_LEN: usize = 8 * 1024;

#[derive(Clone)]
pub struct KaptureMcp {
    app_handle: tauri::AppHandle,
}

impl KaptureMcp {
    pub const fn new(app_handle: tauri::AppHandle) -> Self {
        Self { app_handle }
    }

    fn state(&self) -> Result<tauri::State<'_, AppState>, ErrorData> {
        self.app_handle
            .try_state::<AppState>()
            .ok_or_else(|| ErrorData::internal_error("AppState not initialised", None))
    }
}

#[derive(Deserialize, JsonSchema, Default)]
struct EmptyParams {}

#[derive(Deserialize, JsonSchema)]
struct SnapshotParams {
    /// Maximum number of messages to return. Server caps at 500.
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Deserialize, JsonSchema)]
struct InspectParams {
    /// Message id (UUID) to look up.
    id: String,
}

#[derive(Deserialize, JsonSchema)]
struct SetFilterParams {
    /// Wireshark-style filter expression (see Kapture's filter DSL).
    expression: String,
}

#[derive(Deserialize, JsonSchema)]
struct FollowKeyParams {
    /// Kafka record key to track across topics. Translated to the DSL
    /// expression `envelope.key == "<key>"`.
    key: String,
}

#[derive(Serialize, JsonSchema)]
struct SnapshotResponse {
    total: usize,
    returned: usize,
    messages: Vec<CapturedMessage>,
}

#[derive(Serialize, JsonSchema)]
struct ProfilesResponse {
    profiles: Vec<ProfileMetadata>,
}

#[derive(Serialize, JsonSchema)]
struct StatsResponse {
    capturing: bool,
    stats: CaptureStats,
}

#[derive(Serialize, JsonSchema)]
struct InspectResponse {
    found: bool,
    message: Option<CapturedMessage>,
}

#[derive(Deserialize, JsonSchema)]
struct ProtoFramesParams {
    /// Maximum number of frames to return. Server caps at 500.
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Serialize, JsonSchema)]
struct ProtoFramesResponse {
    returned: usize,
    frames: Vec<ProtoFrameSummary>,
}

#[derive(Deserialize, JsonSchema)]
struct InspectFrameParams {
    /// Frame id (UUID) to look up. Matches the `id` field of any item
    /// previously returned by `kafka_proto_frames`.
    id: String,
}

#[derive(Serialize, JsonSchema)]
struct InspectFrameResponse {
    found: bool,
    frame: Option<ProtoFrame>,
}

#[derive(Serialize, JsonSchema)]
struct AckResponse {
    ok: bool,
    detail: String,
}

#[derive(Deserialize, JsonSchema)]
struct SetProxyTargetParams {
    /// Upstream Kafka bootstrap as `host:port`.
    upstream: String,
    /// Local port to listen on. Use 0 for an ephemeral port.
    listen_port: u16,
    /// Optional TLS config for the upstream connection. When omitted
    /// the proxy connects in plaintext.
    #[serde(default)]
    upstream_tls: Option<McpProxyTlsArgs>,
    /// Optional SASL credentials (`PLAIN`, `SCRAM-SHA-256`, or
    /// `SCRAM-SHA-512`). When omitted no SASL handshake is performed
    /// against the upstream.
    #[serde(default)]
    upstream_sasl: Option<McpProxySaslArgs>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct McpProxyTlsArgs {
    server_name: String,
    #[serde(default)]
    ca_path: Option<String>,
    #[serde(default)]
    skip_hostname_verification: bool,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct McpProxySaslArgs {
    /// One of `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`. Case-insensitive.
    mechanism: String,
    username: String,
    password: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TestUpstreamParams {
    /// Upstream Kafka bootstrap as `host:port`.
    upstream: String,
    #[serde(default)]
    upstream_tls: Option<McpProxyTlsArgs>,
    #[serde(default)]
    upstream_sasl: Option<McpProxySaslArgs>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TestUpstreamResponse {
    ok: bool,
    latency_ms: f64,
    message: String,
    api_versions_count: Option<usize>,
}

#[derive(Serialize, JsonSchema)]
struct ProxyStatusResponse {
    listening: bool,
    listen_addr: Option<String>,
    upstream: Option<String>,
    active_connections: usize,
    /// `((upstream_host, upstream_port), local_port)` mapping. Sorted
    /// by `local_port` so the order is stable across calls.
    broker_mappings: Vec<((String, u16), u16)>,
}

// Resource URIs. Kept as constants so the list_resources entries and the
// read_resource match arms can never drift out of sync.
const RESOURCE_STATS_URI: &str = "kapture://stats/current";
const RESOURCE_MESSAGES_URI: &str = "kapture://messages/recent";
const RESOURCE_PROTOCOL_URI: &str = "kapture://protocol/recent";
/// Cap on protocol frames returned via the MCP surface. Same intent as
/// `SNAPSHOT_HARD_LIMIT` for messages: keep replies bounded against
/// hostile or buggy callers.
const PROTO_FRAMES_HARD_LIMIT: usize = 500;
/// Cap on the `decoded` Debug string returned via `kafka_inspect_frame`.
/// `Debug`-pretty-printing a Fetch response can balloon to MBs;
/// truncate at the MCP boundary so a single tool call can't blow up
/// JSON memory. The GUI path is unaffected.
const PROTO_DECODED_HARD_LIMIT: usize = 32 * 1024;
/// Kafka API keys whose payloads carry credentials and must be redacted
/// before crossing the MCP boundary, even on a localhost-bound,
/// bearer-authenticated channel. The GUI surfaces these locally because
/// the user is inspecting their own traffic; redacting in MCP keeps an
/// agent driving Kapture from accidentally dumping a SASL/PLAIN
/// password into a chat log.
const SASL_HANDSHAKE_API_KEY: i32 = 17;
const SASL_AUTHENTICATE_API_KEY: i32 = 36;

#[tool_router]
impl KaptureMcp {
    #[tool(description = "Return ring-buffer + throughput stats for the active capture.")]
    fn kafka_stats(&self, _: Parameters<EmptyParams>) -> Result<Json<StatsResponse>, ErrorData> {
        let state = self.state()?;
        let elapsed = state.elapsed_secs().max(0.001);
        let total = state.buffer.stats(0.0).total_received;
        #[allow(clippy::cast_precision_loss)]
        let throughput = total as f64 / elapsed;
        Ok(Json(StatsResponse {
            capturing: state.is_capturing(),
            stats: state.buffer.stats(throughput),
        }))
    }

    #[tool(
        description = "Return up to `limit` (cap 500) recent captured messages, with the active filter applied. Iterates the ring buffer newest-first and short-circuits when the limit is reached."
    )]
    fn kafka_snapshot(
        &self,
        Parameters(SnapshotParams { limit }): Parameters<SnapshotParams>,
    ) -> Result<Json<SnapshotResponse>, ErrorData> {
        let state = self.state()?;
        let cap = limit.map_or(SNAPSHOT_HARD_LIMIT, |n| {
            (n as usize).min(SNAPSHOT_HARD_LIMIT)
        });
        // Clone the compiled filter (cheap Arc bump) so the read lock
        // is released before we iterate the ring buffer.
        let filter = state.filter.read().clone();
        let messages = state
            .buffer
            .recent_filtered(cap, |m| filter.as_ref().is_none_or(|f| f.matches(m)));
        let returned = messages.len();
        // Total under filter is best-effort: report what we gathered;
        // computing the true total would require scanning the buffer
        // a second time. The agent can call again with a higher limit
        // to confirm.
        Ok(Json(SnapshotResponse {
            total: returned,
            returned,
            messages,
        }))
    }

    #[tool(
        description = "Install a filter (Wireshark-style DSL). Empty string clears the filter. Expressions are capped at 8 KB."
    )]
    fn kafka_set_filter(
        &self,
        Parameters(SetFilterParams { expression }): Parameters<SetFilterParams>,
    ) -> Result<Json<AckResponse>, ErrorData> {
        let state = self.state()?;
        if expression.len() > FILTER_EXPR_MAX_LEN {
            return Err(ErrorData::invalid_params(
                format!(
                    "filter expression too long ({} bytes, cap {FILTER_EXPR_MAX_LEN})",
                    expression.len()
                ),
                None,
            ));
        }
        let trimmed = expression.trim();
        if trimmed.is_empty() {
            *state.filter.write() = None;
            return Ok(Json(AckResponse {
                ok: true,
                detail: "filter cleared".into(),
            }));
        }
        let compiled = CompiledFilter::compile(trimmed)
            .map_err(|err| ErrorData::invalid_params(err.to_string(), None))?;
        *state.filter.write() = Some(compiled);
        Ok(Json(AckResponse {
            ok: true,
            detail: format!("filter installed: {trimmed}"),
        }))
    }

    #[tool(description = "Clear the active filter.")]
    fn kafka_clear_filter(
        &self,
        _: Parameters<EmptyParams>,
    ) -> Result<Json<AckResponse>, ErrorData> {
        let state = self.state()?;
        *state.filter.write() = None;
        Ok(Json(AckResponse {
            ok: true,
            detail: "filter cleared".into(),
        }))
    }

    #[tool(description = "List saved connection profiles. Secrets are NEVER returned.")]
    fn kafka_list_profiles(
        &self,
        _: Parameters<EmptyParams>,
    ) -> Result<Json<ProfilesResponse>, ErrorData> {
        let state = self.state()?;
        Ok(Json(ProfilesResponse {
            profiles: state.profiles.list(),
        }))
    }

    #[tool(description = "Look up a single captured message by id.")]
    fn kafka_inspect_message(
        &self,
        Parameters(InspectParams { id }): Parameters<InspectParams>,
    ) -> Result<Json<InspectResponse>, ErrorData> {
        let state = self.state()?;
        let snap = state.buffer.snapshot();
        let message = snap.into_iter().find(|m| m.id == id);
        Ok(Json(InspectResponse {
            found: message.is_some(),
            message,
        }))
    }

    #[tool(
        description = "Return up to `limit` (cap 500) recent Kafka API protocol frames as lightweight summaries (no payload bytes, no decoded body). Use `kafka_inspect_frame` to fetch the full payload + decoded fields for one id."
    )]
    fn kafka_proto_frames(
        &self,
        Parameters(ProtoFramesParams { limit }): Parameters<ProtoFramesParams>,
    ) -> Result<Json<ProtoFramesResponse>, ErrorData> {
        let state = self.state()?;
        let cap = limit.map_or(PROTO_FRAMES_HARD_LIMIT, |n| {
            (n as usize).min(PROTO_FRAMES_HARD_LIMIT)
        });
        let frames = state
            .correlator()
            .map(|c| c.summaries(cap))
            .unwrap_or_default();
        Ok(Json(ProtoFramesResponse {
            returned: frames.len(),
            frames,
        }))
    }

    #[tool(
        description = "Return the full ProtoFrame for one id — captured wire bytes (lowercase hex, capped at 64 KiB) plus a pretty-printed Debug of the decoded request/response body via the kafka-protocol crate when the api is supported. SASL frames are redacted; the decoded string is truncated at 32 KiB."
    )]
    fn kafka_inspect_frame(
        &self,
        Parameters(InspectFrameParams { id }): Parameters<InspectFrameParams>,
    ) -> Result<Json<InspectFrameResponse>, ErrorData> {
        let state = self.state()?;
        let frame = state
            .correlator()
            .and_then(|c| c.frame_detail(&id))
            .map(redact_frame_for_mcp);
        Ok(Json(InspectFrameResponse {
            found: frame.is_some(),
            frame,
        }))
    }

    #[tool(
        description = "Follow a Kafka record key across all subscribed topics. Installs `envelope.key == \"<key>\"` as the active filter."
    )]
    fn kafka_follow_key(
        &self,
        Parameters(FollowKeyParams { key }): Parameters<FollowKeyParams>,
    ) -> Result<Json<AckResponse>, ErrorData> {
        if key.len() > FILTER_EXPR_MAX_LEN / 2 {
            return Err(ErrorData::invalid_params(
                "key too long for follow_key",
                None,
            ));
        }
        let state = self.state()?;
        let escaped = key.replace('\\', "\\\\").replace('"', "\\\"");
        let expr = format!("envelope.key == \"{escaped}\"");
        let compiled = CompiledFilter::compile(&expr)
            .map_err(|err| ErrorData::invalid_params(err.to_string(), None))?;
        *state.filter.write() = Some(compiled);
        Ok(Json(AckResponse {
            ok: true,
            detail: format!("filter installed: {expr}"),
        }))
    }

    #[tool(
        description = "Start the TCP proxy targeting `upstream` (host:port) on `listen_port` (0 for an ephemeral port). Stops any previous proxy first. Requires the user to have armed MCP connect from the GUI — proxy mode opens listening sockets, so an explicit-consent gate applies."
    )]
    async fn kapture_set_proxy_target(
        &self,
        Parameters(SetProxyTargetParams {
            upstream,
            listen_port,
            upstream_tls,
            upstream_sasl,
        }): Parameters<SetProxyTargetParams>,
    ) -> Result<Json<ProxyStatusResponse>, ErrorData> {
        {
            let state = self.state()?;
            if !state.mcp_connect_allowed() {
                return Err(ErrorData::invalid_request(
                    "MCP-initiated proxy is not allowed; arm MCP connect from the GUI first",
                    None,
                ));
            }
        }
        let tls_cfg = upstream_tls.map(|t| crate::proxy::UpstreamTlsConfig {
            server_name: t.server_name,
            ca_path: t.ca_path.and_then(|s| {
                if s.trim().is_empty() {
                    None
                } else {
                    Some(std::path::PathBuf::from(s))
                }
            }),
            skip_hostname_verification: t.skip_hostname_verification,
        });
        let sasl_cfg = match upstream_sasl {
            Some(s) => {
                let mech = match s.mechanism.to_uppercase().as_str() {
                    "PLAIN" => crate::proxy::UpstreamSaslMechanism::Plain,
                    "SCRAM-SHA-256" => crate::proxy::UpstreamSaslMechanism::ScramSha256,
                    "SCRAM-SHA-512" => crate::proxy::UpstreamSaslMechanism::ScramSha512,
                    other => {
                        return Err(ErrorData::invalid_request(
                            format!(
                                "unsupported upstream SASL mechanism `{other}` (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)"
                            ),
                            None,
                        ));
                    }
                };
                if s.username.is_empty() || s.password.is_empty() {
                    return Err(ErrorData::invalid_request(
                        "upstream SASL username and password must be non-empty",
                        None,
                    ));
                }
                Some(crate::proxy::UpstreamSaslConfig {
                    mechanism: mech,
                    username: s.username,
                    password: s.password,
                })
            }
            None => None,
        };
        // The Tauri State<'_, AppState> wrapper is only needed by the
        // command-layer entry point; the shared impl takes &AppState
        // directly so MCP can call it without faking a State.
        let state_ref = self
            .app_handle
            .try_state::<AppState>()
            .ok_or_else(|| ErrorData::internal_error("AppState not initialised", None))?;
        crate::commands::start_proxy_impl(
            &self.app_handle,
            &state_ref,
            upstream,
            listen_port,
            tls_cfg,
            sasl_cfg,
            true, // MCP path: re-check gate before slot claim (race fix)
        )
        .await
        .map_err(|err| ErrorData::internal_error(err.to_string(), None))?;
        // Read the freshly-installed proxy summary so the agent gets a
        // proper status object (listen address etc.) on the same call.
        Ok(Json(proxy_status_response(&state_ref)))
    }

    #[tool(
        description = "Stop the active TCP proxy. Idempotent — succeeds when no proxy is running. Always allowed regardless of the MCP connect arm state (administrative)."
    )]
    async fn kapture_stop_proxy(
        &self,
        _: Parameters<EmptyParams>,
    ) -> Result<Json<AckResponse>, ErrorData> {
        let handle = {
            let state = self.state()?;
            state.take_proxy()
        };
        match handle {
            Some(h) => {
                h.stop().await;
                Ok(Json(AckResponse {
                    ok: true,
                    detail: "proxy stopped".into(),
                }))
            }
            None => Ok(Json(AckResponse {
                ok: true,
                detail: "no active proxy".into(),
            })),
        }
    }

    #[tool(
        description = "Probe an upstream Kafka broker without starting the proxy. Opens a fresh TCP connection (TLS / SASL if configured), exchanges a single ApiVersionsRequest v3, and closes. Reports OK + latency + count of supported APIs, or FAIL + error. Read-only — no listening sockets, no AppState mutation; not gated behind `mcp_connect_allowed`."
    )]
    async fn kapture_test_upstream(
        &self,
        Parameters(TestUpstreamParams {
            upstream,
            upstream_tls,
            upstream_sasl,
        }): Parameters<TestUpstreamParams>,
    ) -> Result<Json<TestUpstreamResponse>, ErrorData> {
        // Translate the MCP arg shapes into the Tauri-command arg
        // shapes so we can call the same probe path the GUI uses.
        let tls_args = upstream_tls.map(|t| crate::commands::ProxyTlsArgs {
            server_name: t.server_name,
            ca_path: t.ca_path,
            skip_hostname_verification: t.skip_hostname_verification,
        });
        let sasl_args = upstream_sasl.map(|s| crate::commands::ProxySaslArgs {
            mechanism: s.mechanism,
            username: s.username,
            password: s.password,
        });
        let result = crate::commands::test_proxy_upstream(upstream, tls_args, sasl_args).await;
        Ok(Json(TestUpstreamResponse {
            ok: result.ok,
            latency_ms: result.latency_ms,
            message: result.message,
            api_versions_count: result.api_versions_count,
        }))
    }

    #[tool(
        description = "Snapshot of the running TCP proxy: listen address, upstream, count of currently-active client→broker connection pumps, and the (upstream_host:port, local_port) broker map inferred from observed Metadata responses. Returns `listening: false` (and zeroed/empty fields) when no proxy is active. Always allowed (administrative)."
    )]
    fn kapture_proxy_status(
        &self,
        _: Parameters<EmptyParams>,
    ) -> Result<Json<ProxyStatusResponse>, ErrorData> {
        let state = self.state()?;
        Ok(Json(proxy_status_response(&state)))
    }
}

fn proxy_status_response(state: &AppState) -> ProxyStatusResponse {
    state.proxy_summary().map_or(
        ProxyStatusResponse {
            listening: false,
            listen_addr: None,
            upstream: None,
            active_connections: 0,
            broker_mappings: Vec::new(),
        },
        |s| ProxyStatusResponse {
            listening: true,
            listen_addr: Some(s.listen_addr),
            upstream: Some(s.upstream),
            active_connections: s.active_connections,
            broker_mappings: s.broker_mappings,
        },
    )
}

#[tool_handler]
impl ServerHandler for KaptureMcp {
    fn get_info(&self) -> ServerInfo {
        // Deliberately bare `enable_resources()` — we DO NOT advertise
        // `subscribe` (no push channel implemented) nor `listChanged` (the
        // resource list is static; only the *content* changes as messages
        // flow). Agents poll read_resource at their own cadence; the bearer
        // token middleware is the rate gate.
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_instructions(
            "Kapture — Wireshark-for-Kafka. Tools steer the live capture; \
             resources expose read-only views of the same data.",
        )
    }

    async fn list_resources(
        &self,
        _params: Option<PaginatedRequestParams>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        // No pagination: three resources, fit in a single page.
        let stats = RawResource::new(RESOURCE_STATS_URI, "Capture stats")
            .with_description(
                "Current ring-buffer stats + throughput. Same payload as \
                 the kafka_stats tool.",
            )
            .with_mime_type("application/json")
            .no_annotation();
        let messages = RawResource::new(RESOURCE_MESSAGES_URI, "Recent messages")
            .with_description(
                "Up to the server hard limit of recent messages with the \
                 active filter applied. Same payload as kafka_snapshot \
                 called without an explicit limit.",
            )
            .with_mime_type("application/json")
            .no_annotation();
        let protocol = RawResource::new(RESOURCE_PROTOCOL_URI, "Recent protocol frames")
            .with_description(
                "Up to the server hard limit of recent Kafka API protocol \
                 frames as lightweight summaries (no payload bytes, no \
                 decoded body). Same payload as kafka_proto_frames called \
                 without an explicit limit. Use kafka_inspect_frame to \
                 fetch the full payload + decoded fields for one id.",
            )
            .with_mime_type("application/json")
            .no_annotation();
        Ok(ListResourcesResult::with_all_items(vec![
            stats, messages, protocol,
        ]))
    }

    async fn read_resource(
        &self,
        params: ReadResourceRequestParams,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        // Reuse the exact tool implementations so resources and tools cannot
        // drift. The tools take Parameters wrappers; we feed them the same
        // empty / default params an agent would use.
        let body = match params.uri.as_str() {
            RESOURCE_STATS_URI => {
                let Json(payload) = self.kafka_stats(Parameters(EmptyParams::default()))?;
                serde_json::to_string(&payload)
                    .map_err(|err| ErrorData::internal_error(err.to_string(), None))?
            }
            RESOURCE_MESSAGES_URI => {
                let Json(payload) =
                    self.kafka_snapshot(Parameters(SnapshotParams { limit: None }))?;
                serde_json::to_string(&payload)
                    .map_err(|err| ErrorData::internal_error(err.to_string(), None))?
            }
            RESOURCE_PROTOCOL_URI => {
                let Json(payload) =
                    self.kafka_proto_frames(Parameters(ProtoFramesParams { limit: None }))?;
                serde_json::to_string(&payload)
                    .map_err(|err| ErrorData::internal_error(err.to_string(), None))?
            }
            _ => {
                // Codex review: don't echo the caller-supplied URI back —
                // it ends up in JSON-RPC error logs and could carry
                // attacker-controlled junk.
                return Err(ErrorData::invalid_params("unknown resource URI", None));
            }
        };
        Ok(ReadResourceResult::new(vec![ResourceContents::text(
            body,
            &params.uri,
        )]))
    }
}

/// Redact / truncate a `ProtoFrame` before it crosses the MCP boundary.
///
/// Two protections:
///  1. **SASL credentials**. `SaslHandshake` (key 17) and
///     `SaslAuthenticate` (key 36) request payloads carry the raw SASL
///     blob — for SASL/PLAIN this is `\0username\0password` in the
///     clear. The proto-hook captures these so the GUI can show the
///     handshake locally; we MUST NOT propagate them to MCP callers
///     (an agent could echo them into a chat log).
///  2. **Decoded-body bomb**. Pretty-printed `Debug` of a Fetch
///     response with hundreds of records can balloon to MBs. Truncate
///     at `PROTO_DECODED_HARD_LIMIT` so a single `inspect_frame` call
///     can't blow up the JSON response.
fn redact_frame_for_mcp(mut f: ProtoFrame) -> ProtoFrame {
    if matches!(
        f.api_key,
        SASL_HANDSHAKE_API_KEY | SASL_AUTHENTICATE_API_KEY
    ) {
        f.payload_hex = String::new();
        f.captured = 0;
        f.decoded = Some("[redacted: SASL credentials]".to_owned());
        return f;
    }
    if let Some(d) = f.decoded.as_mut() {
        if d.len() > PROTO_DECODED_HARD_LIMIT {
            d.truncate(PROTO_DECODED_HARD_LIMIT);
            d.push_str("\n... [truncated by MCP boundary]");
        }
    }
    f
}

// ─────────────────────────── auth + bootstrap ───────────────────────────

#[derive(Clone)]
struct AuthState {
    token: Arc<String>,
}

async fn require_bearer(
    State(auth): State<AuthState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let provided = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    match provided {
        Some(t) if bool::from(t.as_bytes().ct_eq(auth.token.as_bytes())) => next.run(request).await,
        _ => {
            let mut resp = Response::new(Body::from("missing or invalid bearer token"));
            *resp.status_mut() = StatusCode::UNAUTHORIZED;
            resp
        }
    }
}

/// Read the persisted token from `<config_dir>/mcp-token`, generating
/// a fresh one if the file does not exist. The file is written with
/// `chmod 0600` on Unix; on Windows we rely on the per-user
/// `app_config_dir` ACLs.
fn ensure_token(config_dir: &std::path::Path) -> std::io::Result<String> {
    let path = config_dir.join("mcp-token");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let trimmed = existing.trim().to_owned();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }
    let token = Uuid::new_v4().simple().to_string();
    std::fs::create_dir_all(config_dir)?;
    std::fs::write(&path, &token)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(token)
}

/// Spawn the MCP server bound to `127.0.0.1:<port>`. Persists / loads
/// the bearer token from `config_dir/mcp-token`. Returns the chosen
/// port (in case 0 is requested for ephemeral binding).
pub async fn spawn(
    app_handle: tauri::AppHandle,
    port: u16,
    config_dir: PathBuf,
) -> std::io::Result<u16> {
    let token = ensure_token(&config_dir)?;
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let bound = listener.local_addr()?.port();
    info!(
        port = bound,
        token_path = %config_dir.join("mcp-token").display(),
        "mcp server listening on http://127.0.0.1:{bound}/mcp"
    );

    let factory_handle = app_handle.clone();
    let service = StreamableHttpService::new(
        move || Ok(KaptureMcp::new(factory_handle.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    );
    let auth = AuthState {
        token: Arc::new(token),
    };
    let router = axum::Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(auth, require_bearer));

    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, router).await {
            warn!(error = %err, "mcp server stopped");
        }
    });
    Ok(bound)
}

/// Default localhost port used by Kapture's MCP server.
#[must_use]
pub const fn default_port() -> u16 {
    DEFAULT_PORT
}
