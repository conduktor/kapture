//! MCP (Model Context Protocol) server: lets AI agents inspect and
//! steer the running Kapture capture via standardised tools.
//!
//! Mounted as a `tower::Service` under `/mcp` on a localhost-bound
//! axum router. Bound to `127.0.0.1` only, with the strict
//! `allowed_hosts` defaults from rmcp (loopback + DNS-rebinding
//! protection). Designed to share the same `AppState` as the GUI
//! so an agent and a human see the same capture in real time.
//!
//! ### Tool surface
//!
//!  * `kafka_stats` — current ring-buffer + throughput stats
//!  * `kafka_snapshot` — recent messages (filter applied, limit cap)
//!  * `kafka_set_filter` / `kafka_clear_filter` — Wireshark-style DSL
//!  * `kafka_list_profiles` / `kafka_inspect_message`
//!
//! The agent can never see SASL passwords or TLS key passwords —
//! they stay server-side in the keychain. Connect-by-profile happens
//! by name.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::ErrorData;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::tower::{
    StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::{tool, tool_router, Json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tauri::Manager;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::filter::CompiledFilter;
use crate::message::CapturedMessage;
use crate::profiles::ProfileMetadata;
use crate::ring_buffer::CaptureStats;
use crate::state::AppState;

const DEFAULT_PORT: u16 = 7878;
const SNAPSHOT_HARD_LIMIT: usize = 500;

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

#[derive(Serialize, JsonSchema)]
struct AckResponse {
    ok: bool,
    detail: String,
}

#[tool_router(server_handler = true)]
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
        description = "Return up to `limit` (cap 500) recent captured messages, with the active filter applied."
    )]
    fn kafka_snapshot(
        &self,
        Parameters(SnapshotParams { limit }): Parameters<SnapshotParams>,
    ) -> Result<Json<SnapshotResponse>, ErrorData> {
        let state = self.state()?;
        let snap = state.buffer.snapshot();
        let filtered: Vec<CapturedMessage> = match state.filter.read().as_ref() {
            Some(filter) => snap.into_iter().filter(|m| filter.matches(m)).collect(),
            None => snap,
        };
        let total = filtered.len();
        let cap = limit
            .map_or(SNAPSHOT_HARD_LIMIT, |n| {
                (n as usize).min(SNAPSHOT_HARD_LIMIT)
            })
            .min(total);
        let messages = filtered
            .into_iter()
            .rev()
            .take(cap)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        Ok(Json(SnapshotResponse {
            total,
            returned: cap,
            messages,
        }))
    }

    #[tool(description = "Install a filter (Wireshark-style DSL). Empty string clears the filter.")]
    fn kafka_set_filter(
        &self,
        Parameters(SetFilterParams { expression }): Parameters<SetFilterParams>,
    ) -> Result<Json<AckResponse>, ErrorData> {
        let state = self.state()?;
        let trimmed = expression.trim();
        if trimmed.is_empty() {
            *state.filter.write() = None;
            return Ok(Json(AckResponse {
                ok: true,
                detail: "filter cleared".into(),
            }));
        }
        let compiled = CompiledFilter::compile(trimmed)
            .map_err(|err| ErrorData::invalid_params(format!("{err}"), None))?;
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
}

/// Spawn the MCP server bound to `127.0.0.1:<port>`. Returns the
/// chosen port (in case 0 is requested for ephemeral binding).
pub async fn spawn(app_handle: tauri::AppHandle, port: u16) -> std::io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let bound = listener.local_addr()?.port();
    info!(
        port = bound,
        "mcp server listening on http://127.0.0.1:{bound}/mcp"
    );

    let factory_handle = app_handle.clone();
    let service = StreamableHttpService::new(
        move || Ok(KaptureMcp::new(factory_handle.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    );
    let router = axum::Router::new().nest_service("/mcp", service);

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
