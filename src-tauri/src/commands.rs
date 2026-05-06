use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};
use tracing::info;

use serde::Deserialize;

use crate::correlator::{ProtoCorrelator, ProtoFrame, ProtoFrameSummary};
use crate::error::{KaptureError, Result};
use crate::filter::CompiledFilter;
use crate::message::CapturedMessage;
use crate::profiles::{AuthMetadata, LoadedProfile, ProfileMetadata, TlsMetadata};
use crate::proxy_upstream::{UpstreamSaslConfig, UpstreamSaslMechanism, UpstreamTlsConfig};
use crate::ring_buffer::CaptureStats;
use crate::state::AppState;

#[derive(Debug, Serialize)]
pub struct AppInfo {
    pub name: &'static str,
    pub version: &'static str,
    pub status: &'static str,
}

#[tauri::command]
pub const fn app_info() -> AppInfo {
    AppInfo {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        status: "ok",
    }
}

fn empty_to_none(value: Option<String>) -> Option<String> {
    value.and_then(|s| if s.trim().is_empty() { None } else { Some(s) })
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeResult {
    pub bootstrap_servers: Option<String>,
    pub schema_registry_url: Option<String>,
    pub flavour: Option<String>,
}

/// Probe well-known localhost ports for a running Kafka + Schema Registry
/// pair. Returns the first reachable broker and the matching SR if the
/// flavour is recognised. Pure TCP probe — no Kafka protocol exchange — so
/// this never authenticates and is safe to run with no user input.
#[tauri::command]
pub async fn probe_localhost_brokers() -> ProbeResult {
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    // (port, flavour-label, paired-SR-port). SR is also probed
    // independently in case it's the only piece running.
    const CANDIDATES: &[(u16, &str, u16)] = &[
        (19092, "Redpanda", 18081),
        (29092, "Apache Kafka", 28081),
        (9092, "Apache Kafka", 8081),
    ];
    const PROBE_TIMEOUT: Duration = Duration::from_millis(200);

    let mut bootstrap = None;
    let mut sr = None;
    let mut flavour = None;
    for (port, label, sr_port) in CANDIDATES {
        let broker_ok = timeout(
            PROBE_TIMEOUT,
            TcpStream::connect(format!("127.0.0.1:{port}")),
        )
        .await
        .ok()
        .and_then(std::result::Result::ok)
        .is_some();
        if broker_ok && bootstrap.is_none() {
            bootstrap = Some(format!("localhost:{port}"));
            flavour = Some((*label).to_owned());
        }
        let sr_ok = timeout(
            PROBE_TIMEOUT,
            TcpStream::connect(format!("127.0.0.1:{sr_port}")),
        )
        .await
        .ok()
        .and_then(std::result::Result::ok)
        .is_some();
        if sr_ok && sr.is_none() {
            sr = Some(format!("http://localhost:{sr_port}"));
        }
    }
    ProbeResult {
        bootstrap_servers: bootstrap,
        schema_registry_url: sr,
        flavour,
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyStatus {
    pub listen_addr: String,
    pub upstream: String,
}

/// TLS args for the upstream connection. Mirrors `UpstreamTlsConfig`
/// with serde-friendly types so the GUI can pass it through `invoke`.
#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyTlsArgs {
    /// SNI / cert hostname. Defaults to the bootstrap host parsed from
    /// `upstream` when the GUI leaves this empty (caller decides — the
    /// command takes the value as-is).
    pub server_name: String,
    pub ca_path: Option<String>,
    #[serde(default)]
    pub skip_hostname_verification: bool,
}

impl ProxyTlsArgs {
    fn into_config(self) -> UpstreamTlsConfig {
        UpstreamTlsConfig {
            server_name: self.server_name,
            ca_path: empty_to_none(self.ca_path).map(std::path::PathBuf::from),
            skip_hostname_verification: self.skip_hostname_verification,
        }
    }
}

/// SASL credentials for the upstream connection. Supports `PLAIN`,
/// `SCRAM-SHA-256`, and `SCRAM-SHA-512` (case-insensitive).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxySaslArgs {
    /// One of `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`. Case-insensitive.
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for ProxySaslArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxySaslArgs")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl ProxySaslArgs {
    fn into_config(self) -> Result<UpstreamSaslConfig> {
        let mechanism = match self.mechanism.to_uppercase().as_str() {
            "PLAIN" => UpstreamSaslMechanism::Plain,
            "SCRAM-SHA-256" => UpstreamSaslMechanism::ScramSha256,
            "SCRAM-SHA-512" => UpstreamSaslMechanism::ScramSha512,
            other => {
                return Err(KaptureError::Config(format!(
                    "unsupported upstream SASL mechanism `{other}` (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)"
                )));
            }
        };
        if self.username.is_empty() || self.password.is_empty() {
            return Err(KaptureError::Config(
                "upstream SASL username and password must be non-empty".to_owned(),
            ));
        }
        Ok(UpstreamSaslConfig {
            mechanism,
            username: self.username,
            password: self.password,
        })
    }
}

/// Start the proxy listener. Bound to `127.0.0.1:listen_port`. The
/// previous capture (if any) is stopped first so client mode and
/// proxy mode are mutually exclusive — exactly one Protocol tab at
/// a time.
#[tauri::command]
pub async fn start_proxy(
    state: State<'_, AppState>,
    app: AppHandle,
    upstream: String,
    listen_port: u16,
    upstream_tls: Option<ProxyTlsArgs>,
    upstream_sasl: Option<ProxySaslArgs>,
) -> Result<ProxyStatus> {
    let tls = upstream_tls.map(ProxyTlsArgs::into_config);
    let sasl = upstream_sasl.map(ProxySaslArgs::into_config).transpose()?;
    start_proxy_impl(&app, &state, upstream, listen_port, tls, sasl, false).await
}

/// Implementation shared between the Tauri command and the MCP
/// `kapture_set_proxy_target` tool. The argument layering mirrors
/// `start_capture_from_profile` in `mcp.rs`.
///
/// `mcp_authorized` MUST be `true` for the MCP path. When true, this
/// function re-checks `mcp_connect_allowed` immediately before claiming
/// the capture slot so a revoke that lands during the prior awaits
/// (`take_capture`, `take_proxy`) cannot be bypassed. The Tauri
/// command path passes `false` because the user clicking Connect IS
/// the explicit consent.
pub async fn start_proxy_impl(
    app: &AppHandle,
    state: &AppState,
    upstream: String,
    listen_port: u16,
    tls: Option<UpstreamTlsConfig>,
    sasl: Option<UpstreamSaslConfig>,
    mcp_authorized: bool,
) -> Result<ProxyStatus> {
    if let Some(handle) = state.take_proxy() {
        handle.stop().await;
    }
    // Re-check the MCP gate after the awaits above. A revocation that
    // lands while we were stopping the previous proxy must take
    // effect before we open new sockets. No awaits between this check
    // and `try_claim_proxy_slot` below.
    if mcp_authorized && !state.mcp_connect_allowed() {
        return Err(KaptureError::Config(
            "MCP-initiated proxy revoked before slot claim".to_owned(),
        ));
    }
    if !state.try_claim_proxy_slot() {
        return Err(KaptureError::AlreadyProxying);
    }
    state.buffer.clear();

    let trimmed_upstream = upstream.trim().to_owned();
    if trimmed_upstream.is_empty() {
        state.release_proxy_slot();
        return Err(KaptureError::Config(
            "upstream must be non-empty".to_owned(),
        ));
    }

    let correlator = Arc::new(ProtoCorrelator::new());
    let mut cfg = crate::proxy::ProxyConfig::new(trimmed_upstream.clone(), listen_port);
    cfg.upstream_tls = tls;
    cfg.upstream_sasl = sasl;
    let buffer = Arc::clone(&state.buffer);
    let filter = Arc::clone(&state.filter);
    let app_for_messages = app.clone();
    let sink: crate::proxy_handle::RecordSink = Arc::new(move |message| {
        buffer.push(message.clone());
        let pass = filter.read().as_ref().is_none_or(|f| f.matches(&message));
        if pass {
            let _ = app_for_messages.emit("kapture:message", &message);
        }
    });
    let handle = match crate::proxy::ProxyHandle::start(cfg, Arc::clone(&correlator), sink).await {
        Ok(h) => h,
        Err(err) => {
            state.release_proxy_slot();
            return Err(KaptureError::Proxy(err.to_string()));
        }
    };
    let listen_addr = handle.local_addr().to_string();
    state.install_proxy(handle, correlator);
    spawn_stats_emitter(app);
    info!(listen = %listen_addr, upstream = %trimmed_upstream, "proxy started");

    Ok(ProxyStatus {
        listen_addr,
        upstream: trimmed_upstream,
    })
}

#[tauri::command]
pub async fn stop_proxy(state: State<'_, AppState>) -> Result<()> {
    let Some(handle) = state.take_proxy() else {
        return Err(KaptureError::NotProxying);
    };
    handle.stop().await;
    info!("proxy stopped");
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyStatusSummary {
    pub listening: bool,
    pub listen_addr: Option<String>,
    pub upstream: Option<String>,
    pub active_connections: usize,
    /// `((upstream_host, upstream_port), local_port)` mapping for the
    /// `SidePanel` summary. Sorted by `local_port` so the order is
    /// stable across polls.
    pub broker_mappings: Vec<((String, u16), u16)>,
}

/// Snapshot of the running proxy. `listening: false` (with all other
/// fields zeroed / empty) when no proxy is active. Polled by the
/// `SidePanel` once per second to render the proxy summary.
#[tauri::command]
pub fn proxy_status(state: State<'_, AppState>) -> ProxyStatusSummary {
    state.proxy_summary().map_or(
        ProxyStatusSummary {
            listening: false,
            listen_addr: None,
            upstream: None,
            active_connections: 0,
            broker_mappings: Vec::new(),
        },
        |s| ProxyStatusSummary {
            listening: true,
            listen_addr: Some(s.listen_addr),
            upstream: Some(s.upstream),
            active_connections: s.active_connections,
            broker_mappings: s.broker_mappings,
        },
    )
}

/// Snapshot of recent observed Kafka protocol frames as **summaries**
/// (no payload bytes, no decoded body — those are megabyte-scale on
/// busy clusters and don't belong in the 1 Hz polling path). Returns
/// up to `limit` (cap 2000) entries, oldest first. Empty when no
/// capture is running.
#[tauri::command]
pub fn proto_frames(state: State<'_, AppState>, limit: Option<u32>) -> Vec<ProtoFrameSummary> {
    let cap = limit.map_or(2000_usize, |n| (n as usize).min(2000));
    state
        .correlator()
        .map(|c| c.summaries(cap))
        .unwrap_or_default()
}

/// Full frame (summary + captured bytes + decoded body) for one id.
/// Used by the UI when the user selects a row in the Protocol list —
/// avoids paying for the heavy fields on every poll.
#[tauri::command]
pub fn proto_frame_detail(state: State<'_, AppState>, id: String) -> Option<ProtoFrame> {
    state.correlator().and_then(|c| c.frame_detail(&id))
}

#[tauri::command]
pub fn snapshot(state: State<'_, AppState>) -> Vec<CapturedMessage> {
    let snap = state.buffer.snapshot();
    let guard = state.filter.read();
    match guard.as_ref() {
        Some(filter) => snap.into_iter().filter(|m| filter.matches(m)).collect(),
        None => snap,
    }
}

#[tauri::command]
pub fn stats(state: State<'_, AppState>) -> CaptureStats {
    let elapsed = state.elapsed_secs().max(0.001);
    let total = state.buffer.stats(0.0).total_received;
    let throughput = if elapsed > 0.0 {
        total as f64 / elapsed
    } else {
        0.0
    };
    state.buffer.stats(throughput)
}

#[tauri::command]
pub fn clear_buffer(state: State<'_, AppState>) {
    state.buffer.clear();
}

#[tauri::command]
pub fn set_filter(state: State<'_, AppState>, expression: String) -> Result<()> {
    let trimmed = expression.trim();
    if trimmed.is_empty() {
        *state.filter.write() = None;
        info!("filter cleared");
        return Ok(());
    }
    let compiled = CompiledFilter::compile(trimmed).map_err(KaptureError::Filter)?;
    *state.filter.write() = Some(compiled);
    info!(expr = trimmed, "filter installed");
    Ok(())
}

// ─────────────────────── Connection profiles ───────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveProfileArgs {
    pub name: String,
    pub bootstrap_servers: String,
    /// Optional topic regex; `None` records the default pattern intent.
    #[serde(default)]
    pub topic_pattern: Option<String>,
    pub schema_registry_url: Option<String>,
    pub auth: Option<SaveProfileAuth>,
    pub from_beginning: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveProfileAuth {
    pub mechanism: String,
    pub username: String,
    pub use_tls: bool,
    /// `Some(...)` to set or replace the SASL keychain password,
    /// `None` to leave any existing entry untouched, `Some("")`
    /// to clear it.
    pub password: Option<String>,
    /// Optional TLS metadata + key password.
    #[serde(default)]
    pub tls: Option<SaveProfileTls>,
}

impl std::fmt::Debug for SaveProfileAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaveProfileAuth")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("use_tls", &self.use_tls)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("tls", &self.tls)
            .finish()
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveProfileTls {
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// Same `Some/None/Some("")` semantics as `password`.
    pub key_password: Option<String>,
}

impl std::fmt::Debug for SaveProfileTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaveProfileTls")
            .field("ca_path", &self.ca_path)
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .field(
                "key_password",
                &self.key_password.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[tauri::command]
pub fn list_profiles(state: State<'_, AppState>) -> Vec<ProfileMetadata> {
    state.profiles.list()
}

#[tauri::command]
pub fn load_profile(state: State<'_, AppState>, name: String) -> Result<LoadedProfile> {
    Ok(state.profiles.load(&name)?)
}

#[tauri::command]
pub fn delete_profile(state: State<'_, AppState>, name: String) -> Result<()> {
    state.profiles.delete(&name)?;
    Ok(())
}

/// Allow or revoke MCP-initiated `kafka_connect_profile`. Defaults
/// to revoked at startup; the GUI exposes a toggle so the user must
/// explicitly arm agent-driven captures.
#[tauri::command]
pub fn set_mcp_connect_allowed(state: State<'_, AppState>, allowed: bool) {
    state.set_mcp_connect_allowed(allowed);
}

#[tauri::command]
pub fn mcp_connect_allowed(state: State<'_, AppState>) -> bool {
    state.mcp_connect_allowed()
}

#[tauri::command]
pub fn save_profile(state: State<'_, AppState>, args: SaveProfileArgs) -> Result<ProfileMetadata> {
    let auth = args.auth.as_ref().map(|a| {
        let tls = a.tls.as_ref().map(|t| TlsMetadata {
            ca_path: empty_to_none(t.ca_path.clone()),
            cert_path: empty_to_none(t.cert_path.clone()),
            key_path: empty_to_none(t.key_path.clone()),
            // overwritten by ProfileStore::save
            has_key_password: false,
        });
        AuthMetadata {
            mechanism: a.mechanism.clone(),
            username: a.username.clone(),
            use_tls: a.use_tls,
            // overwritten by ProfileStore::save
            has_password: false,
            tls,
        }
    });
    let meta = ProfileMetadata {
        name: args.name,
        bootstrap_servers: args.bootstrap_servers,
        topic_pattern: args.topic_pattern,
        schema_registry_url: args.schema_registry_url,
        auth,
        from_beginning: args.from_beginning,
    };
    let mut sasl_password: Option<String> = None;
    let mut key_password: Option<String> = None;
    if let Some(a) = args.auth {
        sasl_password = a.password;
        if let Some(t) = a.tls {
            key_password = t.key_password;
        }
    }
    Ok(state.profiles.save(meta, sasl_password, key_password)?)
}

fn spawn_stats_emitter(app: &AppHandle) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let Some(state) = app.try_state::<AppState>() else {
                break;
            };
            if !state.is_capturing() {
                break;
            }
            let elapsed = state.elapsed_secs().max(0.001);
            let total = state.buffer.stats(0.0).total_received;
            let throughput = total as f64 / elapsed;
            let stats = state.buffer.stats(throughput);
            let _ = app.emit("kapture:stats", &stats);
        }
    });
}
