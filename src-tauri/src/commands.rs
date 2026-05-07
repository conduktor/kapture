use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};
use tracing::info;

use serde::Deserialize;

use crate::correlator::{ProtoCorrelator, ProtoFrame, ProtoFrameSummary};
use crate::error::{KaptureError, Result};
use crate::filter::CompiledFilter;
use crate::message::{CapturedMessage, MessageSummary};
use crate::profiles::{
    AuthMetadata, LoadedProfile, ProfileMetadata, TlsMetadata, UpstreamSaslMetadata,
    UpstreamTlsMetadata,
};
use crate::proxy_upstream::{
    test_upstream, UpstreamSaslConfig, UpstreamSaslMechanism, UpstreamTlsConfig,
};
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpInfo {
    pub url: String,
    pub port: u16,
    pub token: String,
    pub token_path: String,
}

/// Return the MCP server URL + bearer token so the UI can render a
/// copy-paste-ready config snippet. The token already lives at
/// `<config_dir>/mcp-token` (created by `mcp::ensure_token` at boot);
/// here we just read it back for display. No regeneration on read —
/// the file is the source of truth.
#[tauri::command]
pub fn mcp_info(app: AppHandle) -> Result<McpInfo> {
    let config_dir = app
        .path()
        .app_config_dir()
        .map_err(|err| KaptureError::Config(format!("config dir unavailable: {err}")))?;
    let token_path = config_dir.join("mcp-token");
    let token = std::fs::read_to_string(&token_path)
        .map_err(|err| KaptureError::Config(format!("read mcp-token: {err}")))?
        .trim()
        .to_owned();
    let port = crate::mcp::default_port();
    Ok(McpInfo {
        url: format!("http://127.0.0.1:{port}/mcp"),
        port,
        token,
        token_path: token_path.to_string_lossy().into_owned(),
    })
}

fn empty_to_none(value: Option<String>) -> Option<String> {
    value.and_then(|s| if s.trim().is_empty() { None } else { Some(s) })
}

/// IPC batching cadence + chunk size. 50 ms is short enough to feel
/// live in the UI (the rAF batcher flushes in <16 ms anyway) and long
/// enough to coalesce a high-rate stream into ~1 emit per render
/// frame. 256 is the max chunk; any longer and the JSON payload
/// itself becomes the bottleneck.
const MESSAGE_BATCH_FLUSH_INTERVAL: Duration = Duration::from_millis(50);
const MESSAGE_BATCH_FLUSH_LEN: usize = 256;

/// Apply the active filter, drain `pending`, and emit one batch of
/// message summaries. The full `CapturedMessage` stays in the ring
/// buffer; the GUI fetches it lazily via `inspect_message_by_id`
/// when the user selects a row. Measured: 4 KiB payload yields a
/// 41 KiB `CapturedMessage` JSON vs ~500 B as a summary — at 3 k msg/s
/// that's the difference between 130 MB/s and 1.5 MB/s of IPC.
///
/// The vec is left empty (capacity preserved) so the caller can keep
/// reusing it.
fn emit_message_batch(
    app: &AppHandle,
    filter: &Arc<parking_lot::RwLock<Option<crate::filter::CompiledFilter>>>,
    pending: &mut Vec<crate::message::CapturedMessage>,
) {
    if pending.is_empty() {
        return;
    }
    let summaries: Vec<MessageSummary> = {
        // Brief read-lock on the filter; clone the optional Arc once
        // to avoid holding the lock while we walk the batch.
        let guard = filter.read();
        let f = guard.as_ref().cloned();
        drop(guard);
        pending
            .drain(..)
            .filter(|m| f.as_ref().is_none_or(|f| f.matches(m)))
            .map(|m| MessageSummary::from_full(&m))
            .collect()
    };
    if !summaries.is_empty() {
        let _ = app.emit("kapture:messages", &summaries);
    }
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
    // IPC batching: per-event `emit` saturated the Tauri channel under
    // load (>5k msg/s caused producer-side latency to balloon to >5s).
    // Sink pushes into an unbounded mpsc; a batcher task drains it on
    // a 50 ms timer or when the buffer hits MESSAGE_BATCH_FLUSH_LEN —
    // whichever first — and emits a single `kapture:messages` event
    // with a Vec. Filter eval moves out of the sink hot path into the
    // batcher (sink stays as small as possible so the proxy pump's
    // tail-call latency doesn't blow up under load).
    let (msg_tx, mut msg_rx) =
        tokio::sync::mpsc::unbounded_channel::<crate::message::CapturedMessage>();
    let sink: crate::proxy_handle::RecordSink = Arc::new(move |message| {
        buffer.push(message.clone());
        // unbounded send only fails when the receiver is gone (proxy
        // stopped) — at that point dropping the message is correct.
        let _ = msg_tx.send(message);
    });
    let app_for_batcher = app.clone();
    let filter_for_batcher = Arc::clone(&filter);
    tauri::async_runtime::spawn(async move {
        let mut pending: Vec<crate::message::CapturedMessage> =
            Vec::with_capacity(MESSAGE_BATCH_FLUSH_LEN);
        let mut flush_timer = tokio::time::interval(MESSAGE_BATCH_FLUSH_INTERVAL);
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                // Drain whatever is on the channel right now into
                // `pending`. `recv_many` is bounded by capacity; we
                // top up to BATCH_FLUSH_LEN so the agent / UI never
                // sees an unbounded vector.
                n = msg_rx.recv_many(&mut pending, MESSAGE_BATCH_FLUSH_LEN) => {
                    if n == 0 {
                        // channel closed — stop_proxy dropped the sender.
                        if !pending.is_empty() {
                            emit_message_batch(&app_for_batcher, &filter_for_batcher, &mut pending);
                        }
                        break;
                    }
                    if pending.len() >= MESSAGE_BATCH_FLUSH_LEN {
                        emit_message_batch(&app_for_batcher, &filter_for_batcher, &mut pending);
                    }
                }
                _ = flush_timer.tick() => {
                    if !pending.is_empty() {
                        emit_message_batch(&app_for_batcher, &filter_for_batcher, &mut pending);
                    }
                }
            }
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

/// Result of [`test_proxy_upstream`]. Reported back to the dialog
/// so the user gets a one-line OK/FAIL with handshake latency before
/// committing to a full `start_proxy`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestUpstreamResult {
    pub ok: bool,
    pub latency_ms: f64,
    pub message: String,
    /// `Some(n)` when the broker replied to `ApiVersions` with `error_code == 0`.
    /// `None` on any failure (connect, TLS, SASL, decode).
    pub api_versions_count: Option<usize>,
}

/// Connect + handshake timeout for the Test button. A hung TLS or SASL
/// handshake against an unreachable broker must not lock up the UI;
/// 5s is generous for a healthy broker on the same continent.
const TEST_UPSTREAM_TIMEOUT: Duration = Duration::from_secs(5);

/// Probe the upstream the same way [`start_proxy`] would: open TCP,
/// optionally TLS-wrap, optionally run SASL, then exchange one
/// `ApiVersionsRequest` v3 and close. Fully ephemeral — does **not**
/// claim the proxy slot, opens no listening sockets, mutates no
/// `AppState`.
#[tauri::command]
pub async fn test_proxy_upstream(
    upstream: String,
    upstream_tls: Option<ProxyTlsArgs>,
    upstream_sasl: Option<ProxySaslArgs>,
) -> TestUpstreamResult {
    let trimmed = upstream.trim().to_owned();
    if trimmed.is_empty() {
        return TestUpstreamResult {
            ok: false,
            latency_ms: 0.0,
            message: "upstream must be non-empty".to_owned(),
            api_versions_count: None,
        };
    }
    let (host, port) = match split_host_port(&trimmed) {
        Ok(parts) => parts,
        Err(message) => {
            return TestUpstreamResult {
                ok: false,
                latency_ms: 0.0,
                message,
                api_versions_count: None,
            };
        }
    };
    // Apply the same `server_name` fallback the proxy uses so the
    // probe behaves identically when the user leaves SNI blank.
    let tls_cfg = upstream_tls.map(|t| {
        let cfg = t.into_config();
        crate::proxy_upstream::resolve_server_name(&host, &cfg)
    });
    let sasl_cfg = match upstream_sasl.map(ProxySaslArgs::into_config).transpose() {
        Ok(c) => c,
        Err(err) => {
            return TestUpstreamResult {
                ok: false,
                latency_ms: 0.0,
                message: err.to_string(),
                api_versions_count: None,
            };
        }
    };
    let started = std::time::Instant::now();
    let probe = tokio::time::timeout(
        TEST_UPSTREAM_TIMEOUT,
        test_upstream(&host, port, tls_cfg.as_ref(), sasl_cfg.as_ref()),
    )
    .await;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    match probe {
        Ok(Ok(outcome)) => TestUpstreamResult {
            ok: true,
            latency_ms: elapsed_ms,
            message: format!("ApiVersions v{} OK", outcome.api_versions_version),
            api_versions_count: Some(outcome.api_versions_count),
        },
        Ok(Err(err)) => TestUpstreamResult {
            ok: false,
            latency_ms: elapsed_ms,
            message: err.to_string(),
            api_versions_count: None,
        },
        Err(_) => TestUpstreamResult {
            ok: false,
            latency_ms: elapsed_ms,
            message: format!("timed out after {}s", TEST_UPSTREAM_TIMEOUT.as_secs()),
            api_versions_count: None,
        },
    }
}

/// Split `host:port`. Accepts bare IPv4 / DNS names; rejects IPv6
/// literals (the proxy does not ship IPv6 support — same constraint
/// as `start_proxy`'s upstream parser, but the proxy never explicitly
/// validated this either, so any IPv6 input falls through here).
fn split_host_port(addr: &str) -> std::result::Result<(String, u16), String> {
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| format!("upstream `{addr}` missing :port"))?;
    let port: u16 = port
        .parse()
        .map_err(|_| format!("upstream `{addr}` has invalid port"))?;
    let host = host.trim();
    if host.is_empty() {
        return Err(format!("upstream `{addr}` missing host"));
    }
    Ok((host.to_owned(), port))
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
/// up to `limit` (cap 5000) entries, oldest first. Empty when no
/// capture is running. Cap aligned with `UI_MAX_MESSAGES` so both
/// tabs show the same recent-history depth.
#[tauri::command]
pub fn proto_frames(state: State<'_, AppState>, limit: Option<u32>) -> Vec<ProtoFrameSummary> {
    let cap = limit.map_or(5000_usize, |n| (n as usize).min(5000));
    state
        .correlator()
        .map(|c| c.summaries(cap))
        .unwrap_or_default()
}

/// Full frame (summary + captured bytes + decoded body) for one id.
/// Used by the UI when the user selects a row in the Protocol list —
/// avoids paying for the heavy fields on every poll.
///
/// When the UI is paused, the pinned-frames map (snapshotted at pause
/// time) is consulted first so a row the user can still see in the
/// frozen list resolves even after the live ring evicts it.
#[tauri::command]
pub fn proto_frame_detail(state: State<'_, AppState>, id: String) -> Option<ProtoFrame> {
    if state.is_paused() {
        if let Some(frame) = state.pinned_proto_frame(&id) {
            return Some(frame);
        }
    }
    state.correlator().and_then(|c| c.frame_detail(&id))
}

#[tauri::command]
pub fn snapshot(state: State<'_, AppState>) -> Vec<MessageSummary> {
    let snap = state.buffer.snapshot();
    let filter = state.filter.read().clone();
    snap.into_iter()
        .filter(|m| filter.as_ref().is_none_or(|f| f.matches(m)))
        .map(|m| MessageSummary::from_full(&m))
        .collect()
}

/// Return the full `CapturedMessage` for one id (payload, `raw_hex`,
/// headers — everything the live event omits). Returns `None` if the
/// message has aged out of the ring buffer. Called by the UI when
/// the user selects a row, so `LayerTree` / `HexDump` can render the
/// heavy fields lazily.
///
/// When the UI is paused, the pinned-messages map (snapshotted at
/// pause time) is consulted first so a row the user can still see in
/// the frozen list resolves even after the live ring evicts it.
#[tauri::command]
pub fn inspect_message_by_id(state: State<'_, AppState>, id: String) -> Option<CapturedMessage> {
    if state.is_paused() {
        if let Some(message) = state.pinned_message(&id) {
            return Some(message);
        }
    }
    state.buffer.find_by_id(&id)
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

/// Wipe BOTH the captured-message ring buffer AND the protocol frame
/// ring buffer. Used by the GUI's "Clear" button so the user can
/// reset to an empty list before testing a new scenario without
/// restarting the proxy. The proxy keeps running; only the
/// observation history is dropped.
#[tauri::command]
pub fn clear_capture(state: State<'_, AppState>) {
    state.buffer.clear();
    if let Some(correlator) = state.correlator() {
        correlator.clear();
    }
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
    /// Proxy-mode upstream TLS (saved from the connection dialog).
    /// `None` to clear / not record TLS for this profile.
    #[serde(default)]
    pub upstream_tls: Option<SaveProfileUpstreamTls>,
    /// Proxy-mode upstream SASL.
    #[serde(default)]
    pub upstream_sasl: Option<SaveProfileUpstreamSasl>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveProfileUpstreamTls {
    /// SNI / cert hostname; empty string means "derive from upstream".
    #[serde(default)]
    pub server_name: String,
    pub ca_path: Option<String>,
    #[serde(default)]
    pub skip_hostname_verification: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveProfileUpstreamSasl {
    pub mechanism: String,
    pub username: String,
    /// `Some(secret)` to set/replace the keychain entry, `Some("")`
    /// to clear it, `None` to leave any existing entry untouched.
    pub password: Option<String>,
}

impl std::fmt::Debug for SaveProfileUpstreamSasl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaveProfileUpstreamSasl")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
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

/// Toggle the user-driven UI pause. On `true`, snapshot both ring
/// buffers (messages + proto frames) into pinned maps so detail
/// lookups for rows the user is staring at keep resolving even after
/// the live ring evicts them. On `false`, drop the snapshots — back
/// to live mode, the rings are again the source of truth.
#[tauri::command]
pub fn set_capture_paused(state: State<'_, AppState>, paused: bool) {
    if paused {
        let messages: std::collections::HashMap<String, CapturedMessage> = state
            .buffer
            .snapshot()
            .into_iter()
            .map(|m| (m.id.clone(), m))
            .collect();
        state.set_pinned_messages(Some(messages));

        let frames: std::collections::HashMap<String, ProtoFrame> = state
            .correlator()
            .map(|c| {
                c.frames_snapshot()
                    .into_iter()
                    .map(|f| (f.id.clone(), f))
                    .collect()
            })
            .unwrap_or_default();
        state.set_pinned_proto_frames(Some(frames));
    } else {
        state.set_pinned_messages(None);
        state.set_pinned_proto_frames(None);
    }
    state.set_paused(paused);
}

/// Mirror of `mcp_connect_allowed` for the pause flag — lets the GUI
/// reconcile its local toggle with backend truth on reload.
#[tauri::command]
pub fn capture_paused(state: State<'_, AppState>) -> bool {
    state.is_paused()
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
    let upstream_tls = args.upstream_tls.as_ref().map(|t| UpstreamTlsMetadata {
        server_name: t.server_name.clone(),
        ca_path: empty_to_none(t.ca_path.clone()),
        skip_hostname_verification: t.skip_hostname_verification,
    });
    let upstream_sasl = args.upstream_sasl.as_ref().map(|s| UpstreamSaslMetadata {
        mechanism: s.mechanism.clone(),
        username: s.username.clone(),
        // overwritten by ProfileStore::save
        has_password: false,
    });
    let meta = ProfileMetadata {
        name: args.name,
        bootstrap_servers: args.bootstrap_servers,
        topic_pattern: args.topic_pattern,
        schema_registry_url: args.schema_registry_url,
        auth,
        from_beginning: args.from_beginning,
        upstream_tls,
        upstream_sasl,
    };
    let mut sasl_password: Option<String> = None;
    let mut key_password: Option<String> = None;
    if let Some(a) = args.auth {
        sasl_password = a.password;
        if let Some(t) = a.tls {
            key_password = t.key_password;
        }
    }
    let upstream_sasl_password = args.upstream_sasl.and_then(|s| s.password);
    Ok(state
        .profiles
        .save(meta, sasl_password, key_password, upstream_sasl_password)?)
}

fn spawn_stats_emitter(app: &AppHandle) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        // Rolling baseline so we can derive both rates as deltas
        // between consecutive ticks. Earlier the throughput was a
        // since-start average (total / elapsed), which lied when
        // traffic stopped — the bar would still show ~300 msg/s
        // long after the producer terminated. Both rates now decay
        // to 0 within one tick when nothing arrives.
        let mut last_total: u64 = 0;
        let mut last_drops: u64 = 0;
        let mut last_tick = std::time::Instant::now();
        loop {
            interval.tick().await;
            let Some(state) = app.try_state::<AppState>() else {
                break;
            };
            if !state.is_capturing() {
                break;
            }
            let snapshot = state.buffer.stats(0.0);
            let now = std::time::Instant::now();
            let dt = now.duration_since(last_tick).as_secs_f64().max(0.001);
            let total_delta = snapshot.total_received.saturating_sub(last_total);
            let drops_delta = snapshot.drops.saturating_sub(last_drops);
            #[allow(clippy::cast_precision_loss)]
            let throughput = total_delta as f64 / dt;
            #[allow(clippy::cast_precision_loss)]
            let drops_per_sec = drops_delta as f64 / dt;
            last_total = snapshot.total_received;
            last_drops = snapshot.drops;
            last_tick = now;
            let stats = state
                .buffer
                .stats_with_drops_rate(throughput, drops_per_sec);
            let _ = app.emit("kapture:stats", &stats);
        }
    });
}
