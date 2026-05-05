use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};
use tracing::info;

use crate::capture::{self, CaptureConfig};
use crate::correlator::ProtoCorrelator;
use crate::error::{KaptureError, Result};
use crate::filter::CompiledFilter;
use crate::message::CapturedMessage;
use crate::ring_buffer::CaptureStats;
use crate::schema_registry::SchemaRegistryClient;
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
pub struct ConnectResponse {
    pub topics: Vec<String>,
    pub bootstrap_servers: String,
}

#[tauri::command]
pub fn connect(
    state: State<'_, AppState>,
    app: AppHandle,
    bootstrap_servers: String,
    topics: Vec<String>,
    from_beginning: bool,
    schema_registry_url: Option<String>,
) -> Result<ConnectResponse> {
    if state.is_capturing() {
        return Err(KaptureError::AlreadyCapturing);
    }
    state.buffer.clear();

    let sr_client = schema_registry_url.as_ref().and_then(|url| {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(Arc::new(SchemaRegistryClient::new(trimmed.to_owned())))
        }
    });

    let config = CaptureConfig::new(bootstrap_servers.clone(), topics.clone(), from_beginning);
    let buffer = Arc::clone(&state.buffer);
    let filter = Arc::clone(&state.filter);
    let app_for_messages = app.clone();
    let correlator = Arc::new(ProtoCorrelator::new());
    let handle = capture::start(
        config,
        sr_client.clone(),
        Arc::clone(&correlator),
        move |message| {
            buffer.push(message.clone());
            let pass = filter.read().as_ref().is_none_or(|f| f.matches(&message));
            if pass {
                let _ = app_for_messages.emit("kapture:message", &message);
            }
        },
    )?;

    state.install(handle, sr_client, correlator);
    spawn_stats_emitter(&app);

    info!(
        bootstrap = %bootstrap_servers,
        topics = ?topics,
        sr = schema_registry_url.as_deref().unwrap_or("none"),
        "capture started"
    );
    Ok(ConnectResponse {
        topics,
        bootstrap_servers,
    })
}

#[tauri::command]
pub async fn disconnect(state: State<'_, AppState>) -> Result<()> {
    let Some(handle) = state.take_capture() else {
        return Err(KaptureError::NotCapturing);
    };
    handle.stop().await;
    info!("capture stopped");
    Ok(())
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
