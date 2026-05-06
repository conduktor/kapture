mod avro;
mod capture;
mod commands;
mod correlator;
mod decode;
mod error;
mod filter;
mod mcp;
mod message;
mod profiles;
mod proto_decode;
mod proto_hook;
mod ring_buffer;
mod schema_registry;
mod state;

use std::sync::Arc;

use tauri::Manager;
use tracing_subscriber::EnvFilter;

use crate::profiles::ProfileStore;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .try_init();

    let result = tauri::Builder::default()
        .setup(|app| {
            // Auto-update on desktop. Mobile platforms ship through their
            // store and don't need this. tauri-plugin-process exposes
            // `relaunch()` to the renderer so the UI can apply updates.
            #[cfg(desktop)]
            {
                app.handle()
                    .plugin(tauri_plugin_updater::Builder::new().build())?;
                app.handle().plugin(tauri_plugin_process::init())?;
            }

            // Resolve the per-platform profiles directory through Tauri,
            // falling back to a process-local directory if Tauri can't
            // tell us (e.g., during certain test harnesses).
            let dir = app.path().app_config_dir().unwrap_or_else(|_| {
                dirs::config_dir()
                    .unwrap_or_else(std::env::temp_dir)
                    .join("io.kapture.app")
            });
            let store = Arc::new(ProfileStore::open(dir.clone())?);
            app.manage(state::AppState::new(store));

            // Spawn the MCP server so AI agents can drive Kapture
            // through the standardised protocol. Bound to localhost
            // only and gated behind a bearer token persisted in
            // `<config_dir>/mcp-token` — see `mcp.rs` for the
            // full threat model.
            let mcp_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                if let Err(err) = mcp::spawn(mcp_handle, mcp::default_port(), dir).await {
                    eprintln!("failed to start MCP server: {err}");
                }
            });
            Ok(())
        })
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            commands::app_info,
            commands::connect,
            commands::test_connection,
            commands::probe_localhost_brokers,
            commands::disconnect,
            commands::snapshot,
            commands::proto_frames,
            commands::stats,
            commands::clear_buffer,
            commands::set_filter,
            commands::list_profiles,
            commands::save_profile,
            commands::delete_profile,
            commands::load_profile,
            commands::set_mcp_connect_allowed,
            commands::mcp_connect_allowed,
        ])
        .run(tauri::generate_context!());

    if let Err(error) = result {
        eprintln!("fatal: tauri runtime error: {error}");
        std::process::exit(1);
    }
}
