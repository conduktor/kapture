mod avro;
mod capture;
mod commands;
mod decode;
mod error;
mod filter;
mod message;
mod proto_hook;
mod ring_buffer;
mod schema_registry;
mod state;

use tauri::Manager;
use tracing_subscriber::EnvFilter;

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
            app.manage(state::AppState::new());
            Ok(())
        })
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            commands::app_info,
            commands::connect,
            commands::disconnect,
            commands::snapshot,
            commands::stats,
            commands::clear_buffer,
            commands::set_filter,
        ])
        .run(tauri::generate_context!());

    if let Err(error) = result {
        eprintln!("fatal: tauri runtime error: {error}");
        std::process::exit(1);
    }
}
