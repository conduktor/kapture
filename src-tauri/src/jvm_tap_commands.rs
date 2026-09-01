//! Tauri commands for the JVM tap mode lifecycle + picker UI.
//!
//! Lifted out of `commands.rs` to keep that file under the project's
//! 1000-line budget. Exposes:
//!   * `start_jvm_tap` / `stop_jvm_tap` — capture-session lifecycle
//!     (mutually exclusive with `start_proxy` via the shared
//!     capture slot in `AppState`).
//!   * `list_local_jvms` — frontend picker source.
//!   * `attach_jvm_tap_agent` — dynamic-attach injection via the
//!     `Attacher` Main-Class shipped in the agent JAR.

use serde::Deserialize;
use tauri::path::BaseDirectory;
use tauri::{AppHandle, Manager, State};

use crate::error::{KaptureError, Result};
use crate::jvm_processes::{self, AttachResult, JvmProcess};
use crate::state::AppState;

// Tap lifecycle (start/stop/attach) is Unix-only — the transport is a
// Unix domain socket. These imports back the real implementations; on
// Windows the commands are stubbed below.
#[cfg(unix)]
use crate::commands::pin_capture_snapshot;
#[cfg(unix)]
use crate::correlator::ProtoCorrelator;
#[cfg(unix)]
use crate::jvm_tap::{JvmTapConfig, JvmTapHandle};
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use tracing::info;

/// Message returned by the tap commands on platforms without Unix
/// domain sockets (Windows).
#[cfg(not(unix))]
const JVM_TAP_UNSUPPORTED: &str = "JVM tap mode is only available on macOS and Linux";

/// Default Unix-domain-socket path the tap listener binds to. The
/// matching default in the Java agent's `TapPublisher.SOCKET_PATH`
/// constant means a user can start `kapture start_jvm_tap` and then
/// `java -javaagent:agents/jvm-tap/target/kapture-jvm-agent.jar ...`
/// with no extra wiring.
#[cfg(unix)]
const DEFAULT_JVM_TAP_SOCKET: &str = "/tmp/kapture-tap.sock";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartJvmTapArgs {
    /// Override the UDS path. `None` falls back to the agent's default
    /// (`/tmp/kapture-tap.sock`). Useful for tests that need an
    /// isolated socket per parallel run.
    pub socket_path: Option<String>,
}

/// Start a JVM tap session listening on a Unix domain socket. The
/// Kapture JVM agent (`agents/jvm-tap`) attaches inside a Java Kafka
/// client process via `-javaagent` (or dynamic attach via
/// `attach_jvm_tap_agent`) and streams plaintext Kafka wire bytes
/// back through this socket. Frames decode into the same
/// `ProtoCorrelator` the proxy mode populates — so Protocol /
/// Messages / Expert tabs render identically regardless of source.
///
/// Returns `AlreadyJvmTapping` if a proxy or tap is already running:
/// the two modes share the single capture slot.
#[cfg(unix)]
#[tauri::command]
pub async fn start_jvm_tap(
    state: State<'_, AppState>,
    args: Option<StartJvmTapArgs>,
) -> Result<String> {
    if !state.try_claim_proxy_slot() {
        return Err(KaptureError::AlreadyJvmTapping);
    }

    let socket_path = args
        .and_then(|a| a.socket_path)
        .unwrap_or_else(|| DEFAULT_JVM_TAP_SOCKET.to_owned());
    let config = JvmTapConfig::new(socket_path);
    let correlator = Arc::new(ProtoCorrelator::with_config(state.detector_config()));

    let handle = match JvmTapHandle::start(config, Arc::clone(&correlator)).await {
        Ok(h) => h,
        Err(err) => {
            state.release_proxy_slot();
            return Err(KaptureError::JvmTap(err.to_string()));
        }
    };

    let path = handle.socket_path().display().to_string();
    state.install_jvm_tap(handle, correlator);
    info!(socket = %path, "jvm-tap started");
    Ok(path)
}

/// Windows stub — JVM tap requires Unix domain sockets.
#[cfg(not(unix))]
#[tauri::command]
pub async fn start_jvm_tap(
    _state: State<'_, AppState>,
    _args: Option<StartJvmTapArgs>,
) -> Result<String> {
    Err(KaptureError::JvmTap(JVM_TAP_UNSUPPORTED.into()))
}

/// Stop the running JVM tap session, pin the current capture so
/// detail clicks on still-visible rows keep resolving, and remove
/// the socket file. Returns `NotJvmTapping` if no session is active.
#[cfg(unix)]
#[tauri::command]
pub async fn stop_jvm_tap(state: State<'_, AppState>) -> Result<()> {
    // Gate on is_jvm_tapping FIRST so we don't pin a snapshot when
    // no tap is running. The earlier order always pinned, which
    // overwrote any snapshot left by a prior `stop_proxy` /
    // `stop_jvm_tap` with the current (possibly empty) ring.
    // Pin BEFORE take so `pin_capture_snapshot` still sees the
    // active correlator (`take_jvm_tap` drops it).
    if !state.is_jvm_tapping() {
        return Err(KaptureError::NotJvmTapping);
    }
    pin_capture_snapshot(&state);
    let Some(handle) = state.take_jvm_tap() else {
        // Lost the race against another concurrent stop call.
        return Err(KaptureError::NotJvmTapping);
    };
    handle.stop().await;
    info!("jvm-tap stopped");
    Ok(())
}

/// Windows stub — JVM tap requires Unix domain sockets.
#[cfg(not(unix))]
#[tauri::command]
pub async fn stop_jvm_tap(_state: State<'_, AppState>) -> Result<()> {
    Err(KaptureError::JvmTap(JVM_TAP_UNSUPPORTED.into()))
}

/// List local Java processes for the JVM tap picker. Surfaces every
/// running `java` command on the host, sorted with likely Kafka
/// clients first (detected best-effort via `lsof` against ports
/// 9092/9093/9094).
#[tauri::command]
pub fn list_local_jvms() -> Result<Vec<JvmProcess>> {
    jvm_processes::list_local_jvms().map_err(KaptureError::JvmTap)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachJvmTapAgentArgs {
    /// Target JVM PID, surfaced by `list_local_jvms`.
    pub pid: u32,
    /// Optional override for the agent JAR path. Packaged builds use
    /// the bundled resource; development falls back to Maven's target.
    #[serde(default)]
    pub agent_jar_path: Option<String>,
}

/// Dynamically attach the JVM tap agent to a running target JVM
/// using the JDK Attach API (`com.sun.tools.attach.VirtualMachine`).
/// The target's `agentmain` installs the bytecode hooks and frames
/// start flowing into the active tap session (must already be
/// started via `start_jvm_tap`).
///
/// Returns an `AttachResult` with `success: bool` + `log: String`
/// so the UI can show the JDK attach error verbatim on failure
/// (target uses Conscrypt, attach disabled with
/// `-XX:+DisableAttachMechanism`, JRE-only install, wrong UID).
#[cfg(unix)]
#[tauri::command]
pub async fn attach_jvm_tap_agent(
    app: AppHandle,
    state: State<'_, AppState>,
    args: AttachJvmTapAgentArgs,
) -> Result<AttachResult> {
    if !state.is_jvm_tapping() {
        return Err(KaptureError::NotJvmTapping);
    }
    let agent_jar = args
        .agent_jar_path
        .map_or_else(|| default_agent_jar_path(&app), PathBuf::from);

    // Recover the listener socket so the agent talks back to us, not
    // to the default global path. The handle owns the path; we look
    // it up while holding the inner lock briefly.
    let socket_path = state
        .jvm_tap_socket_path()
        .ok_or(KaptureError::NotJvmTapping)?;

    let pid = args.pid;
    let result = tokio::task::spawn_blocking(move || {
        jvm_processes::attach_jvm_tap_agent(pid, agent_jar, socket_path)
    })
    .await
    .map_err(|e| KaptureError::JvmTap(format!("attach task join: {e}")))?
    .map_err(KaptureError::JvmTap)?;
    Ok(result)
}

/// Windows stub — JVM tap requires Unix domain sockets.
#[cfg(not(unix))]
#[tauri::command]
pub async fn attach_jvm_tap_agent(
    _app: AppHandle,
    _state: State<'_, AppState>,
    _args: AttachJvmTapAgentArgs,
) -> Result<AttachResult> {
    Err(KaptureError::JvmTap(JVM_TAP_UNSUPPORTED.into()))
}

/// Best-effort default for the agent JAR location. Used by the
/// `attach_jvm_tap_agent` command when no override is provided.
#[cfg(unix)]
fn default_agent_jar_path(app: &AppHandle) -> PathBuf {
    let bundled = app
        .path()
        .resolve("resources/kapture-jvm-agent.jar", BaseDirectory::Resource)
        .ok();
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().unwrap_or(&manifest_dir).to_path_buf();
    let development = repo_root.join("agents/jvm-tap/target/kapture-jvm-agent.jar");
    prefer_bundled_file(bundled, development)
}

#[cfg(unix)]
fn prefer_bundled_file(bundled: Option<PathBuf>, development: PathBuf) -> PathBuf {
    bundled.filter(|path| path.is_file()).unwrap_or(development)
}

#[cfg(all(test, unix))]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::prefer_bundled_file;

    #[test]
    fn bundled_agent_wins_when_present() {
        let directory = tempfile::tempdir().unwrap();
        let bundled = directory.path().join("kapture-jvm-agent.jar");
        std::fs::write(&bundled, b"agent").unwrap();
        let development = directory.path().join("development.jar");

        assert_eq!(
            prefer_bundled_file(Some(bundled.clone()), development),
            bundled
        );
    }

    #[test]
    fn development_agent_is_the_fallback() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing.jar");
        let development = directory.path().join("development.jar");

        assert_eq!(
            prefer_bundled_file(Some(missing), development.clone()),
            development
        );
    }
}
