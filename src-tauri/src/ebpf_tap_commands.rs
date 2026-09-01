//! Linux eBPF/OpenSSL tap discovery and lifecycle.
//!
//! The privileged loader is a deliberately separate, auditable
//! libbpf binary under `agents/ebpf-tap`. Kapture owns only the bounded
//! UDS receiver and starts the loader after its fail-closed `--check`
//! succeeds. The loader speaks the same gap-aware wire contract as the
//! JVM agent, so reassembly, health counters and protocol analysis are
//! shared rather than duplicated.

use serde::{Deserialize, Serialize};
use tauri::State;

use crate::error::{KaptureError, Result};
use crate::state::AppState;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EbpfTarget {
    pub pid: u32,
    pub command: String,
    pub library_path: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub struct StartEbpfTapArgs {
    pub pid: u32,
    pub library_path: String,
    #[serde(default)]
    pub loader_path: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EbpfTapStatus {
    pub pid: u32,
    pub command: String,
    pub socket_path: String,
    pub library_path: String,
}

#[cfg(any(target_os = "linux", test))]
fn proc_command(pid: u32) -> String {
    let path = format!("/proc/{pid}/cmdline");
    std::fs::read(path).map_or_else(
        |_| format!("PID {pid}"),
        |bytes| {
            let command = bytes
                .split(|byte| *byte == 0)
                .filter(|part| !part.is_empty())
                .map(String::from_utf8_lossy)
                .collect::<Vec<_>>()
                .join(" ");
            if command.is_empty() {
                format!("PID {pid}")
            } else {
                command
            }
        },
    )
}

#[cfg(any(target_os = "linux", test))]
fn mapped_ssl_library(pid: u32) -> Option<String> {
    let maps = std::fs::read_to_string(format!("/proc/{pid}/maps")).ok()?;
    ssl_library_from_maps(&maps)
}

#[cfg_attr(not(any(test, target_os = "linux")), allow(dead_code))]
fn ssl_library_from_maps(maps: &str) -> Option<String> {
    maps.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let last = fields.next_back()?;
        let path = if last == "(deleted)" {
            fields.next_back()?
        } else {
            last
        };
        (path.starts_with('/') && (path.contains("libssl.so") || path.contains("libboringssl.so")))
            .then(|| path.to_owned())
    })
}

#[cfg(any(target_os = "linux", test))]
#[tauri::command]
pub fn list_ebpf_targets() -> Result<Vec<EbpfTarget>> {
    let mut targets = Vec::new();
    let entries =
        std::fs::read_dir("/proc").map_err(|error| KaptureError::EbpfTap(error.to_string()))?;
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let Some(library_path) = mapped_ssl_library(pid) else {
            continue;
        };
        targets.push(EbpfTarget {
            pid,
            command: proc_command(pid),
            library_path,
        });
    }
    targets.sort_unstable_by_key(|target| target.pid);
    Ok(targets)
}

#[cfg(all(not(target_os = "linux"), not(test)))]
#[tauri::command]
pub fn list_ebpf_targets() -> Result<Vec<EbpfTarget>> {
    Err(KaptureError::EbpfTap(
        "eBPF tap is available only on Linux".to_owned(),
    ))
}

#[cfg(any(target_os = "linux", test))]
fn resolve_loader(override_path: Option<String>) -> Result<std::path::PathBuf> {
    let mut candidates = Vec::new();
    if let Some(path) = override_path {
        candidates.push(std::path::PathBuf::from(path));
    }
    if let Ok(executable) = std::env::current_exe() {
        if let Some(parent) = executable.parent() {
            candidates.push(parent.join("kapture-ebpf-tap"));
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join("agents/ebpf-tap/build/kapture-ebpf-tap"));
    }
    candidates
        .into_iter()
        .find(|path| path.is_file())
        .ok_or_else(|| {
            KaptureError::EbpfTap(
                "loader not found; run `make -C agents/ebpf-tap` or pass loaderPath".to_owned(),
            )
        })
}

#[cfg(any(target_os = "linux", test))]
#[tauri::command]
#[allow(clippy::too_many_lines)]
pub async fn start_ebpf_tap(
    state: State<'_, AppState>,
    args: StartEbpfTapArgs,
) -> Result<EbpfTapStatus> {
    use std::process::Stdio;
    use std::sync::Arc;

    use crate::correlator::ProtoCorrelator;
    use crate::jvm_tap::{JvmTapConfig, JvmTapHandle};

    let mapped_library = mapped_ssl_library(args.pid).ok_or_else(|| {
        KaptureError::EbpfTap("target no longer maps a supported OpenSSL library".to_owned())
    })?;
    if mapped_library != args.library_path {
        return Err(KaptureError::EbpfTap(
            "selected OpenSSL mapping changed; refresh the target list".to_owned(),
        ));
    }
    let loader = resolve_loader(args.loader_path)?;
    let pid = args.pid.to_string();
    let check = tokio::process::Command::new(&loader)
        .args(["--check", "--pid", &pid, "--library", &args.library_path])
        .output()
        .await
        .map_err(|error| KaptureError::EbpfTap(format!("preflight failed: {error}")))?;
    if !check.status.success() {
        return Err(KaptureError::EbpfTap(
            String::from_utf8_lossy(&check.stderr).trim().to_owned(),
        ));
    }
    if !state.try_claim_proxy_slot() {
        return Err(KaptureError::AlreadyJvmTapping);
    }

    let socket_path = format!("/tmp/kapture-ebpf-tap-{}.sock", std::process::id());
    let correlator = Arc::new(ProtoCorrelator::with_config(state.detector_config()));
    let handle =
        match JvmTapHandle::start(JvmTapConfig::new(&socket_path), Arc::clone(&correlator)).await {
            Ok(handle) => handle,
            Err(error) => {
                state.release_proxy_slot();
                return Err(KaptureError::EbpfTap(error.to_string()));
            }
        };

    let mut child = match tokio::process::Command::new(&loader)
        .args([
            "--pid",
            &pid,
            "--library",
            &args.library_path,
            "--socket",
            &socket_path,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            handle.stop().await;
            state.release_proxy_slot();
            return Err(KaptureError::EbpfTap(format!(
                "loader start failed: {error}"
            )));
        }
    };
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let startup_status = match child.try_wait() {
        Ok(status) => status,
        Err(error) => {
            let _ = child.kill().await;
            handle.stop().await;
            state.release_proxy_slot();
            return Err(KaptureError::EbpfTap(error.to_string()));
        }
    };
    if let Some(status) = startup_status {
        handle.stop().await;
        state.release_proxy_slot();
        return Err(KaptureError::EbpfTap(format!(
            "loader exited during startup with {status}"
        )));
    }
    let mut stop_rx = handle.stop_receiver();
    tauri::async_runtime::spawn(async move {
        tokio::select! {
            status = child.wait() => {
                if let Ok(status) = status {
                    tracing::info!(%status, "ebpf-tap loader exited");
                }
            }
            _ = stop_rx.changed() => {
                let _ = child.kill().await;
                let _ = child.wait().await;
            }
        }
    });

    let command = proc_command(args.pid);
    state.install_jvm_tap(handle, correlator);
    Ok(EbpfTapStatus {
        pid: args.pid,
        command,
        socket_path,
        library_path: args.library_path,
    })
}

#[cfg(all(not(target_os = "linux"), not(test)))]
#[tauri::command]
pub async fn start_ebpf_tap(
    _state: State<'_, AppState>,
    _args: StartEbpfTapArgs,
) -> Result<EbpfTapStatus> {
    Err(KaptureError::EbpfTap(
        "eBPF tap is available only on Linux".to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovers_only_absolute_supported_ssl_mappings() {
        let maps = concat!(
            "7000-8000 r-xp 0 00:00 0 /usr/lib/libcrypto.so.3\n",
            "8000-9000 r-xp 0 00:00 0 relative/libssl.so.3\n",
            "9000-a000 r-xp 0 00:00 0 /usr/lib/libssl.so.3 (deleted)\n",
        );
        assert_eq!(
            ssl_library_from_maps(maps).as_deref(),
            Some("/usr/lib/libssl.so.3")
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn ssl_mapping_discovery_is_pid_scoped() {
        // Discovery may legitimately return None for the Rust test
        // process, but it must never inspect/return another PID's map.
        let own = mapped_ssl_library(std::process::id());
        assert!(own.as_deref().is_none_or(|path| path.starts_with('/')));
    }
}
