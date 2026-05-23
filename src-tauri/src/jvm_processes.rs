//! Local JVM process discovery + dynamic-attach helper for the JVM
//! tap mode picker UI.
//!
//! Two surfaces exposed to the frontend:
//!   * `list_local_jvms()` — enumerate Java processes on this machine
//!     so the picker can list them.
//!   * `attach_jvm_tap_agent(pid)` — spawn a tiny Java helper
//!     (`Main-Class: io.kapture.tap.Attacher` inside the agent JAR)
//!     that calls `VirtualMachine.attach(pid).loadAgent(jar)` —
//!     the same dynamic-attach mechanism visualvm and async-profiler
//!     use. The target JVM's `agentmain` then installs the bytecode
//!     hooks and the existing JVM tap listener picks up the frames.

use std::path::PathBuf;
use std::process::Command;

use serde::Serialize;

/// One Java process visible to the local user. Surfaced to the
/// frontend as a row in the tap picker.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JvmProcess {
    pub pid: u32,
    /// Truncated single-line command for display (`java -jar foo.jar
    /// --bootstrap-server ...`). Long enough to recognise the app,
    /// short enough to fit in a list cell.
    pub command: String,
    /// `true` when we detected at least one TCP connection from this
    /// process to a port that smells like Kafka (9092/9093/9094 or
    /// the common alternates). Used by the picker to highlight the
    /// likely candidate. Best-effort — absence does not mean "not a
    /// Kafka client", only "we didn't see a live socket". Some
    /// platforms make this detection expensive enough that we skip
    /// it; in that case all entries return `false` here.
    pub looks_kafka_active: bool,
}

const KAFKA_PORTS: &[&str] = &["9092", "9093", "9094"];
const DISPLAY_COMMAND_MAX: usize = 200;

/// List the local Java processes (`ps` columns: `pid` + `command`,
/// filtered to lines whose argv0 ends with `/java` or whose comm
/// is `java`). Skips Kapture's own JVM helpers so the picker doesn't
/// suggest tapping a tap.
///
/// # Errors
/// Returns the underlying error string if spawning `ps` fails. An
/// empty list is a normal result (no Java processes running).
pub fn list_local_jvms() -> Result<Vec<JvmProcess>, String> {
    // -A : all users (the picker is best-effort, attach will fail
    //      anyway for a different UID — but we'd rather show the row
    //      and surface the EACCES message in the attach step than
    //      silently hide it).
    // -o pid=,command= : only the two columns we need, no header.
    let output = Command::new("ps")
        .args(["-A", "-o", "pid=,command="])
        .output()
        .map_err(|e| format!("ps spawn failed: {e}"))?;
    if !output.status.success() {
        return Err(format!("ps exited with {}", output.status));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);

    let kafka_pids = detect_kafka_pids().unwrap_or_default();

    let mut out: Vec<JvmProcess> = Vec::new();
    for line in stdout.lines() {
        let trimmed = line.trim_start();
        // Split once on whitespace — pid then the rest.
        let (pid_str, command) = match trimmed.split_once(char::is_whitespace) {
            Some((p, c)) => (p, c.trim_start()),
            None => continue,
        };
        let Ok(pid) = pid_str.parse::<u32>() else {
            continue;
        };
        if !is_java_process(command) {
            continue;
        }
        if is_kapture_internal(command) {
            continue;
        }
        let display = truncate_for_display(command);
        let looks_kafka_active = kafka_pids.contains(&pid);
        out.push(JvmProcess {
            pid,
            command: display,
            looks_kafka_active,
        });
    }
    // Likely Kafka clients first, then by PID.
    out.sort_by(|a, b| {
        b.looks_kafka_active
            .cmp(&a.looks_kafka_active)
            .then(a.pid.cmp(&b.pid))
    });
    Ok(out)
}

fn is_java_process(command: &str) -> bool {
    // Heuristic: argv0 ends with `/java` (typical OpenJDK / Temurin),
    // or contains `/java ` followed by JVM flags, OR is exactly the
    // word `java` (homebrew java symlink).
    let first = command.split_whitespace().next().unwrap_or("");
    first.ends_with("/java") || first == "java" || first.ends_with("/java.exe")
}

fn is_kapture_internal(command: &str) -> bool {
    // Skip our own attacher invocations — they're transient and
    // listing them would let the user attack themselves.
    command.contains("kapture-jvm-agent.jar attach")
}

fn truncate_for_display(command: &str) -> String {
    let mut s = command.to_string();
    if s.len() > DISPLAY_COMMAND_MAX {
        s.truncate(DISPLAY_COMMAND_MAX - 1);
        s.push('…');
    }
    s
}

/// Best-effort: find pids that have at least one ESTABLISHED TCP
/// connection to a port we associate with Kafka. We shell out to
/// `lsof -nP -iTCP -sTCP:ESTABLISHED` which works on macOS and most
/// Linux distros (procps + lsof package). Returns `None` when lsof
/// is absent — the picker falls back to "show all java procs".
fn detect_kafka_pids() -> Option<Vec<u32>> {
    let output = Command::new("lsof")
        .args(["-nP", "-iTCP", "-sTCP:ESTABLISHED"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut pids: Vec<u32> = Vec::new();
    for line in stdout.lines().skip(1) {
        // lsof columns: COMMAND  PID  USER  FD  TYPE  DEVICE  SIZE/OFF  NODE  NAME
        let mut cols = line.split_whitespace();
        let _cmd = cols.next();
        let pid_str = cols.next();
        let name = cols.last().unwrap_or("");
        let Some(pid_str) = pid_str else {
            continue;
        };
        let Ok(pid) = pid_str.parse::<u32>() else {
            continue;
        };
        if KAFKA_PORTS.iter().any(|p| {
            name.contains(&format!(":{p}-"))
                || name.ends_with(&format!(":{p}"))
                || name.contains(&format!(":{p} "))
        }) {
            pids.push(pid);
        }
    }
    pids.sort_unstable();
    pids.dedup();
    Some(pids)
}

/// Outcome of an attach attempt, surfaced to the frontend so the
/// picker can show a useful error instead of a generic spinner-of-doom.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachResult {
    pub success: bool,
    /// Concatenated stdout + stderr from the attacher invocation.
    /// Used for the diagnostic toast on failure.
    pub log: String,
}

/// Spawn `java -jar <agent_jar> attach <pid> kapture.tap.socket=<socket>`
/// and wait for completion. Returns the captured output so the UI can
/// show what went wrong (target uses Conscrypt, attach disabled, JRE
/// instead of JDK, etc.).
///
/// `agent_jar` must be the absolute path to a built
/// `kapture-jvm-agent.jar` whose manifest has `Main-Class:
/// io.kapture.tap.Attacher`. `socket_path` is the same path the
/// `JvmTapHandle` is bound to — the agent injects it into the target
/// JVM via the `-Dkapture.tap.socket=` system property when the
/// target was started, OR via the agentArgs we pass here for
/// post-start dynamic attach (parsed by `TapPublisher`).
///
/// # Errors
/// Returns a string describing the failure path. Spawn failures,
/// non-zero exits, and explicit attach errors are all surfaced.
pub fn attach_jvm_tap_agent(
    pid: u32,
    agent_jar: PathBuf,
    socket_path: PathBuf,
) -> Result<AttachResult, String> {
    if !agent_jar.exists() {
        return Err(format!(
            "agent jar not found at {}; build with: (cd agents/jvm-tap && mvn package)",
            agent_jar.display()
        ));
    }
    // Pass the socket path as agent args so TapPublisher inside the
    // target JVM picks it up via System.getProperty("kapture.tap.socket")
    // when the attach injects them.
    let agent_args = format!("kapture.tap.socket={}", socket_path.display());

    // `-Dio.kapture.tap.shaded.bytebuddy.experimental=true` covers the
    // ByteBuddy 1.14 / Java 25 gap. Until the BB bump lands, this
    // makes the attach succeed on bleeding-edge JDKs.
    let output = Command::new("java")
        .args([
            "-Dio.kapture.tap.shaded.bytebuddy.experimental=true",
            "-jar",
        ])
        .arg(&agent_jar)
        .args(["attach", &pid.to_string(), &agent_args])
        .output()
        .map_err(|e| format!("failed to spawn `java -jar`: {e}"))?;

    let mut log = String::new();
    log.push_str(&String::from_utf8_lossy(&output.stdout));
    if !output.stderr.is_empty() {
        if !log.is_empty() && !log.ends_with('\n') {
            log.push('\n');
        }
        log.push_str(&String::from_utf8_lossy(&output.stderr));
    }
    Ok(AttachResult {
        success: output.status.success(),
        log: log.trim_end().to_string(),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn is_java_process_accepts_common_forms() {
        // Tap mode is Unix-only (UDS), so we don't need to match
        // Windows path-with-spaces shapes; just the POSIX forms.
        assert!(is_java_process("/usr/bin/java -jar foo.jar"));
        assert!(is_java_process(
            "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home/bin/java"
        ));
        assert!(is_java_process("java -version"));
    }

    #[test]
    fn is_java_process_rejects_non_java() {
        assert!(!is_java_process("/usr/bin/python3 app.py"));
        assert!(!is_java_process("/usr/local/bin/node server.js"));
        assert!(!is_java_process(""));
    }

    #[test]
    fn is_kapture_internal_filters_our_own_attacher() {
        assert!(is_kapture_internal(
            "/usr/bin/java -jar /path/to/kapture-jvm-agent.jar attach 12345 args"
        ));
        assert!(!is_kapture_internal("/usr/bin/java -jar your-app.jar"));
    }

    #[test]
    fn truncate_for_display_appends_ellipsis_when_too_long() {
        let huge = "a".repeat(DISPLAY_COMMAND_MAX + 50);
        let t = truncate_for_display(&huge);
        assert!(t.chars().count() <= DISPLAY_COMMAND_MAX);
        assert!(t.ends_with('…'));
    }

    #[test]
    fn truncate_for_display_keeps_short_command_untouched() {
        let s = "/usr/bin/java -jar foo.jar";
        assert_eq!(truncate_for_display(s), s);
    }
}
