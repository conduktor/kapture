import { useEffect, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import type { AttachResult, JvmProcess } from "../types";

/** Named no-op so we can pass it to .catch() without tripping the
 * `@typescript-eslint/no-empty-function` rule on inline `() => {}`.
 * Used for best-effort cleanup paths where the failure is already
 * accounted for in the outer flow. */
function swallow(_err: unknown): void {
  // intentionally empty
}

interface Props {
  /** Called after a successful `start_jvm_tap` + `attach_jvm_tap_agent`
   * pair so the parent can flip the connection state to "connected"
   * and show the tap-active cluster pill. */
  onTapStarted: (info: { pid: number; command: string; socketPath: string }) => void;
  onTapError: (message: string) => void;
  onTapStarting: () => void;
  onCancel: () => void;
  pending: boolean;
}

/**
 * JVM tap picker. Lists local Java processes (via `list_local_jvms`),
 * lets the user click one, then:
 *
 *   1. starts a tap listener on a per-session UDS path
 *   2. dynamic-attaches the agent JAR to the picked PID
 *      (`com.sun.tools.attach.VirtualMachine.attach(pid).loadAgent(jar)`)
 *
 * On success, frames flow into the same Protocol / Messages / Expert
 * tabs as proxy mode — the inspector decoder doesn't know or care
 * which source provided the bytes.
 *
 * The dialog refreshes the process list on mount and on user request,
 * not on a timer: a Kafka client process rarely appears mid-debug, and
 * polling would log noise into `lsof`.
 */
export function TapDialog({
  onTapStarted,
  onTapError,
  onTapStarting,
  onCancel,
  pending,
}: Props): JSX.Element {
  const [processes, setProcesses] = useState<JvmProcess[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [attaching, setAttaching] = useState<number | null>(null);
  const [attachLog, setAttachLog] = useState<string | null>(null);

  const refresh = (): void => {
    setLoading(true);
    setError(null);
    invoke<JvmProcess[]>("list_local_jvms")
      .then((rows) => {
        setProcesses(rows);
      })
      .catch((err: unknown) => {
        setError(String(err));
      })
      .finally(() => {
        setLoading(false);
      });
  };

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- initial picker scan must run on mount; the alternative (force the user to click Refresh first) is worse UX than the rule guards against
    refresh();
  }, []);

  const attach = (proc: JvmProcess): void => {
    if (attaching !== null || pending) return;
    setAttaching(proc.pid);
    setAttachLog(null);
    setError(null);
    onTapStarting();
    // 1. start the listener — yields the socket path we then feed
    //    the JDK attacher so the agent talks back to OUR listener.
    invoke<string>("start_jvm_tap", { args: { socketPath: null } })
      .then((socketPath) =>
        invoke<AttachResult>("attach_jvm_tap_agent", {
          args: { pid: proc.pid, agentJarPath: null },
        }).then((res) => ({ socketPath, attach: res })),
      )
      .then(({ socketPath, attach: res }) => {
        if (!res.success) {
          // Attach failed — tear the listener down so the next
          // attempt starts clean. The user sees the attacher log.
          setAttachLog(res.log);
          return invoke("stop_jvm_tap")
            .catch(swallow)
            .then(() => {
              onTapError("Attach failed — see log below");
            });
        }
        onTapStarted({
          pid: proc.pid,
          command: proc.command,
          socketPath,
        });
        return undefined;
      })
      .catch((err: unknown) => {
        const msg = String(err);
        setError(msg);
        onTapError(msg);
        // Best-effort cleanup so the slot is free for retry.
        invoke("stop_jvm_tap").catch(swallow);
      })
      .finally(() => {
        setAttaching(null);
      });
  };

  return (
    <div className="dialog-backdrop" onClick={onCancel} role="presentation">
      <div
        className="dialog tap-dialog"
        role="dialog"
        aria-labelledby="tap-dialog-title"
        onClick={(e) => {
          e.stopPropagation();
        }}
      >
        <header className="dialog__header">
          <h2 id="tap-dialog-title" className="dialog__title">
            Tap a JVM process
          </h2>
          <button type="button" className="tap-dialog__close" onClick={onCancel} aria-label="Close">
            ×
          </button>
        </header>

        <div>
          <p className="dialog__hint">
            Inject the Kapture agent into a running Java Kafka client. No proxy, no cert swap — the
            TLS connection stays end-to-end with the real broker.
          </p>
        </div>

        <div className="tap-dialog__toolbar">
          <button
            type="button"
            className="btn btn--ghost"
            onClick={refresh}
            disabled={loading || pending}
          >
            {loading ? "Scanning…" : "Refresh"}
          </button>
          <span className="tap-dialog__count">
            {processes.length} Java process{processes.length === 1 ? "" : "es"}
          </span>
        </div>

        {error !== null ? <div className="tap-dialog__error">{error}</div> : null}

        <div className="tap-dialog__list">
          {processes.length === 0 && !loading ? (
            <div className="tap-dialog__empty">
              <p>No Java processes found.</p>
              <p>
                Start your Kafka client, then click <strong>Refresh</strong>. No need to pass{" "}
                <code>-javaagent</code> — Kapture injects the agent dynamically when you click
                Inject &amp; tap.
              </p>
            </div>
          ) : null}
          {processes.map((proc) => (
            <ProcessRow
              key={proc.pid}
              proc={proc}
              busy={attaching !== null}
              busyForMe={attaching === proc.pid}
              onAttach={() => {
                attach(proc);
              }}
            />
          ))}
        </div>

        {attachLog !== null ? (
          <div className="tap-dialog__log">
            <div className="tap-dialog__log-label">Attacher output:</div>
            <pre>{attachLog}</pre>
          </div>
        ) : null}

        <footer className="tap-dialog__footer">
          <button type="button" className="btn" onClick={onCancel}>
            Cancel
          </button>
        </footer>
      </div>
    </div>
  );
}

function ProcessRow({
  proc,
  busy,
  busyForMe,
  onAttach,
}: {
  proc: JvmProcess;
  busy: boolean;
  busyForMe: boolean;
  onAttach: () => void;
}): JSX.Element {
  return (
    <div className={`tap-row${proc.looksKafkaActive ? " tap-row--kafka" : ""}`}>
      <div className="tap-row__id">
        <span className="tap-row__pid">PID {proc.pid}</span>
        {proc.looksKafkaActive ? <span className="tap-row__badge">kafka</span> : null}
      </div>
      <div className="tap-row__command" title={proc.command}>
        {proc.command}
      </div>
      <button
        type="button"
        className="btn btn--primary tap-row__attach"
        onClick={onAttach}
        disabled={busy}
      >
        {busyForMe ? "Attaching…" : "Inject & tap"}
      </button>
    </div>
  );
}
