import { useEffect, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import type { EbpfTapStatus, EbpfTarget } from "../types";

interface Props {
  onTapStarted: (info: { pid: number; command: string; socketPath: string }) => void;
  onTapError: (message: string) => void;
  onTapStarting: () => void;
  onCancel: () => void;
  pending: boolean;
}

export function EbpfTapDialog({
  onTapStarted,
  onTapError,
  onTapStarting,
  onCancel,
  pending,
}: Props): JSX.Element {
  const [targets, setTargets] = useState<EbpfTarget[]>([]);
  const [loading, setLoading] = useState(false);
  const [starting, setStarting] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = (): void => {
    setLoading(true);
    setError(null);
    invoke<EbpfTarget[]>("list_ebpf_targets")
      .then(setTargets)
      .catch((reason: unknown) => {
        setError(String(reason));
      })
      .finally(() => {
        setLoading(false);
      });
  };

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- process discovery is the dialog's initial data source
    refresh();
  }, []);

  const start = (target: EbpfTarget): void => {
    if (starting !== null || pending) return;
    setStarting(target.pid);
    setError(null);
    onTapStarting();
    invoke<EbpfTapStatus>("start_ebpf_tap", {
      args: { pid: target.pid, libraryPath: target.libraryPath, loaderPath: null },
    })
      .then((status) => {
        onTapStarted(status);
      })
      .catch((reason: unknown) => {
        const message = String(reason);
        setError(message);
        onTapError(message);
      })
      .finally(() => {
        setStarting(null);
      });
  };

  return (
    <div className="dialog-backdrop" onClick={onCancel} role="presentation">
      <div
        className="dialog tap-dialog"
        role="dialog"
        aria-labelledby="ebpf-dialog-title"
        onClick={(event) => {
          event.stopPropagation();
        }}
      >
        <header className="dialog__header">
          <h2 id="ebpf-dialog-title" className="dialog__title">
            Tap a Linux OpenSSL process
          </h2>
          <button type="button" className="tap-dialog__close" onClick={onCancel} aria-label="Close">
            ×
          </button>
        </header>
        <p className="dialog__hint">
          PID-scoped eBPF uprobes observe plaintext at <code>SSL_read</code>/<code>SSL_write</code>.
          Kapture checks kernel BTF, privileges and symbols before attaching; unsupported targets
          fail closed.
        </p>
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
            {targets.length} OpenSSL process{targets.length === 1 ? "" : "es"}
          </span>
        </div>
        {error !== null ? <div className="tap-dialog__error">{error}</div> : null}
        <div className="tap-dialog__list">
          {targets.length === 0 && !loading ? (
            <div className="tap-dialog__empty">
              <p>No process mapping libssl was found.</p>
              <p>
                Start the Kafka client, then refresh. Attaching may require CAP_BPF/CAP_PERFMON or
                root.
              </p>
            </div>
          ) : null}
          {targets.map((target) => (
            <div className="tap-row tap-row--kafka" key={target.pid}>
              <div className="tap-row__id">
                <span className="tap-row__pid">PID {target.pid}</span>
                <span className="tap-row__badge">openssl</span>
              </div>
              <div className="tap-row__command" title={`${target.command}\n${target.libraryPath}`}>
                {target.command}
              </div>
              <button
                type="button"
                className="btn btn--primary tap-row__attach"
                disabled={starting !== null || pending}
                onClick={() => {
                  start(target);
                }}
              >
                {starting === target.pid ? "Attaching…" : "Attach eBPF"}
              </button>
            </div>
          ))}
        </div>
        <footer className="tap-dialog__footer">
          <button type="button" className="btn" onClick={onCancel}>
            Cancel
          </button>
        </footer>
      </div>
    </div>
  );
}
