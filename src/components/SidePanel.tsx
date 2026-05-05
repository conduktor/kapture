import { useEffect, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import type { AppInfo, CaptureStats, ConnectionState } from "../types";

interface Props {
  appInfo: AppInfo | null;
  connection: ConnectionState;
  stats: CaptureStats;
}

export function SidePanel({ appInfo, connection, stats }: Props): JSX.Element {
  const fillPct = stats.bufferCapacity === 0 ? 0 : (stats.inBuffer / stats.bufferCapacity) * 100;
  const byteFillPct =
    stats.bufferByteCapacity === 0 ? 0 : (stats.bufferBytes / stats.bufferByteCapacity) * 100;
  const [mcpAllowed, setMcpAllowed] = useState(false);

  useEffect(() => {
    void (async () => {
      try {
        const allowed = await invoke<boolean>("mcp_connect_allowed");
        setMcpAllowed(allowed);
      } catch {
        /* command may not be ready before the app is fully booted */
      }
    })();
  }, []);

  const toggleMcp = (next: boolean): void => {
    setMcpAllowed(next);
    void invoke("set_mcp_connect_allowed", { allowed: next }).catch((err: unknown) => {
      console.error("set_mcp_connect_allowed failed", err);
    });
  };

  return (
    <aside className="side" aria-label="Status panel">
      <section className="side__section">
        <h2 className="side__title">Connection</h2>
        <dl className="side__kv">
          <dt>status</dt>
          <dd data-status={connection.status}>{connection.status}</dd>
          <dt>cluster</dt>
          <dd>{connection.cluster ?? "—"}</dd>
          {connection.error ? (
            <>
              <dt>error</dt>
              <dd className="side__error">{connection.error}</dd>
            </>
          ) : null}
        </dl>
      </section>
      <section className="side__section">
        <h2 className="side__title">Capture</h2>
        <dl className="side__kv">
          <dt>received</dt>
          <dd>{stats.totalReceived.toLocaleString()}</dd>
          <dt>buffer</dt>
          <dd>
            {stats.inBuffer.toLocaleString()} / {stats.bufferCapacity.toLocaleString()}
          </dd>
          <dt>bytes</dt>
          <dd>
            {humanBytes(stats.bufferBytes)} / {humanBytes(stats.bufferByteCapacity)}
          </dd>
          <dt>drops</dt>
          <dd>{stats.drops.toLocaleString()}</dd>
          <dt>throughput</dt>
          <dd>{stats.throughputPerSec.toLocaleString()} msg/s</dd>
        </dl>
        <div
          className="side__bar"
          role="progressbar"
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={Math.round(fillPct)}
        >
          <span className="side__bar-fill" style={{ width: `${fillPct}%` }} />
        </div>
        <div
          className="side__bar"
          role="progressbar"
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={Math.round(byteFillPct)}
          title="Byte fill"
        >
          <span className="side__bar-fill" style={{ width: `${byteFillPct}%` }} />
        </div>
      </section>
      <section className="side__section">
        <h2 className="side__title">MCP</h2>
        <p className="side__note">
          Local agent endpoint at <code>http://127.0.0.1:7878/mcp</code>. Bearer token in
          <code>$XDG_CONFIG_HOME/io.kapture.app/mcp-token</code>.
        </p>
        <label className="side__check">
          <input
            type="checkbox"
            checked={mcpAllowed}
            onChange={(e) => {
              toggleMcp(e.target.checked);
            }}
          />
          <span>Allow MCP-initiated connect (kafka_connect_profile)</span>
        </label>
      </section>
      <section className="side__section side__section--meta">
        <h2 className="side__title">App</h2>
        <dl className="side__kv">
          <dt>name</dt>
          <dd>{appInfo?.name ?? "—"}</dd>
          <dt>version</dt>
          <dd>{appInfo?.version ?? "—"}</dd>
          <dt>ipc</dt>
          <dd data-status={appInfo ? "ok" : "pending"}>{appInfo ? "ok" : "pending"}</dd>
        </dl>
      </section>
    </aside>
  );
}

function humanBytes(n: number): string {
  if (n < 1024) {
    return `${n} B`;
  }
  if (n < 1024 * 1024) {
    return `${(n / 1024).toFixed(1)} KB`;
  }
  if (n < 1024 * 1024 * 1024) {
    return `${(n / 1024 / 1024).toFixed(1)} MB`;
  }
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
}
