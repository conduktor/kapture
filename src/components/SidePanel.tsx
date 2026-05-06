import { useEffect, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import type { AppInfo, CaptureStats, ConnectionState, ProxyStatusSummary } from "../types";

interface Props {
  appInfo: AppInfo | null;
  connection: ConnectionState;
  stats: CaptureStats;
}

const PROXY_POLL_MS = 1000;

export function SidePanel({ appInfo, connection, stats }: Props): JSX.Element {
  const fillPct = stats.bufferCapacity === 0 ? 0 : (stats.inBuffer / stats.bufferCapacity) * 100;
  const byteFillPct =
    stats.bufferByteCapacity === 0 ? 0 : (stats.bufferBytes / stats.bufferByteCapacity) * 100;
  const [mcpAllowed, setMcpAllowed] = useState(false);
  const [proxy, setProxy] = useState<ProxyStatusSummary | null>(null);

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

  // Poll proxy status while connected. We intentionally do NOT call
  // `setProxy(null)` from inside the effect body when the connection
  // drops (cascading renders, see the React docs `set-state-in-effect`)
  // — the section is hidden in render when `proxy?.listening` is false.
  useEffect(() => {
    if (connection.status !== "connected") {
      return undefined;
    }
    let cancelled = false;
    const tick = async (): Promise<void> => {
      try {
        const next = await invoke<ProxyStatusSummary>("proxy_status");
        if (!cancelled) {
          setProxy(next);
        }
      } catch {
        /* command may transiently fail during connect/disconnect */
      }
    };
    void tick();
    const id = window.setInterval(() => {
      void tick();
    }, PROXY_POLL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [connection.status]);

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
          <dt>upstream</dt>
          <dd>{connection.upstream ?? "—"}</dd>
          {connection.error !== null ? (
            <>
              <dt>error</dt>
              <dd className="side__error">{connection.error}</dd>
            </>
          ) : null}
        </dl>
      </section>
      {proxy?.listening === true ? (
        <section className="side__section">
          <h2 className="side__title">Proxy</h2>
          <p className="side__note">
            proxy <code>{proxy.listenAddr ?? "—"}</code> → <code>{proxy.upstream ?? "—"}</code>
          </p>
          <dl className="side__kv">
            <dt>active</dt>
            <dd>
              {proxy.activeConnections.toLocaleString()}{" "}
              {proxy.activeConnections === 1 ? "connection" : "connections"}
            </dd>
            <dt>mappings</dt>
            <dd>
              {proxy.brokerMappings.length.toLocaleString()}{" "}
              {proxy.brokerMappings.length === 1 ? "broker" : "brokers"}
              {proxy.brokerMappings.length > 0
                ? ` (${proxy.brokerMappings.map(([, localPort]) => String(localPort)).join(", ")})`
                : ""}
            </dd>
          </dl>
        </section>
      ) : null}
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
          <span>Allow MCP-initiated proxy start (kapture_set_proxy_target)</span>
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
