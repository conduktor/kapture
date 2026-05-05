import type { JSX } from "react";
import type { AppInfo, CaptureStats, ConnectionState } from "../types";

interface Props {
  appInfo: AppInfo | null;
  connection: ConnectionState;
  stats: CaptureStats;
}

export function SidePanel({ appInfo, connection, stats }: Props): JSX.Element {
  const fillPct = stats.bufferCapacity === 0 ? 0 : (stats.inBuffer / stats.bufferCapacity) * 100;
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
