import type { JSX } from "react";
import type { CaptureStats, ConnectionState, ProxyStatusSummary } from "../types";

interface Props {
  connection: ConnectionState;
  stats: CaptureStats;
  /** 1Hz proxy snapshot, polled at the App level and shared with BrokersTab. */
  proxy: ProxyStatusSummary | null;
}

/**
 * Thin status row pinned to the bottom of the workspace. Renders a compact,
 * monospace summary of the live proxy + capture state. Only shown when the
 * proxy is connected — disconnected we render an empty placeholder so the
 * grid row stays reserved (no layout shift on first connect).
 *
 * The proxy snapshot is owned by App.tsx (see the lifted `proxy_status`
 * poll) so a single 1 Hz tick feeds both this row and the Brokers tab.
 */
export function StatusBar({ connection, stats, proxy }: Props): JSX.Element {
  const isConnected = connection.status === "connected";
  // Empty placeholder keeps the row reserved when disconnected.
  if (!isConnected) {
    return <footer className="statusbar statusbar--empty" aria-hidden="true" />;
  }

  const listenAddr = proxy?.listenAddr ?? null;
  const upstream = proxy?.upstream ?? connection.upstream ?? null;
  const route = listenAddr !== null && upstream !== null ? `${listenAddr} → ${upstream}` : null;
  const routeTitle = route ?? undefined;

  const activeConn = proxy?.activeConnections ?? 0;
  const brokers = proxy?.brokerMappings.length ?? 0;

  return (
    <footer className="statusbar" aria-label="Proxy status bar">
      <span className="statusbar__group statusbar__group--status">
        <span className="statusbar__dot" data-status="connected" aria-hidden="true">
          ●
        </span>
        connected
      </span>
      {route !== null ? (
        <>
          <span className="statusbar__sep" aria-hidden="true">
            ·
          </span>
          <span className="statusbar__group statusbar__route" title={routeTitle}>
            {route}
          </span>
        </>
      ) : null}
      <span className="statusbar__sep" aria-hidden="true">
        ·
      </span>
      <span className="statusbar__group">{activeConn.toLocaleString()} conn</span>
      <span className="statusbar__sep" aria-hidden="true">
        ·
      </span>
      <span className="statusbar__group">{brokers.toLocaleString()} brokers</span>
      <span className="statusbar__sep" aria-hidden="true">
        ·
      </span>
      <span className="statusbar__group" title="In-buffer / capacity (messages)">
        {abbrev(stats.inBuffer)} / {abbrev(stats.bufferCapacity)}
      </span>
      {stats.throughputPerSec > 0 ? (
        <>
          <span className="statusbar__sep" aria-hidden="true">
            ·
          </span>
          <span className="statusbar__group">{stats.throughputPerSec.toLocaleString()} msg/s</span>
        </>
      ) : null}
      {stats.drops > 0 ? (
        <>
          <span className="statusbar__sep" aria-hidden="true">
            ·
          </span>
          <span className="statusbar__group statusbar__group--danger">
            {stats.drops.toLocaleString()} drops
          </span>
        </>
      ) : null}
    </footer>
  );
}

/** Compact integer formatter: 1234 → "1.2k", 1_500_000 → "1.5M". Bare
 * integers under 1k stay as-is to avoid stripping precision from small
 * counts. */
function abbrev(n: number): string {
  if (n < 1000) {
    return n.toLocaleString();
  }
  if (n < 1_000_000) {
    const v = n / 1000;
    return `${v.toFixed(v < 10 ? 1 : 0)}k`;
  }
  const v = n / 1_000_000;
  return `${v.toFixed(v < 10 ? 1 : 0)}M`;
}
