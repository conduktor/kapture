import { useEffect, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import type { CaptureStats, ConnectionState, ProxyStatusSummary } from "../types";

interface Props {
  connection: ConnectionState;
  stats: CaptureStats;
}

const PROXY_POLL_MS = 1000;

/**
 * Thin status row pinned to the bottom of the workspace. Renders a compact,
 * monospace summary of the live proxy + capture state. Only shown when the
 * proxy is connected — disconnected we render an empty placeholder so the
 * grid row stays reserved (no layout shift on first connect).
 *
 * Polls `proxy_status` at 1Hz (same cadence the old SidePanel used) and
 * folds in the `kapture:stats` event-driven snapshot from App.tsx.
 */
export function StatusBar({ connection, stats }: Props): JSX.Element {
  const [proxy, setProxy] = useState<ProxyStatusSummary | null>(null);

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
