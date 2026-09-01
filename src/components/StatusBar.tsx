import type { JSX } from "react";
import { openUrl } from "@tauri-apps/plugin-opener";
import { formatBytes } from "../lib/formatBytes";
import type { CaptureStats, ConnectionState, ProxyStatusSummary } from "../types";

interface Props {
  connection: ConnectionState;
  stats: CaptureStats;
  /** 1Hz proxy snapshot, polled at the App level and shared with BrokersTab. */
  proxy: ProxyStatusSummary | null;
  /** Latest protocol frame's capture→analysis and capture→renderer lag. */
  captureLatency: { analysisMs: number; renderMs: number } | null;
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
export function StatusBar({ connection, stats, proxy, captureLatency }: Props): JSX.Element {
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
      {(() => {
        // Buffer fill — only surface when it's actually getting full.
        // Below threshold the bar would just show "0%" forever, which
        // is noise. The drops chip already signals overflow when it
        // matters; this is the early warning.
        const msgFill = stats.bufferCapacity > 0 ? stats.inBuffer / stats.bufferCapacity : 0;
        const byteFill =
          stats.bufferByteCapacity > 0 ? stats.bufferBytes / stats.bufferByteCapacity : 0;
        const fill = Math.max(msgFill, byteFill);
        if (fill < 0.25) {
          return null;
        }
        const pct = Math.round(fill * 100);
        const danger = fill >= 0.9;
        const warn = !danger && fill >= 0.75;
        return (
          <>
            <span className="statusbar__sep" aria-hidden="true">
              ·
            </span>
            <span
              className={
                danger
                  ? "statusbar__group statusbar__group--danger"
                  : warn
                    ? "statusbar__group statusbar__group--warn"
                    : "statusbar__group"
              }
              title={`Long-term capture buffer — ${stats.inBuffer.toLocaleString()} / ${stats.bufferCapacity.toLocaleString()} messages, ${formatBytes(stats.bufferBytes)} / ${formatBytes(stats.bufferByteCapacity, 0)}. This is the depth available to filters: a query (e.g. \`topic == "rare-topic"\`) scans the whole buffer, not just the 5,000 rows currently visible. At 100% the oldest captured messages are evicted to make room. Kafka traffic itself is unaffected — TCP forwarding is independent of the inspection buffer.`}
            >
              buf {pct}%
            </span>
          </>
        );
      })()}
      {stats.throughputPerSec >= 1 ? (
        <>
          <span className="statusbar__sep" aria-hidden="true">
            ·
          </span>
          <span
            className="statusbar__group"
            title={`${stats.throughputPerSec.toFixed(1)} msg/s (last tick)`}
          >
            {Math.round(stats.throughputPerSec).toLocaleString()} msg/s
          </span>
        </>
      ) : null}
      {captureLatency !== null ? (
        <>
          <span className="statusbar__sep" aria-hidden="true">
            ·
          </span>
          <span
            className={
              captureLatency.renderMs >= 2_000
                ? "statusbar__group statusbar__group--warn"
                : "statusbar__group"
            }
            title={`Observation → bounded analyzer: ${captureLatency.analysisMs.toFixed(1)} ms. Observation → renderer: ${captureLatency.renderMs.toFixed(1)} ms (includes the 1 Hz UI polling interval). Kafka RTT is measured independently at the wire boundary.`}
          >
            render lag {formatLag(captureLatency.renderMs)}
          </span>
        </>
      ) : null}
      {stats.drops > 0 ? (
        <>
          <span className="statusbar__sep" aria-hidden="true">
            ·
          </span>
          <CaptureHealth stats={stats} />
        </>
      ) : null}
      <span className="statusbar__spacer" />
      <button
        type="button"
        className="statusbar__link"
        title="Email feedback to the author"
        onClick={() => {
          void openUrl("mailto:kapture@conduktor.io?subject=Kapture%20feedback");
        }}
      >
        feedback
      </button>
      <span className="statusbar__sep" aria-hidden="true">
        ·
      </span>
      <button
        type="button"
        className="statusbar__link"
        title="Open an issue on GitHub"
        onClick={() => {
          void openUrl("https://github.com/conduktor/kapture/issues");
        }}
      >
        github
      </button>
    </footer>
  );
}

function formatLag(milliseconds: number): string {
  return milliseconds < 1_000
    ? `${Math.round(milliseconds).toLocaleString()}ms`
    : `${(milliseconds / 1_000).toFixed(1)}s`;
}

function CaptureHealth({ stats }: { stats: CaptureStats }): JSX.Element {
  const incomplete =
    stats.oversizedDrops +
    stats.uiSummaryDrops +
    stats.analyzerDrops +
    stats.recordExtractionDrops +
    stats.agentDrops;
  const details = [
    ["history evictions", stats.bufferEvictions],
    ["oversized records", stats.oversizedDrops],
    ["UI summaries", stats.uiSummaryDrops],
    ["protocol analyzer", stats.analyzerDrops],
    ["record extraction", stats.recordExtractionDrops],
    ["external agent", stats.agentDrops],
  ]
    .filter((entry) => entry[1] !== 0)
    .map(([label, count]) => `${label}: ${Number(count).toLocaleString()}`)
    .join("; ");
  const activeLoss = stats.dropsPerSec > 0;
  const label =
    incomplete > 0
      ? `capture incomplete · ${incomplete.toLocaleString()} lost`
      : `history · ${stats.bufferEvictions.toLocaleString()} evicted`;
  return (
    <span
      className={
        activeLoss
          ? "statusbar__group statusbar__group--danger"
          : "statusbar__group statusbar__group--warn"
      }
      title={`${details}. ${activeLoss ? `${stats.dropsPerSec.toFixed(1)} drops/s in the last tick. ` : ""}Kafka forwarding remains non-blocking; these counters describe inspector loss, not broker data loss.`}
      role="status"
    >
      {label}
    </span>
  );
}
