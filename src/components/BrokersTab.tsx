import type { JSX } from "react";
import type { ProxyStatusSummary } from "../types";

interface Props {
  /** Latest 1Hz snapshot from App.tsx. `null` between connect and the
   *  first poll tick. */
  proxyStatus: ProxyStatusSummary | null;
}

/**
 * Per-broker view of the proxy. Lists each upstream broker and the
 * local TCP port Kapture is bound to for it. The bootstrap row is the
 * one whose local port matches the listener's port — it's always
 * bound, every other listener is lazily created when a
 * `MetadataResponse` advertises a new broker.
 *
 * Reuses the snapshot already polled by App.tsx for the StatusBar
 * (one 1 Hz `proxy_status` call feeds both). No per-broker active
 * connection count: the backend only exposes a TOTAL — we surface
 * that once at the top of the table instead of fabricating per-row
 * numbers.
 */
export function BrokersTab({ proxyStatus }: Props): JSX.Element {
  const mappings = proxyStatus?.brokerMappings ?? [];
  const listenAddr = proxyStatus?.listenAddr ?? null;
  const bootstrapPort = parseListenPort(listenAddr);
  const listenHost = parseListenHost(listenAddr);

  if (mappings.length === 0) {
    return (
      <div className="brokers brokers--empty">
        <div className="brokers__empty">
          Waiting for first MetadataResponse — start a Kafka client pointed at{" "}
          <code>{listenAddr ?? "the proxy"}</code>.
        </div>
      </div>
    );
  }

  const activeConns = proxyStatus?.activeConnections ?? 0;
  const summary = `${mappings.length.toString()} broker${mappings.length === 1 ? "" : "s"} · ${activeConns.toLocaleString()} active connection${activeConns === 1 ? "" : "s"}${listenHost !== null ? ` · listening on ${listenHost}` : ""}`;

  return (
    <div className="brokers">
      <div className="brokers__summary" role="status">
        {summary}
      </div>
      <div className="brokers__table" role="table" aria-label="Broker port mappings">
        <div className="brokers__row brokers__row--head" role="row">
          <div className="brokers__cell brokers__cell--port" role="columnheader">
            Local port
          </div>
          <div
            className="brokers__cell brokers__cell--arrow"
            role="columnheader"
            aria-hidden="true"
          >
            →
          </div>
          <div className="brokers__cell brokers__cell--upstream" role="columnheader">
            Upstream
          </div>
          <div className="brokers__cell brokers__cell--status" role="columnheader">
            Status
          </div>
        </div>
        {mappings.map(([[host, upstreamPort], localPort]) => {
          const isBootstrap = bootstrapPort !== null && localPort === bootstrapPort;
          return (
            <div
              className="brokers__row"
              role="row"
              key={`${String(localPort)}-${host}-${String(upstreamPort)}`}
            >
              <div className="brokers__cell brokers__cell--port" role="cell">
                {localPort}
              </div>
              <div className="brokers__cell brokers__cell--arrow" role="cell" aria-hidden="true">
                →
              </div>
              <div
                className="brokers__cell brokers__cell--upstream"
                role="cell"
                title={`${host}:${String(upstreamPort)}`}
              >
                {host}:{upstreamPort}
              </div>
              <div className="brokers__cell brokers__cell--status" role="cell">
                {isBootstrap ? (
                  <span className="brokers__badge brokers__badge--bootstrap">bootstrap</span>
                ) : (
                  <span className="brokers__badge brokers__badge--lazy">lazy-bound</span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/**
 * Parse `host:port` → `port`. Listen addrs are always plain
 * `127.0.0.1:NNNN` (the proxy never binds an IPv6 literal), so a
 * single `lastIndexOf(":")` split is enough. Returns `null` on any
 * shape we don't recognise — the bootstrap badge then simply
 * doesn't render rather than tagging the wrong row.
 */
function parseListenPort(addr: string | null): number | null {
  if (addr === null) {
    return null;
  }
  const idx = addr.lastIndexOf(":");
  if (idx < 0) {
    return null;
  }
  const portStr = addr.slice(idx + 1);
  const port = Number.parseInt(portStr, 10);
  if (!Number.isFinite(port) || port <= 0 || port > 65535) {
    return null;
  }
  return port;
}

function parseListenHost(addr: string | null): string | null {
  if (addr === null) {
    return null;
  }
  const idx = addr.lastIndexOf(":");
  if (idx <= 0) {
    return null;
  }
  return addr.slice(0, idx);
}
