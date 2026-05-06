import { useMemo, type JSX } from "react";

import { aggregateByBroker, totalCounts, type BrokerCounts } from "../lib/brokerCounts";
import type { ProtoFrame, ProxyStatusSummary } from "../types";

interface Props {
  /** Latest 1Hz snapshot from App.tsx. `null` between connect and the
   *  first poll tick. */
  proxyStatus: ProxyStatusSummary | null;
  /**
   * Same `proto_frames` snapshot the Protocol tab consumes, polled
   * once at the App level. Used to aggregate per-broker send/recv
   * counters by `localPort` — every frame is stamped with the
   * listener port that owned its pump, so closed connections retain
   * their broker attribution as long as they're in the ring buffer.
   */
  protoFrames: ProtoFrame[];
}

const ZERO_COUNTS: BrokerCounts = { send: 0, recv: 0 };

/**
 * Per-broker view of the proxy. Lists each upstream broker, the local
 * TCP port Kapture is bound to for it, and the running send/recv
 * frame counts observed since the proxy started.
 *
 * The bootstrap row is the one whose local port matches the
 * listener's port — it's always bound; every other listener is
 * lazily created when a `MetadataResponse` advertises a new broker.
 *
 * Reuses the snapshot already polled by App.tsx for the StatusBar
 * (one 1 Hz `proxy_status` call feeds both) and the proto_frames
 * snapshot already polled for the Protocol tab — no extra IPC.
 */
export function BrokersTab({ proxyStatus, protoFrames }: Props): JSX.Element {
  const mappings = proxyStatus?.brokerMappings ?? [];
  const listenAddr = proxyStatus?.listenAddr ?? null;
  const bootstrapPort = parseListenPort(listenAddr);
  const listenHost = parseListenHost(listenAddr);

  const counts = useMemo(() => aggregateByBroker(protoFrames), [protoFrames]);
  const totals = useMemo(() => totalCounts(counts), [counts]);
  const maxFrames = useMemo(() => {
    let max = 0;
    for (const c of counts.values()) {
      const sum = c.send + c.recv;
      if (sum > max) {
        max = sum;
      }
    }
    return max;
  }, [counts]);

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
  const listenSuffix = listenHost !== null ? ` · listening on ${listenHost}` : "";
  const summary =
    `${mappings.length.toString()} broker${mappings.length === 1 ? "" : "s"}` +
    ` · ${activeConns.toLocaleString()} active connection${activeConns === 1 ? "" : "s"}` +
    ` · ${totals.send.toLocaleString()} send / ${totals.recv.toLocaleString()} recv total` +
    listenSuffix;

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
          <div
            className="brokers__cell brokers__cell--count"
            role="columnheader"
            title="Frames sent client → broker through this listener"
          >
            Send
          </div>
          <div
            className="brokers__cell brokers__cell--count"
            role="columnheader"
            title="Frames received broker → client through this listener"
          >
            Recv
          </div>
          <div
            className="brokers__cell brokers__cell--load"
            role="columnheader"
            title="Relative load: send + recv normalised against the busiest broker"
          >
            Load
          </div>
          <div className="brokers__cell brokers__cell--status" role="columnheader">
            Status
          </div>
        </div>
        {mappings.map(([[host, upstreamPort], localPort]) => {
          const isBootstrap = bootstrapPort !== null && localPort === bootstrapPort;
          const c = counts.get(localPort) ?? ZERO_COUNTS;
          const total = c.send + c.recv;
          const loadPct = maxFrames === 0 ? 0 : Math.round((total / maxFrames) * 100);
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
              <div
                className="brokers__cell brokers__cell--count"
                role="cell"
                aria-label={`${c.send.toString()} frames sent`}
              >
                {c.send.toLocaleString()}
              </div>
              <div
                className="brokers__cell brokers__cell--count"
                role="cell"
                aria-label={`${c.recv.toString()} frames received`}
              >
                {c.recv.toLocaleString()}
              </div>
              <div
                className="brokers__cell brokers__cell--load"
                role="cell"
                title={`${total.toLocaleString()} total frames (${loadPct.toString()}% of busiest)`}
              >
                <div
                  className="brokers__bar"
                  aria-hidden="true"
                  style={{ width: `${loadPct.toString()}%` }}
                />
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
