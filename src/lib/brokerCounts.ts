/**
 * Aggregate per-broker send/recv frame counts from a `ProtoFrame[]`
 * snapshot.
 *
 * The proxy stamps `localPort` on every frame at emission time —
 * that's the listener port that owned the per-connection pump. Because
 * each listener owns exactly one upstream broker mapping, grouping
 * frames by `localPort` is equivalent to grouping by broker, even for
 * frames from connections that have since closed (the stamp survives
 * in the ring buffer).
 *
 * Pure function — extracted from `BrokersTab` so it can be tested
 * without spinning up React.
 */
import type { ProtoDirection, ProtoFrame } from "../types";

export interface BrokerCounts {
  send: number;
  recv: number;
}

export function aggregateByBroker(frames: ProtoFrame[]): Map<number, BrokerCounts> {
  const counts = new Map<number, BrokerCounts>();
  for (const f of frames) {
    let cur = counts.get(f.localPort);
    if (cur === undefined) {
      cur = { send: 0, recv: 0 };
      counts.set(f.localPort, cur);
    }
    bump(cur, f.direction);
  }
  return counts;
}

/**
 * Sum send/recv across every broker. Cheaper than re-walking the
 * frames once we already have the aggregated map.
 */
export function totalCounts(by: Map<number, BrokerCounts>): BrokerCounts {
  let send = 0;
  let recv = 0;
  for (const c of by.values()) {
    send += c.send;
    recv += c.recv;
  }
  return { send, recv };
}

function bump(c: BrokerCounts, dir: ProtoDirection): void {
  if (dir === "send") {
    c.send += 1;
  } else {
    c.recv += 1;
  }
}
