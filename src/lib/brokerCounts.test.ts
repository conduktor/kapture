import { describe, expect, it } from "vitest";

import type { ProtoFrame } from "../types";
import { aggregateByBroker, totalCounts } from "./brokerCounts";

function frame(localPort: number, direction: "send" | "recv", id: string): ProtoFrame {
  return {
    id,
    timestamp: "2025-01-01T00:00:00.000000Z",
    direction,
    apiKey: 1,
    apiName: "Fetch",
    apiVersion: 13,
    connectionId: 1,
    localPort,
    corrId: 1,
    size: 100,
    captured: 50,
    rttMs: 0,
  };
}

describe("aggregateByBroker", () => {
  it("returns an empty map for no frames", () => {
    const out = aggregateByBroker([]);
    expect(out.size).toBe(0);
  });

  it("counts send and recv per local port", () => {
    const frames = [
      frame(9092, "send", "a"),
      frame(9092, "send", "b"),
      frame(9092, "recv", "c"),
      frame(9093, "send", "d"),
      frame(9093, "recv", "e"),
      frame(9093, "recv", "f"),
    ];
    const out = aggregateByBroker(frames);
    expect(out.get(9092)).toEqual({ send: 2, recv: 1 });
    expect(out.get(9093)).toEqual({ send: 1, recv: 2 });
  });

  it("buckets `localPort: 0` separately from real brokers", () => {
    const frames = [frame(0, "send", "a"), frame(9092, "send", "b")];
    const out = aggregateByBroker(frames);
    expect(out.get(0)).toEqual({ send: 1, recv: 0 });
    expect(out.get(9092)).toEqual({ send: 1, recv: 0 });
  });

  it("does not include unseen brokers", () => {
    const out = aggregateByBroker([frame(9092, "send", "a")]);
    expect(out.has(9093)).toBe(false);
  });
});

describe("totalCounts", () => {
  it("sums across brokers", () => {
    const map = new Map([
      [9092, { send: 3, recv: 1 }],
      [9093, { send: 5, recv: 7 }],
    ]);
    expect(totalCounts(map)).toEqual({ send: 8, recv: 8 });
  });

  it("returns zeros for empty map", () => {
    expect(totalCounts(new Map())).toEqual({ send: 0, recv: 0 });
  });
});
