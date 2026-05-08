import { describe, expect, it } from "vitest";

import type { FrameSummary, ProtoDirection, ProtoFrame } from "../types";
import { aggregateSession } from "./sessionStats";

let frameSeq = 0;

function frame(apiName: string, direction: ProtoDirection, summary?: FrameSummary): ProtoFrame {
  frameSeq += 1;
  const f: ProtoFrame = {
    id: `f${String(frameSeq)}`,
    timestamp: `2026-05-07T10:00:${String(frameSeq).padStart(2, "0")}.000000Z`,
    direction,
    apiKey: 0,
    apiName,
    apiVersion: 0,
    connectionId: 1,
    localPort: 9092,
    corrId: frameSeq,
    size: 100,
    captured: 100,
    rttMs: 0,
  };
  if (summary !== undefined) {
    f.summary = summary;
  }
  return f;
}

describe("aggregateSession", () => {
  it("returns an empty session for no frames", () => {
    const s = aggregateSession([]);
    expect(s.client).toBeNull();
    expect(s.topics.size).toBe(0);
    expect(s.groups.size).toBe(0);
    expect(s.errors).toEqual([]);
  });

  it("captures client lib + version from ApiVersionsRequest v3+", () => {
    const s = aggregateSession([
      frame("ApiVersions", "send", {
        kind: "apiVersionsRequest",
        clientSoftwareName: "librdkafka",
        clientSoftwareVersion: "2.3.0",
      }),
    ]);
    expect(s.client).toEqual({ software: "librdkafka", version: "2.3.0" });
  });

  it("collects topics from MetadataResponse + Produce + Fetch", () => {
    const s = aggregateSession([
      frame("Metadata", "recv", {
        kind: "metadataResponse",
        topics: ["events", "audit"],
        brokers: 1,
      }),
      frame("Produce", "send", {
        kind: "produceRequest",
        topics: ["events"],
      }),
      frame("Fetch", "send", {
        kind: "fetchRequest",
        topics: ["audit", "commands"],
      }),
    ]);
    expect([...s.topics.keys()].sort()).toEqual(["audit", "commands", "events"]);
    expect(s.topics.get("events")?.produced).toBe(true);
    expect(s.topics.get("events")?.consumed).toBe(false);
    expect(s.topics.get("audit")?.consumed).toBe(true);
    expect(s.topics.get("commands")?.metadata).toBe(false);
    expect(s.topics.get("audit")?.metadata).toBe(true);
  });

  it("tracks groups across the lifecycle RPCs", () => {
    const s = aggregateSession([
      frame("FindCoordinator", "send", {
        kind: "findCoordinatorRequest",
        keys: ["worker-1"],
      }),
      frame("JoinGroup", "send", {
        kind: "joinGroupRequest",
        groupId: "worker-1",
        memberId: "",
      }),
      frame("JoinGroup", "recv", {
        kind: "joinGroupResponse",
        errorCode: 0,
        generationId: 7,
        memberId: "consumer-1-abc",
      }),
      frame("Heartbeat", "send", {
        kind: "heartbeatRequest",
        groupId: "worker-1",
        memberId: "consumer-1-abc",
        generationId: 7,
      }),
      frame("OffsetCommit", "send", {
        kind: "offsetCommitRequest",
        groupId: "worker-1",
        memberId: "consumer-1-abc",
        topics: ["events"],
      }),
    ]);
    const g = s.groups.get("worker-1");
    expect(g).toBeDefined();
    expect(g?.members.has("consumer-1-abc")).toBe(true);
    expect(g?.generation).toBe(7);
    expect(g?.commitCount).toBe(1);
    expect(g?.heartbeatCount).toBe(1);
  });

  it("captures errors with context", () => {
    const s = aggregateSession([
      frame("Heartbeat", "recv", { kind: "heartbeatResponse", errorCode: 27 }),
      frame("OffsetCommit", "recv", {
        kind: "offsetCommitResponse",
        maxErrorCode: 16,
      }),
      frame("JoinGroup", "recv", {
        kind: "joinGroupResponse",
        errorCode: 0,
        generationId: 1,
        memberId: "m",
      }),
    ]);
    expect(s.errors.length).toBe(2);
    expect(s.errors[0]?.errorCode).toBe(27);
    expect(s.errors[0]?.errorName).toBe("REBALANCE_IN_PROGRESS");
    expect(s.errors[1]?.errorCode).toBe(16);
  });

  it("aggregates connections by localPort", () => {
    frameSeq = 0;
    const f1 = frame("Metadata", "send");
    f1.localPort = 9092;
    const f2 = frame("Metadata", "recv");
    f2.localPort = 9092;
    const f3 = frame("Fetch", "send");
    f3.localPort = 9093;
    const s = aggregateSession([f1, f2, f3]);
    expect(s.connections.length).toBe(2);
    const c92 = s.connections.find((c) => c.localPort === 9092);
    expect(c92?.frameCount).toBe(2);
  });
});
