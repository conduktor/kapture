import { describe, expect, it } from "vitest";

import { matchJsonPath } from "./jsonField";

describe("matchJsonPath", () => {
  it("matches a top-level field", () => {
    const body = { error_code: 35, throttle_time_ms: 0 };
    expect(matchJsonPath(body, "error_code", "35")).toBe(true);
    expect(matchJsonPath(body, "error_code", "0")).toBe(false);
  });

  it("descends through arrays without index segments", () => {
    const body = {
      topics: [
        { name: "events", error_code: 0 },
        { name: "audit", error_code: 0 },
      ],
    };
    expect(matchJsonPath(body, "topics.name", "events")).toBe(true);
    expect(matchJsonPath(body, "topics.name", "audit")).toBe(true);
    expect(matchJsonPath(body, "topics.name", "missing")).toBe(false);
  });

  it("does not bleed across parent paths even when the leaf field name overlaps", () => {
    // Same `name` field under two different parents — paths must
    // disambiguate them.
    const body = {
      topics: [{ name: "events" }],
      partitions: [{ name: "audit" }],
    };
    expect(matchJsonPath(body, "topics.name", "audit")).toBe(false);
    expect(matchJsonPath(body, "partitions.name", "audit")).toBe(true);
  });

  it("rejects a bare field name when the field is nested (strict path)", () => {
    // `name` lives under `topics[]`, not at the root — a bare path
    // `name` only matches root-level `name`.
    const body = { topics: [{ name: "events" }] };
    expect(matchJsonPath(body, "name", "events")).toBe(false);
    expect(matchJsonPath(body, "topics.name", "events")).toBe(true);
  });

  it("compares numbers / booleans via string view", () => {
    const body = { topics: [{ partition_count: 3, leader: true }] };
    expect(matchJsonPath(body, "topics.partition_count", "3")).toBe(true);
    expect(matchJsonPath(body, "topics.leader", "true")).toBe(true);
    expect(matchJsonPath(body, "topics.leader", "false")).toBe(false);
  });

  it("walks deeply nested arrays", () => {
    const body = {
      topics: [
        {
          name: "events",
          partitions: [{ partition_index: 0 }, { partition_index: 1 }],
        },
      ],
    };
    expect(matchJsonPath(body, "topics.partitions.partition_index", "1")).toBe(true);
    expect(matchJsonPath(body, "topics.partitions.partition_index", "5")).toBe(false);
  });

  it("returns false when an intermediate segment is missing", () => {
    const body = { error_code: 0 };
    expect(matchJsonPath(body, "topics.name", "events")).toBe(false);
  });

  it("returns false for non-object inputs and empty paths", () => {
    expect(matchJsonPath(null, "a", "b")).toBe(false);
    expect(matchJsonPath("string", "a", "b")).toBe(false);
    expect(matchJsonPath(42, "a", "b")).toBe(false);
    expect(matchJsonPath({ a: 1 }, "", "1")).toBe(false);
  });
});
