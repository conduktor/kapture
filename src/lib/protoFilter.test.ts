/**
 * Round-trip tests for the Protocol-tab filter DSL.
 *
 * The DSL is the source of truth in the UI, so the parser/serializer
 * must be strict inverses for any canonical input. The chip-removal
 * flow relies on this: removing a chip = parse → removePredicate →
 * serialize, and that has to land on a textbox value the user would
 * recognize.
 */
import { describe, expect, it } from "vitest";

import {
  EMPTY_PROTO_FILTER,
  appendClause,
  isFilterEmpty,
  parseExpression,
  serializeFilter,
} from "./protoFilter";

describe("parseExpression", () => {
  it("returns empty filter on empty input with no error", () => {
    const r = parseExpression("");
    expect(isFilterEmpty(r.filter)).toBe(true);
    expect(r.error).toBeNull();
  });

  it("returns empty filter on whitespace-only input with no error", () => {
    const r = parseExpression("   \t\n");
    expect(isFilterEmpty(r.filter)).toBe(true);
    expect(r.error).toBeNull();
  });

  it("parses a single apiName include", () => {
    const r = parseExpression('apiName == "Fetch"');
    expect(r.error).toBeNull();
    expect(r.filter.apiNames.include).toEqual(["Fetch"]);
    expect(r.filter.apiNames.exclude).toEqual([]);
  });

  it("parses a single apiName exclude", () => {
    const r = parseExpression('apiName != "Heartbeat"');
    expect(r.error).toBeNull();
    expect(r.filter.apiNames.include).toEqual([]);
    expect(r.filter.apiNames.exclude).toEqual(["Heartbeat"]);
  });

  it("parses two clauses across kinds joined by &&", () => {
    const r = parseExpression('apiName == "Fetch" && conn == 42');
    expect(r.error).toBeNull();
    expect(r.filter.apiNames.include).toEqual(["Fetch"]);
    expect(r.filter.connectionIds.include).toEqual([42]);
  });

  it("parses two includes within the same kind (OR)", () => {
    const r = parseExpression("conn == 42 && conn == 43");
    expect(r.error).toBeNull();
    expect(r.filter.connectionIds.include).toEqual([42, 43]);
  });

  it("parses bareword direction values", () => {
    const r = parseExpression("direction == send");
    expect(r.error).toBeNull();
    expect(r.filter.directions.include).toEqual(["send"]);
  });

  it("parses decoded substring", () => {
    const r = parseExpression('decoded == "topic_id"');
    expect(r.error).toBeNull();
    expect(r.filter.decodedContains.include).toEqual(["topic_id"]);
  });

  it("preserves && inside quoted strings", () => {
    const r = parseExpression('apiName == "Fetch && weird"');
    expect(r.error).toBeNull();
    expect(r.filter.apiNames.include).toEqual(["Fetch && weird"]);
  });

  it("rejects unknown kind with a positional error", () => {
    const r = parseExpression('foo == "bar"');
    expect(r.error).toMatch(/unknown filter kind/);
    expect(isFilterEmpty(r.filter)).toBe(true);
  });

  it("rejects integer for apiName", () => {
    const r = parseExpression("apiName == 42");
    expect(r.error).toMatch(/quoted string/);
  });

  it("rejects non-integer for conn", () => {
    const r = parseExpression('conn == "42"');
    expect(r.error).toMatch(/integer/);
  });

  it("rejects unknown direction bareword", () => {
    const r = parseExpression("direction == upward");
    expect(r.error).toMatch(/'send' or 'recv'/);
  });

  it("rejects trailing &&", () => {
    const r = parseExpression('apiName == "Fetch" &&');
    expect(r.error).toMatch(/expected another clause/);
  });

  it("accepts negative integer for corrId", () => {
    const r = parseExpression("corrId == -1");
    expect(r.error).toBeNull();
    expect(r.filter.corrIds.include).toEqual([-1]);
  });
});

describe("serializeFilter", () => {
  it("emits empty string for empty filter", () => {
    expect(serializeFilter(EMPTY_PROTO_FILTER)).toBe("");
  });

  it("renders canonical form with fixed kind order", () => {
    const r = parseExpression('corrId == 7 && apiName == "Fetch" && conn == 42');
    expect(r.error).toBeNull();
    expect(serializeFilter(r.filter)).toBe('apiName == "Fetch" && conn == 42 && corrId == 7');
  });

  it("places includes before excludes within a kind", () => {
    const r = parseExpression('apiName != "Heartbeat" && apiName == "Fetch"');
    expect(r.error).toBeNull();
    expect(serializeFilter(r.filter)).toBe('apiName == "Fetch" && apiName != "Heartbeat"');
  });

  it("sorts numeric values numerically", () => {
    const r = parseExpression("conn == 42 && conn == 7 && conn == 100");
    expect(r.error).toBeNull();
    expect(serializeFilter(r.filter)).toBe("conn == 7 && conn == 42 && conn == 100");
  });

  it("emits direction as bareword", () => {
    const r = parseExpression("direction == send");
    expect(serializeFilter(r.filter)).toBe("direction == send");
  });

  it("escapes quotes inside strings", () => {
    const r = parseExpression('apiName == "weird\\"name"');
    expect(r.error).toBeNull();
    expect(r.filter.apiNames.include).toEqual(['weird"name']);
    expect(serializeFilter(r.filter)).toBe('apiName == "weird\\"name"');
  });
});

describe("round-trip parseExpression ∘ serializeFilter", () => {
  const cases: string[] = [
    "",
    'apiName == "Fetch"',
    'apiName != "Heartbeat"',
    'apiName == "Fetch" && conn == 42',
    "conn == 7 && conn == 42 && conn == 100",
    "direction == send",
    "direction != recv",
    'decoded == "topic_id"',
    'apiName == "Fetch" && conn != 42 && corrId == 7',
  ];
  for (const input of cases) {
    it(`stable for: ${input || "<empty>"}`, () => {
      const first = parseExpression(input);
      expect(first.error).toBeNull();
      const canonical = serializeFilter(first.filter);
      const second = parseExpression(canonical);
      expect(second.error).toBeNull();
      expect(serializeFilter(second.filter)).toBe(canonical);
    });
  }
});

describe("appendClause", () => {
  it("creates first clause from empty", () => {
    expect(appendClause("", "apiName", "Fetch", "include")).toBe('apiName == "Fetch"');
  });

  it("appends a clause across kinds", () => {
    expect(appendClause('apiName == "Fetch"', "corrId", 7, "include")).toBe(
      'apiName == "Fetch" && corrId == 7',
    );
  });

  it("appends an exclude", () => {
    expect(appendClause("", "apiName", "Fetch", "exclude")).toBe('apiName != "Fetch"');
  });

  it("is idempotent for identical predicates", () => {
    const once = appendClause("", "apiName", "Fetch", "include");
    const twice = appendClause(once, "apiName", "Fetch", "include");
    expect(twice).toBe(once);
  });

  it("flips include ↔ exclude for the same value+kind", () => {
    const inc = appendClause("", "apiName", "Fetch", "include");
    const flipped = appendClause(inc, "apiName", "Fetch", "exclude");
    expect(flipped).toBe('apiName != "Fetch"');
  });

  it("recovers from broken text by starting fresh", () => {
    expect(appendClause("garbage !! nope", "connectionId", 5, "include")).toBe("conn == 5");
  });
});
