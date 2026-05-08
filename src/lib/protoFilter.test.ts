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
  addPredicate,
  appendClause,
  applyFilter,
  encodeDecodedField,
  isFilterEmpty,
  parseExpression,
  removePredicate,
  serializeFilter,
} from "./protoFilter";

const decodedFieldPredicate = encodeDecodedField({
  path: "topics.name",
  value: "orders.avro",
});

const protoFrame = {
  id: "frame-1",
  apiName: "Metadata",
  direction: "send",
  connectionId: 42,
  corrId: 7,
} as Parameters<typeof applyFilter>[1];

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

  it("parses a dotted-path field predicate", () => {
    const r = parseExpression('topics.name == "orders.avro"');
    expect(r.error).toBeNull();
    expect(r.filter.decodedField.include).toEqual([decodedFieldPredicate]);
    expect(r.filter.decodedField.exclude).toEqual([]);
  });

  it("accepts integer values on field predicates (compared via string view)", () => {
    const r = parseExpression("error_code == 35");
    expect(r.error).toBeNull();
    expect(r.filter.decodedField.include).toEqual(["error_code=35"]);
  });

  it("preserves && inside quoted strings", () => {
    const r = parseExpression('apiName == "Fetch && weird"');
    expect(r.error).toBeNull();
    expect(r.filter.apiNames.include).toEqual(["Fetch && weird"]);
  });

  it("treats unknown idents as field names (no reserved-kind error)", () => {
    // Any non-reserved ident becomes a JSON field-name predicate.
    // The matcher just won't find `foo` in any frame body, so the
    // filter rejects everything — but the parse itself succeeds.
    const r = parseExpression('foo == "bar"');
    expect(r.error).toBeNull();
    expect(r.filter.decodedField.include).toEqual(["foo=bar"]);
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

  it("renders field-path predicates path-first", () => {
    const r = parseExpression('topics.name == "orders.avro"');
    expect(r.error).toBeNull();
    expect(serializeFilter(r.filter)).toBe('topics.name == "orders.avro"');
  });

  it("stringifies integer field values with quotes", () => {
    const r = parseExpression("error_code == 35");
    expect(r.error).toBeNull();
    expect(serializeFilter(r.filter)).toBe('error_code == "35"');
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
    'topics.name == "orders.avro"',
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

  it("preserves equivalent shape for apiName plus decoded field", () => {
    const input = 'apiName == "Fetch" && topics.name == "orders.avro"';
    const first = parseExpression(input);
    expect(first.error).toBeNull();
    const canonical = serializeFilter(first.filter);
    const second = parseExpression(canonical);
    expect(second.error).toBeNull();
    expect(second.filter).toEqual(first.filter);
  });
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

describe("applyFilter decodedField", () => {
  // Filter syntax is path-aware — the user qualifies the leaf with
  // its parent chain so a `name` under `topics[]` doesn't collide
  // with an unrelated `name` under another RPC's `topic_data[]`.
  it("accepts a frame whose decoded body matches the path predicate", () => {
    const r = parseExpression('topics.name == "orders.avro"');
    expect(r.error).toBeNull();
    expect(
      applyFilter(r.filter, protoFrame, () => ({
        topics: [
          {
            topic_id: "00000000-0000-0000-0000-000000000000",
            name: "orders.avro",
          },
        ],
      })),
    ).toBe(true);
  });

  it("rejects an include predicate when the decoded body is not cached", () => {
    const r = parseExpression('topics.name == "orders.avro"');
    expect(r.error).toBeNull();
    expect(applyFilter(r.filter, protoFrame, () => undefined)).toBe(false);
  });

  it("keeps frames under an exclude-only predicate when the body is uncached", () => {
    // `field != x` is a negative constraint — we can't confirm the
    // frame matches the predicate to be excluded, so we have to
    // include it. Otherwise an exclude on a body-touching predicate
    // would silently drop every uncached row, defeating the purpose
    // of "exclude only the few I don't want".
    const r = parseExpression('topics.name != "orders.avro"');
    expect(r.error).toBeNull();
    expect(applyFilter(r.filter, protoFrame, () => undefined)).toBe(true);
  });

  it("still excludes uncached frames once an include sits alongside the exclude", () => {
    const r = parseExpression('topics.name == "events" && topics.name != "orders.avro"');
    expect(r.error).toBeNull();
    expect(applyFilter(r.filter, protoFrame, () => undefined)).toBe(false);
  });

  it("rejects a bare field predicate when the field is nested", () => {
    // Strict path semantics: `name == "..."` only matches the root
    // `name`, not a `topics[].name` further down. The user must
    // write the qualifying path.
    const r = parseExpression('name == "events"');
    expect(r.error).toBeNull();
    expect(applyFilter(r.filter, protoFrame, () => ({ topics: [{ name: "events" }] }))).toBe(false);
    expect(applyFilter(r.filter, protoFrame, () => ({ name: "events" }))).toBe(true);
  });

  it("does not bleed across parent contexts with the same leaf name", () => {
    // Two RPCs surface a `name` field under different parents;
    // `topics.name` matches only the topics chain.
    const r = parseExpression('topics.name == "audit"');
    expect(r.error).toBeNull();
    expect(
      applyFilter(r.filter, protoFrame, () => ({
        topics: [{ name: "events" }],
        topic_data: [{ name: "audit" }],
      })),
    ).toBe(false);
  });

  it("rejects when the path is absent from the body", () => {
    const r = parseExpression("topics.acks == 1");
    expect(r.error).toBeNull();
    expect(applyFilter(r.filter, protoFrame, () => ({ topics: [{ name: "events" }] }))).toBe(false);
  });
});

describe("decodedField predicates", () => {
  it("can be added and removed back to an empty filter", () => {
    const added = addPredicate(
      EMPTY_PROTO_FILTER,
      "decodedField",
      decodedFieldPredicate,
      "include",
    );
    expect(added.decodedField.include).toEqual([decodedFieldPredicate]);
    const removed = removePredicate(added, "decodedField", decodedFieldPredicate, "include");
    expect(isFilterEmpty(removed)).toBe(true);
  });
});
