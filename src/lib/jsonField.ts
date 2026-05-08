/**
 * Path-aware predicate over a decoded protocol body (JSON shape).
 *
 * `path` is a dotted chain of object-key segments rooted at the
 * decoded body (`topic_data.partition_data.records.base_offset`,
 * `responses.partitions.error_code`, ...). Array layers are
 * descended per-element automatically — a path segment refers to
 * the *key*, never the index, so the user doesn't have to know how
 * many topics or partitions a frame happens to carry.
 *
 * Bare-field predicates (`name == "x"`) are deliberately strict:
 * they only match when `name` lives at the root. Nested fields
 * require the qualifying path so a `name: "events"` under
 * `topics[]` doesn't collide with a `name: "events"` under, say,
 * `topic_data[]` of a different RPC shape.
 *
 * Equality is on the *string view*: numbers, bools, and strings
 * all compare against `value` via `String(...)`, matching the
 * click affordance in `ProtoDetail` which captures the value as
 * displayed.
 */

export function matchJsonPath(json: unknown, path: string, value: string): boolean {
  const segments = path.split(".").filter((s) => s.length > 0);
  if (segments.length === 0) {
    return false;
  }
  return walk(json, segments, value);
}

function walk(node: unknown, segments: string[], value: string): boolean {
  if (segments.length === 0) {
    return leafMatches(node, value);
  }
  if (Array.isArray(node)) {
    // Path segment refers to the array's *contents* — descend per
    // element with the segment list unchanged. (The user types
    // `topic_data.name`, not `topic_data.0.name`.)
    return (node as unknown[]).some((item) => walk(item, segments, value));
  }
  if (node === null || typeof node !== "object") {
    return false;
  }
  const head = segments[0];
  if (head === undefined) {
    return false;
  }
  const next = (node as Record<string, unknown>)[head];
  if (next === undefined) {
    return false;
  }
  return walk(next, segments.slice(1), value);
}

function leafMatches(leaf: unknown, value: string): boolean {
  if (leaf === null) {
    return value === "null";
  }
  switch (typeof leaf) {
    case "string":
      return leaf === value;
    case "number":
    case "boolean":
    case "bigint":
      return String(leaf) === value;
    default:
      return false;
  }
}
