/**
 * Build filter DSL expressions from UI interactions, escaping strings safely.
 *
 * The grammar lives in `src-tauri/src/filter.pest`. Keep this in sync:
 *   - strings are double-quoted; backslash, double-quote and control bytes
 *     are escaped to keep the input single-line and unambiguous
 *   - numbers are emitted as-is
 *   - booleans → `true` / `false`
 *   - null is not a first-class literal in the DSL (callers must avoid
 *     emitting it; this module returns `false` as a defensive no-match)
 *   - identifier path segments must match the grammar
 *     (letter-led `[A-Za-z][A-Za-z0-9_-]*` or pure digits for array indices)
 */

export interface PrimitiveLiteral {
  kind: "string" | "number" | "boolean" | "null";
  value: string;
}

const PATH_SEGMENT_RE = /^(?:[A-Za-z][A-Za-z0-9_-]*|\d+)$/u;

/** True when `segment` parses as a single identifier segment in the DSL. */
export function isValidPathSegment(segment: string): boolean {
  return PATH_SEGMENT_RE.test(segment);
}

/** True when every dotted segment of `path` is grammar-valid. */
export function isValidPath(path: string): boolean {
  if (path.length === 0) {
    return false;
  }
  return path.split(".").every(isValidPathSegment);
}

export function escapeString(raw: string): string {
  let out = "";
  for (const ch of raw) {
    switch (ch) {
      case "\\":
        out += "\\\\";
        break;
      case '"':
        out += '\\"';
        break;
      case "\n":
        out += "\\n";
        break;
      case "\r":
        out += "\\r";
        break;
      case "\t":
        out += "\\t";
        break;
      default:
        out += ch;
    }
  }
  return out;
}

export function literalToken(literal: PrimitiveLiteral): string {
  switch (literal.kind) {
    case "string":
      return `"${escapeString(literal.value)}"`;
    case "number":
      return literal.value;
    case "boolean":
      return literal.value === "true" ? "true" : "false";
    case "null":
      // Defensive only — callers should gate on kind != "null". The DSL has
      // no null literal; we return an always-false token so a stray call
      // produces a well-formed (if useless) filter rather than malformed input.
      return "false";
    default:
      return ((_: never): string => "false")(literal.kind);
  }
}

/**
 * Build `path == literal`. Returns null if `path` is not valid in the DSL,
 * so callers can gate the filter UI on a successful build.
 */
export function equalityExpr(path: string, literal: PrimitiveLiteral): string | null {
  if (!isValidPath(path)) {
    return null;
  }
  return `${path} == ${literalToken(literal)}`;
}

/** Build `path != literal`, mirror of `equalityExpr`. */
export function inequalityExpr(path: string, literal: PrimitiveLiteral): string | null {
  if (!isValidPath(path)) {
    return null;
  }
  return `${path} != ${literalToken(literal)}`;
}

/**
 * Append `clause` to `existing` with `&&`. Empty / whitespace existing
 * filter is treated as no constraint and replaced wholesale by `clause`,
 * so the "AND" affordance remains useful when the filter bar is empty.
 * Existing filters that already contain `||` are wrapped in parens to
 * keep precedence (`a || b && c` → `(a || b) && c`).
 */
export function composeAnd(existing: string, clause: string): string {
  const trimmed = existing.trim();
  if (trimmed === "") {
    return clause;
  }
  // Cheap precedence guard: any top-level `||` in the existing expression
  // would bind weaker than `&&` and silently change semantics when we
  // append. Wrapping in parens is safe and keeps the user's original
  // grouping intact.
  const needsParens = trimmed.includes("||");
  return `${needsParens ? `(${trimmed})` : trimmed} && ${clause}`;
}

/** Build `envelope.key == "<value>"` to follow a stream. */
export function followKeyExpr(key: string): string {
  return `envelope.key == "${escapeString(key)}"`;
}
