/**
 * Build filter DSL expressions from UI interactions, escaping strings safely.
 *
 * The grammar lives in `src-tauri/src/filter.pest`. Keep this in sync:
 *   - strings are double-quoted; backslashes and double quotes are escaped
 *   - numbers are emitted as-is
 *   - booleans → `true` / `false`
 *   - null → `<path> != <path>` (no real null literal in grammar; we
 *     approximate as "always false" for null payload values)
 */

export interface PrimitiveLiteral {
  kind: "string" | "number" | "boolean" | "null";
  value: string;
}

export function escapeString(raw: string): string {
  return raw.replace(/\\/gu, "\\\\").replace(/"/gu, '\\"');
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
      // null is not a first-class literal in the DSL; the closest faithful
      // expression is `<path> != <path>` which is always false.
      return "false";
    default:
      return ((_: never): string => "false")(literal.kind);
  }
}

/** Build `path == literal` (or `path != path` for null). */
export function equalityExpr(path: string, literal: PrimitiveLiteral): string {
  return `${path} == ${literalToken(literal)}`;
}

/** Build `envelope.key == "<value>"` to follow a stream. */
export function followKeyExpr(key: string): string {
  return `envelope.key == "${escapeString(key)}"`;
}
