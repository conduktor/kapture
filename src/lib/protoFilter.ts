/**
 * Client-side filter for the Protocol tab. Distinct from the message
 * DSL (`src-tauri/src/filter.pest`) — the proto frame ring buffer is
 * small (≤ 4000 frames) so we filter in JS, no backend round-trip.
 *
 * Predicates are grouped by kind. Within a kind, includes are ORed
 * (a row matches if any include in that kind matches); excludes are
 * also ORed (a row is dropped if any exclude matches). Kinds AND
 * across each other. Empty include + empty exclude on a kind = no
 * constraint from that kind.
 *
 * `decodedContains` matches against the frame's `decoded` Debug-format
 * string (e.g. `topic_id: 86c8d3a0-…`). The summary list rows don't
 * carry `decoded`, so callers pass an opportunistic cache mapping
 * frame id → decoded text. Frames whose detail hasn't been fetched
 * yet bypass the predicate (over-include rather than over-exclude).
 */
import type { ProtoDirection, ProtoFrame } from "../types";
import { matchDebugField, parseDebug } from "./debugTree";

export type ProtoFilterKind =
  | "apiName"
  | "direction"
  | "connectionId"
  | "corrId"
  | "decodedContains"
  | "decodedField";

export type ProtoFilterMode = "include" | "exclude";

export interface ProtoFilter {
  apiNames: { include: string[]; exclude: string[] };
  directions: { include: ProtoDirection[]; exclude: ProtoDirection[] };
  connectionIds: { include: number[]; exclude: number[] };
  corrIds: { include: number[]; exclude: number[] };
  /** Free-text "decoded body must contain this substring" predicates. */
  decodedContains: { include: string[]; exclude: string[] };
  /** Path-aware "<StructName>.<fieldName> == <value>" predicates.
   *  Stored as JSON-encoded triples for primitive equality semantics
   *  in the include/exclude arrays. */
  decodedField: { include: string[]; exclude: string[] };
}

/** Decoded-field triple. `struct` is the parent struct's name (e.g.
 *  "MetadataRequestTopic"), `field` is the field name within it
 *  (e.g. "name"), and `value` is the string view of the leaf (no
 *  surrounding quotes for strings, raw text for primitives).
 *  Encoded into the filter slot as `<struct>.<field>=<value>` —
 *  Rust idents can't contain `.` or `=`, so the separators are
 *  unambiguous; only the value half can carry arbitrary chars. */
export interface DecodedFieldTriple {
  struct: string;
  field: string;
  value: string;
}

export function encodeDecodedField(t: DecodedFieldTriple): string {
  return `${t.struct}.${t.field}=${t.value}`;
}

export function decodeDecodedField(s: string): DecodedFieldTriple | null {
  const eq = s.indexOf("=");
  if (eq < 0) return null;
  const path = s.slice(0, eq);
  const value = s.slice(eq + 1);
  const dot = path.indexOf(".");
  if (dot < 0) return null;
  const struct = path.slice(0, dot);
  const field = path.slice(dot + 1);
  if (struct === "" || field === "") return null;
  return { struct, field, value };
}

export const EMPTY_PROTO_FILTER: ProtoFilter = {
  apiNames: { include: [], exclude: [] },
  directions: { include: [], exclude: [] },
  connectionIds: { include: [], exclude: [] },
  corrIds: { include: [], exclude: [] },
  decodedContains: { include: [], exclude: [] },
  decodedField: { include: [], exclude: [] },
};

export function isFilterEmpty(f: ProtoFilter): boolean {
  return (
    f.apiNames.include.length === 0 &&
    f.apiNames.exclude.length === 0 &&
    f.directions.include.length === 0 &&
    f.directions.exclude.length === 0 &&
    f.connectionIds.include.length === 0 &&
    f.connectionIds.exclude.length === 0 &&
    f.corrIds.include.length === 0 &&
    f.corrIds.exclude.length === 0 &&
    f.decodedContains.include.length === 0 &&
    f.decodedContains.exclude.length === 0 &&
    f.decodedField.include.length === 0 &&
    f.decodedField.exclude.length === 0
  );
}

/**
 * Apply the filter to a frame.
 *
 * `decodedFor(id)` returns the cached decoded body for a frame, or
 * `undefined` when the detail hasn't been fetched yet. When the filter
 * has any `decodedContains` predicate active and the decoded body is
 * not cached, the frame is REJECTED — a filter is a hard constraint,
 * not a hint. The caller is expected to pre-warm the decoded cache
 * (see App.tsx's batched prefetch when a decodedContains predicate is
 * present) so the user doesn't see a near-empty list while details
 * trickle in.
 */
export function applyFilter(
  f: ProtoFilter,
  frame: ProtoFrame,
  decodedFor?: (id: string) => string | undefined,
): boolean {
  if (!matchSet(f.apiNames, frame.apiName)) {
    return false;
  }
  if (!matchSet(f.directions, frame.direction)) {
    return false;
  }
  if (!matchSet(f.connectionIds, frame.connectionId)) {
    return false;
  }
  if (!matchSet(f.corrIds, frame.corrId)) {
    return false;
  }
  const dc = f.decodedContains;
  if (dc.include.length > 0 || dc.exclude.length > 0) {
    const decoded = decodedFor?.(frame.id);
    if (decoded === undefined) {
      // Hard filter semantics: no cached decoded body means we can't
      // confirm a match — reject rather than over-include.
      return false;
    }
    if (dc.exclude.some((s) => decoded.includes(s))) {
      return false;
    }
    if (dc.include.length > 0 && !dc.include.some((s) => decoded.includes(s))) {
      return false;
    }
  }
  const df = f.decodedField;
  if (df.include.length > 0 || df.exclude.length > 0) {
    const decoded = decodedFor?.(frame.id);
    if (decoded === undefined) {
      // Same hard-filter semantics as `decodedContains` — the decoded
      // body must be cached for path-aware matching to evaluate.
      return false;
    }
    const tree = parseDebug(decoded);
    if (tree === null) {
      // Debug-output parse failed (suspicious — kafka-protocol's
      // derive(Debug) shouldn't drift). Reject so the user sees an
      // empty list instead of false-positive matches.
      return false;
    }
    const triples = (slot: string[]): DecodedFieldTriple[] =>
      slot.map(decodeDecodedField).filter((t): t is DecodedFieldTriple => t !== null);
    const excludes = triples(df.exclude);
    if (excludes.some((t) => matchDebugField(tree, t.struct, t.field, t.value))) {
      return false;
    }
    const includes = triples(df.include);
    if (
      includes.length > 0 &&
      !includes.some((t) => matchDebugField(tree, t.struct, t.field, t.value))
    ) {
      return false;
    }
  }
  return true;
}

function matchSet<T>(set: { include: T[]; exclude: T[] }, value: T): boolean {
  if (set.exclude.includes(value)) {
    return false;
  }
  if (set.include.length > 0 && !set.include.includes(value)) {
    return false;
  }
  return true;
}

interface KindMap {
  apiName: string;
  direction: ProtoDirection;
  connectionId: number;
  corrId: number;
  decodedContains: string;
  /** JSON-encoded `DecodedFieldTriple` (see `encodeDecodedField`). */
  decodedField: string;
}

/**
 * Add a predicate. Same value+mode added twice → no-op (idempotent).
 * Adding the opposite mode for the same value moves it to the new
 * mode (a value can't sit in both include and exclude — that would be
 * unsatisfiable for that kind).
 */
export function addPredicate<K extends ProtoFilterKind>(
  f: ProtoFilter,
  kind: K,
  value: KindMap[K],
  mode: ProtoFilterMode,
): ProtoFilter {
  const slot = slotFor(f, kind);
  const opposite = mode === "include" ? "exclude" : "include";
  if (slot[mode].includes(value)) {
    return f;
  }
  const cleanedOpposite = slot[opposite].filter((v) => v !== value);
  const nextMode = [...slot[mode], value];
  const nextSlot: Slot<K> = { include: slot.include, exclude: slot.exclude };
  if (mode === "include") {
    nextSlot.include = nextMode;
    nextSlot.exclude = cleanedOpposite;
  } else {
    nextSlot.exclude = nextMode;
    nextSlot.include = cleanedOpposite;
  }
  return withSlot(f, kind, nextSlot);
}

export function removePredicate<K extends ProtoFilterKind>(
  f: ProtoFilter,
  kind: K,
  value: KindMap[K],
  mode: ProtoFilterMode,
): ProtoFilter {
  const slot = slotFor(f, kind);
  const arr = slot[mode];
  if (!arr.includes(value)) {
    return f;
  }
  const filtered = arr.filter((v) => v !== value);
  const nextSlot: Slot<K> = { include: slot.include, exclude: slot.exclude };
  if (mode === "include") {
    nextSlot.include = filtered;
  } else {
    nextSlot.exclude = filtered;
  }
  return withSlot(f, kind, nextSlot);
}

/** True if the filter currently holds the (kind, value, mode) triple. */
export function hasPredicate<K extends ProtoFilterKind>(
  f: ProtoFilter,
  kind: K,
  value: KindMap[K],
  mode: ProtoFilterMode,
): boolean {
  return slotFor(f, kind)[mode].includes(value);
}

export interface ProtoFilterChip<K extends ProtoFilterKind = ProtoFilterKind> {
  kind: K;
  mode: ProtoFilterMode;
  value: KindMap[K];
  /** Short label shown to the user. */
  label: string;
}

/** Flatten the filter to a list of chips for the chip-bar UI. Stable order. */
export function filterChips(f: ProtoFilter): ProtoFilterChip[] {
  const chips: ProtoFilterChip[] = [];
  for (const v of f.apiNames.include) {
    chips.push({ kind: "apiName", mode: "include", value: v, label: `api: ${v}` });
  }
  for (const v of f.apiNames.exclude) {
    chips.push({ kind: "apiName", mode: "exclude", value: v, label: `api ≠ ${v}` });
  }
  for (const v of f.directions.include) {
    chips.push({ kind: "direction", mode: "include", value: v, label: `dir: ${v}` });
  }
  for (const v of f.directions.exclude) {
    chips.push({ kind: "direction", mode: "exclude", value: v, label: `dir ≠ ${v}` });
  }
  for (const v of f.connectionIds.include) {
    chips.push({ kind: "connectionId", mode: "include", value: v, label: `conn: ${String(v)}` });
  }
  for (const v of f.connectionIds.exclude) {
    chips.push({ kind: "connectionId", mode: "exclude", value: v, label: `conn ≠ ${String(v)}` });
  }
  for (const v of f.corrIds.include) {
    chips.push({ kind: "corrId", mode: "include", value: v, label: `corr: ${String(v)}` });
  }
  for (const v of f.corrIds.exclude) {
    chips.push({ kind: "corrId", mode: "exclude", value: v, label: `corr ≠ ${String(v)}` });
  }
  for (const v of f.decodedContains.include) {
    chips.push({
      kind: "decodedContains",
      mode: "include",
      value: v,
      label: `body ⊃ ${truncate(v)}`,
    });
  }
  for (const v of f.decodedContains.exclude) {
    chips.push({
      kind: "decodedContains",
      mode: "exclude",
      value: v,
      label: `body ⊅ ${truncate(v)}`,
    });
  }
  return chips;
}

function truncate(s: string, max = 40): string {
  return s.length <= max ? s : `${s.slice(0, max - 1)}…`;
}

interface Slot<K extends ProtoFilterKind> {
  include: KindMap[K][];
  exclude: KindMap[K][];
}

function slotFor<K extends ProtoFilterKind>(f: ProtoFilter, kind: K): Slot<K> {
  switch (kind) {
    case "apiName":
      return f.apiNames as unknown as Slot<K>;
    case "direction":
      return f.directions as unknown as Slot<K>;
    case "connectionId":
      return f.connectionIds as unknown as Slot<K>;
    case "corrId":
      return f.corrIds as unknown as Slot<K>;
    case "decodedContains":
      return f.decodedContains as unknown as Slot<K>;
    case "decodedField":
      return f.decodedField as unknown as Slot<K>;
    default: {
      // Exhaustiveness: TS will flag an unhandled kind at compile time.
      const exhaustive: never = kind;
      throw new Error(`unknown kind: ${String(exhaustive)}`);
    }
  }
}

function withSlot<K extends ProtoFilterKind>(f: ProtoFilter, kind: K, slot: Slot<K>): ProtoFilter {
  switch (kind) {
    case "apiName":
      return { ...f, apiNames: slot as unknown as ProtoFilter["apiNames"] };
    case "direction":
      return { ...f, directions: slot as unknown as ProtoFilter["directions"] };
    case "connectionId":
      return { ...f, connectionIds: slot as unknown as ProtoFilter["connectionIds"] };
    case "corrId":
      return { ...f, corrIds: slot as unknown as ProtoFilter["corrIds"] };
    case "decodedContains":
      return { ...f, decodedContains: slot as unknown as ProtoFilter["decodedContains"] };
    case "decodedField":
      return { ...f, decodedField: slot as unknown as ProtoFilter["decodedField"] };
    default: {
      const exhaustive: never = kind;
      throw new Error(`unknown kind: ${String(exhaustive)}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Mini DSL: parse / serialize / append
//
// Grammar (AND-only, no OR):
//   expression := clause ('&&' clause)*
//   clause     := <kind> <op> <value>
//   kind       := 'apiName' | 'direction' | 'conn' | 'corrId' | 'decoded'
//   op         := '==' | '!='
//   value      := <quoted-string> | <bareword> | <integer>
//
// User-facing kind tokens differ from the AST keys:
//   conn    ↔ connectionId
//   decoded ↔ decodedContains
//
// Whitespace flexible. Quoted strings via "..." with `\"` and `\\` escapes.
// ---------------------------------------------------------------------------

type DslKind = "apiName" | "direction" | "conn" | "corrId" | "decoded" | "field";

const DSL_TO_AST: Record<DslKind, ProtoFilterKind> = {
  apiName: "apiName",
  direction: "direction",
  conn: "connectionId",
  corrId: "corrId",
  decoded: "decodedContains",
  field: "decodedField",
};

const AST_TO_DSL: Record<ProtoFilterKind, DslKind> = {
  apiName: "apiName",
  direction: "direction",
  connectionId: "conn",
  corrId: "corrId",
  decodedContains: "decoded",
  decodedField: "field",
};

const KIND_ORDER: ProtoFilterKind[] = [
  "apiName",
  "direction",
  "connectionId",
  "corrId",
  "decodedContains",
  "decodedField",
];

interface ParsedClause {
  kind: ProtoFilterKind;
  mode: ProtoFilterMode;
  // Stored using the AST value type (string for apiName/direction/decoded,
  // number for conn/corrId).
  value: string | number;
}

/**
 * Parse a filter expression into a `ProtoFilter`.
 *
 * On parse error: returns `EMPTY_PROTO_FILTER` plus a string explaining
 * where the parse failed (with column position). Choosing "empty filter
 * on error" over "keep last good filter" is deliberate — broken text
 * means "no filter, all rows visible" which is the least surprising
 * outcome for a typo mid-edit.
 *
 * Empty / whitespace-only input → empty filter, no error.
 */
export function parseExpression(text: string): { filter: ProtoFilter; error: string | null } {
  const trimmed = text.trim();
  if (trimmed === "") {
    return { filter: EMPTY_PROTO_FILTER, error: null };
  }
  try {
    const clauses = parseClauses(text);
    let f: ProtoFilter = EMPTY_PROTO_FILTER;
    for (const c of clauses) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      f = addPredicate(f, c.kind as any, c.value as any, c.mode);
    }
    return { filter: f, error: null };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { filter: EMPTY_PROTO_FILTER, error: message };
  }
}

/**
 * Serialize a `ProtoFilter` to its canonical expression form. Stable,
 * lexicographically/numerically sorted within each kind, kind order
 * fixed by `KIND_ORDER`. Includes come before excludes within a kind.
 *
 * Round-trip: `parseExpression(serializeFilter(f)).filter` ≡ f
 * (for any well-formed f).
 */
export function serializeFilter(f: ProtoFilter): string {
  const parts: string[] = [];
  for (const astKind of KIND_ORDER) {
    const slot = slotForReadOnly(f, astKind);
    const dsl = AST_TO_DSL[astKind];
    const sortedInclude = sortValues(astKind, slot.include);
    const sortedExclude = sortValues(astKind, slot.exclude);
    for (const v of sortedInclude) {
      parts.push(formatClause(astKind, dsl, "==", v));
    }
    for (const v of sortedExclude) {
      parts.push(formatClause(astKind, dsl, "!=", v));
    }
  }
  return parts.join(" && ");
}

/** `decodedField` renders as `field "<struct>.<field>" == "<value>"`
 *  so the filter bar reads like a path expression instead of an
 *  opaque encoded blob. All other kinds use the standard
 *  `<dsl> <op> <value>` shape. */
function formatClause(
  astKind: ProtoFilterKind,
  dsl: string,
  op: "==" | "!=",
  v: string | number,
): string {
  if (astKind === "decodedField") {
    const t = decodeDecodedField(String(v));
    if (t === null) {
      return `${dsl} ${op} ${formatValue(astKind, v)}`;
    }
    return `${dsl} "${escapeQuoted(`${t.struct}.${t.field}`)}" ${op} ${quoteString(t.value)}`;
  }
  return `${dsl} ${op} ${formatValue(astKind, v)}`;
}

function escapeQuoted(s: string): string {
  return s.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

/**
 * Append a clause to an existing expression text. Implementation: parse
 * → addPredicate → serialize. This guarantees the round-trip property
 * (the text is always in canonical form after an append) and avoids
 * fragile string splicing that would mishandle quoted values.
 *
 * If `currentText` doesn't parse, the new text starts fresh from the
 * single appended clause — losing the broken text is the right call;
 * the user is asking us to add a known-good predicate.
 */
export function appendClause<K extends ProtoFilterKind>(
  currentText: string,
  kind: K,
  value: KindMap[K],
  mode: ProtoFilterMode,
): string {
  const parsed = parseExpression(currentText);
  const base = parsed.error === null ? parsed.filter : EMPTY_PROTO_FILTER;
  const next = addPredicate(base, kind, value, mode);
  return serializeFilter(next);
}

// --- internals --------------------------------------------------------------

function slotForReadOnly(
  f: ProtoFilter,
  kind: ProtoFilterKind,
): { include: (string | number)[]; exclude: (string | number)[] } {
  switch (kind) {
    case "apiName":
      return f.apiNames;
    case "direction":
      return f.directions;
    case "connectionId":
      return f.connectionIds;
    case "corrId":
      return f.corrIds;
    case "decodedContains":
      return f.decodedContains;
    case "decodedField":
      return f.decodedField;
  }
}

function sortValues(
  kind: ProtoFilterKind,
  values: readonly (string | number)[],
): (string | number)[] {
  const copy = values.slice();
  if (kind === "connectionId" || kind === "corrId") {
    copy.sort((a, b) => (a as number) - (b as number));
  } else {
    copy.sort((a, b) => String(a).localeCompare(String(b)));
  }
  return copy;
}

function formatValue(kind: ProtoFilterKind, value: string | number): string {
  if (kind === "connectionId" || kind === "corrId") {
    return String(value);
  }
  if (kind === "direction") {
    // Bareword form for direction — matches what the popover would emit.
    return String(value);
  }
  // apiName, decodedContains: always quoted.
  return quoteString(String(value));
}

function quoteString(s: string): string {
  return `"${s.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

// --- parser -----------------------------------------------------------------

function parseClauses(text: string): ParsedClause[] {
  const tokens = tokenize(text);
  const clauses: ParsedClause[] = [];
  let i = 0;
  while (i < tokens.length) {
    const kindTok = tokens[i];
    if (kindTok?.type !== "ident") {
      throw new Error(parseErrAt(text, kindTok?.pos ?? text.length, "expected filter kind"));
    }
    const dslKind = kindTok.text as DslKind;
    if (!(dslKind in DSL_TO_AST)) {
      throw new Error(
        parseErrAt(
          text,
          kindTok.pos,
          `unknown filter kind "${kindTok.text}" (expected apiName/direction/conn/corrId/decoded)`,
        ),
      );
    }
    const astKind = DSL_TO_AST[dslKind];
    i += 1;

    // `field "<struct>.<field>" <op> "<value>"` — path-aware
    // predicate. The struct/field path is a quoted string between
    // the kind token and the operator. All other kinds keep the
    // standard `<kind> <op> <value>` shape.
    let pathString: string | null = null;
    if (astKind === "decodedField") {
      const pathTok = tokens[i];
      if (pathTok?.type !== "string") {
        throw new Error(
          parseErrAt(
            text,
            pathTok?.pos ?? text.length,
            'field requires a quoted "<Struct>.<field>" path',
          ),
        );
      }
      pathString = pathTok.value as string;
      i += 1;
    }

    const opTok = tokens[i];
    if (opTok?.type !== "op") {
      throw new Error(parseErrAt(text, opTok?.pos ?? text.length, "expected '==' or '!='"));
    }
    const mode: ProtoFilterMode = opTok.text === "==" ? "include" : "exclude";
    i += 1;

    const valTok = tokens[i];
    if (!valTok || (valTok.type !== "string" && valTok.type !== "ident" && valTok.type !== "int")) {
      throw new Error(parseErrAt(text, valTok?.pos ?? text.length, "expected a value"));
    }
    let value: string | number;
    if (astKind === "decodedField") {
      // The field-path was captured before the operator; combine it
      // with the value into the encoded triple.
      if (valTok.type !== "string") {
        throw new Error(parseErrAt(text, valTok.pos, "field value must be a quoted string"));
      }
      const dot = (pathString ?? "").indexOf(".");
      if (dot < 0 || pathString === null || pathString === "") {
        throw new Error(
          parseErrAt(text, valTok.pos, 'field path must look like "<Struct>.<field>"'),
        );
      }
      value = encodeDecodedField({
        struct: pathString.slice(0, dot),
        field: pathString.slice(dot + 1),
        value: valTok.value as string,
      });
    } else {
      value = coerceValue(astKind, valTok, text);
    }
    i += 1;
    clauses.push({ kind: astKind, mode, value });

    if (i < tokens.length) {
      const sep = tokens[i];
      if (sep?.type !== "and") {
        throw new Error(parseErrAt(text, sep?.pos ?? text.length, "expected '&&'"));
      }
      i += 1;
      // Trailing '&&' with nothing after → error.
      if (i >= tokens.length) {
        throw new Error(parseErrAt(text, sep.pos + 2, "expected another clause after '&&'"));
      }
    }
  }
  return clauses;
}

function coerceValue(kind: ProtoFilterKind, tok: Token, text: string): string | number {
  if (kind === "connectionId" || kind === "corrId") {
    if (tok.type !== "int") {
      throw new Error(parseErrAt(text, tok.pos, `${AST_TO_DSL[kind]} requires an integer value`));
    }
    return tok.value as number;
  }
  if (kind === "direction") {
    let v: string;
    if (tok.type === "ident") {
      v = tok.text;
    } else if (tok.type === "string") {
      v = tok.value as string;
    } else {
      throw new Error(parseErrAt(text, tok.pos, "direction requires 'send' or 'recv'"));
    }
    if (v !== "send" && v !== "recv") {
      throw new Error(parseErrAt(text, tok.pos, `direction must be 'send' or 'recv', got "${v}"`));
    }
    return v;
  }
  // apiName, decodedContains — string only.
  if (tok.type === "string") {
    return tok.value as string;
  }
  throw new Error(parseErrAt(text, tok.pos, `${AST_TO_DSL[kind]} requires a quoted string value`));
}

function parseErrAt(_text: string, pos: number, msg: string): string {
  return `parse error at col ${String(pos + 1)}: ${msg}`;
}

// --- tokenizer --------------------------------------------------------------

interface Token {
  type: "ident" | "op" | "and" | "string" | "int";
  text: string;
  pos: number;
  value?: string | number;
}

function tokenize(text: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (ch === undefined) {
      break;
    }
    if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
      i += 1;
      continue;
    }
    // && separator
    if (ch === "&" && text[i + 1] === "&") {
      tokens.push({ type: "and", text: "&&", pos: i });
      i += 2;
      continue;
    }
    // == / != operators
    if ((ch === "=" || ch === "!") && text[i + 1] === "=") {
      tokens.push({ type: "op", text: ch + "=", pos: i });
      i += 2;
      continue;
    }
    // Quoted string
    if (ch === '"') {
      const start = i;
      i += 1;
      let value = "";
      let closed = false;
      while (i < text.length) {
        const c = text[i];
        if (c === undefined) {
          break;
        }
        if (c === "\\" && i + 1 < text.length) {
          const esc = text[i + 1];
          if (esc === '"' || esc === "\\") {
            value += esc;
            i += 2;
            continue;
          }
          // Unknown escape: take the next char verbatim.
          value += esc ?? "";
          i += 2;
          continue;
        }
        if (c === '"') {
          closed = true;
          i += 1;
          break;
        }
        value += c;
        i += 1;
      }
      if (!closed) {
        throw new Error(parseErrAt(text, start, "unterminated string literal"));
      }
      tokens.push({ type: "string", text: text.slice(start, i), pos: start, value });
      continue;
    }
    // Integer (with optional leading '-')
    if (ch === "-" || (ch >= "0" && ch <= "9")) {
      const start = i;
      if (ch === "-") {
        i += 1;
      }
      const digitStart = i;
      while (i < text.length) {
        const c = text[i];
        if (c === undefined) {
          break;
        }
        if (c >= "0" && c <= "9") {
          i += 1;
          continue;
        }
        break;
      }
      if (i === digitStart) {
        // Lone '-' with no digits after.
        throw new Error(parseErrAt(text, start, "expected digits after '-'"));
      }
      const slice = text.slice(start, i);
      const n = Number.parseInt(slice, 10);
      if (!Number.isFinite(n)) {
        throw new Error(parseErrAt(text, start, `invalid integer "${slice}"`));
      }
      tokens.push({ type: "int", text: slice, pos: start, value: n });
      continue;
    }
    // Bareword: [A-Za-z_][A-Za-z0-9_]*
    if ((ch >= "a" && ch <= "z") || (ch >= "A" && ch <= "Z") || ch === "_") {
      const start = i;
      while (i < text.length) {
        const c = text[i];
        if (c === undefined) {
          break;
        }
        if (
          (c >= "a" && c <= "z") ||
          (c >= "A" && c <= "Z") ||
          (c >= "0" && c <= "9") ||
          c === "_"
        ) {
          i += 1;
          continue;
        }
        break;
      }
      tokens.push({ type: "ident", text: text.slice(start, i), pos: start });
      continue;
    }
    throw new Error(parseErrAt(text, i, `unexpected character "${ch}"`));
  }
  return tokens;
}
