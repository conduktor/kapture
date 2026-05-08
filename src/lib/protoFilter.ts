/**
 * Client-side filter for the Protocol tab. Distinct from the message
 * DSL (`src-tauri/src/filter.pest`) — the proto frame ring buffer is
 * small (≤ 5000 frames) so we filter in JS, no backend round-trip.
 *
 * Predicates are grouped by kind. Within a kind, includes are ORed
 * (a row matches if any include in that kind matches); excludes are
 * also ORed (a row is dropped if any exclude matches). Kinds AND
 * across each other. Empty include + empty exclude on a kind = no
 * constraint from that kind.
 *
 * `decodedContains` and `decodedField` match against the frame's
 * `decodedJson` — the typed body emitted by the Kapture fork of
 * kafka-protocol (which derives `serde::Serialize` on every message
 * struct). The summary list rows don't carry `decodedJson`, so
 * callers pass an opportunistic cache mapping frame id → JSON value.
 * Frames whose detail hasn't been fetched yet are REJECTED — a
 * filter is a hard constraint, not a hint; the caller pre-warms the
 * cache when one of these predicates is active.
 */
import type { ProtoDirection, ProtoFrame } from "../types";
import { matchJsonPath } from "./jsonField";

export type ProtoFilterKind = "apiName" | "direction" | "connectionId" | "corrId" | "decodedField";

export type ProtoFilterMode = "include" | "exclude";

export interface ProtoFilter {
  apiNames: { include: string[]; exclude: string[] };
  directions: { include: ProtoDirection[]; exclude: ProtoDirection[] };
  connectionIds: { include: number[]; exclude: number[] };
  corrIds: { include: number[]; exclude: number[] };
  /** Path-aware "<dotted.path> == <value>" predicates over the typed
   *  decoded body. Stored as JSON-encoded `DecodedFieldPair`s for
   *  primitive equality semantics in the include/exclude arrays. */
  decodedField: { include: string[]; exclude: string[] };
}

/** Decoded-field pair. `path` is a dotted chain of object-key
 *  segments rooted at the decoded body
 *  (`topic_data.partition_data.records.base_offset`); `value` is
 *  the string view of the leaf. Encoded into the filter slot as
 *  `<path>=<value>` — segments use `.`, `=` separates path from
 *  value; the value half can carry arbitrary chars including `=`,
 *  captured by `slice(eq + 1)`.
 *
 *  Semantics: walked via `matchJsonPath` which descends through
 *  arrays per-element. The path is *strict* — a bare `name`
 *  matches only the root-level `name`, never a nested
 *  `topics[].name`. The user disambiguates by writing the full
 *  path. */
export interface DecodedFieldPair {
  path: string;
  value: string;
}

export function encodeDecodedField(p: DecodedFieldPair): string {
  return `${p.path}=${p.value}`;
}

export function decodeDecodedField(s: string): DecodedFieldPair | null {
  const eq = s.indexOf("=");
  if (eq < 0) return null;
  const path = s.slice(0, eq);
  const value = s.slice(eq + 1);
  if (path === "") return null;
  return { path, value };
}

export const EMPTY_PROTO_FILTER: ProtoFilter = {
  apiNames: { include: [], exclude: [] },
  directions: { include: [], exclude: [] },
  connectionIds: { include: [], exclude: [] },
  corrIds: { include: [], exclude: [] },
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
    !hasBodyTouchingPredicate(f)
  );
}

/**
 * Does the filter need the decoded body to evaluate?
 *
 * `decodedField` is a hard-filter predicate that REJECTS a frame
 * whose `decodedJson` isn't cached yet (`applyFilter` semantics).
 * The App-level prefetch loop reads this to decide whether to walk
 * the ring and warm the cache for every visible frame — without
 * that, a filter on a not-yet-clicked frame would silently drop it
 * from the list.
 */
export function hasBodyTouchingPredicate(f: ProtoFilter): boolean {
  return f.decodedField.include.length > 0 || f.decodedField.exclude.length > 0;
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
  decodedFor?: (id: string) => unknown,
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
  const df = f.decodedField;
  if (df.include.length === 0 && df.exclude.length === 0) {
    return true;
  }
  const json = decodedFor?.(frame.id);
  if (json === undefined) {
    // Hard filter semantics: no cached decoded body means we can't
    // confirm a match — reject rather than over-include.
    return false;
  }
  const pairs = (slot: string[]): DecodedFieldPair[] =>
    slot.map(decodeDecodedField).filter((p): p is DecodedFieldPair => p !== null);
  const excludes = pairs(df.exclude);
  if (excludes.some((p) => matchJsonPath(json, p.path, p.value))) {
    return false;
  }
  const includes = pairs(df.include);
  if (includes.length > 0 && !includes.some((p) => matchJsonPath(json, p.path, p.value))) {
    return false;
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
  /** JSON-encoded `DecodedFieldPair` (see `encodeDecodedField`). */
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
  return chips;
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
//   kind       := 'apiName' | 'direction' | 'conn' | 'corrId' | <ident>
//   op         := '==' | '!='
//   value      := <quoted-string> | <bareword> | <integer>
//
// User-facing kind tokens differ from the AST keys:
//   conn    ↔ connectionId
//
// Any non-reserved ident (including dotted paths like
// `topic_data.partition_data.records.base_offset`) lands in the
// `decodedField` slot — walked strictly against the typed JSON body.
//
// Whitespace flexible. Quoted strings via "..." with `\"` and `\\` escapes.
// ---------------------------------------------------------------------------

type DslKind = "apiName" | "direction" | "conn" | "corrId";

const DSL_TO_AST: Record<DslKind, ProtoFilterKind> = {
  apiName: "apiName",
  direction: "direction",
  conn: "connectionId",
  corrId: "corrId",
};

const AST_TO_DSL: Record<Exclude<ProtoFilterKind, "decodedField">, DslKind> = {
  apiName: "apiName",
  direction: "direction",
  connectionId: "conn",
  corrId: "corrId",
};

const KIND_ORDER: ProtoFilterKind[] = [
  "apiName",
  "direction",
  "connectionId",
  "corrId",
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
    const sortedInclude = sortValues(astKind, slot.include);
    const sortedExclude = sortValues(astKind, slot.exclude);
    if (astKind === "decodedField") {
      for (const v of sortedInclude) {
        parts.push(formatFieldClause("==", String(v)));
      }
      for (const v of sortedExclude) {
        parts.push(formatFieldClause("!=", String(v)));
      }
      continue;
    }
    const dsl = AST_TO_DSL[astKind];
    for (const v of sortedInclude) {
      parts.push(`${dsl} == ${formatValue(astKind, v)}`);
    }
    for (const v of sortedExclude) {
      parts.push(`${dsl} != ${formatValue(astKind, v)}`);
    }
  }
  return parts.join(" && ");
}

/** A decodedField clause renders path-first: `<a.b.c> == "<value>"`.
 *  No leading kind keyword — the parser recognises any non-reserved
 *  ident (dotted segments allowed) as a path predicate. Reads like
 *  a direct property check
 *  (`error_code == "0"`, `topic_data.name == "events"`). */
function formatFieldClause(op: "==" | "!=", encoded: string): string {
  const p = decodeDecodedField(encoded);
  if (p === null) {
    // Defensive fallback: shouldn't happen since the slot only ever
    // carries values produced by `encodeDecodedField`.
    return `field ${op} ${quoteString(encoded)}`;
  }
  return `${p.path} ${op} ${quoteString(p.value)}`;
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
  // apiName: always quoted.
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
    // Reserved kind tokens (`apiName`, `direction`, `conn`, `corrId`,
    // `decoded`) take the existing typed path. Any other ident —
    // including dotted paths like `topic_data.partition_data.records.base_offset` —
    // is a JSON path predicate, walked strictly from the body root
    // by `matchJsonPath`.
    const dslKind = kindTok.text as DslKind;
    const isReservedKind = dslKind in DSL_TO_AST;
    const astKind: ProtoFilterKind = isReservedKind ? DSL_TO_AST[dslKind] : "decodedField";
    const fieldPath: string | null = isReservedKind ? null : kindTok.text;
    i += 1;

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
      // Comparison is on the *string view* of the JSON leaf —
      // accept the same value forms as other kinds (quoted string,
      // bareword ident, integer literal) and stringify each into
      // the encoded `<path>=<value>` slot.
      if (fieldPath === null || fieldPath === "") {
        throw new Error(parseErrAt(text, valTok.pos, "field path must be non-empty"));
      }
      const stringValue =
        valTok.type === "string"
          ? (valTok.value as string)
          : valTok.type === "int"
            ? String(valTok.value)
            : valTok.text;
      value = encodeDecodedField({ path: fieldPath, value: stringValue });
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

function coerceValue(
  kind: Exclude<ProtoFilterKind, "decodedField">,
  tok: Token,
  text: string,
): string | number {
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
    // Bareword: [A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*
    // Dotted segments are allowed so `topic_data.partition_data.records.base_offset`
    // tokenises as a single ident — that's the JSON path the user
    // wrote, navigated as-is by `matchJsonPath`. A trailing dot or
    // double dot fails the second-segment check and stops the lex
    // loop, leaving the bare leading ident intact.
    if ((ch >= "a" && ch <= "z") || (ch >= "A" && ch <= "Z") || ch === "_") {
      const start = i;
      const consumeSegment = (): boolean => {
        const segStart = i;
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
        return i > segStart;
      };
      consumeSegment();
      while (text[i] === ".") {
        const dotPos = i;
        i += 1;
        if (!consumeSegment()) {
          // Roll back the dot so the parser sees a clean ident
          // followed by an unexpected character it can complain about.
          i = dotPos;
          break;
        }
      }
      tokens.push({ type: "ident", text: text.slice(start, i), pos: start });
      continue;
    }
    throw new Error(parseErrAt(text, i, `unexpected character "${ch}"`));
  }
  return tokens;
}
