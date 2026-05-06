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

export type ProtoFilterKind =
  | "apiName"
  | "direction"
  | "connectionId"
  | "corrId"
  | "decodedContains";

export type ProtoFilterMode = "include" | "exclude";

export interface ProtoFilter {
  apiNames: { include: string[]; exclude: string[] };
  directions: { include: ProtoDirection[]; exclude: ProtoDirection[] };
  connectionIds: { include: number[]; exclude: number[] };
  corrIds: { include: number[]; exclude: number[] };
  /** Free-text "decoded body must contain this substring" predicates. */
  decodedContains: { include: string[]; exclude: string[] };
}

export const EMPTY_PROTO_FILTER: ProtoFilter = {
  apiNames: { include: [], exclude: [] },
  directions: { include: [], exclude: [] },
  connectionIds: { include: [], exclude: [] },
  corrIds: { include: [], exclude: [] },
  decodedContains: { include: [], exclude: [] },
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
    f.decodedContains.exclude.length === 0
  );
}

/**
 * Apply the filter to a frame.
 *
 * `decodedFor(id)` returns the cached decoded body for a frame, or
 * `undefined` when the detail hasn't been fetched yet. When undefined,
 * decodedContains predicates are *bypassed* on that frame (we'd rather
 * over-include than hide rows the user can't yet inspect).
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
    if (decoded !== undefined) {
      if (dc.exclude.some((s) => decoded.includes(s))) {
        return false;
      }
      if (dc.include.length > 0 && !dc.include.some((s) => decoded.includes(s))) {
        return false;
      }
    }
    // decoded === undefined: bypass — the frame's detail is not in cache.
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
    default: {
      const exhaustive: never = kind;
      throw new Error(`unknown kind: ${String(exhaustive)}`);
    }
  }
}
