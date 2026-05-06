import {
  useCallback,
  useMemo,
  useRef,
  type JSX,
  type KeyboardEvent,
  type MouseEvent,
  type ReactNode,
} from "react";
import { List, type ListImperativeAPI, type RowComponentProps } from "react-window";
import type { ProtoDirection, ProtoFrame } from "../types";
import {
  applyFilter,
  hasPredicate,
  isFilterEmpty,
  type ProtoFilter,
  type ProtoFilterChip,
  type ProtoFilterKind,
  type ProtoFilterMode,
} from "../lib/protoFilter";

interface Props {
  frames: ProtoFrame[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  /** Parsed filter (derived from the top-bar text by the parent). */
  filter: ProtoFilter;
  /** Append a predicate clause to the parent's filter text. */
  onAddPredicate: (kind: ProtoFilterKind, value: number | string, mode: ProtoFilterMode) => void;
  /** Remove a chip → drop the matching clause from the filter text. */
  onRemoveChip: (chip: ProtoFilterChip) => void;
  /** Clear the entire filter text. */
  onClearFilter: () => void;
  /** Cache lookup for decodedContains predicates (frame id → decoded body). */
  decodedFor?: (id: string) => string | undefined;
}

interface RowProps {
  frames: ProtoFrame[];
  selectedId: string | null;
  /** Id of the request/response pair partner of the selected frame. */
  pairedId: string | null;
  onSelect: (id: string) => void;
  onAddPredicate: AddPredicateFn;
  filter: ProtoFilter;
}

type AddPredicateFn = <K extends ProtoFilterKind>(
  kind: K,
  value: K extends "direction"
    ? ProtoDirection
    : K extends "connectionId" | "corrId"
      ? number
      : string,
  mode: ProtoFilterMode,
) => void;

const ROW_HEIGHT = 24;

// Wireshark-style protocol view. Lists every Kafka API frame the proto-hook
// observed (Send + Recv) in chronological order. Pairing of request to
// response is left to the eye for now (same corr_id + connection_id) — backend
// pairing is a follow-up.
export function ProtoList({
  frames,
  selectedId,
  onSelect,
  filter,
  onAddPredicate: onAddPredicateRaw,
  onRemoveChip: _onRemoveChip,
  onClearFilter: _onClearFilter,
  decodedFor,
}: Props): JSX.Element {
  // Apply the filter. Stable identity for `frames` ref keeps reconciliation
  // cheap across renders that don't change the predicates.
  const visibleFrames = useMemo<ProtoFrame[]>(() => {
    if (isFilterEmpty(filter)) {
      return frames;
    }
    return frames.filter((f) => applyFilter(filter, f, decodedFor));
  }, [frames, filter, decodedFor]);

  // Find the request/response partner of the selected frame.
  //
  // Why `(connectionId, corrId)` alone isn't a unique key: librdkafka
  // tracks corrId per `rd_kafka_broker_t::rkb_corrid` and the *logical*
  // bootstrap broker (broker_id = -1) is reused across every TCP
  // connection during discovery. Each new bootstrap connection restarts
  // at corrId = 1, so the same (-1, 1, ApiVersions) exchange shows up
  // many times. Pairing by id alone would highlight all of them.
  //
  // Trick: pick the *nearest* opposite-direction frame in the temporal
  // direction the matching half lives:
  //   SEND selected → first RECV *after* it
  //   RECV selected → last  SEND *before* it
  // The frames array is chronological so we can short-circuit.
  const pairedId = useMemo<string | null>(() => {
    if (selectedId === null) {
      return null;
    }
    const idx = visibleFrames.findIndex((f) => f.id === selectedId);
    if (idx < 0) {
      return null;
    }
    const sel = visibleFrames[idx];
    if (!sel) {
      return null;
    }
    const matches = (f: ProtoFrame): boolean =>
      f.corrId === sel.corrId && f.connectionId === sel.connectionId;
    if (sel.direction === "send") {
      for (let i = idx + 1; i < visibleFrames.length; i += 1) {
        const f = visibleFrames[i];
        if (f?.direction === "recv" && matches(f)) {
          return f.id;
        }
        // Stop early if we hit another SEND with the same key — that
        // means a new connection started and the original RECV will
        // never come.
        if (f?.direction === "send" && matches(f)) {
          return null;
        }
      }
      return null;
    }
    for (let i = idx - 1; i >= 0; i -= 1) {
      const f = visibleFrames[i];
      if (f?.direction === "send" && matches(f)) {
        return f.id;
      }
      if (f?.direction === "recv" && matches(f)) {
        return null;
      }
    }
    return null;
  }, [visibleFrames, selectedId]);

  // The parent owns the filter text; the popover just forwards the
  // (kind, value, mode) triple. AddPredicateFn's per-kind value type
  // is enforced at the call site (each FilterableCell knows its kind),
  // so the cast at this hand-off is safe.
  const onAddPredicate = useCallback<AddPredicateFn>(
    (kind, value, mode) => {
      onAddPredicateRaw(kind, value, mode);
    },
    [onAddPredicateRaw],
  );

  const rowProps = useMemo<RowProps>(
    () => ({ frames: visibleFrames, selectedId, pairedId, onSelect, onAddPredicate, filter }),
    [visibleFrames, selectedId, pairedId, onSelect, onAddPredicate, filter],
  );
  const listRef = useRef<ListImperativeAPI | null>(null);
  const onKeyDown = useCallback(
    (event: KeyboardEvent<HTMLElement>) => {
      if (event.key !== "ArrowDown" && event.key !== "ArrowUp") {
        return;
      }
      if (visibleFrames.length === 0) {
        return;
      }
      event.preventDefault();
      const dir = event.key === "ArrowDown" ? 1 : -1;
      const cur = selectedId === null ? -1 : visibleFrames.findIndex((f) => f.id === selectedId);
      const next =
        cur < 0
          ? dir > 0
            ? 0
            : visibleFrames.length - 1
          : Math.max(0, Math.min(visibleFrames.length - 1, cur + dir));
      const nextFrame = visibleFrames[next];
      if (!nextFrame) {
        return;
      }
      onSelect(nextFrame.id);
      listRef.current?.scrollToRow({ index: next, align: "auto" });
    },
    [visibleFrames, selectedId, onSelect],
  );

  const total = frames.length;

  return (
    <section className="msglist" aria-label="Protocol frames" tabIndex={0} onKeyDown={onKeyDown}>
      <div className="msglist__head">
        <span className="proto__col proto__col--dir" aria-hidden="true" />
        <span
          className="proto__col proto__col--ts"
          title="Wall-clock time the frame was observed (HH:MM:SS.µs); the dim suffix shows the delta since the previous visible frame."
        >
          Timestamp
        </span>
        <span
          className="proto__col proto__col--api"
          title="Kafka protocol API name + version (e.g. Metadata v12, Fetch v16). Version is the dim suffix."
        >
          api
        </span>
        <span
          className="proto__col proto__col--size"
          title="Wire size of the frame in bytes (4-byte length prefix + body)."
        >
          size
        </span>
        <span
          className="proto__col proto__col--broker"
          title="Connection ID · Correlation ID — the pair (conn, corr) is the unique key that pairs one request with its one response. Filter button targets conn (groups a client session); for corr-only, type `corrId == N` in the DSL above."
        >
          conn·corr
        </span>
        <span
          className="proto__col proto__col--rtt"
          title="Round-trip time in milliseconds. Only meaningful on Recv frames — the time between the matching Send and this Recv."
        >
          rtt
        </span>
      </div>
      <div className="msglist__body">
        {visibleFrames.length === 0 ? (
          <div className="msglist__empty">
            {total === 0 ? (
              <>
                <p>No protocol frames yet.</p>
                <p className="muted">
                  Connect to a cluster — every Kafka API call (Metadata, Fetch, Heartbeat, …) the
                  consumer makes will appear here.
                </p>
              </>
            ) : (
              <>
                <p>No frames match the current filter.</p>
                <p className="muted">{total} frame(s) hidden — clear filters to show all.</p>
              </>
            )}
          </div>
        ) : (
          <List
            className="msglist__virtual"
            listRef={listRef}
            rowComponent={ProtoRow}
            rowCount={visibleFrames.length}
            rowHeight={ROW_HEIGHT}
            rowProps={rowProps}
            overscanCount={8}
          />
        )}
      </div>
    </section>
  );
}

interface FilterableCellProps {
  className: string;
  title?: string;
  children: ReactNode;
  /** Predicate kind. */
  kind: ProtoFilterKind;
  /** Value to filter on; type varies with `kind` (string covers ProtoDirection). */
  value: number | string;
  /**
   * Toggle callback. Adds the (kind, value, mode) predicate if absent,
   * removes it if already present. Wiring lives in App.tsx so the
   * top-of-page DSL textbox stays the canonical source of truth.
   */
  onAdd: AddPredicateFn;
  /** Current filter — used to decide whether each button is active. */
  filter: ProtoFilter;
}

/**
 * Cell wrapper that reveals two tiny filter buttons on hover: `=`
 * (include this value) and `≠` (exclude this value). Each button
 * toggles its own predicate — clicking an already-active button
 * removes that predicate. Active buttons are highlighted in the
 * accent / danger colors so the current state of the row is visible
 * at a glance.
 */
function FilterableCell({
  className,
  title,
  children,
  kind,
  value,
  onAdd,
  filter,
}: FilterableCellProps): JSX.Element {
  const includeActive = hasPredicate(filter, kind as never, value as never, "include");
  const excludeActive = hasPredicate(filter, kind as never, value as never, "exclude");
  const handle =
    (mode: ProtoFilterMode) =>
    (event: MouseEvent<HTMLButtonElement>): void => {
      event.preventDefault();
      event.stopPropagation();
      onAdd(kind as never, value as never, mode);
    };
  return (
    <span className={`${className} proto-cell--filterable`} title={title}>
      <span className="proto-cell__content">{children}</span>
      <span className="proto-cell__filters">
        <button
          type="button"
          className={`proto-cell__filter proto-cell__filter--include${
            includeActive ? " is-active" : ""
          }`}
          tabIndex={-1}
          aria-label={includeActive ? "Remove include filter" : "Filter to this value"}
          aria-pressed={includeActive}
          title={includeActive ? "Click to remove this include filter" : "Filter to this value"}
          onClick={handle("include")}
        >
          =
        </button>
        <button
          type="button"
          className={`proto-cell__filter proto-cell__filter--exclude${
            excludeActive ? " is-active" : ""
          }`}
          tabIndex={-1}
          aria-label={excludeActive ? "Remove exclude filter" : "Exclude this value"}
          aria-pressed={excludeActive}
          title={excludeActive ? "Click to remove this exclude filter" : "Exclude this value"}
          onClick={handle("exclude")}
        >
          ≠
        </button>
      </span>
    </span>
  );
}

/** Parse an RFC3339 µs timestamp to milliseconds-since-epoch with
 *  sub-ms precision. `Date.parse` truncates to ms; we splice the µs
 *  trailer back in so the inter-frame deltas reflect the wire ordering
 *  even on bursts (multiple frames in the same ms). */
function tsToMs(ts: string): number {
  const d = new Date(ts).getTime();
  const dotIdx = ts.indexOf(".");
  const zIdx = ts.indexOf("Z");
  if (dotIdx < 0 || zIdx < 0 || zIdx <= dotIdx) {
    return d;
  }
  const frac = ts.slice(dotIdx + 1, zIdx);
  if (frac.length <= 3) {
    return d;
  }
  const tail = frac.slice(3, 6).padEnd(3, "0");
  const micro = Number.parseInt(tail, 10);
  return Number.isNaN(micro) ? d : d + micro / 1000;
}

function formatDelta(deltaMs: number): string {
  if (deltaMs < 1) {
    return "+<1ms";
  }
  if (deltaMs < 1000) {
    return `+${Math.round(deltaMs).toString()}ms`;
  }
  if (deltaMs < 60_000) {
    return `+${(deltaMs / 1000).toFixed(1)}s`;
  }
  return `+${Math.floor(deltaMs / 60_000).toString()}m`;
}

function ProtoRow({
  ariaAttributes,
  index,
  style,
  frames,
  selectedId,
  pairedId,
  onSelect,
  onAddPredicate,
  filter,
}: RowComponentProps<RowProps>): JSX.Element | null {
  const frame = frames[index];
  if (!frame) {
    return null;
  }
  const isSelected = selectedId === frame.id;
  const isPaired = pairedId === frame.id;
  // Trim to time-of-day for the list; full timestamp is in the detail view.
  // Backend emits RFC3339 with microseconds → keep HH:MM:SS.ffffff.
  const ts = frame.timestamp.slice(11, 26);
  const prev = index > 0 ? frames[index - 1] : undefined;
  const delta =
    prev !== undefined ? formatDelta(tsToMs(frame.timestamp) - tsToMs(prev.timestamp)) : null;
  return (
    <div
      style={style}
      className={`msglist__row${isSelected ? " is-selected" : ""}${isPaired ? " is-paired" : ""}`}
      onClick={() => {
        onSelect(frame.id);
      }}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onSelect(frame.id);
        }
      }}
      role={ariaAttributes.role}
      tabIndex={0}
      aria-posinset={ariaAttributes["aria-posinset"]}
      aria-setsize={ariaAttributes["aria-setsize"]}
      aria-selected={isSelected}
    >
      <span
        className="proto__col proto__col--dir"
        title={frame.direction === "send" ? "request out" : "response in"}
      >
        <span className={`proto__dir--${frame.direction}`}>
          {frame.direction === "send" ? "→" : "←"}
        </span>
      </span>
      <span className="proto__col proto__col--ts">
        {ts}
        {delta !== null ? <span className="proto__ts-delta"> {delta}</span> : null}
      </span>
      <FilterableCell
        className="proto__col proto__col--api"
        kind="apiName"
        value={frame.apiName}
        onAdd={onAddPredicate}
        filter={filter}
      >
        {frame.apiName}
        <span className="proto__api-suffix">
          {frame.direction === "send" ? "Request" : "Response"}
        </span>
        <span className="proto__api-ver"> v{frame.apiVersion}</span>
      </FilterableCell>
      <span className="proto__col proto__col--size">{frame.size}b</span>
      <FilterableCell
        className="proto__col proto__col--broker"
        kind="connectionId"
        value={frame.connectionId}
        onAdd={onAddPredicate}
        filter={filter}
        title="Click = / ≠ to filter on this connection. The number after · is the correlation id."
      >
        {frame.connectionId}
        <span className="proto__corr-suffix">·{frame.corrId}</span>
      </FilterableCell>
      <span className="proto__col proto__col--rtt">
        {frame.direction === "recv" ? (
          <>
            {frame.rttMs.toFixed(2)}
            <span className="proto__rtt-unit"> ms</span>
          </>
        ) : (
          "—"
        )}
      </span>
    </div>
  );
}
