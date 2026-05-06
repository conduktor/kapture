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
  EMPTY_PROTO_FILTER,
  addPredicate,
  applyFilter,
  filterChips,
  isFilterEmpty,
  removePredicate,
  type ProtoFilter,
  type ProtoFilterChip,
  type ProtoFilterKind,
  type ProtoFilterMode,
} from "../lib/protoFilter";

interface Props {
  frames: ProtoFrame[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  filter: ProtoFilter;
  onFilterChange: (next: ProtoFilter) => void;
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
  onFilterChange,
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

  // TS can't narrow the `value` conditional inside AddPredicateFn back
  // to the matching `KindMap[K]` slot — addPredicate is generic so we
  // forward the (kind, value) pair through an `any`-cast. The public
  // AddPredicateFn signature still enforces the correct value-per-kind
  // at every call site outside this component.
  const onAddPredicate = useCallback<AddPredicateFn>(
    (kind, value, mode) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      onFilterChange(addPredicate(filter, kind as any, value as any, mode));
    },
    [filter, onFilterChange],
  );

  const rowProps = useMemo<RowProps>(
    () => ({ frames: visibleFrames, selectedId, pairedId, onSelect, onAddPredicate }),
    [visibleFrames, selectedId, pairedId, onSelect, onAddPredicate],
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

  const chips = useMemo<ProtoFilterChip[]>(() => filterChips(filter), [filter]);
  const total = frames.length;
  const shown = visibleFrames.length;

  return (
    <section className="msglist" aria-label="Protocol frames" tabIndex={0} onKeyDown={onKeyDown}>
      {chips.length > 0 ? (
        <ProtoFilterBar
          chips={chips}
          shown={shown}
          total={total}
          onRemove={(chip) => {
            onFilterChange(removePredicate(filter, chip.kind, chip.value, chip.mode));
          }}
          onClear={() => {
            onFilterChange(EMPTY_PROTO_FILTER);
          }}
        />
      ) : null}
      <div className="msglist__head">
        <span className="proto__col proto__col--ts">ts</span>
        <span className="proto__col proto__col--dir">dir</span>
        <span className="proto__col proto__col--api">api</span>
        <span className="proto__col proto__col--v">v</span>
        <span className="proto__col proto__col--broker">conn</span>
        <span className="proto__col proto__col--corr">corr</span>
        <span className="proto__col proto__col--size">size</span>
        <span className="proto__col proto__col--rtt">rtt (ms)</span>
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

function ProtoFilterBar({
  chips,
  shown,
  total,
  onRemove,
  onClear,
}: {
  chips: ProtoFilterChip[];
  shown: number;
  total: number;
  onRemove: (chip: ProtoFilterChip) => void;
  onClear: () => void;
}): JSX.Element {
  return (
    <div className="proto-filterbar" role="region" aria-label="Active proto filters">
      <span className="proto-filterbar__label">Filters</span>
      {chips.map((chip, i) => (
        <button
          key={`${chip.kind}:${chip.mode}:${String(chip.value)}:${String(i)}`}
          type="button"
          className={`proto-chip proto-chip--${chip.mode}`}
          title={`Remove: ${chip.label}`}
          onClick={() => {
            onRemove(chip);
          }}
        >
          <span className="proto-chip__label">{chip.label}</span>
          <span className="proto-chip__x" aria-hidden="true">
            ×
          </span>
        </button>
      ))}
      <button type="button" className="proto-filterbar__clear" onClick={onClear}>
        clear all
      </button>
      <span className="proto-filterbar__count">
        showing {shown} of {total}
      </span>
    </div>
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
  onAdd: AddPredicateFn;
}

/**
 * Cell wrapper that reveals a tiny ⊕/⊖ filter button on hover. Click
 * adds an include predicate; alt/option-click adds an exclude. Stops
 * row-click propagation so filtering doesn't also select the row.
 */
function FilterableCell({
  className,
  title,
  children,
  kind,
  value,
  onAdd,
}: FilterableCellProps): JSX.Element {
  // Clicks on the cell body itself bubble up to the row so row
  // selection still works. Only the ⊕ icon stops propagation —
  // adding a filter MUST NOT also flip the selected frame.
  const onIconClick = (event: MouseEvent<HTMLButtonElement>): void => {
    event.preventDefault();
    event.stopPropagation();
    const mode: ProtoFilterMode = event.altKey ? "exclude" : "include";
    onAdd(kind as never, value as never, mode);
  };
  return (
    <span
      className={`${className} proto-cell--filterable`}
      title={title ?? "Click ⊕ to filter • Alt-click to exclude"}
    >
      <span className="proto-cell__content">{children}</span>
      <button
        type="button"
        className="proto-cell__filter"
        tabIndex={-1}
        aria-label="Filter on this value"
        title="Click: filter ⊕ this value • Alt/Option-click: exclude ⊖"
        onClick={onIconClick}
      >
        ⊕
      </button>
    </span>
  );
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
      <span className="proto__col proto__col--ts">{ts}</span>
      <FilterableCell
        className="proto__col proto__col--dir"
        kind="direction"
        value={frame.direction}
        onAdd={onAddPredicate}
        title={frame.direction === "send" ? "request out" : "response in"}
      >
        <span className={`proto__dir--${frame.direction}`}>
          {frame.direction === "send" ? "→" : "←"}
        </span>
      </FilterableCell>
      <FilterableCell
        className="proto__col proto__col--api"
        kind="apiName"
        value={frame.apiName}
        onAdd={onAddPredicate}
      >
        {frame.apiName}
      </FilterableCell>
      <span className="proto__col proto__col--v">v{frame.apiVersion}</span>
      <FilterableCell
        className="proto__col proto__col--broker"
        kind="connectionId"
        value={frame.connectionId}
        onAdd={onAddPredicate}
      >
        {frame.connectionId}
      </FilterableCell>
      <FilterableCell
        className="proto__col proto__col--corr"
        kind="corrId"
        value={frame.corrId}
        onAdd={onAddPredicate}
      >
        {frame.corrId}
      </FilterableCell>
      <span className="proto__col proto__col--size">{frame.size}b</span>
      <span className="proto__col proto__col--rtt">
        {frame.direction === "recv" ? frame.rttMs.toFixed(2) : "—"}
      </span>
    </div>
  );
}
