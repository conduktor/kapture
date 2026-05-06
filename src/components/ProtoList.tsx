import { useCallback, useMemo, useRef, type JSX, type KeyboardEvent } from "react";
import { List, type RowComponentProps } from "react-window";
import type { ProtoFrame } from "../types";
import { ensureRowVisible } from "../lib/listNav";

interface Props {
  frames: ProtoFrame[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}

interface RowProps {
  frames: ProtoFrame[];
  selectedId: string | null;
  /** Id of the request/response pair partner of the selected frame. */
  pairedId: string | null;
  onSelect: (id: string) => void;
}

const ROW_HEIGHT = 24;

// Wireshark-style protocol view. Lists every Kafka API frame the proto-hook
// observed (Send + Recv) in chronological order. Pairing of request to
// response is left to the eye for now (same corr_id + broker_id) — backend
// pairing is a follow-up.
export function ProtoList({ frames, selectedId, onSelect }: Props): JSX.Element {
  // Find the request/response partner of the selected frame.
  //
  // Why `(brokerId, corrId)` alone isn't a unique key: librdkafka
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
    const idx = frames.findIndex((f) => f.id === selectedId);
    if (idx < 0) {
      return null;
    }
    const sel = frames[idx];
    if (!sel) {
      return null;
    }
    const matches = (f: ProtoFrame): boolean =>
      f.corrId === sel.corrId && f.brokerId === sel.brokerId;
    if (sel.direction === "send") {
      for (let i = idx + 1; i < frames.length; i += 1) {
        const f = frames[i];
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
      const f = frames[i];
      if (f?.direction === "send" && matches(f)) {
        return f.id;
      }
      if (f?.direction === "recv" && matches(f)) {
        return null;
      }
    }
    return null;
  }, [frames, selectedId]);

  const rowProps = useMemo<RowProps>(
    () => ({ frames, selectedId, pairedId, onSelect }),
    [frames, selectedId, pairedId, onSelect],
  );
  const bodyRef = useRef<HTMLDivElement | null>(null);
  const onKeyDown = useCallback(
    (event: KeyboardEvent<HTMLElement>) => {
      if (event.key !== "ArrowDown" && event.key !== "ArrowUp") {
        return;
      }
      if (frames.length === 0) {
        return;
      }
      event.preventDefault();
      const dir = event.key === "ArrowDown" ? 1 : -1;
      const cur = selectedId === null ? -1 : frames.findIndex((f) => f.id === selectedId);
      const next =
        cur < 0
          ? dir > 0
            ? 0
            : frames.length - 1
          : Math.max(0, Math.min(frames.length - 1, cur + dir));
      const nextFrame = frames[next];
      if (!nextFrame) {
        return;
      }
      onSelect(nextFrame.id);
      ensureRowVisible(bodyRef.current, next, ROW_HEIGHT);
    },
    [frames, selectedId, onSelect],
  );

  return (
    <section className="msglist" aria-label="Protocol frames" tabIndex={0} onKeyDown={onKeyDown}>
      <div className="msglist__head">
        <span className="proto__col proto__col--ts">ts</span>
        <span className="proto__col proto__col--dir">dir</span>
        <span className="proto__col proto__col--api">api</span>
        <span className="proto__col proto__col--v">v</span>
        <span className="proto__col proto__col--broker">broker</span>
        <span className="proto__col proto__col--corr">corr</span>
        <span className="proto__col proto__col--size">size</span>
        <span className="proto__col proto__col--rtt">rtt (ms)</span>
      </div>
      <div className="msglist__body" ref={bodyRef}>
        {frames.length === 0 ? (
          <div className="msglist__empty">
            <p>No protocol frames yet.</p>
            <p className="muted">
              Connect to a cluster — every Kafka API call (Metadata, Fetch, Heartbeat, …) the
              consumer makes will appear here.
            </p>
          </div>
        ) : (
          <List
            className="msglist__virtual"
            rowComponent={ProtoRow}
            rowCount={frames.length}
            rowHeight={ROW_HEIGHT}
            rowProps={rowProps}
            overscanCount={8}
          />
        )}
      </div>
    </section>
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
    <button
      type="button"
      style={style}
      className={`msglist__row${isSelected ? " is-selected" : ""}${isPaired ? " is-paired" : ""}`}
      onClick={() => {
        onSelect(frame.id);
      }}
      aria-posinset={ariaAttributes["aria-posinset"]}
      aria-setsize={ariaAttributes["aria-setsize"]}
      role={ariaAttributes.role}
    >
      <span className="proto__col proto__col--ts">{ts}</span>
      <span
        className={`proto__col proto__col--dir proto__dir--${frame.direction}`}
        title={frame.direction === "send" ? "request out" : "response in"}
      >
        {frame.direction === "send" ? "→" : "←"}
      </span>
      <span className="proto__col proto__col--api">{frame.apiName}</span>
      <span className="proto__col proto__col--v">v{frame.apiVersion}</span>
      <span className="proto__col proto__col--broker">{frame.brokerId}</span>
      <span className="proto__col proto__col--corr">{frame.corrId}</span>
      <span className="proto__col proto__col--size">{frame.size}b</span>
      <span className="proto__col proto__col--rtt">
        {frame.direction === "recv" ? frame.rttMs.toFixed(2) : "—"}
      </span>
    </button>
  );
}
