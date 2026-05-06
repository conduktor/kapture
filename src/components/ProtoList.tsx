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
  onSelect: (id: string) => void;
}

const ROW_HEIGHT = 24;

// Wireshark-style protocol view. Lists every Kafka API frame the proto-hook
// observed (Send + Recv) in chronological order. Pairing of request to
// response is left to the eye for now (same corr_id + broker_id) — backend
// pairing is a follow-up.
export function ProtoList({ frames, selectedId, onSelect }: Props): JSX.Element {
  const rowProps = useMemo<RowProps>(
    () => ({ frames, selectedId, onSelect }),
    [frames, selectedId, onSelect],
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
  onSelect,
}: RowComponentProps<RowProps>): JSX.Element | null {
  const frame = frames[index];
  if (!frame) {
    return null;
  }
  const isSelected = selectedId === frame.id;
  // Trim to time-of-day for the list; full timestamp is in the detail view.
  // Backend emits RFC3339 with microseconds → keep HH:MM:SS.ffffff.
  const ts = frame.timestamp.slice(11, 26);
  return (
    <button
      type="button"
      style={style}
      className={`msglist__row${isSelected ? " is-selected" : ""}`}
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
