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
import type { KafkaMessage } from "../types";
import type { FilterTarget } from "./FilterMenu";
import { formatBytes } from "../lib/formatBytes";
import { formatLocalTime } from "../lib/formatTimestamp";
import { useAutoFollow } from "../lib/useAutoFollow";
import { useFreshRows } from "../lib/useFreshRows";

type OpenFilterMenu = (target: FilterTarget, position: { x: number; y: number }) => void;

interface Props {
  messages: KafkaMessage[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onOpenFilterMenu: OpenFilterMenu;
  /** Stable key of the FilterTarget whose popover is currently
   *  open (parent state). The matching cell pins its filter icon
   *  visible so the popover-anchor stays obvious when the cursor
   *  drifts off the row. `null` when no popover is open. */
  activeMenuKey: string | null;
}

interface RowProps {
  messages: KafkaMessage[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onOpenFilterMenu: OpenFilterMenu;
  freshIds: ReadonlySet<string>;
  activeMenuKey: string | null;
}

const ROW_HEIGHT = 26;

const messageId = (m: KafkaMessage): string => m.id;

export function MessageList({
  messages,
  selectedId,
  onSelect,
  onOpenFilterMenu,
  activeMenuKey,
}: Props): JSX.Element {
  // Click-to-select must keep keyboard focus on the section (see
  // ProtoList for the rationale): under heavy traffic react-window
  // can unmount a focused row, snapping focus to <body> and breaking
  // arrow nav. Funneling focus to the section sidesteps it.
  const sectionRef = useRef<HTMLElement | null>(null);
  const focusSection = useCallback((): void => {
    sectionRef.current?.focus({ preventScroll: true });
  }, []);
  const onSelectRow = useCallback(
    (id: string): void => {
      onSelect(id);
      focusSection();
    },
    [onSelect, focusSection],
  );
  const freshIds = useFreshRows(messages, messageId);
  const rowProps = useMemo<RowProps>(
    () => ({
      messages,
      selectedId,
      onSelect: onSelectRow,
      onOpenFilterMenu,
      freshIds,
      activeMenuKey,
    }),
    [messages, selectedId, onSelectRow, onOpenFilterMenu, freshIds, activeMenuKey],
  );
  // react-window manages its own scrolling container. Driving body
  // scrollTop directly (an earlier attempt) didn't work because the
  // virtualisation host element is INSIDE .msglist__body and has its
  // own overflow. The library's imperative `scrollToRow` is the right
  // hook — `align: "auto"` is a no-op when the row is already in view.
  const listRef = useRef<ListImperativeAPI | null>(null);
  const { listProps: autoFollowListProps, armUserInput } = useAutoFollow(messages, listRef);

  const onKeyDown = useCallback(
    (event: KeyboardEvent<HTMLElement>) => {
      armUserInput();
      if (event.key !== "ArrowDown" && event.key !== "ArrowUp") {
        return;
      }
      if (messages.length === 0) {
        return;
      }
      event.preventDefault();
      const dir = event.key === "ArrowDown" ? 1 : -1;
      const cur = selectedId === null ? -1 : messages.findIndex((m) => m.id === selectedId);
      const next =
        cur < 0
          ? dir > 0
            ? 0
            : messages.length - 1
          : Math.max(0, Math.min(messages.length - 1, cur + dir));
      const nextMessage = messages[next];
      if (!nextMessage) {
        return;
      }
      onSelect(nextMessage.id);
      listRef.current?.scrollToRow({ index: next, align: "auto" });
    },
    [messages, selectedId, onSelect, armUserInput],
  );

  return (
    <section
      ref={sectionRef}
      className="msglist"
      aria-label="Captured messages"
      tabIndex={0}
      onKeyDown={onKeyDown}
    >
      <div className="msglist__head">
        <span
          className="msglist__col msglist__col--ts"
          title="Wall-clock time the message was captured (HH:MM:SS.µs, local timezone)."
        >
          Timestamp
        </span>
        <span className="msglist__col msglist__col--topic">topic</span>
        <span
          className="msglist__col msglist__col--paroff"
          title="Partition · Offset — the (partition, offset) pair uniquely locates the record on the topic. Filter button targets partition; for offset-only, type `envelope.offset == N` in the DSL above."
        >
          par·off
        </span>
        <span className="msglist__col msglist__col--key">key</span>
        <span className="msglist__col msglist__col--size">size</span>
      </div>
      <div className="msglist__body">
        {messages.length === 0 ? (
          <div className="msglist__empty">
            <p>No messages yet.</p>
            <p className="muted">Press Start to begin capture.</p>
          </div>
        ) : (
          <List
            className="msglist__virtual"
            listRef={listRef}
            rowComponent={MessageRow}
            rowCount={messages.length}
            rowHeight={ROW_HEIGHT}
            rowProps={rowProps}
            overscanCount={8}
            {...autoFollowListProps}
          />
        )}
      </div>
    </section>
  );
}

function MessageRow({
  ariaAttributes,
  index,
  style,
  messages,
  selectedId,
  onSelect,
  onOpenFilterMenu,
  freshIds,
  activeMenuKey,
}: RowComponentProps<RowProps>): JSX.Element | null {
  const message = messages[index];
  if (!message) {
    return null;
  }
  const isSelected = selectedId === message.id;
  const isFresh = freshIds.has(message.id);
  // Inline duplicate of `App.tsx::filterTargetKey` — keep the row
  // pure (no cross-module import for one-line concat) and fast.
  const targetKey = (target: FilterTarget): string =>
    `${target.path}|${target.literal.kind}|${target.literal.value}`;

  // Hover-revealed icon is the primary affordance. Right-click on the cell
  // is kept as a power-user shortcut (no UI cost). Both routes open the
  // same menu at the cursor position.
  const ctxHandler =
    (target: FilterTarget) =>
    (event: MouseEvent<HTMLSpanElement>): void => {
      event.preventDefault();
      event.stopPropagation();
      onOpenFilterMenu(target, { x: event.clientX, y: event.clientY });
    };
  const iconHandler =
    (target: FilterTarget) =>
    (event: MouseEvent<HTMLSpanElement>): void => {
      event.preventDefault();
      event.stopPropagation();
      const rect = event.currentTarget.getBoundingClientRect();
      onOpenFilterMenu(target, { x: rect.right, y: rect.bottom });
    };

  // Cell + hover-revealed filter icon. Inline so we don't have to thread
  // every cell through a wrapper component just for the affordance.
  const filterableCell = (
    extraClass: string,
    target: FilterTarget,
    children: ReactNode,
  ): ReactNode => (
    <span
      className={`msglist__col ${extraClass} msglist__col--filterable${
        activeMenuKey !== null && activeMenuKey === targetKey(target) ? " is-menu-anchor" : ""
      }`}
      onContextMenu={ctxHandler(target)}
    >
      <span className="msglist__col-content">{children}</span>
      <span
        className="msglist__filter-icon"
        role="button"
        aria-label="Filter actions"
        title="Filter actions"
        tabIndex={-1}
        onClick={iconHandler(target)}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            event.stopPropagation();
            const rect = event.currentTarget.getBoundingClientRect();
            onOpenFilterMenu(target, { x: rect.right, y: rect.bottom });
          }
        }}
      >
        {/* Funnel glyph — three lines tapering — reads as "filter
         *  actions" at small sizes far better than the bare chevron
         *  we used to ship. `currentColor` so the hover-accent rule
         *  paints stroke + fill in one go. */}
        <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true" focusable="false">
          <path
            d="M2.5 3h11l-4 5v4l-3 1.2V8l-4-5z"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinejoin="round"
          />
        </svg>
      </span>
    </span>
  );

  return (
    <button
      type="button"
      style={style}
      className={`msglist__row${isSelected ? " is-selected" : ""}${isFresh ? " msglist__row--fresh" : ""}`}
      onClick={() => {
        onSelect(message.id);
      }}
      title="Click to inspect — hover a cell for filter options"
      aria-posinset={ariaAttributes["aria-posinset"]}
      aria-setsize={ariaAttributes["aria-setsize"]}
      role={ariaAttributes.role}
    >
      <span className="msglist__col msglist__col--ts" title={message.timestamp}>
        {formatLocalTime(message.timestamp)}
      </span>
      {filterableCell(
        "msglist__col--topic",
        { path: "topic", literal: { kind: "string", value: message.topic } },
        message.topic,
      )}
      {filterableCell(
        "msglist__col--paroff",
        {
          path: "envelope.partition",
          literal: { kind: "number", value: String(message.partition) },
        },
        <>
          {message.partition}
          <span className="msglist__offset-suffix">·{message.offset.toLocaleString()}</span>
        </>,
      )}
      {message.key === null ? (
        <span className="msglist__col msglist__col--key">—</span>
      ) : (
        filterableCell(
          "msglist__col--key",
          {
            path: "envelope.key",
            literal: { kind: "string", value: message.key },
            supportsPresence: true,
          },
          message.key,
        )
      )}
      <span
        className="msglist__col msglist__col--size"
        title={`key ${message.keySize.toLocaleString()} B + value ${message.valueSize.toLocaleString()} B = ${message.sizeBytes.toLocaleString()} B (wire framing not counted)`}
      >
        {formatBytes(message.sizeBytes)}
      </span>
    </button>
  );
}
