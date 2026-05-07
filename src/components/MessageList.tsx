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

type OpenFilterMenu = (target: FilterTarget, position: { x: number; y: number }) => void;
type JumpToFetchFrame = (connectionId: number, corrId: number) => void;

interface Props {
  messages: KafkaMessage[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onOpenFilterMenu: OpenFilterMenu;
  onJumpToFetchFrame: JumpToFetchFrame;
}

interface RowProps {
  messages: KafkaMessage[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onOpenFilterMenu: OpenFilterMenu;
  onJumpToFetchFrame: JumpToFetchFrame;
}

const ROW_HEIGHT = 26;

export function MessageList({
  messages,
  selectedId,
  onSelect,
  onOpenFilterMenu,
  onJumpToFetchFrame,
}: Props): JSX.Element {
  const rowProps = useMemo<RowProps>(
    () => ({ messages, selectedId, onSelect, onOpenFilterMenu, onJumpToFetchFrame }),
    [messages, selectedId, onSelect, onOpenFilterMenu, onJumpToFetchFrame],
  );
  // react-window manages its own scrolling container. Driving body
  // scrollTop directly (an earlier attempt) didn't work because the
  // virtualisation host element is INSIDE .msglist__body and has its
  // own overflow. The library's imperative `scrollToRow` is the right
  // hook — `align: "auto"` is a no-op when the row is already in view.
  const listRef = useRef<ListImperativeAPI | null>(null);

  const onKeyDown = useCallback(
    (event: KeyboardEvent<HTMLElement>) => {
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
    [messages, selectedId, onSelect],
  );

  return (
    <section className="msglist" aria-label="Captured messages" tabIndex={0} onKeyDown={onKeyDown}>
      <div className="msglist__head">
        <span className="msglist__col msglist__col--ts">ts</span>
        <span className="msglist__col msglist__col--topic">topic</span>
        <span className="msglist__col msglist__col--p">p</span>
        <span className="msglist__col msglist__col--offset">offset</span>
        <span className="msglist__col msglist__col--key">key</span>
        <span className="msglist__col msglist__col--schema">schema</span>
        <span className="msglist__col msglist__col--size">size</span>
        <span className="msglist__col msglist__col--fetch" title="Originating Fetch frame">
          fetch
        </span>
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
  onJumpToFetchFrame,
}: RowComponentProps<RowProps>): JSX.Element | null {
  const message = messages[index];
  if (!message) {
    return null;
  }
  const isSelected = selectedId === message.id;

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
      className={`msglist__col ${extraClass} msglist__col--filterable`}
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
        ⌄
      </span>
    </span>
  );

  return (
    <button
      type="button"
      style={style}
      className={`msglist__row${isSelected ? " is-selected" : ""}`}
      onClick={() => {
        onSelect(message.id);
      }}
      title="Click to inspect — hover a cell for filter options"
      aria-posinset={ariaAttributes["aria-posinset"]}
      aria-setsize={ariaAttributes["aria-setsize"]}
      role={ariaAttributes.role}
    >
      <span className="msglist__col msglist__col--ts">{message.timestamp}</span>
      {filterableCell(
        "msglist__col--topic",
        { path: "topic", literal: { kind: "string", value: message.topic } },
        message.topic,
      )}
      {filterableCell(
        "msglist__col--p",
        {
          path: "envelope.partition",
          literal: { kind: "number", value: String(message.partition) },
        },
        message.partition,
      )}
      <span className="msglist__col msglist__col--offset">{message.offset}</span>
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
      {message.schemaName === null ? (
        <span className="msglist__col msglist__col--schema">
          <em className="muted">raw</em>
        </span>
      ) : (
        filterableCell(
          "msglist__col--schema",
          { path: "schema.name", literal: { kind: "string", value: message.schemaName } },
          message.schemaName,
        )
      )}
      <span
        className="msglist__col msglist__col--size"
        title={`${message.sizeBytes.toLocaleString()} bytes`}
      >
        {formatBytes(message.sizeBytes)}
      </span>
      {message.fetch === null ? (
        <span className="msglist__col msglist__col--fetch">
          <em className="muted">—</em>
        </span>
      ) : (
        <span className="msglist__col msglist__col--fetch">
          <button
            type="button"
            className="msglist__fetch-link"
            title={`Jump to Fetch frame (conn ${String(message.fetch.connectionId)}, corr ${String(
              message.fetch.corrId,
            )})`}
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              if (message.fetch !== null) {
                onJumpToFetchFrame(message.fetch.connectionId, message.fetch.corrId);
              }
            }}
          >
            ↗ Fetch
          </button>
        </span>
      )}
    </button>
  );
}
