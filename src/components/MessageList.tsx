import { useMemo, type JSX, type MouseEvent, type ReactNode } from "react";
import { List, type RowComponentProps } from "react-window";
import type { KafkaMessage } from "../types";
import type { FilterTarget } from "./FilterMenu";

type OpenFilterMenu = (target: FilterTarget, position: { x: number; y: number }) => void;

interface Props {
  messages: KafkaMessage[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onFollow: (message: KafkaMessage) => void;
  onOpenFilterMenu: OpenFilterMenu;
}

interface RowProps {
  messages: KafkaMessage[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onFollow: (message: KafkaMessage) => void;
  onOpenFilterMenu: OpenFilterMenu;
}

const ROW_HEIGHT = 26;

export function MessageList({
  messages,
  selectedId,
  onSelect,
  onFollow,
  onOpenFilterMenu,
}: Props): JSX.Element {
  const rowProps = useMemo<RowProps>(
    () => ({ messages, selectedId, onSelect, onFollow, onOpenFilterMenu }),
    [messages, selectedId, onSelect, onFollow, onOpenFilterMenu],
  );

  return (
    <section className="msglist" aria-label="Captured messages">
      <div className="msglist__head">
        <span className="msglist__col msglist__col--ts">ts</span>
        <span className="msglist__col msglist__col--topic">topic</span>
        <span className="msglist__col msglist__col--p">p</span>
        <span className="msglist__col msglist__col--offset">offset</span>
        <span className="msglist__col msglist__col--key">key</span>
        <span className="msglist__col msglist__col--schema">schema</span>
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
  onFollow,
  onOpenFilterMenu,
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
      onDoubleClick={() => {
        onFollow(message);
      }}
      title="Click to inspect — double-click to follow this key — hover a cell for filter options"
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
          { path: "envelope.key", literal: { kind: "string", value: message.key } },
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
      <span className="msglist__col msglist__col--size">{message.sizeBytes}b</span>
    </button>
  );
}
