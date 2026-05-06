import { useMemo, type JSX, type MouseEvent } from "react";
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

  // Right-click on a filterable cell opens the filter menu at the cursor.
  // Cells without a sensible filter (offset, ts, size) intentionally have
  // no handler — the row's default behaviour (select on click) still works.
  const ctxHandler =
    (target: FilterTarget) =>
    (event: MouseEvent<HTMLSpanElement>): void => {
      event.preventDefault();
      event.stopPropagation();
      onOpenFilterMenu(target, { x: event.clientX, y: event.clientY });
    };

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
      title="Click to inspect — double-click to follow this key — right-click a cell to filter"
      aria-posinset={ariaAttributes["aria-posinset"]}
      aria-setsize={ariaAttributes["aria-setsize"]}
      role={ariaAttributes.role}
    >
      <span className="msglist__col msglist__col--ts">{message.timestamp}</span>
      <span
        className="msglist__col msglist__col--topic msglist__col--filterable"
        onContextMenu={ctxHandler({
          path: "topic",
          literal: { kind: "string", value: message.topic },
        })}
      >
        {message.topic}
      </span>
      <span
        className="msglist__col msglist__col--p msglist__col--filterable"
        onContextMenu={ctxHandler({
          path: "envelope.partition",
          literal: { kind: "number", value: String(message.partition) },
        })}
      >
        {message.partition}
      </span>
      <span className="msglist__col msglist__col--offset">{message.offset}</span>
      {message.key === null ? (
        <span className="msglist__col msglist__col--key">—</span>
      ) : (
        <span
          className="msglist__col msglist__col--key msglist__col--filterable"
          onContextMenu={ctxHandler({
            path: "envelope.key",
            literal: { kind: "string", value: message.key },
          })}
        >
          {message.key}
        </span>
      )}
      {message.schemaName === null ? (
        <span className="msglist__col msglist__col--schema">
          <em className="muted">raw</em>
        </span>
      ) : (
        <span
          className="msglist__col msglist__col--schema msglist__col--filterable"
          onContextMenu={ctxHandler({
            path: "schema.name",
            literal: { kind: "string", value: message.schemaName },
          })}
        >
          {message.schemaName}
        </span>
      )}
      <span className="msglist__col msglist__col--size">{message.sizeBytes}b</span>
    </button>
  );
}
