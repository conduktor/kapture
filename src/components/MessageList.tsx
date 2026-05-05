import { useMemo, type JSX } from "react";
import { List, type RowComponentProps } from "react-window";
import type { KafkaMessage } from "../types";

interface Props {
  messages: KafkaMessage[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onFollow: (message: KafkaMessage) => void;
}

interface RowProps {
  messages: KafkaMessage[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onFollow: (message: KafkaMessage) => void;
}

const ROW_HEIGHT = 26;

export function MessageList({ messages, selectedId, onSelect, onFollow }: Props): JSX.Element {
  const rowProps = useMemo<RowProps>(
    () => ({ messages, selectedId, onSelect, onFollow }),
    [messages, selectedId, onSelect, onFollow],
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
}: RowComponentProps<RowProps>): JSX.Element | null {
  const message = messages[index];
  if (!message) {
    return null;
  }
  const isSelected = selectedId === message.id;
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
      title="Click to inspect — double-click to follow this key"
      aria-posinset={ariaAttributes["aria-posinset"]}
      aria-setsize={ariaAttributes["aria-setsize"]}
      role={ariaAttributes.role}
    >
      <span className="msglist__col msglist__col--ts">{message.timestamp}</span>
      <span className="msglist__col msglist__col--topic">{message.topic}</span>
      <span className="msglist__col msglist__col--p">{message.partition}</span>
      <span className="msglist__col msglist__col--offset">{message.offset}</span>
      <span className="msglist__col msglist__col--key">{message.key ?? "—"}</span>
      <span className="msglist__col msglist__col--schema">
        {message.schemaName ?? <em className="muted">raw</em>}
      </span>
      <span className="msglist__col msglist__col--size">{message.sizeBytes}b</span>
    </button>
  );
}
