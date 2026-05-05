import type { JSX } from "react";
import type { KafkaMessage } from "../types";

interface Props {
  messages: KafkaMessage[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}

export function MessageList({ messages, selectedId, onSelect }: Props): JSX.Element {
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
      <div className="msglist__body" role="rowgroup">
        {messages.length === 0 ? (
          <div className="msglist__empty">
            <p>No messages yet.</p>
            <p className="muted">Press Start to begin capture.</p>
          </div>
        ) : (
          messages.map((message) => (
            <button
              type="button"
              key={message.id}
              className={`msglist__row${selectedId === message.id ? " is-selected" : ""}`}
              onClick={() => {
                onSelect(message.id);
              }}
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
          ))
        )}
      </div>
    </section>
  );
}
