import type { JSX, ReactNode } from "react";
import type { DecodedValue, KafkaMessage } from "../types";

interface Props {
  message: KafkaMessage | null;
}

export function LayerTree({ message }: Props): JSX.Element {
  if (!message) {
    return (
      <section className="layers layers--empty" aria-label="Decoded layers">
        <p className="muted">Select a message to inspect its layers.</p>
      </section>
    );
  }
  return (
    <section className="layers" aria-label="Decoded layers">
      <Layer title="envelope">
        <Field name="topic" value={message.topic} />
        <Field name="partition" value={String(message.partition)} />
        <Field name="offset" value={String(message.offset)} />
        <Field name="timestamp" value={message.timestamp} />
        <Field name="key" value={message.key ?? "—"} />
        <Field name="size" value={`${message.sizeBytes} bytes`} />
      </Layer>
      <Layer title={`headers (${message.headers.length})`}>
        {message.headers.length === 0 ? (
          <span className="muted">no headers</span>
        ) : (
          message.headers.map((header) => (
            <Field key={header.key} name={header.key} value={header.value} />
          ))
        )}
      </Layer>
      <Layer
        title={
          message.schemaName
            ? `schema: ${message.schemaName} (id ${message.schemaId ?? "?"})`
            : "schema: none (raw payload)"
        }
      >
        {message.schemaName ? (
          <Field name="name" value={message.schemaName} />
        ) : (
          <span className="muted">no schema resolved</span>
        )}
      </Layer>
      <Layer title="payload">
        <DecodedNode value={message.payload} />
      </Layer>
    </section>
  );
}

function Layer({ title, children }: { title: string; children: ReactNode }): JSX.Element {
  return (
    <details className="layer" open>
      <summary className="layer__title">{title}</summary>
      <div className="layer__body">{children}</div>
    </details>
  );
}

function Field({ name, value }: { name: string; value: string }): JSX.Element {
  return (
    <div className="field">
      <span className="field__name">{name}</span>
      <span className="field__value">{value}</span>
    </div>
  );
}

function DecodedNode({ value }: { value: DecodedValue }): JSX.Element {
  switch (value.kind) {
    case "primitive":
      return <span className={`token token--${value.type}`}>{value.value}</span>;
    case "bytes":
      return <span className="token token--bytes">{`<${value.length} bytes> ${value.hex}`}</span>;
    case "object":
      return (
        <div className="object">
          {value.fields.map((field) => (
            <div key={field.name} className="object__entry">
              <span className="object__key">{field.name}</span>
              <DecodedNode value={field.value} />
            </div>
          ))}
        </div>
      );
    case "array":
      return (
        <div className="array">
          {value.items.map((item, index) => (
            <div key={index} className="array__entry">
              <span className="array__idx">[{index}]</span>
              <DecodedNode value={item} />
            </div>
          ))}
        </div>
      );
    default:
      return ((_: never) => <span className="muted">unknown</span>)(value);
  }
}
