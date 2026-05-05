import type { JSX, ReactNode } from "react";
import type { DecodedValue, KafkaMessage } from "../types";
import { equalityExpr, type PrimitiveLiteral } from "../lib/filterExpr";

interface Props {
  message: KafkaMessage | null;
  onApplyFilter: (expression: string) => void;
}

export function LayerTree({ message, onApplyFilter }: Props): JSX.Element {
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
        <Field
          name="topic"
          value={message.topic}
          onFilter={() => {
            onApplyFilter(equalityExpr("topic", { kind: "string", value: message.topic }));
          }}
        />
        <Field
          name="partition"
          value={String(message.partition)}
          onFilter={() => {
            onApplyFilter(
              equalityExpr("envelope.partition", {
                kind: "number",
                value: String(message.partition),
              }),
            );
          }}
        />
        <Field name="offset" value={String(message.offset)} />
        <Field name="timestamp" value={message.timestamp} />
        <Field
          name="key"
          value={message.key ?? "—"}
          onFilter={
            message.key
              ? () => {
                  onApplyFilter(
                    equalityExpr("envelope.key", {
                      kind: "string",
                      value: message.key ?? "",
                    }),
                  );
                }
              : undefined
          }
        />
        <Field name="size" value={`${message.sizeBytes} bytes`} />
      </Layer>
      <Layer title={`headers (${message.headers.length})`}>
        {message.headers.length === 0 ? (
          <span className="muted">no headers</span>
        ) : (
          message.headers.map((header) => (
            <Field
              key={header.key}
              name={header.key}
              value={header.value}
              onFilter={() => {
                onApplyFilter(
                  equalityExpr(`headers.${header.key}`, {
                    kind: "string",
                    value: header.value,
                  }),
                );
              }}
            />
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
          <Field
            name="name"
            value={message.schemaName}
            onFilter={() => {
              onApplyFilter(
                equalityExpr("schema.name", {
                  kind: "string",
                  value: message.schemaName ?? "",
                }),
              );
            }}
          />
        ) : (
          <span className="muted">no schema resolved</span>
        )}
      </Layer>
      <Layer title="payload">
        <DecodedNode value={message.payload} basePath="payload" onApplyFilter={onApplyFilter} />
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

function Field({
  name,
  value,
  onFilter,
}: {
  name: string;
  value: string;
  onFilter?: (() => void) | undefined;
}): JSX.Element {
  return (
    <div className="field">
      <span className="field__name">{name}</span>
      <span className="field__value">{value}</span>
      {onFilter ? <FilterButton onClick={onFilter} /> : null}
    </div>
  );
}

function FilterButton({ onClick }: { onClick: () => void }): JSX.Element {
  return (
    <button
      type="button"
      className="field__filter"
      onClick={onClick}
      title="Filter on this value"
      aria-label="Filter on this value"
    >
      ⛶
    </button>
  );
}

function DecodedNode({
  value,
  basePath,
  onApplyFilter,
}: {
  value: DecodedValue;
  basePath: string;
  onApplyFilter: (expression: string) => void;
}): JSX.Element {
  switch (value.kind) {
    case "primitive": {
      const literal: PrimitiveLiteral = { kind: value.type, value: value.value };
      return (
        <span className="token-row">
          <span className={`token token--${value.type}`}>{value.value}</span>
          {value.type !== "null" ? (
            <FilterButton
              onClick={() => {
                onApplyFilter(equalityExpr(basePath, literal));
              }}
            />
          ) : null}
        </span>
      );
    }
    case "bytes":
      return <span className="token token--bytes">{`<${value.length} bytes> ${value.hex}`}</span>;
    case "object":
      return (
        <div className="object">
          {value.fields.map((field) => (
            <div key={field.name} className="object__entry">
              <span className="object__key">{field.name}</span>
              <DecodedNode
                value={field.value}
                basePath={`${basePath}.${field.name}`}
                onApplyFilter={onApplyFilter}
              />
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
              <DecodedNode
                value={item}
                basePath={`${basePath}.${index}`}
                onApplyFilter={onApplyFilter}
              />
            </div>
          ))}
        </div>
      );
    default:
      return ((_: never) => <span className="muted">unknown</span>)(value);
  }
}
