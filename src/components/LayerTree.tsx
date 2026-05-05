import type { JSX, ReactNode } from "react";
import type { DecodedValue, KafkaMessage } from "../types";
import { equalityExpr, isValidPathSegment, type PrimitiveLiteral } from "../lib/filterExpr";

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
          expression={equalityExpr("topic", { kind: "string", value: message.topic })}
          onApplyFilter={onApplyFilter}
        />
        <Field
          name="partition"
          value={String(message.partition)}
          expression={equalityExpr("envelope.partition", {
            kind: "number",
            value: String(message.partition),
          })}
          onApplyFilter={onApplyFilter}
        />
        <Field name="offset" value={String(message.offset)} />
        <Field name="timestamp" value={message.timestamp} />
        <Field
          name="key"
          value={message.key ?? "—"}
          expression={
            message.key === null
              ? null
              : equalityExpr("envelope.key", { kind: "string", value: message.key })
          }
          onApplyFilter={onApplyFilter}
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
              // Header names containing dots, spaces, or characters outside
              // the DSL identifier grammar can't be addressed directly —
              // hide the filter button rather than emit a broken expression.
              expression={
                isValidPathSegment(header.key)
                  ? equalityExpr(`headers.${header.key}`, {
                      kind: "string",
                      value: header.value,
                    })
                  : null
              }
              onApplyFilter={onApplyFilter}
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
            expression={equalityExpr("schema.name", {
              kind: "string",
              value: message.schemaName,
            })}
            onApplyFilter={onApplyFilter}
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
  expression,
  onApplyFilter,
}: {
  name: string;
  value: string;
  expression?: string | null;
  onApplyFilter?: (expression: string) => void;
}): JSX.Element {
  return (
    <div className="field">
      <span className="field__name">{name}</span>
      <span className="field__value">{value}</span>
      {expression && onApplyFilter ? (
        <FilterButton
          onClick={() => {
            onApplyFilter(expression);
          }}
        />
      ) : null}
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
      const expression = value.type === "null" ? null : equalityExpr(basePath, literal);
      return (
        <span className="token-row">
          <span className={`token token--${value.type}`}>{value.value}</span>
          {expression ? (
            <FilterButton
              onClick={() => {
                onApplyFilter(expression);
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
          {value.fields.map((field) => {
            // JSON object keys are arbitrary strings; only descend with a
            // filterable path when the segment is grammar-valid. Keys that
            // contain dots / spaces / quotes still render but without the
            // filter affordance.
            const childPath = isValidPathSegment(field.name) ? `${basePath}.${field.name}` : null;
            return (
              <div key={field.name} className="object__entry">
                <span className="object__key">{field.name}</span>
                {childPath === null ? (
                  <DecodedDisplayOnly value={field.value} />
                ) : (
                  <DecodedNode
                    value={field.value}
                    basePath={childPath}
                    onApplyFilter={onApplyFilter}
                  />
                )}
              </div>
            );
          })}
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

/** Render-only variant for nodes whose path can't be expressed in the DSL. */
function DecodedDisplayOnly({ value }: { value: DecodedValue }): JSX.Element {
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
              <DecodedDisplayOnly value={field.value} />
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
              <DecodedDisplayOnly value={item} />
            </div>
          ))}
        </div>
      );
    default:
      return ((_: never) => <span className="muted">unknown</span>)(value);
  }
}
