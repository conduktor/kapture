import type { JSX, MouseEvent, ReactNode } from "react";
import type { DecodedValue, KafkaMessage } from "../types";
import { isValidPath, isValidPathSegment, type PrimitiveLiteral } from "../lib/filterExpr";
import type { FilterTarget } from "./FilterMenu";

type OpenFilterMenu = (target: FilterTarget, position: { x: number; y: number }) => void;

interface Props {
  message: KafkaMessage | null;
  onOpenFilterMenu: OpenFilterMenu;
}

export function LayerTree({ message, onOpenFilterMenu }: Props): JSX.Element {
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
          target={{ path: "topic", literal: { kind: "string", value: message.topic } }}
          onOpenFilterMenu={onOpenFilterMenu}
        />
        <Field
          name="partition"
          value={String(message.partition)}
          target={{
            path: "envelope.partition",
            literal: { kind: "number", value: String(message.partition) },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
        />
        <Field
          name="offset"
          value={String(message.offset)}
          target={{
            path: "envelope.offset",
            literal: { kind: "number", value: String(message.offset) },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
        />
        <Field
          name="timestamp"
          value={message.timestamp}
          target={{
            path: "envelope.timestamp",
            literal: { kind: "string", value: message.timestamp },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
        />
        <Field
          name="key"
          value={message.key ?? "—"}
          target={
            message.key === null
              ? null
              : {
                  path: "envelope.key",
                  literal: { kind: "string", value: message.key },
                  supportsPresence: true,
                }
          }
          onOpenFilterMenu={onOpenFilterMenu}
        />
        <Field
          name="size"
          value={`${message.sizeBytes} bytes`}
          target={{
            path: "envelope.size",
            literal: { kind: "number", value: String(message.sizeBytes) },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
        />
      </Layer>
      {message.fetch ? (
        <Layer title={`fetch — ${message.fetch.apiName} v${message.fetch.apiVersion}`}>
          <Field
            name="api"
            value={`${message.fetch.apiName} v${message.fetch.apiVersion}`}
            target={{
              path: "fetch.api_name",
              literal: { kind: "string", value: message.fetch.apiName },
            }}
            onOpenFilterMenu={onOpenFilterMenu}
          />
          <Field
            name="broker_id"
            value={String(message.fetch.brokerId)}
            target={{
              path: "fetch.broker_id",
              literal: { kind: "number", value: String(message.fetch.brokerId) },
            }}
            onOpenFilterMenu={onOpenFilterMenu}
          />
          <Field
            name="corr_id"
            value={`0x${message.fetch.corrId.toString(16).padStart(8, "0")}`}
            target={{
              path: "fetch.corr_id",
              literal: { kind: "number", value: String(message.fetch.corrId) },
            }}
            onOpenFilterMenu={onOpenFilterMenu}
          />
          <Field
            name="response_size"
            value={`${message.fetch.responseSize.toLocaleString()} bytes`}
            target={{
              path: "fetch.response_size",
              literal: { kind: "number", value: String(message.fetch.responseSize) },
            }}
            onOpenFilterMenu={onOpenFilterMenu}
          />
          <Field
            name="rtt_ms"
            value={`${message.fetch.rttMs.toFixed(2)} ms`}
            target={{
              path: "fetch.rtt_ms",
              literal: { kind: "number", value: message.fetch.rttMs.toString() },
            }}
            onOpenFilterMenu={onOpenFilterMenu}
          />
        </Layer>
      ) : null}
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
              // hide the filter affordance rather than build a broken path.
              target={
                isValidPathSegment(header.key)
                  ? {
                      path: `headers.${header.key}`,
                      literal: { kind: "string", value: header.value },
                      supportsPresence: true,
                    }
                  : null
              }
              onOpenFilterMenu={onOpenFilterMenu}
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
            target={{
              path: "schema.name",
              literal: { kind: "string", value: message.schemaName },
            }}
            onOpenFilterMenu={onOpenFilterMenu}
          />
        ) : (
          <span className="muted">no schema resolved</span>
        )}
      </Layer>
      <Layer title="payload">
        <DecodedNode
          value={message.payload}
          basePath="payload"
          onOpenFilterMenu={onOpenFilterMenu}
        />
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
  target,
  onOpenFilterMenu,
}: {
  name: string;
  value: string;
  target?: FilterTarget | null;
  onOpenFilterMenu?: OpenFilterMenu;
}): JSX.Element {
  // The whole field row is right-clickable when filterable. Saves the user
  // from having to aim at the small ⛶ button — Wireshark-style.
  const ctxHandler =
    target && onOpenFilterMenu
      ? (event: MouseEvent<HTMLDivElement>): void => {
          event.preventDefault();
          event.stopPropagation();
          onOpenFilterMenu(target, { x: event.clientX, y: event.clientY });
        }
      : undefined;

  return (
    <div
      className={`field${ctxHandler ? " field--filterable" : ""}`}
      {...(ctxHandler ? { onContextMenu: ctxHandler } : {})}
    >
      <span className="field__name">{name}</span>
      <span className="field__value">{value}</span>
      {target && onOpenFilterMenu ? (
        <FilterButton
          onClick={(event) => {
            onOpenFilterMenu(target, { x: event.clientX, y: event.clientY });
          }}
        />
      ) : null}
    </div>
  );
}

function FilterButton({
  onClick,
}: {
  onClick: (event: MouseEvent<HTMLButtonElement>) => void;
}): JSX.Element {
  return (
    <button
      type="button"
      className="field__filter"
      onClick={onClick}
      title="Filter actions (== / != / AND)"
      aria-label="Filter actions"
    >
      ⛶
    </button>
  );
}

function DecodedNode({
  value,
  basePath,
  onOpenFilterMenu,
}: {
  value: DecodedValue;
  basePath: string;
  onOpenFilterMenu: OpenFilterMenu;
}): JSX.Element {
  switch (value.kind) {
    case "primitive": {
      const literal: PrimitiveLiteral = { kind: value.type, value: value.value };
      // null is not a first-class DSL literal; render the value but skip
      // the filter affordance.
      const target: FilterTarget | null =
        value.type === "null" || !isValidPath(basePath) ? null : { path: basePath, literal };
      const ctxHandler = target
        ? (event: MouseEvent<HTMLSpanElement>): void => {
            event.preventDefault();
            event.stopPropagation();
            onOpenFilterMenu(target, { x: event.clientX, y: event.clientY });
          }
        : undefined;
      return (
        <span className="token-row" {...(ctxHandler ? { onContextMenu: ctxHandler } : {})}>
          <span className={`token token--${value.type}`}>{value.value}</span>
          {target ? (
            <FilterButton
              onClick={(event) => {
                onOpenFilterMenu(target, { x: event.clientX, y: event.clientY });
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
                    onOpenFilterMenu={onOpenFilterMenu}
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
                onOpenFilterMenu={onOpenFilterMenu}
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
