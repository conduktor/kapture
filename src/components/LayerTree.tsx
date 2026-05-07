import type { JSX, MouseEvent, ReactNode } from "react";
import type { DecodedValue, KafkaMessageDetail } from "../types";
import { isValidPath, isValidPathSegment, type PrimitiveLiteral } from "../lib/filterExpr";
import { formatBytes } from "../lib/formatBytes";
import type { FilterTarget } from "./FilterMenu";

type OpenFilterMenu = (target: FilterTarget, position: { x: number; y: number }) => void;

// Below 1 KiB `formatBytes` already prints the exact byte count
// ("25 B"), so a `(25 B)` parenthetical is just noise. Above the
// threshold the formatted value is rounded, so the raw count is
// useful disambiguation.
function sizeWithRaw(n: number): string {
  return n < 1024 ? formatBytes(n) : `${formatBytes(n)} (${n.toLocaleString()} B)`;
}

/** Title for the schema Layer. Five states:
 *   - no envelope            → "schema: none (raw payload)"
 *   - id, no registry wired  → "schema id N (no registry connected)"
 *   - id, registry rejected  → "schema id N (registry error)"
 *   - id, resolved           → "schema: NAME (id N) — KIND"
 *   - id, awaiting resolver  → "schema id N (resolving…)"
 */
function schemaLayerTitle(m: KafkaMessageDetail): string {
  if (m.schemaId === null) {
    return "schema: none (raw payload)";
  }
  if (m.schemaKind === "NO_REGISTRY") {
    return `schema id ${String(m.schemaId)} (no registry connected)`;
  }
  if (m.schemaKind === "UNRESOLVED") {
    return `schema id ${String(m.schemaId)} (registry error)`;
  }
  if (m.schemaName !== null) {
    const kind = m.schemaKind !== null ? ` — ${m.schemaKind}` : "";
    return `schema: ${m.schemaName} (id ${String(m.schemaId)})${kind}`;
  }
  return `schema id ${String(m.schemaId)} (resolving…)`;
}

interface Props {
  /** Full message body — lazy-fetched on selection. `null` while
   *  loading or when no row is selected. */
  message: KafkaMessageDetail | null;
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
        {message.topicId !== null ? (
          <Field
            name="topic_id"
            value={message.topicId}
            target={{
              path: "envelope.topic_id",
              literal: { kind: "string", value: message.topicId },
            }}
            onOpenFilterMenu={onOpenFilterMenu}
          />
        ) : null}
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
          value={sizeWithRaw(message.sizeBytes)}
          target={{
            path: "envelope.size",
            literal: { kind: "number", value: String(message.sizeBytes) },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
        />
        <Field
          name="key size"
          value={sizeWithRaw(message.keySize)}
          target={{
            path: "envelope.key_size",
            literal: { kind: "number", value: String(message.keySize) },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
        />
        <Field
          name="value size"
          value={sizeWithRaw(message.valueSize)}
          target={{
            path: "envelope.value_size",
            literal: { kind: "number", value: String(message.valueSize) },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
        />
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
      {message.schemaId !== null ? (
        // Hide the schema layer entirely on raw-payload records — no
        // envelope means no useful detail to show, and the "schema:
        // none" placeholder was just noise.
        <Layer title={schemaLayerTitle(message)}>
          <Field
            name="id"
            value={String(message.schemaId)}
            target={{
              path: "schema.id",
              literal: { kind: "number", value: String(message.schemaId) },
            }}
            onOpenFilterMenu={onOpenFilterMenu}
          />
          {message.schemaName !== null ? (
            <Field
              name="name"
              value={message.schemaName}
              target={{
                path: "schema.name",
                literal: { kind: "string", value: message.schemaName },
              }}
              onOpenFilterMenu={onOpenFilterMenu}
            />
          ) : null}
          {message.schemaKind !== null &&
          message.schemaKind !== "NO_REGISTRY" &&
          message.schemaKind !== "UNRESOLVED" ? (
            // Hide the sentinel kinds — the layer title surfaces
            // those states verbatim. Only render `kind` when it's
            // a real Confluent label users would filter on
            // (`AVRO` / `JSON` / `PROTOBUF`).
            <Field
              name="kind"
              value={message.schemaKind}
              target={{
                path: "schema.kind",
                literal: { kind: "string", value: message.schemaKind },
              }}
              onOpenFilterMenu={onOpenFilterMenu}
            />
          ) : null}
        </Layer>
      ) : null}
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

  // Filter chevron lives only on the value cell — the name carries no
  // independent semantic ("topic" alone isn't a predicate). Right-click
  // on the whole row still opens the menu (handled via ctxHandler).
  const openAt = (event: MouseEvent<HTMLButtonElement>): void => {
    if (target && onOpenFilterMenu) {
      onOpenFilterMenu(target, { x: event.clientX, y: event.clientY });
    }
  };

  return (
    <div
      className={`field${ctxHandler ? " field--filterable" : ""}`}
      {...(ctxHandler ? { onContextMenu: ctxHandler } : {})}
    >
      <span className="field__name">{name}</span>
      <span className="field__value field__value--with-icon">
        {value}
        {target && onOpenFilterMenu ? <FilterButton onClick={openAt} /> : null}
      </span>
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
      ⌄
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
