import type { JSX, ReactNode } from "react";
import type { DecodedValue, KafkaMessageDetail } from "../types";
import { isValidPath, isValidPathSegment, type PrimitiveLiteral } from "../lib/filterExpr";
import { formatBytes } from "../lib/formatBytes";
import type { FilterTarget } from "./FilterMenu";
import { FilterableField } from "./FilterableField";

type OpenFilterMenu = (
  target: FilterTarget,
  position: { x: number; y: number },
  anchorId?: string,
) => void;

// Below 1 KiB `formatBytes` already prints the exact byte count
// ("25 B"), so a `(25 B)` parenthetical is just noise. Above the
// threshold the formatted value is rounded, so the raw count is
// useful disambiguation.
function sizeWithRaw(n: number): string {
  return n < 1024 ? formatBytes(n) : `${formatBytes(n)} (${n.toLocaleString()} B)`;
}

/** Title for the schema Layer. Two reference paths (legacy id /
 *  header GUID — Confluent CP 8.1.1+) × five resolution states.
 *  `schemaRef` returns the human-readable token for whichever
 *  path the record uses.
 */
function schemaLayerTitle(m: KafkaMessageDetail): string {
  const ref = schemaRefLabel(m);
  if (ref === null) {
    return "schema: none (raw payload)";
  }
  if (m.schemaKind === "NO_REGISTRY") {
    return `${ref} (no registry connected)`;
  }
  if (m.schemaKind === "UNRESOLVED") {
    return `${ref} (registry error)`;
  }
  if (m.schemaName !== null) {
    const kind = m.schemaKind !== null ? ` — ${m.schemaKind}` : "";
    return `schema: ${m.schemaName} (${ref})${kind}`;
  }
  return `${ref} (resolving…)`;
}

function schemaRefLabel(m: KafkaMessageDetail): string | null {
  if (m.schemaGuid !== null) {
    return `schema guid ${m.schemaGuid}`;
  }
  if (m.schemaId !== null) {
    return `schema id ${String(m.schemaId)}`;
  }
  return null;
}

interface Props {
  /** Full message body — lazy-fetched on selection. `null` while
   *  loading or when no row is selected. */
  message: KafkaMessageDetail | null;
  onOpenFilterMenu: OpenFilterMenu;
  /** Stable key of the FilterTarget whose popover is currently open
   *  (parent state). Threaded into each Field so the funnel stays
   *  visible + accent-pinned while its popover is up. */
  activeMenuKey: string | null;
}

export function LayerTree({ message, onOpenFilterMenu, activeMenuKey }: Props): JSX.Element {
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
          activeMenuKey={activeMenuKey}
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
          activeMenuKey={activeMenuKey}
        />
        <Field
          name="offset"
          value={String(message.offset)}
          target={{
            path: "envelope.offset",
            literal: { kind: "number", value: String(message.offset) },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
          activeMenuKey={activeMenuKey}
        />
        <Field
          name="timestamp"
          value={message.timestamp}
          target={{
            path: "envelope.timestamp",
            literal: { kind: "string", value: message.timestamp },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
          activeMenuKey={activeMenuKey}
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
          activeMenuKey={activeMenuKey}
        />
        <Field
          name="size"
          value={sizeWithRaw(message.sizeBytes)}
          target={{
            path: "envelope.size",
            literal: { kind: "number", value: String(message.sizeBytes) },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
          activeMenuKey={activeMenuKey}
        />
        <Field
          name="key size"
          value={sizeWithRaw(message.keySize)}
          target={{
            path: "envelope.key_size",
            literal: { kind: "number", value: String(message.keySize) },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
          activeMenuKey={activeMenuKey}
        />
        <Field
          name="value size"
          value={sizeWithRaw(message.valueSize)}
          target={{
            path: "envelope.value_size",
            literal: { kind: "number", value: String(message.valueSize) },
          }}
          onOpenFilterMenu={onOpenFilterMenu}
          activeMenuKey={activeMenuKey}
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
      {message.schemaId !== null || message.schemaGuid !== null ? (
        // Hide the schema layer entirely on raw-payload records (no
        // legacy id, no header guid) — the "schema: none"
        // placeholder was just noise.
        <Layer title={schemaLayerTitle(message)}>
          {message.schemaGuid !== null ? (
            <Field
              name="guid"
              value={message.schemaGuid}
              target={{
                path: "schema.guid",
                literal: { kind: "string", value: message.schemaGuid },
              }}
              onOpenFilterMenu={onOpenFilterMenu}
            />
          ) : null}
          {message.schemaId !== null ? (
            <Field
              name="id"
              value={String(message.schemaId)}
              target={{
                path: "schema.id",
                literal: { kind: "number", value: String(message.schemaId) },
              }}
              onOpenFilterMenu={onOpenFilterMenu}
            />
          ) : null}
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
          activeMenuKey={activeMenuKey}
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
  activeMenuKey,
}: {
  name: string;
  value: string;
  target?: FilterTarget | null;
  onOpenFilterMenu?: OpenFilterMenu;
  activeMenuKey?: string | null;
}): JSX.Element {
  return (
    <div className="field">
      <span className="field__name">{name}</span>
      {target && onOpenFilterMenu ? (
        <FilterableField
          target={target}
          // Path is unique per Field within a single LayerTree
          // render (only one selected detail at a time), so it's a
          // safe stable anchor id without threading message.id
          // through every Field call site.
          anchorId={target.path}
          activeMenuKey={activeMenuKey ?? null}
          onOpenFilterMenu={onOpenFilterMenu}
          className="field__value"
        >
          {value}
        </FilterableField>
      ) : (
        <span className="field__value">{value}</span>
      )}
    </div>
  );
}

function DecodedNode({
  value,
  basePath,
  onOpenFilterMenu,
  activeMenuKey,
}: {
  value: DecodedValue;
  basePath: string;
  onOpenFilterMenu: OpenFilterMenu;
  activeMenuKey: string | null;
}): JSX.Element {
  switch (value.kind) {
    case "primitive": {
      const literal: PrimitiveLiteral = { kind: value.type, value: value.value };
      // null is not a first-class DSL literal; render the value but skip
      // the filter affordance.
      const target: FilterTarget | null =
        value.type === "null" || !isValidPath(basePath) ? null : { path: basePath, literal };
      return (
        <FilterableField
          target={target}
          // basePath is unique within a single LayerTree render
          // (each leaf has a distinct dotted path), so it doubles
          // as the anchor id without threading message.id down.
          anchorId={basePath}
          activeMenuKey={activeMenuKey}
          onOpenFilterMenu={onOpenFilterMenu}
          className="token-row"
        >
          <span className={`token token--${value.type}`}>{value.value}</span>
        </FilterableField>
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
                    activeMenuKey={activeMenuKey}
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
                activeMenuKey={activeMenuKey}
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
