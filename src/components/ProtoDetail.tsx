import { type JSX, type MouseEvent } from "react";
import type { ProtoFrameDetail } from "../types";
import { formatBytes } from "../lib/formatBytes";
import { formatRtt } from "../lib/formatRtt";
import type { DecodedFieldPair, ProtoFilterMode } from "../lib/protoFilter";

/**
 * Add a `decodedField` predicate from a clicked decoded leaf. The
 * pair carries the dotted JSON path from the body root to the leaf
 * (`topic_data.partition_data.records.base_offset`) and the leaf
 * value — `matchJsonPath` then walks the path strictly so a click
 * on a nested `name` doesn't match a sibling `name` under a
 * different parent.
 */
type AddDecodedFn = (pair: DecodedFieldPair, mode: ProtoFilterMode) => void;

interface Props {
  frame: ProtoFrameDetail | null;
  onAddDecodedFilter?: AddDecodedFn;
}

export function ProtoDetail({ frame, onAddDecodedFilter }: Props): JSX.Element {
  if (!frame) {
    return (
      <section className="layers layers--empty" aria-label="Frame detail">
        <p className="muted">Select a protocol frame to inspect its bytes.</p>
      </section>
    );
  }
  // Group hex into 16-byte rows for readability.
  const rows = chunkHex(frame.payloadHex, 16);
  const decoded = frame.decodedJson;
  return (
    <section className="layers" aria-label="Frame detail">
      {frame.frameError !== undefined ? (
        <div className="frame-error" role="status" aria-live="polite">
          <span className="frame-error__icon" aria-hidden="true">
            ✗
          </span>
          <span className="frame-error__text">
            <strong>Not forwarded.</strong> {frame.frameError}. Kapture decoded the request from the
            client side but the upstream broker was unreachable; the client got no response and will
            likely retry.
          </span>
        </div>
      ) : null}
      <details className="layer" open>
        <summary className="layer__title">frame</summary>
        <div className="layer__body">
          <Field name="direction" value={frame.direction} />
          <Field name="api" value={`${frame.apiName} (${String(frame.apiKey)})`} />
          <Field name="api_version" value={`v${String(frame.apiVersion)}`} />
          <Field name="connection_id" value={String(frame.connectionId)} />
          <Field name="corr_id" value={String(frame.corrId)} />
          <Field name="size" value={formatBytes(frame.size)} />
          {frame.captured < frame.size ? (
            <Field
              name="captured"
              value={`${formatBytes(frame.captured)} of ${formatBytes(frame.size)} (truncated at 64 KiB cap — hex/decoded views show only this prefix)`}
            />
          ) : null}
          {frame.direction === "recv"
            ? (() => {
                const fmt = formatRtt(frame.rttMs);
                return <Field name="rtt" value={`${fmt.value}${fmt.unit ? ` ${fmt.unit}` : ""}`} />;
              })()
            : null}
          <Field name="timestamp" value={frame.timestamp} />
        </div>
      </details>
      {decoded !== undefined && decoded !== null ? (
        <DecodedTree
          decoded={decoded}
          apiName={frame.apiName}
          {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
        />
      ) : null}
      {rows.length > 0 ? (
        <details
          className="layer"
          {...(decoded !== undefined && decoded !== null ? {} : { open: true })}
        >
          <summary className="layer__title">payload — hex view</summary>
          <div className="layer__body">
            <pre className="proto-hex">
              {rows.map((row, i) => (
                <span key={i} className="proto-hex__row">
                  <span className="proto-hex__off">{(i * 16).toString(16).padStart(6, "0")}</span>
                  <span className="proto-hex__hex">{row.hex}</span>
                  <span className="proto-hex__ascii">{row.ascii}</span>
                </span>
              ))}
            </pre>
          </div>
        </details>
      ) : null}
    </section>
  );
}

function Field({
  name,
  value,
  onAddDecodedFilter,
  pair,
}: {
  name: string;
  value: string;
  /** When set, value cell renders a hover-revealed funnel that
   *  emits the `decodedField` predicate. */
  onAddDecodedFilter?: AddDecodedFn;
  /** Pair captured by the JSON walker. `null` when the leaf isn't
   *  filterable (frame metadata rows, array indices, complex
   *  values). */
  pair?: DecodedFieldPair | null;
}): JSX.Element {
  return (
    <div className="field">
      <span className="field__name">{name}</span>
      {onAddDecodedFilter && pair ? (
        <FilterableValue value={value} onAdd={onAddDecodedFilter} pair={pair} />
      ) : (
        <span className="field__value">{value}</span>
      )}
    </div>
  );
}

/**
 * Hover-revealed filter affordance for a decoded leaf value. Click ⊕
 * adds an include predicate, alt-click adds an exclude.
 */
function FilterableValue({
  value,
  onAdd,
  pair,
}: {
  value: string;
  onAdd: AddDecodedFn;
  pair: DecodedFieldPair;
}): JSX.Element {
  const onClick = (event: MouseEvent<HTMLButtonElement>): void => {
    event.preventDefault();
    event.stopPropagation();
    onAdd(pair, event.altKey ? "exclude" : "include");
  };
  return (
    <span className="filterable-field filterable-field--filterable field__value">
      <span className="filterable-field__content">{value}</span>
      <span
        className="filterable-field__icon"
        role="button"
        tabIndex={-1}
        aria-label="Filter on this field value"
        title={`Filter on ${pair.path} == "${pair.value}" • Alt/Option-click to exclude`}
        onClick={onClick}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            event.stopPropagation();
            onAdd(pair, event.altKey ? "exclude" : "include");
          }
        }}
      >
        <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true" focusable="false">
          <path
            d="M2.5 3h11l-4 5v4l-3 1.2V8l-4-5z"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinejoin="round"
          />
        </svg>
      </span>
    </span>
  );
}

function DecodedTree({
  decoded,
  apiName,
  onAddDecodedFilter,
}: {
  decoded: unknown;
  apiName: string;
  onAddDecodedFilter?: AddDecodedFn;
}): JSX.Element {
  // The layer already provides the top-level disclosure (`apiName`).
  // The root JSON value is almost always the body's root object — we
  // inline its fields directly under the layer instead of letting
  // `JsonNodeView` build a second `<details>` with an empty summary
  // (which collapses into a stray empty row when the layer is closed
  // and re-opened).
  let body: JSX.Element;
  if (decoded !== null && typeof decoded === "object" && !Array.isArray(decoded)) {
    const entries = Object.entries(decoded as Record<string, unknown>);
    body =
      entries.length === 0 ? (
        <span className="muted">empty</span>
      ) : (
        <>
          {entries.map(([key, value]) => (
            <JsonNodeView
              key={key}
              node={value}
              name={key}
              path={[key]}
              {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
            />
          ))}
        </>
      );
  } else {
    body = (
      <JsonNodeView
        node={decoded}
        path={[]}
        {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
      />
    );
  }
  return (
    <details className="layer" open>
      <summary className="layer__title">{apiName}</summary>
      <div className="layer__body proto-decoded-tree">{body}</div>
    </details>
  );
}

/**
 * Render a typed JSON value. Compound nodes (objects, arrays) get a
 * chevron disclosure; leaves render as `name | value` rows that can
 * carry the filter affordance — clicking the funnel emits a
 * `field "<name>" == "<value>"` predicate which the matcher walks
 * the body for, anywhere in the tree.
 */
function JsonNodeView({
  node,
  name,
  path,
  onAddDecodedFilter,
}: {
  node: unknown;
  /** Field name in the enclosing object, or `[idx]` when this is an
   *  array element. `undefined` at the document root. */
  name?: string | undefined;
  /** Full chain of object-key segments from the body root down to
   *  this node — used by leaves to build a `decodedField` predicate
   *  that the matcher can walk strictly. Array indices are *not*
   *  segments (the matcher descends arrays per-element on the same
   *  path), so the chain only grows on object-key descents. */
  path: string[];
  onAddDecodedFilter?: AddDecodedFn | undefined;
}): JSX.Element | null {
  // Hide the routine empty `unknown_tagged_fields: {}` lines. Every
  // generated message struct has one and they're almost always
  // empty; a sea of `unknown_tagged_fields: {}` is just noise.
  if (
    name === "unknown_tagged_fields" &&
    node !== null &&
    typeof node === "object" &&
    !Array.isArray(node) &&
    Object.keys(node).length === 0
  ) {
    return null;
  }
  if (Array.isArray(node)) {
    // Narrow to unknown[] explicitly: TypeScript's Array.isArray
    // refinement gives any[] when --strict isn't aggressive enough,
    // and lint's no-unsafe-assignment then flags every recursive
    // descent. The cast is a typing nudge; runtime is identical.
    const items = node as unknown[];
    return (
      <details className="tree-node" open>
        <summary className="tree-node__summary">
          <span className="field__name">{name ?? ""}</span>
          <span className="field__value">[{items.length}]</span>
        </summary>
        <div className="tree-node__children">
          {items.length === 0 ? (
            <span className="muted">empty</span>
          ) : (
            items.map((item, i) => (
              <JsonNodeView
                key={i}
                node={item}
                name={`[${String(i)}]`}
                // Array index isn't a JSON-path segment — the matcher
                // descends arrays per-element on the same path. So
                // children inherit the path unchanged.
                path={path}
                {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
              />
            ))
          )}
        </div>
      </details>
    );
  }
  if (node !== null && typeof node === "object") {
    const entries = Object.entries(node as Record<string, unknown>);
    return (
      <details className="tree-node" open>
        <summary className="tree-node__summary">
          <span className="field__name">{name ?? ""}</span>
          <span className="field__value">{entries.length === 0 ? "{}" : ""}</span>
        </summary>
        <div className="tree-node__children">
          {entries.length === 0 ? (
            <span className="muted">empty</span>
          ) : (
            entries.map(([key, value]) => (
              <JsonNodeView
                key={key}
                node={value}
                name={key}
                path={[...path, key]}
                {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
              />
            ))
          )}
        </div>
      </details>
    );
  }
  // Leaf: build the path/value pair from the accumulated segment
  // chain. Strings render with their surrounding quotes; numbers /
  // booleans render raw. The pair's `value` carries the unquoted
  // string view, matching how `matchJsonPath` compares.
  const displayed = renderLeaf(node);
  const matchValue = leafMatchValue(node);
  const fieldName = name ?? "";
  // Need a non-empty path AND a primitive leaf value to build a
  // useful predicate. Empty path means the root scalar (rare —
  // Kafka bodies are always objects) which can't anchor a path
  // match.
  const pair: DecodedFieldPair | null =
    path.length > 0 && matchValue !== null ? { path: path.join("."), value: matchValue } : null;
  return (
    <Field
      name={fieldName}
      value={displayed}
      pair={pair}
      {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
    />
  );
}

function renderLeaf(node: unknown): string {
  if (node === null) {
    return "null";
  }
  if (typeof node === "string") {
    return `"${node}"`;
  }
  if (typeof node === "number" || typeof node === "boolean" || typeof node === "bigint") {
    return String(node);
  }
  // Defensive: should be unreachable since arrays + objects are
  // handled in the caller. Render as JSON for visibility.
  return JSON.stringify(node);
}

function leafMatchValue(node: unknown): string | null {
  if (node === null) {
    return "null";
  }
  if (typeof node === "string") {
    return node;
  }
  if (typeof node === "number" || typeof node === "boolean" || typeof node === "bigint") {
    return String(node);
  }
  return null;
}

interface HexRow {
  hex: string;
  ascii: string;
}

function chunkHex(hexStr: string, bytesPerRow: number): HexRow[] {
  const rows: HexRow[] = [];
  // Two hex chars per byte → row stride is bytesPerRow * 2 chars.
  const stride = bytesPerRow * 2;
  for (let i = 0; i < hexStr.length; i += stride) {
    const chunk = hexStr.slice(i, i + stride);
    // Group bytes with spaces for readability.
    const grouped = chunk.match(/.{2}/g)?.join(" ") ?? chunk;
    // ASCII-printable view: substitute non-printable bytes with `.`.
    let ascii = "";
    for (let j = 0; j < chunk.length; j += 2) {
      const byte = Number.parseInt(chunk.slice(j, j + 2), 16);
      ascii += byte >= 0x20 && byte < 0x7f ? String.fromCharCode(byte) : ".";
    }
    rows.push({ hex: grouped, ascii });
  }
  return rows;
}
