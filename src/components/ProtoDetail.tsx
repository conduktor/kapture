import { useState, type JSX, type MouseEvent } from "react";
import { List, type RowComponentProps } from "react-window";
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

const PROTO_BYTES_PER_ROW = 16;
const PROTO_HEX_ROW_HEIGHT = 18;
const EAGER_COMPOUND_CHILDREN = 32;

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
  const hexRowCount = Math.ceil(frame.payloadHex.length / (PROTO_BYTES_PER_ROW * 2));
  const decoded = frame.decodedJson;
  const frameClipboard = buildFrameClipboard(frame);
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
        <summary className="layer__title">
          <span className="layer__title-text">frame</span>
          <LayerCopyButton text={frameClipboard} label="frame metadata" />
        </summary>
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
          {frame.captureLagMs > 0 ? (
            <Field name="capture_lag" value={`${frame.captureLagMs.toFixed(3)} ms`} />
          ) : null}
          <Field name="analysis_lag" value={`${frame.analysisLagMs.toFixed(3)} ms`} />
          <Field name="timestamp" value={frame.timestamp} />
        </div>
      </details>
      {decoded !== undefined && decoded !== null ? (
        <DecodedTree
          decoded={decoded}
          apiName={frame.apiName}
          clipboardText={() => JSON.stringify(decoded, null, 2)}
          {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
        />
      ) : null}
      {hexRowCount > 0 ? (
        <details
          className="layer"
          {...(decoded !== undefined && decoded !== null ? {} : { open: true })}
        >
          <summary className="layer__title">
            <span className="layer__title-text">payload — hex view</span>
            <LayerCopyButton
              text={() => formatHexClipboard(frame.payloadHex)}
              label="payload hex"
            />
          </summary>
          <div className="layer__body">
            <List
              className="proto-hex"
              style={{ height: Math.min(hexRowCount * PROTO_HEX_ROW_HEIGHT + 16, 320) }}
              rowComponent={ProtoHexRow}
              rowCount={hexRowCount}
              rowHeight={PROTO_HEX_ROW_HEIGHT}
              rowProps={{ payloadHex: frame.payloadHex }}
              overscanCount={12}
            />
          </div>
        </details>
      ) : null}
    </section>
  );
}

/** Plain-text dump of the frame metadata, ready for pasting in a
 *  Slack thread / GitHub issue without losing context. Mirrors the
 *  rows shown in the `frame` layer so the recipient sees the same
 *  fields in the same order. */
function buildFrameClipboard(frame: ProtoFrameDetail): string {
  const lines: string[] = [];
  if (frame.frameError !== undefined) {
    lines.push(`error: ${frame.frameError} (request was not forwarded; client got no response)`);
  }
  lines.push(`direction: ${frame.direction}`);
  lines.push(`api: ${frame.apiName} (${String(frame.apiKey)})`);
  lines.push(`api_version: v${String(frame.apiVersion)}`);
  lines.push(`connection_id: ${String(frame.connectionId)}`);
  lines.push(`corr_id: ${String(frame.corrId)}`);
  lines.push(`size: ${formatBytes(frame.size)}`);
  if (frame.captured < frame.size) {
    lines.push(
      `captured: ${formatBytes(frame.captured)} of ${formatBytes(frame.size)} (truncated)`,
    );
  }
  if (frame.direction === "recv") {
    const fmt = formatRtt(frame.rttMs);
    lines.push(`rtt: ${fmt.value}${fmt.unit ? ` ${fmt.unit}` : ""}`);
  }
  if (frame.captureLagMs > 0) {
    lines.push(`capture_lag: ${frame.captureLagMs.toFixed(3)} ms`);
  }
  lines.push(`analysis_lag: ${frame.analysisLagMs.toFixed(3)} ms`);
  lines.push(`timestamp: ${frame.timestamp}`);
  return lines.join("\n");
}

/** Copy-to-clipboard chip pinned to the right of a layer summary.
 *  Stops the click from toggling the `<details>` so the user doesn't
 *  collapse the section they're trying to copy. Brief "copied"
 *  affordance, then back to "copy" — no toast, no modal. */
function LayerCopyButton({
  text,
  label,
}: {
  text: string | (() => string);
  label: string;
}): JSX.Element {
  const [copied, setCopied] = useState(false);
  const onClick = (e: MouseEvent<HTMLButtonElement>): void => {
    e.preventDefault();
    e.stopPropagation();
    const clipboardText = typeof text === "function" ? text() : text;
    void navigator.clipboard.writeText(clipboardText).then(() => {
      setCopied(true);
      window.setTimeout(() => {
        setCopied(false);
      }, 1500);
    });
  };
  return (
    <button
      type="button"
      className="layer__copy"
      onClick={onClick}
      aria-label={`Copy ${label} to clipboard`}
      title={`Copy ${label}`}
    >
      {copied ? "copied" : "copy"}
    </button>
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
  clipboardText,
  onAddDecodedFilter,
}: {
  decoded: unknown;
  apiName: string;
  clipboardText: string | (() => string);
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
      ) : entries.length > EAGER_COMPOUND_CHILDREN ? (
        <JsonObjectNode
          node={decoded as Record<string, unknown>}
          name="body"
          path={[]}
          {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
        />
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
      <summary className="layer__title">
        <span className="layer__title-text">{apiName}</span>
        <LayerCopyButton text={clipboardText} label={`${apiName} JSON`} />
      </summary>
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
      <JsonArrayNode
        items={items}
        name={name}
        path={path}
        {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
      />
    );
  }
  if (node !== null && typeof node === "object") {
    return (
      <JsonObjectNode
        node={node as Record<string, unknown>}
        name={name}
        path={path}
        {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
      />
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

function JsonArrayNode({
  items,
  name,
  path,
  onAddDecodedFilter,
}: {
  items: unknown[];
  name?: string | undefined;
  path: string[];
  onAddDecodedFilter?: AddDecodedFn | undefined;
}): JSX.Element {
  const [expanded, setExpanded] = useState(items.length <= EAGER_COMPOUND_CHILDREN);
  return (
    <details
      className="tree-node"
      open={expanded}
      onToggle={(event) => {
        setExpanded(event.currentTarget.open);
      }}
    >
      <summary className="tree-node__summary">
        <span className="field__name">{name ?? ""}</span>
        <span className="field__value">[{items.length}]</span>
      </summary>
      {expanded ? (
        <div className="tree-node__children">
          {items.length === 0 ? (
            <span className="muted">empty</span>
          ) : (
            items.map((item, i) => (
              <JsonNodeView
                key={i}
                node={item}
                name={`[${String(i)}]`}
                path={path}
                {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
              />
            ))
          )}
        </div>
      ) : null}
    </details>
  );
}

function JsonObjectNode({
  node,
  name,
  path,
  onAddDecodedFilter,
}: {
  node: Record<string, unknown>;
  name?: string | undefined;
  path: string[];
  onAddDecodedFilter?: AddDecodedFn | undefined;
}): JSX.Element {
  const keys = Object.keys(node);
  const [expanded, setExpanded] = useState(keys.length <= EAGER_COMPOUND_CHILDREN);
  return (
    <details
      className="tree-node"
      open={expanded}
      onToggle={(event) => {
        setExpanded(event.currentTarget.open);
      }}
    >
      <summary className="tree-node__summary">
        <span className="field__name">{name ?? ""}</span>
        <span className="field__value">
          {keys.length === 0 ? "{}" : `{${String(keys.length)}}`}
        </span>
      </summary>
      {expanded ? (
        <div className="tree-node__children">
          {keys.length === 0 ? (
            <span className="muted">empty</span>
          ) : (
            keys.map((key) => (
              <JsonNodeView
                key={key}
                node={node[key]}
                name={key}
                path={[...path, key]}
                {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
              />
            ))
          )}
        </div>
      ) : null}
    </details>
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

function ProtoHexRow({
  ariaAttributes,
  index,
  style,
  payloadHex,
}: RowComponentProps<{ payloadHex: string }>): JSX.Element {
  const firstByte = index * PROTO_BYTES_PER_ROW;
  const chunk = payloadHex.slice(firstByte * 2, (firstByte + PROTO_BYTES_PER_ROW) * 2);
  let grouped = "";
  let ascii = "";
  for (let i = 0; i < chunk.length; i += 2) {
    if (i > 0) grouped += " ";
    const cell = chunk.slice(i, i + 2);
    grouped += cell;
    const byte = Number.parseInt(cell, 16);
    ascii += byte >= 0x20 && byte < 0x7f ? String.fromCharCode(byte) : ".";
  }
  return (
    <div className="proto-hex__row" style={style} {...ariaAttributes}>
      <span className="proto-hex__off">{firstByte.toString(16).padStart(6, "0")}</span>
      <span className="proto-hex__hex">{grouped}</span>
      <span className="proto-hex__ascii">{ascii}</span>
    </div>
  );
}

function formatHexClipboard(payloadHex: string): string {
  const stride = PROTO_BYTES_PER_ROW * 2;
  const rows: string[] = [];
  for (let offset = 0; offset < payloadHex.length; offset += stride) {
    const chunk = payloadHex.slice(offset, offset + stride);
    const cells: string[] = [];
    for (let i = 0; i < chunk.length; i += 2) {
      cells.push(chunk.slice(i, i + 2));
    }
    rows.push(cells.join(" "));
  }
  return rows.join("\n");
}
