import { type JSX, type MouseEvent } from "react";
import type { ProtoFrameDetail } from "../types";
import { parseDebug, type DebugNode } from "../lib/debugTree";
import { formatBytes } from "../lib/formatBytes";
import type { DecodedFieldTriple, ProtoFilterMode } from "../lib/protoFilter";

/**
 * Add a path-aware `decodedField` predicate from a clicked decoded
 * leaf. The triple captures the parent struct's name, the field
 * name, and the value — matched against the parsed Debug tree
 * server-side (`matchDebugField`), so a click on
 * `MetadataRequestTopic.name = "orders.avro"` does NOT bleed into
 * a `TopicProduceData.name = "orders.avro"` row even though both
 * Debug outputs contain the substring `name: "orders.avro"`.
 */
type AddDecodedFn = (triple: DecodedFieldTriple, mode: ProtoFilterMode) => void;

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
  // Group hex into 16-byte rows for readability. Phase 2b will replace
  // this with a typed decode (kafka-protocol crate).
  const rows = chunkHex(frame.payloadHex, 16);
  return (
    <section className="layers" aria-label="Frame detail">
      <details className="layer" open>
        <summary className="layer__title">frame</summary>
        <div className="layer__body">
          <Field name="direction" value={frame.direction} />
          <Field name="api" value={`${frame.apiName} (${frame.apiKey})`} />
          <Field name="api_version" value={`v${frame.apiVersion}`} />
          <Field name="connection_id" value={String(frame.connectionId)} />
          <Field name="corr_id" value={String(frame.corrId)} />
          <Field name="size (wire)" value={formatBytes(frame.size)} />
          <Field
            name="captured"
            value={
              frame.captured === frame.size
                ? `${formatBytes(frame.captured)} (full)`
                : `${formatBytes(frame.captured)} of ${formatBytes(frame.size)} (truncated at 64 KiB cap)`
            }
          />
          {frame.direction === "recv" ? (
            <Field name="rtt" value={`${frame.rttMs.toFixed(2)} ms`} />
          ) : null}
          <Field name="timestamp" value={frame.timestamp} />
        </div>
      </details>
      {frame.decoded ? (
        <DecodedTree
          decoded={frame.decoded}
          apiName={frame.apiName}
          {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
        />
      ) : null}
      {rows.length > 0 ? (
        <details className="layer" {...(frame.decoded ? {} : { open: true })}>
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
  triple,
}: {
  name: string;
  value: string;
  /** When set, value cell renders a hover-revealed funnel that
   *  emits the path-aware `decodedField` predicate. */
  onAddDecodedFilter?: AddDecodedFn;
  /** Triple captured by the recursive walker — `null` when the
   *  enclosing context isn't a struct (e.g. plain frame metadata
   *  rows from the `frame` block, which have no Rust-side
   *  parent struct to anchor against). */
  triple?: DecodedFieldTriple | null;
}): JSX.Element {
  return (
    <div className="field">
      <span className="field__name">{name}</span>
      {onAddDecodedFilter && triple ? (
        <FilterableValue value={value} onAdd={onAddDecodedFilter} triple={triple} />
      ) : (
        <span className="field__value">{value}</span>
      )}
    </div>
  );
}

/**
 * Hover-revealed filter affordance for a decoded leaf value. Click ⊕
 * adds an include predicate, alt-click adds an exclude. The actual
 * substring matched against the frame's `decoded` Debug output is
 * `"<fieldName>: <renderedValue>"` — that's exactly what the Rust
 * `{:#?}` Debug formatter emits for a struct field, so the substring
 * is reliable enough without parsing.
 */
function FilterableValue({
  value,
  onAdd,
  triple,
}: {
  value: string;
  onAdd: AddDecodedFn;
  triple: DecodedFieldTriple;
}): JSX.Element {
  const onClick = (event: MouseEvent<HTMLButtonElement>): void => {
    event.preventDefault();
    event.stopPropagation();
    onAdd(triple, event.altKey ? "exclude" : "include");
  };
  return (
    // Reuse the canonical funnel chrome from `<FilterableField>` —
    // naked SVG, hover-revealed, accent on hover. Behaviour is
    // distinct from the popover-based path (substring decoded
    // filter, alt/option-click = exclude) so we can't share the
    // component itself yet, but the visual stays unified.
    <span className="filterable-field filterable-field--filterable field__value">
      <span className="filterable-field__content">{value}</span>
      <span
        className="filterable-field__icon"
        role="button"
        tabIndex={-1}
        aria-label="Filter on this field value"
        title={`Filter on ${triple.struct}.${triple.field} == "${triple.value}" • Alt/Option-click to exclude`}
        onClick={onClick}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            event.stopPropagation();
            onAdd(triple, event.altKey ? "exclude" : "include");
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
  decoded: string;
  apiName: string;
  onAddDecodedFilter?: AddDecodedFn;
}): JSX.Element {
  const tree = parseDebug(decoded);
  if (!tree) {
    // Parser bailed out: surface the raw Debug string rather than nothing.
    // This shouldn't happen for derive(Debug) output but the kafka-protocol
    // crate may grow a hand-rolled Debug impl that breaks the grammar.
    return (
      <details className="layer" open>
        <summary className="layer__title">decoded ({apiName})</summary>
        <div className="layer__body">
          <pre className="proto-decoded">{decoded}</pre>
        </div>
      </details>
    );
  }
  // Top-level kafka-protocol message root is always a struct; inline
  // its fields under the layer title rather than render a degenerate
  // empty-name row carrying just "MetadataRequest" — the layer title
  // already names the API.
  return (
    <details className="layer" open>
      <summary className="layer__title">
        {tree.kind === "struct" && tree.name !== "" ? tree.name : apiName}
      </summary>
      <div className="layer__body proto-decoded-tree">
        {tree.kind === "struct" ? (
          tree.fields.length === 0 ? (
            <span className="muted">empty</span>
          ) : (
            tree.fields.map((f, i) => (
              <DebugNodeView
                key={i}
                node={f.value}
                name={f.name}
                parentStruct={tree.name}
                {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
              />
            ))
          )
        ) : (
          <DebugNodeView
            node={tree}
            parentStruct={undefined}
            {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
          />
        )}
      </div>
    </details>
  );
}

/**
 * Render a parsed Debug tree as a flat indented tree — same visual
 * vocabulary as the `frame` block (`name | value` rows). Compound
 * nodes (struct, seq, tuple) get a chevron disclosure but no boxed
 * card around their children, so nesting reads as indentation rather
 * than a stack of nested containers.
 */
function DebugNodeView({
  node,
  name,
  onAddDecodedFilter,
  parentStruct,
}: {
  node: DebugNode;
  name?: string;
  onAddDecodedFilter?: AddDecodedFn;
  /** Name of the enclosing `struct` node, threaded down so leaf
   *  fields can build a path-aware `decodedField` predicate
   *  (`<parentStruct>.<fieldName> == <value>`). `undefined` at the
   *  document root and inside non-struct containers (seq / tuple
   *  inherit their own parent's `parentStruct`). */
  parentStruct: string | undefined;
}): JSX.Element | null {
  // Hide kafka-protocol's empty `unknown_tagged_fields: {}` lines —
  // they're meaningless noise (every struct has one, almost always
  // empty). When the field is non-empty (rare, only when the broker
  // sent unknown KIP tags), let it render normally.
  if (name === "unknown_tagged_fields" && node.kind === "struct" && node.fields.length === 0) {
    return null;
  }
  if (node.kind === "struct") {
    // Children fields are anchored against THIS struct's name —
    // leaf-level `decodedField` predicates use it.
    const childParent = node.name === "" ? parentStruct : node.name;
    return (
      <details className="tree-node" open>
        <summary className="tree-node__summary">
          <span className="field__name">{name ?? ""}</span>
          <span className="field__value">{node.name === "" ? "{}" : node.name}</span>
        </summary>
        <div className="tree-node__children">
          {node.fields.length === 0 ? (
            <span className="muted">empty</span>
          ) : (
            node.fields.map((f, i) => (
              <DebugNodeView
                key={i}
                node={f.value}
                name={f.name}
                parentStruct={childParent}
                {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
              />
            ))
          )}
        </div>
      </details>
    );
  }
  if (node.kind === "seq") {
    return (
      <details className="tree-node" open>
        <summary className="tree-node__summary">
          <span className="field__name">{name ?? ""}</span>
          <span className="field__value">[{node.items.length}]</span>
        </summary>
        <div className="tree-node__children">
          {node.items.length === 0 ? (
            <span className="muted">empty</span>
          ) : (
            node.items.map((item, i) => (
              <DebugNodeView
                key={i}
                node={item}
                name={`[${i}]`}
                parentStruct={parentStruct}
                {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
              />
            ))
          )}
        </div>
      </details>
    );
  }
  if (node.kind === "tuple") {
    // Single-item tuple wrappers (`Some(x)`, `TopicName("foo")`) are
    // visually noisy when nested — flatten by combining the wrapper
    // name into the field label. `Some` specifically is hidden: it's
    // pure Rust noise (Option::Some) — the value's presence already
    // implies it. `Ok(_)` is treated the same way for symmetry.
    if (node.items.length === 1) {
      const inner = node.items[0];
      if (!inner) {
        return (
          <Field
            name={name ?? ""}
            value={`${node.name}()`}
            {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
          />
        );
      }
      const isOptionalWrapper = node.name === "Some" || node.name === "Ok";
      const label = isOptionalWrapper ? (name ?? "") : name ? `${name} (${node.name})` : node.name;
      return (
        <DebugNodeView
          node={inner}
          name={label}
          parentStruct={parentStruct}
          {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
        />
      );
    }
    return (
      <details className="tree-node" open>
        <summary className="tree-node__summary">
          <span className="field__name">{name ?? ""}</span>
          <span className="field__value">{node.name}(…)</span>
        </summary>
        <div className="tree-node__children">
          {node.items.map((item, i) => (
            <DebugNodeView
              key={i}
              node={item}
              name={`[${i}]`}
              parentStruct={parentStruct}
              {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
            />
          ))}
        </div>
      </details>
    );
  }
  // Leaf: build a path-aware triple `<parentStruct>.<field> = <value>`.
  // Strings strip the surrounding quotes (the Debug-tree walker
  // matches against the unquoted leaf text via `debugNodeText`).
  // Primitives use their raw text (numbers, booleans, idents like
  // `None`).
  const displayed = node.kind === "string" ? `"${node.value}"` : node.text;
  const fieldName = name ?? "";
  // Tuple-flattened labels like `topic_id (TopicName)` aren't real
  // field names — the bare segment before the parenthesis is.
  const baseName = fieldName.replace(/\s*\([^)]*\)\s*$/, "");
  const matchValue = node.kind === "string" ? node.value : node.text;
  const triple: DecodedFieldTriple | null =
    parentStruct !== undefined && baseName !== ""
      ? { struct: parentStruct, field: baseName, value: matchValue }
      : null;
  return (
    <Field
      name={fieldName}
      value={displayed}
      triple={triple}
      {...(onAddDecodedFilter ? { onAddDecodedFilter } : {})}
    />
  );
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
