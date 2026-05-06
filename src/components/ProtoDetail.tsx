import { type JSX } from "react";
import type { ProtoFrameDetail } from "../types";
import { parseDebug, type DebugNode } from "../lib/debugTree";

interface Props {
  frame: ProtoFrameDetail | null;
}

export function ProtoDetail({ frame }: Props): JSX.Element {
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
          <Field name="size (wire)" value={`${frame.size} bytes`} />
          <Field
            name="captured"
            value={
              frame.captured === frame.size
                ? `${frame.captured} bytes (full)`
                : `${frame.captured} of ${frame.size} bytes (truncated at 64 KiB cap)`
            }
          />
          {frame.direction === "recv" ? (
            <Field name="rtt" value={`${frame.rttMs.toFixed(2)} ms`} />
          ) : null}
          <Field name="timestamp" value={frame.timestamp} />
        </div>
      </details>
      {frame.decoded ? <DecodedTree decoded={frame.decoded} apiName={frame.apiName} /> : null}
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

function Field({ name, value }: { name: string; value: string }): JSX.Element {
  return (
    <div className="field">
      <span className="field__name">{name}</span>
      <span className="field__value">{value}</span>
    </div>
  );
}

function DecodedTree({ decoded, apiName }: { decoded: string; apiName: string }): JSX.Element {
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
        decoded ({apiName}
        {tree.kind === "struct" && tree.name !== "" ? ` → ${tree.name}` : ""})
      </summary>
      <div className="layer__body proto-decoded-tree">
        {tree.kind === "struct" ? (
          tree.fields.length === 0 ? (
            <span className="muted">empty</span>
          ) : (
            tree.fields.map((f, i) => <DebugNodeView key={i} node={f.value} name={f.name} />)
          )
        ) : (
          <DebugNodeView node={tree} />
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
function DebugNodeView({ node, name }: { node: DebugNode; name?: string }): JSX.Element {
  if (node.kind === "struct") {
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
            node.fields.map((f, i) => <DebugNodeView key={i} node={f.value} name={f.name} />)
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
            node.items.map((item, i) => <DebugNodeView key={i} node={item} name={`[${i}]`} />)
          )}
        </div>
      </details>
    );
  }
  if (node.kind === "tuple") {
    // Single-item tuple wrappers (`Some(x)`, `TopicName("foo")`) are
    // visually noisy when nested — flatten by combining the wrapper
    // name into the field label.
    if (node.items.length === 1) {
      const inner = node.items[0];
      if (!inner) {
        return <Field name={name ?? ""} value={`${node.name}()`} />;
      }
      const label = name ? `${name} (${node.name})` : node.name;
      return <DebugNodeView node={inner} name={label} />;
    }
    return (
      <details className="tree-node" open>
        <summary className="tree-node__summary">
          <span className="field__name">{name ?? ""}</span>
          <span className="field__value">{node.name}(…)</span>
        </summary>
        <div className="tree-node__children">
          {node.items.map((item, i) => (
            <DebugNodeView key={i} node={item} name={`[${i}]`} />
          ))}
        </div>
      </details>
    );
  }
  if (node.kind === "string") {
    return <Field name={name ?? ""} value={`"${node.value}"`} />;
  }
  // primitive
  return <Field name={name ?? ""} value={node.text} />;
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
