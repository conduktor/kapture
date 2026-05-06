import { type JSX } from "react";
import type { ProtoFrame } from "../types";

interface Props {
  frame: ProtoFrame | null;
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
      <div className="layer">
        <div className="layer__title">frame</div>
        <div className="layer__body">
          <Field name="direction" value={frame.direction} />
          <Field name="api" value={`${frame.apiName} (${frame.apiKey})`} />
          <Field name="api_version" value={`v${frame.apiVersion}`} />
          <Field name="broker_id" value={String(frame.brokerId)} />
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
      </div>
      {rows.length > 0 ? (
        <div className="layer">
          <div className="layer__title">payload — hex view (phase 2a)</div>
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
            <p className="muted" style={{ marginTop: 8 }}>
              Phase 2b will decode this into typed fields per ApiKey/Version using the{" "}
              <code>kafka-protocol</code> crate.
            </p>
          </div>
        </div>
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
