import type { JSX } from "react";
import type { KafkaMessageDetail } from "../types";

interface Props {
  /** Full message body (lazy-fetched via inspect_message_by_id).
   *  `null` when no row is selected or while the fetch is in flight. */
  message: KafkaMessageDetail | null;
}

export function HexDump({ message }: Props): JSX.Element {
  if (!message) {
    return <section className="hex hex--empty muted">No message selected.</section>;
  }
  const bytes = message.rawHex.split(/\s+/u).filter(Boolean);
  const rows: string[][] = [];
  for (let i = 0; i < bytes.length; i += 16) {
    rows.push(bytes.slice(i, i + 16));
  }
  return (
    <section className="hex" aria-label="Hex dump">
      {rows.map((row, rowIndex) => {
        const offset = (rowIndex * 16).toString(16).padStart(8, "0");
        const ascii = row
          .map((cell) => {
            const code = parseInt(cell, 16);
            return code >= 0x20 && code <= 0x7e ? String.fromCharCode(code) : ".";
          })
          .join("");
        return (
          <div key={offset} className="hex__row">
            <span className="hex__offset">{offset}</span>
            <span className="hex__bytes">{row.join(" ")}</span>
            <span className="hex__ascii">{ascii}</span>
          </div>
        );
      })}
    </section>
  );
}
