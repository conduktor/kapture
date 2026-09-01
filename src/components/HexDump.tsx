import type { JSX } from "react";
import { List, type RowComponentProps } from "react-window";
import type { KafkaMessageDetail } from "../types";

const BYTES_PER_ROW = 16;
const HEX_ROW_HEIGHT = 18;

interface Props {
  /** Full message body (lazy-fetched via inspect_message_by_id).
   *  `null` when no row is selected or while the fetch is in flight. */
  message: KafkaMessageDetail | null;
}

export function HexDump({ message }: Props): JSX.Element {
  if (!message) {
    return <section className="hex hex--empty muted">No message selected.</section>;
  }
  // `rawHex` is `xx xx ...`, so every complete byte occupies three
  // characters except the final byte. Derive only visible rows in the
  // virtual list instead of splitting a multi-megabyte value into tens
  // of thousands of JS strings on every selection.
  const byteCount = message.rawHex.length === 0 ? 0 : Math.floor((message.rawHex.length + 1) / 3);
  const rowCount = Math.ceil(byteCount / BYTES_PER_ROW);
  return (
    <section className="hex" aria-label="Hex dump">
      <List
        className="hex__virtual"
        rowComponent={HexRow}
        rowCount={rowCount}
        rowHeight={HEX_ROW_HEIGHT}
        rowProps={{ rawHex: message.rawHex, byteCount }}
        overscanCount={12}
      />
    </section>
  );
}

function HexRow({
  ariaAttributes,
  index,
  style,
  rawHex,
  byteCount,
}: RowComponentProps<{ rawHex: string; byteCount: number }>): JSX.Element {
  const firstByte = index * BYTES_PER_ROW;
  const rowBytes = Math.min(BYTES_PER_ROW, byteCount - firstByte);
  const firstChar = firstByte * 3;
  const lastChar = firstChar + rowBytes * 3 - 1;
  const cells = rawHex.slice(firstChar, lastChar);
  let ascii = "";
  for (let i = 0; i < rowBytes; i += 1) {
    const code = Number.parseInt(cells.slice(i * 3, i * 3 + 2), 16);
    ascii += code >= 0x20 && code <= 0x7e ? String.fromCharCode(code) : ".";
  }
  return (
    <div className="hex__row" style={style} {...ariaAttributes}>
      <span className="hex__offset">{firstByte.toString(16).padStart(8, "0")}</span>
      <span className="hex__bytes">{cells}</span>
      <span className="hex__ascii">{ascii}</span>
    </div>
  );
}
