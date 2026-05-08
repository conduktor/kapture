/**
 * Tiny parser for Rust pretty-printed `Debug` output (`format!("{:#?}", v)`).
 *
 * The kafka-protocol crate doesn't derive `Serialize` on its message types,
 * so we have no clean structured path from the Rust side. The decoder
 * surfaces `format!("{:#?}", msg)` instead, and this parser turns that
 * pretty-printed string into a tree the UI can render with the same
 * collapsible Layer/Field components used everywhere else.
 *
 * Grammar (best-effort, matches what `derive(Debug)` produces):
 *
 *   value      := struct | tupleCall | seq | string | identOrLit
 *   struct     := IDENT '{' (IDENT ':' value ',')* '}'
 *   tupleCall  := IDENT '(' value (',' value)* ','? ')'   // covers Some(...), TopicName(...)
 *   seq        := '[' value (',' value)* ','? ']'
 *   string     := '"' (escape | non-quote)* '"'
 *   identOrLit := IDENT | NUMBER                          // None, Nan, true/false, 42, -1.5
 *
 * Anything we can't parse falls through to a single primitive node carrying
 * the raw substring — the caller can always render the original text.
 */

export type DebugNode =
  | { kind: "struct"; name: string; fields: DebugField[] }
  | { kind: "tuple"; name: string; items: DebugNode[] }
  | { kind: "seq"; items: DebugNode[] }
  | { kind: "string"; value: string }
  | { kind: "primitive"; text: string };

export interface DebugField {
  name: string;
  value: DebugNode;
}

/**
 * Walk a parsed `DebugNode` tree and check whether any node of kind
 * `struct` with `name === structName` has a direct field
 * `name === fieldName` whose value (string or primitive) equals
 * `value`. Returns `true` on the first match.
 *
 * Path-aware sibling of `String.includes()` substring matching: a
 * frame whose Debug output happens to contain `name: "x"` *somewhere*
 * (e.g. inside a nested array of `TopicProduceData`) does NOT match
 * a click that targeted `MetadataRequestTopic.name` — the struct
 * scope keeps them distinct.
 */
export function matchDebugField(
  root: DebugNode,
  structName: string,
  fieldName: string,
  value: string,
): boolean {
  if (root.kind === "struct") {
    if (root.name === structName) {
      for (const f of root.fields) {
        if (f.name !== fieldName) continue;
        if (debugNodeText(f.value) === value) return true;
      }
    }
    for (const f of root.fields) {
      if (matchDebugField(f.value, structName, fieldName, value)) return true;
    }
    return false;
  }
  if (root.kind === "tuple") {
    for (const item of root.items) {
      if (matchDebugField(item, structName, fieldName, value)) return true;
    }
    return false;
  }
  if (root.kind === "seq") {
    for (const item of root.items) {
      if (matchDebugField(item, structName, fieldName, value)) return true;
    }
    return false;
  }
  return false;
}

/** String/primitive view of a leaf node — what equality compares
 *  against. Strings unwrap their quotes; primitives return their
 *  raw text (numbers, booleans, idents like `None`). */
function debugNodeText(n: DebugNode): string | null {
  if (n.kind === "string") return n.value;
  if (n.kind === "primitive") return n.text;
  return null;
}

/**
 * Parse a complete Debug-formatted value. Returns `null` when the parse
 * fails so the caller can fall back to displaying the raw string.
 */
export function parseDebug(src: string): DebugNode | null {
  const p = new Parser(src);
  try {
    const node = p.parseValue();
    p.skipWs();
    if (!p.atEnd()) {
      // Trailing junk → suspicious; fall through to raw display.
      return null;
    }
    return node;
  } catch {
    return null;
  }
}

class Parser {
  private pos = 0;

  constructor(private readonly src: string) {}

  atEnd(): boolean {
    return this.pos >= this.src.length;
  }

  peek(offset = 0): string | undefined {
    return this.src[this.pos + offset];
  }

  consume(): string {
    const c = this.src[this.pos];
    if (c === undefined) {
      throw new Error("unexpected end of input");
    }
    this.pos += 1;
    return c;
  }

  skipWs(): void {
    while (this.pos < this.src.length) {
      const c = this.src[this.pos];
      if (c === " " || c === "\t" || c === "\n" || c === "\r") {
        this.pos += 1;
      } else {
        break;
      }
    }
  }

  expect(s: string): void {
    if (!this.src.startsWith(s, this.pos)) {
      throw new Error(`expected '${s}' at ${String(this.pos)}`);
    }
    this.pos += s.length;
  }

  parseValue(): DebugNode {
    this.skipWs();
    const c = this.peek();
    if (c === undefined) {
      throw new Error("unexpected EOF in value");
    }
    if (c === '"') {
      return this.parseString();
    }
    if (c === "[") {
      return this.parseSeq();
    }
    if (c === "{") {
      // Anonymous map / set (`BTreeMap`, `HashMap`, `HashSet`,
      // `unknown_tagged_fields` in kafka-protocol — all print as
      // `{key: value, ...}` or `{}` when empty).
      return this.parseAnonMap();
    }
    // UUID literal: kafka-protocol's `topic_id: Uuid` field prints
    // verbatim as `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (no quotes,
    // no wrapping tuple). Match the 8-4-4-4-12 hex shape FIRST so the
    // primitive / ident parsers don't choke on hex letters or dashes.
    const uuid = this.tryParseUuidLiteral();
    if (uuid !== null) {
      return uuid;
    }
    if (c === "-" || (c >= "0" && c <= "9")) {
      return this.parsePrimitiveLiteral();
    }
    if (isIdentStart(c)) {
      const ident = this.parseIdent();
      this.skipWs();
      const next = this.peek();
      if (next === "{") {
        return this.parseStructBody(ident);
      }
      if (next === "(") {
        return this.parseTupleBody(ident);
      }
      if (next === '"' && (ident === "b" || ident === "r" || ident === "br" || ident === "c")) {
        // Rust prefixed string literal: byte-string `b"…"`, raw
        // `r"…"`, byte-raw `br"…"`, cstring `c"…"`. Parse the inner
        // string body and reattach the prefix so the rendered value
        // is unambiguous (`b"\0\x03…"` reads differently from a
        // regular UTF-8 string). Use a primitive so the value
        // renders verbatim — the render path adds quotes around
        // `kind: string` values which would double-up here.
        const inner = this.parseString();
        const innerValue = inner.kind === "string" ? inner.value : "";
        return {
          kind: "primitive",
          text: `${ident}"${escapeForDisplay(innerValue)}"`,
        };
      }
      // Bare identifier (None, true, false, NaN, …)
      return { kind: "primitive", text: ident };
    }
    throw new Error(`unexpected '${c}' at ${String(this.pos)}`);
  }

  /**
   * Anonymous map literal: `{key: value, ...}`. Map keys are usually
   * either string-quoted or bare identifiers/numbers. We parse a key as
   * a single value and stringify it for the field label so structural
   * fidelity stays intact.
   */
  parseAnonMap(): DebugNode {
    this.expect("{");
    const fields: DebugField[] = [];
    while (this.pos < this.src.length) {
      this.skipWs();
      if (this.peek() === "}") {
        this.consume();
        return { kind: "struct", name: "", fields };
      }
      const key = this.parseValue();
      this.skipWs();
      this.expect(":");
      const value = this.parseValue();
      fields.push({ name: keyLabel(key), value });
      this.skipWs();
      if (this.peek() === ",") {
        this.consume();
      }
    }
    throw new Error("unterminated map");
  }

  parseString(): DebugNode {
    this.expect('"');
    let value = "";
    while (this.pos < this.src.length) {
      const c = this.consume();
      if (c === '"') {
        return { kind: "string", value };
      }
      if (c === "\\") {
        const esc = this.consume();
        switch (esc) {
          case "n":
            value += "\n";
            break;
          case "r":
            value += "\r";
            break;
          case "t":
            value += "\t";
            break;
          case "\\":
            value += "\\";
            break;
          case '"':
            value += '"';
            break;
          case "0":
            value += "\0";
            break;
          case "x": {
            const hex = this.consume() + this.consume();
            value += String.fromCharCode(Number.parseInt(hex, 16));
            break;
          }
          default:
            // Unknown escape — preserve verbatim so we never lose data.
            value += esc;
        }
      } else {
        value += c;
      }
    }
    throw new Error("unterminated string");
  }

  parseSeq(): DebugNode {
    this.expect("[");
    const items: DebugNode[] = [];
    while (this.pos < this.src.length) {
      this.skipWs();
      if (this.peek() === "]") {
        this.consume();
        return { kind: "seq", items };
      }
      items.push(this.parseValue());
      this.skipWs();
      if (this.peek() === ",") {
        this.consume();
      }
    }
    throw new Error("unterminated sequence");
  }

  parseStructBody(name: string): DebugNode {
    this.expect("{");
    const fields: DebugField[] = [];
    while (this.pos < this.src.length) {
      this.skipWs();
      if (this.peek() === "}") {
        this.consume();
        return { kind: "struct", name, fields };
      }
      const fieldName = this.parseIdent();
      this.skipWs();
      this.expect(":");
      const value = this.parseValue();
      fields.push({ name: fieldName, value });
      this.skipWs();
      if (this.peek() === ",") {
        this.consume();
      }
    }
    throw new Error("unterminated struct body");
  }

  parseTupleBody(name: string): DebugNode {
    this.expect("(");
    const items: DebugNode[] = [];
    while (this.pos < this.src.length) {
      this.skipWs();
      if (this.peek() === ")") {
        this.consume();
        return { kind: "tuple", name, items };
      }
      items.push(this.parseValue());
      this.skipWs();
      if (this.peek() === ",") {
        this.consume();
      }
    }
    throw new Error("unterminated tuple body");
  }

  parseIdent(): string {
    const start = this.pos;
    while (this.pos < this.src.length) {
      const c = this.src[this.pos];
      if (c === undefined) {
        break;
      }
      if (isIdentCont(c)) {
        this.pos += 1;
      } else {
        break;
      }
    }
    if (this.pos === start) {
      throw new Error(`expected identifier at ${String(this.pos)}`);
    }
    return this.src.slice(start, this.pos);
  }

  /**
   * Match a UUID literal (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) at
   * the current position without consuming on miss. UUIDs come from
   * the `uuid` crate's Debug impl which forwards to `LowerHex`, so
   * topic IDs land in kafka-protocol output as bare hex+dash strings
   * — neither identifiers nor numbers nor strings.
   */
  tryParseUuidLiteral(): DebugNode | null {
    const layout = [8, 4, 4, 4, 12];
    let cursor = this.pos;
    for (let group = 0; group < layout.length; group += 1) {
      const len = layout[group] ?? 0;
      for (let i = 0; i < len; i += 1) {
        const ch = this.src[cursor];
        if (ch === undefined || !isHexDigit(ch)) {
          return null;
        }
        cursor += 1;
      }
      if (group < layout.length - 1) {
        if (this.src[cursor] !== "-") {
          return null;
        }
        cursor += 1;
      }
    }
    const text = this.src.slice(this.pos, cursor);
    this.pos = cursor;
    return { kind: "primitive", text };
  }

  parsePrimitiveLiteral(): DebugNode {
    const start = this.pos;
    if (this.peek() === "-") {
      this.pos += 1;
    }
    // numbers: digits with optional decimal / exponent
    while (this.pos < this.src.length) {
      const c = this.src[this.pos];
      if (c === undefined) {
        break;
      }
      if ((c >= "0" && c <= "9") || c === "." || c === "e" || c === "E" || c === "+" || c === "-") {
        this.pos += 1;
      } else {
        break;
      }
    }
    return { kind: "primitive", text: this.src.slice(start, this.pos) };
  }
}

/**
 * Re-escape unprintable bytes back into Rust-style escapes for
 * display. Used after parseString has folded `\0`, `\xNN`, `\n`, …
 * into the actual byte values — for byte-string literals we want to
 * show the original escaped form so the value stays single-line
 * and unambiguous against control / non-printable bytes.
 */
function escapeForDisplay(raw: string): string {
  let out = "";
  for (let i = 0; i < raw.length; i += 1) {
    const code = raw.charCodeAt(i);
    if (code === 0x5c) {
      // backslash
      out += "\\\\";
    } else if (code === 0x22) {
      // quote
      out += '\\"';
    } else if (code === 0x0a) {
      out += "\\n";
    } else if (code === 0x0d) {
      out += "\\r";
    } else if (code === 0x09) {
      out += "\\t";
    } else if (code < 0x20 || code === 0x7f) {
      out += `\\x${code.toString(16).padStart(2, "0")}`;
    } else {
      out += raw[i] ?? "";
    }
  }
  return out;
}

/**
 * Stringify a parsed key node for use as a struct-field label. Keeps
 * primitive idents/numbers as-is; quotes string keys; falls back to a
 * generic "<key>" placeholder for compound keys we don't expect to see
 * (tuples, sequences) but the parser still happens to consume.
 */
function keyLabel(node: DebugNode): string {
  switch (node.kind) {
    case "string":
      return `"${node.value}"`;
    case "primitive":
      return node.text;
    default:
      return "<key>";
  }
}

function isIdentStart(c: string): boolean {
  return (c >= "a" && c <= "z") || (c >= "A" && c <= "Z") || c === "_";
}
function isIdentCont(c: string): boolean {
  return isIdentStart(c) || (c >= "0" && c <= "9");
}
function isHexDigit(c: string): boolean {
  return (c >= "0" && c <= "9") || (c >= "a" && c <= "f") || (c >= "A" && c <= "F");
}
