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
