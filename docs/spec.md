# Kapture — Design Spec

_Last updated: 2026-05-05_

## Vision

Chrome DevTools for streaming pipelines. Not a cluster manager, not a topic browser. **Understand and debug what is flowing between services through Kafka.**

The market is saturated with UIs that browse what is _stored_ in Kafka. Kapture is for what is _moving_.

## Three pillars (sequential delivery)

1. **Inspector** — Wireshark-like capture and dissection of live Kafka traffic. **MVP.**
2. **Debugger** — Time-travel debugger for Kafka Streams / Flink / consumer applications. Breakpoints by predicate, step through messages, inspect state stores at each step. Post-MVP.
3. **Notebook** — Reactive workspace for stream exploration, repro sharing, onboarding. Post-MVP.

Each pillar is shippable independently. The Inspector alone is a complete product.

## MVP scope (Inspector)

- Live-streaming capture (in-memory ring buffer, no persistence at v1)
- Wireshark-style filter DSL (Pest parser)
- Layered decode: envelope → schema → payload
- Follow-stream by key across topics
- Three-pane UI: list / decoded layers / hex
- Auto-detect schemas: Confluent Schema Registry magic byte → registry lookup → JSON heuristic → raw fallback
- Connections: PLAINTEXT, SASL/PLAIN, SASL/SCRAM, mTLS, OAuth/OIDC, AWS IAM (MSK)
- Wire-compatible with Apache Kafka, Redpanda, MSK, Confluent Cloud, Aiven, WarpStream

Out of scope at v1: persistence and `.kcap` files, debugger, notebook, cluster management (configs, ACLs, brokers), produce UI, internationalisation, mobile.

## Audience

The Inspector serves every Kafka persona that needs to understand traffic in real time:

- Stream developers debugging non-deterministic behaviour
- Platform engineers reverse-engineering inherited topologies
- SREs investigating production incidents
- Hobbyists prototyping pipelines

No persona-specific mode at v1. The same UI serves all of them.

## Architecture

```
Frontend (React, Tauri webview)
┌──────────────────────────────────────────────────┐
│ Filter bar │ Message list │ Layer tree │ Hexdump │
└────────────────────────┬─────────────────────────┘
                         │ Tauri IPC (typed commands and events)
Backend (Rust core)      ▼
┌──────────────────────────────────────────────────┐
│ Connection Mgr → Capture Engine → Ring Buffer    │
│        (rdkafka)        (FIFO, bounded)          │
│                              │                   │
│ Filter Engine ◀── Decode Pipeline ◀── Schema Res │
│ (DSL parser,     (envelope/schema/    (SR + magic│
│  AST eval)        payload layers)      byte +    │
│                                        heuristic)│
└──────────────────────────────────────────────────┘
            │
       Kafka cluster(s)
```

The capture engine and filter engine never reference each other. The ring buffer is the only shared dependency. This boundary is the foundation of testability and future evolution.

## Components

### Connection Manager

- Multiple cluster profiles, persisted encrypted in the OS keychain
- Auth: PLAINTEXT, SASL/PLAIN, SASL/SCRAM-SHA-256/512, mTLS, OAuth/OIDC, AWS IAM (MSK)
- TLS with custom CA and SNI
- Connection test before saving a profile

### Capture Engine

- One Rust task per connection
- rdkafka consumer in `latest` mode by default; optional `earliest` or `from-timestamp`
- User selects topics, regex supported
- Backpressure: the ring buffer drops oldest with an exposed counter when the UI cannot keep up

### Ring Buffer

- In-memory FIFO with a configurable bound (default: 100k messages or 1 GB, whichever is hit first)
- Indexed by ingestion order, topic, partition, key (key index enables follow-stream)
- The ring buffer is the session — there is no persistence at v1

### Schema Resolver (auto-detect)

The decode path tries strategies in order:

1. **Confluent magic byte** (`0x00 + 4-byte schema id`) → call Schema Registry, cache the result in an LRU keyed by `(registry, id)`
2. **Schema Registry discovery** — explicit user config or inference from typical bootstrap conventions; on connect, the resolver pulls subjects
3. **JSON heuristic** — payload starts with `{` or `[`? Try `serde_json`, fall back on parse error
4. **Bring your own** — user drops `.proto` / `.avsc` / `.json schema` files into the profile directory
5. **Hex fallback** — show raw bytes with ASCII gutter if every strategy fails

If the registry is offline, decode degrades cleanly to step 3+ and a banner indicates the loss.

### Filter Engine

Wireshark-style DSL parsed with Pest.

```
filter      := expr
expr        := and ( "||" and )*
and         := comparison ( "&&" comparison )*
comparison  := unary ( ( "==" | "!=" | "=~" | "<" | ">" | "<=" | ">=" | "in" ) literal )?
unary       := ( "!" )? primary
primary     := "(" expr ")" | identifier
identifier  := name ( "." name )*
literal     := string | number | boolean | regex | list

# Identifier namespaces:
#   topic, envelope.*, headers.*, schema.*, payload.*
```

Examples:

```
topic == "orders.raw"
topic =~ "orders\..*" && headers.tenant == "acme"
payload.amount > 1000 && envelope.partition in (3, 7)
schema.name == "OrderCreated" && !payload.refunded
```

The AST evaluates lazily per message with short-circuit. The filter bar evaluates against the ring buffer for instant re-filtering. Saved filters live per cluster profile.

### Decode Pipeline

Layered, fault-tolerant:

- **L1 envelope**: offset, partition, timestamp, key, headers, size
- **L2 schema**: schema id and resolved name/version (when applicable)
- **L3 payload**: deserialised tree (object / array / primitive / bytes)

A failure at L3 does not break L1 and L2 — the message still appears with raw payload.

### UI (Wireshark three-pane)

- **Top bar**: connection picker, filter bar with autocomplete (paths discovered dynamically), saved filters menu, capture controls
- **Message list**: virtualised, configurable columns (timestamp, topic, partition, offset, key, schema, size), colour coding by schema
- **Layer tree**: collapsible, each layer expandable, right-click → _filter on this field_
- **Hex pane**: raw bytes with key/value highlight, toggle to collapse
- **Side panel**: connection status, ring buffer stats, throughput indicator, drop counter
- **Follow stream**: select a message → action _follow key_ → filter bar pins `headers.traceid == "X"` or `key == "Y"` as a clearable chip

### Data flow per message

1. rdkafka delivers a raw record to the capture engine
2. Capture engine pushes into the ring buffer (always, regardless of filter)
3. UI demands the visible window → decode runs lazily (may hit Schema Registry)
4. Filter engine evaluates against the decoded view → message included or excluded
5. Virtualised list renders

Lazy decode is the key to performance under high throughput.

## Error handling

| Failure                 | Behaviour                                                                          |
| ----------------------- | ---------------------------------------------------------------------------------- |
| Connection error        | Surfaced in the side panel, retry with exponential backoff, never crash            |
| Schema Registry offline | Decode degrades to JSON / hex, banner indicates the state, capture continues       |
| Filter parse error      | Red underline + tooltip, ring buffer untouched                                     |
| Backpressure overflow   | Drop oldest with a counter visible in the status bar                               |
| Auth refused            | Broker error verbatim (Kafka error messages are useful) plus a plain-language hint |

## Testing strategy

| Layer           | Approach                                                                    |
| --------------- | --------------------------------------------------------------------------- |
| Filter parser   | Unit tests + property-based fuzzing with `proptest`                         |
| Decode pipeline | Golden tests — canned bytes → expected decoded tree                         |
| Capture engine  | Integration tests with `testcontainers-rs` against real Kafka and Redpanda  |
| UI components   | Vitest component tests, visual regression on the three-pane layout          |
| End-to-end      | Tauri test harness, scripted scenarios (connect → produce → assert visible) |

## Quality bar

Strict from the first commit. Pre-commit hook runs all of:

- `tsc --noEmit` with strict + `noUncheckedIndexedAccess` + `exactOptionalPropertyTypes`
- ESLint flat config with `typescript-eslint` strict-type-checked + react/react-hooks/react-refresh
- Prettier (TS, CSS, HTML, JSON, MD)
- `cargo fmt --check`
- `cargo clippy -- -D warnings` with `pedantic` + `nursery` denied; `unwrap`, `expect`, `panic`, `dbg!`, `todo!`, `unimplemented!` forbidden

## OSS strategy

- License: Apache-2.0 (Kafka-friendly)
- GitHub public from day one
- Filter DSL grammar published as documentation
- No telemetry without explicit opt-in
- Roadmap visible via GitHub Projects
- Future format spec for `.kcap` files (post-MVP) will be public for ecosystem adoption

## Risks and tradeoffs

| Risk                                     | Mitigation                                                                                                  |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Live-only flow loses share-a-session     | `.kcap` export and offline replay planned for v0.2                                                          |
| Auto-detect schemas inflates MVP scope   | If it overruns, descope to Schema Registry only with hex fallback; BYO schemas and JSON heuristic to v0.1.5 |
| In-memory ring buffer caps long captures | Clear UX about the bound, drop counter visible, "you're seeing the last N messages" indicator               |
| Tauri + Rust learning curve              | Two-day spike on connection + capture loop before committing to the full stack                              |

## Decision log

| Decision        | Choice                                                          | Rationale                                                              |
| --------------- | --------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Tech stack      | Tauri 2 + Rust + React                                          | Native binary size, Rust capture performance, React dev velocity       |
| Core flow       | Live-streaming-first                                            | Faster MVP path; differentiation comes from filter / decode / follow   |
| Filter language | Wireshark-style with namespaces                                 | Aligns with the product positioning, autocomplete-friendly             |
| Schema handling | Auto-detect (registry + magic + heuristic + BYO + hex fallback) | Zero-config first-run; adoption depends on it                          |
| UI metaphor     | Wireshark three-pane                                            | Familiar to developers, dense, info-rich                               |
| Distribution    | OSS pure (Apache-2.0)                                           | Personal project, no business model, network effects via ecosystem fit |
| Brokers         | Wire-compatible Kafka (incl. Redpanda)                          | One protocol, broadest reach                                           |
