# Kapture

**Wireshark for Kafka events.** A desktop inspector for live Kafka traffic with deep decoding, expressive filters, and key-aware stream following. Built for developers debugging streaming pipelines, not browsing topics.

> Status: pre-alpha. UI shell + Tauri scaffold + strict lint pipeline. Capture engine and filter parser are next.

## Why

Kafka tooling is saturated with topic browsers (Conduktor, Redpanda Console, AKHQ, Kafdrop). None of them help you _understand what's flowing right now_. Kapture is the missing layer: see messages live, decode them through schema layers, follow a single key across topics, and write filters that actually express intent.

## The vision (2 pillars)

1. **Inspector** — Wireshark-style live capture with decoded layers, filter DSL, follow-by-key. **MVP.**
2. **Debugger** — Time-travel debugger for Kafka Streams / Flink consumers (breakpoints by predicate, step messages, inspect state stores).

Kapture also exposes an **MCP server** on `http://127.0.0.1:7878/mcp` so AI agents (Claude Desktop, IDE assistants, custom clients) can drive captures directly: list / load profiles, set filters, snapshot recent messages, inspect a single message by id. Secrets stay server-side — agents never see SASL or TLS-key passwords.

The full design lives in [`docs/spec.md`](./docs/spec.md).

## Tech stack

- **Tauri 2** + **Rust** for the capture engine and IPC core
- **React 19** + **TypeScript** + **Vite** for the UI
- **Wireshark-style filter DSL** (Pest parser, planned)
- **rdkafka** for cluster I/O (planned)

Wire-compatible with Apache Kafka and any derivative (Redpanda, MSK, Confluent Cloud, Aiven, WarpStream...).

## Quick start

Prerequisites: Node ≥ 20, pnpm ≥ 9, Rust ≥ 1.82, Docker. No system Kafka or SASL package required — Kapture's vendored librdkafka builds with built-in SASL (PLAIN / SCRAM-SHA-256/512 / OAUTHBEARER) and no `libsasl2` runtime dependency.

The dev stack runs **two Kafka clusters side-by-side** so Kapture can be smoke-tested against canonical Apache Kafka and Redpanda in parallel:

| Cluster      | Kafka API         | Schema Registry          |
| ------------ | ----------------- | ------------------------ |
| Redpanda     | `localhost:19092` | `http://localhost:18081` |
| Apache Kafka | `localhost:29092` | `http://localhost:28081` |

```bash
# 1. Install JS deps and build the Kapture-patched librdkafka
pnpm install
# librdkafka is vendored under vendor/librdkafka with two Kapture
# patches: rd_kafka_set_proto_hook_cb (per-message protocol
# context) and a CMakeLists tweak that lets WITH_SASL_CYRUS=OFF
# disable the libsasl2 link entirely. The result is a single
# librdkafka.dylib that needs no system SASL package at runtime.
pnpm librdkafka:build

# 2. Boot one (or both) local clusters
pnpm stack:up:redpanda     # Redpanda only
pnpm stack:up:kafka        # Apache Kafka + cp-schema-registry only
pnpm stack:up              # both

# 3. Inject test data — five topics, mixing payload encodings:
#      orders.raw         JSON (no Schema Registry)
#      orders.enriched    JSON (no Schema Registry)
#      users.events       JSON (no Schema Registry)
#      orders.avro        Avro via Confluent Schema Registry
#      orders.jsonschema  JSON Schema via Confluent Schema Registry
pnpm seed                  # Redpanda, one-shot 200 msg
pnpm seed:loop             # Redpanda, continuous ~10 msg/s
pnpm seed:kafka            # Apache Kafka, one-shot
pnpm seed:loop:kafka       # Apache Kafka, continuous

# 4. Smoke-test the Rust capture pipeline
pnpm rust:smoke            # plain JSON path
pnpm rust:sr-smoke         # SR + Avro + JSON Schema decode
cargo run --manifest-path src-tauri/Cargo.toml --example proto_smoke
                           # proto-hook end-to-end (per-message protocol context)

# 5. Launch the desktop app
pnpm tauri dev
```

The Connection dialog supports PLAINTEXT and **SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512** with optional TLS (`SASL_SSL`). Use it directly against managed Kafka offerings (Confluent Cloud, Aiven, MSK, WarpStream) by entering bootstrap, mechanism, username, password, and toggling the TLS box.

When you're done:

```bash
pnpm stack:down
```

## Quality gates

Strict from day one. The pre-commit hook runs all of these:

| Layer      | Tool                                                                                                      |
| ---------- | --------------------------------------------------------------------------------------------------------- |
| TypeScript | `tsc --noEmit` (strict + `noUncheckedIndexedAccess` + `exactOptionalPropertyTypes`)                       |
| JS / React | ESLint flat config — `typescript-eslint` strict-type-checked + react / react-hooks / react-refresh        |
| Formatting | Prettier (TS, CSS, HTML, JSON, MD)                                                                        |
| Rust style | `cargo fmt --check`                                                                                       |
| Rust lints | `cargo clippy -- -D warnings` with `pedantic` + `nursery` denied, `unwrap` / `expect` / `panic` forbidden |

Manual run:

```bash
pnpm check          # all checks
pnpm lint:fix       # auto-fix JS/TS
pnpm format         # auto-format
pnpm rust:fmt:fix   # auto-format Rust
```

## License

Apache-2.0.
