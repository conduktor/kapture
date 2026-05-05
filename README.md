# Kapture

**Wireshark for Kafka events.** A desktop inspector for live Kafka traffic with deep decoding, expressive filters, and key-aware stream following. Built for developers debugging streaming pipelines, not browsing topics.

> Status: pre-alpha. UI shell + Tauri scaffold + strict lint pipeline. Capture engine and filter parser are next.

## Why

Kafka tooling is saturated with topic browsers (Conduktor, Redpanda Console, AKHQ, Kafdrop). None of them help you _understand what's flowing right now_. Kapture is the missing layer: see messages live, decode them through schema layers, follow a single key across topics, and write filters that actually express intent.

## The vision (3 pillars)

1. **Inspector** — Wireshark-style live capture with decoded layers, filter DSL, follow-by-key. **MVP.**
2. **Debugger** — Time-travel debugger for Kafka Streams / Flink consumers (breakpoints by predicate, step messages, inspect state stores).
3. **Notebook** — Reactive workspace for sketching topologies, sharing repros, onboarding.

The full design lives in [`docs/spec.md`](./docs/spec.md).

## Tech stack

- **Tauri 2** + **Rust** for the capture engine and IPC core
- **React 19** + **TypeScript** + **Vite** for the UI
- **Wireshark-style filter DSL** (Pest parser, planned)
- **rdkafka** for cluster I/O (planned)

Wire-compatible with Apache Kafka and any derivative (Redpanda, MSK, Confluent Cloud, Aiven, WarpStream...).

## Quick start

Prerequisites: Node ≥ 20, pnpm ≥ 9, Rust ≥ 1.78, Docker (for the local Kafka), `librdkafka` (`brew install librdkafka` on macOS).

```bash
# 1. Install JS dependencies
pnpm install

# 2. Boot a local Redpanda (Kafka API + Schema Registry, single node)
pnpm stack:up

# 3. Inject some test data (loop produces ~10 msg/s indefinitely)
pnpm seed          # one-shot: 100 messages
pnpm seed:loop     # continuous: ~10 msg/s

# 4. Smoke-test the Rust capture pipeline against the live cluster
pnpm rust:smoke

# 5. Launch the desktop app
pnpm tauri dev
```

The Connection dialog defaults to `localhost:19092` and the three seeded topics. The optional Redpanda Console (web UI at <http://localhost:18888>) is included for cross-checking what the cluster sees.

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
