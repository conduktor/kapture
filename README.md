# Kapture

**Wireshark for Kafka.** A desktop app that speaks the Kafka protocol natively, intercepts the traffic between your client and the broker, and shows you what's really going through the wire.

![Kapture Protocol tab — live capture of a Kafka producer + consumer through the proxy, with a decoded MetadataResponse opened on the right](docs/images/protocol-tab.png)

## Why

Most engineers building apps on Kafka have no good way to see what their clients actually do. Logs and dashboards lie about latency and frequency, and topic browsers (Conduktor Console, Redpanda Console, AKHQ, Kafdrop) show data at rest, not the protocol exchange.

That's where the bad patterns hide:

- `OffsetCommit` after every single record. Yes, it happens.
- A fresh producer (full `ApiVersions` + `Metadata` + `InitProducerId` handshake) per record. Yes, it happens.
- A `Metadata` storm because someone disabled the cache.
- Tiny Produce batches behind a high message rate (`linger.ms=0` + tiny `batch.size`).
- A consumer group rebalancing every few seconds because the heartbeat config is wrong.

Invisible from logs. Obvious once you see the protocol.

If you debugged HTTP with Fiddler ten years ago, this is the same idea, for Kafka.

## How it works

Kapture runs a local TCP proxy. You point your Kafka client at `127.0.0.1:9092` (instead of your real broker), Kapture forwards every byte upstream, and copies a decoded view into the inspector.

```
your client ──▶ 127.0.0.1:9092 ──▶ real broker
                    │
                    ▼
                Kapture inspector (live)
```

No instrumentation, no SDK swap, no broker plugin. The client doesn't know it's there. SASL/PLAIN, SASL/SCRAM-SHA-256/512, and TLS upstream are all passed through correctly.

## What you get

- **Live wire view.** Every Kafka API request/response with `corr_id`, RTT, payload size, decoded body. Apache Kafka 4.x covered, including KIP-516 (topic IDs in Produce/Fetch v13+) and KIP-932 (Share Groups: `ShareFetch`, `ShareGroupHeartbeat`, `ShareAcknowledge`).
- **Messages tab.** Decoded records flattened from Produce requests and Fetch responses. Backlinks every record to the originating frame so you can jump from the message to the wire.
- **Filter DSL.** Wireshark-style: `topic == "orders" && envelope.size > 1024 && headers.tenant == "acme"`. Compose, autocomplete, save.
- **MCP server.** `http://127.0.0.1:7878/mcp` exposes 13 tools so an agentic IDE (Claude Code, Cursor, Windsurf, etc.) can drive captures, set filters, and inspect frames. Bearer-authenticated; SASL frames are redacted before crossing the boundary. Open the MCP modal in the app for one-click setup snippets.
- **Drop-aware ring buffer.** 100k messages or 256 MiB, whichever fills first. `drops/sec` surfaced in the status bar so you can tell hemorrhage from a single spike. Optional auto-pause when the rate is unsustainable.
- **Connection profiles.** Bootstrap, TLS, SASL — saved locally; passwords in the OS keychain. Last-used profile is pre-selected on launch.

## Install & run

Prerequisites: Node ≥ 20, pnpm ≥ 9, Rust ≥ 1.82.

```bash
pnpm install
pnpm tauri dev
```

That's it. The app boots, the MCP server comes up, and the Connection dialog opens.

In the Connection dialog: point Kapture's listener (default `127.0.0.1:9092`) at your upstream broker (`localhost:29092`, your Confluent Cloud endpoint, your MSK, etc.). Configure SASL/TLS if your broker needs it. Hit Start.

Then point any Kafka client at `127.0.0.1:9092` and watch.

## Test it locally

A docker stack is included with two clusters side-by-side so you can validate against canonical Apache Kafka and Redpanda in parallel:

| Cluster      | Bootstrap         | Schema Registry          |
| ------------ | ----------------- | ------------------------ |
| Redpanda     | `localhost:19092` | `http://localhost:18081` |
| Apache Kafka | `localhost:29092` | `http://localhost:28081` |

```bash
pnpm stack:up              # both clusters
pnpm seed                  # 200 messages of mixed encodings (JSON, Avro, JSON Schema)
pnpm seed:loop             # continuous ~10 msg/s
pnpm stack:down            # tear it down
```

Then in Kapture, set upstream to one of the cluster addresses and produce/consume normally.

## Stress test

The numbers from a recent local run on M-series hardware, 80k × 4 KiB at 1.2k msg/s sustained:

- Ring buffer at the byte cap (256 MiB exact).
- Drop-oldest active, no crash, mem stable.
- Producer p99 latency dominated by the proxy IPC path, not the broker.
- All MCP tools (`kafka_snapshot`, `kafka_set_filter`, `kafka_inspect_frame`) responsive under load.

`drops` you see are observability eviction (oldest captured frames being recycled to make room). The Kafka traffic itself is never lost — TCP flow control is the only backpressure mechanism between client and broker, and it works whether Kapture is in the middle or not.

## Quality gates

Strict from day one. Pre-commit runs all of these:

| Layer      | Tool                                                                                                  |
| ---------- | ----------------------------------------------------------------------------------------------------- |
| TypeScript | `tsc --noEmit` (strict + `noUncheckedIndexedAccess` + `exactOptionalPropertyTypes`)                   |
| JS / React | ESLint flat config — `typescript-eslint` strict-type-checked + react / react-hooks / react-refresh    |
| Formatting | Prettier                                                                                              |
| Rust style | `cargo fmt --check`                                                                                   |
| Rust lints | `cargo clippy -- -D warnings`, `pedantic` + `nursery` denied, `unwrap` / `expect` / `panic` forbidden |
| Tests      | `cargo test --lib` — 116 tests, including a cross-check against the Kafka protocol enum               |

```bash
pnpm check          # all checks
```

## Roadmap

- **Pattern detector.** Spot the anti-patterns above (overcommit, producer-per-record, metadata storm, tiny batches, rebalance loop) and surface them as Wireshark-style "Expert info".
- **Chaos.** Inject latency, error codes, connection drops at the proxy layer to validate client behaviour under adversarial conditions. Toxiproxy, but Kafka-aware.
- **Time-travel debugger.** Breakpoints by predicate against Kafka Streams / Flink consumers; step through messages; inspect state stores.

## Feedback

- Email: [stephane@conduktor.io](mailto:stephane@conduktor.io)
- Issues: https://github.com/sderosiaux/kapture/issues (will move to `conduktor/kapture`)

## License

Apache-2.0.
