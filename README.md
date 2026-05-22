# Kapture

**Wireshark for Kafka.** A desktop app that speaks the Kafka protocol natively, intercepts the traffic between your client and the broker, and shows you what's really going through the wire.

![Kapture Protocol tab — live capture of a Kafka producer + consumer through the proxy, with a decoded MetadataResponse opened on the right](docs/images/protocol-tab.png)

## Why

Most engineers building apps on Kafka have no good way to see what their clients actually do. Logs and dashboards don't show protocol exchanges, and topic browsers (Conduktor Console, Redpanda Console, AKHQ, Kafdrop) show data at rest, not the wire.

That's where the trouble hides — both the bad patterns in your code and the silent drift bugs in your SDK:

- `OffsetCommit` after every single record. [[example]](https://github.com/confluentinc/confluent-kafka-dotnet/issues/1081)
- A fresh producer (full `ApiVersions` + `Metadata` + `InitProducerId` handshake) per record. [[example]](https://www.pagerduty.com/eng/august-28-kafka-outages-what-happened-and-how-were-improving/)
- Tiny Produce batches behind a high message rate (`linger.ms=0` + tiny `batch.size`). [[example]](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1030%3A+Change+constraints+and+default+values+for+various+configurations)
- A consumer group rebalancing every few seconds because the heartbeat config is wrong. [[example]](https://medium.com/@nishada/fixing-kafka-stream-consumer-rebalancing-babda7f2e333)
- A producer pinned to a stale leader after a broker restart — `Metadata` says leader is broker 50, `Produce` still goes to broker 34. [[example]](https://github.com/confluentinc/librdkafka/issues/4976)
- A rolling upgrade where the client locks in the seed broker's `api_version` and breaks against older brokers. [[example]](https://github.com/tulios/kafkajs/issues/1656)
- A SASL re-auth that dies on a clock-like cadence — `Session too short` on the second 2h hop. [[example]](https://github.com/aws/aws-msk-iam-auth/issues/176)

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

No instrumentation, no SDK swap, no broker plugin. The client doesn't know it's there. SASL/PLAIN, SASL/SCRAM-SHA-256/512, TLS, and mTLS upstream are all passed through correctly.

## What you get

- **Live wire view.** Every Kafka API request/response decoded — `corr_id`, RTT, payload size, full body tree. Apache Kafka 4.x compatible (including KIP-516 topic IDs and KIP-932 Share Groups).
- **Messages tab.** Decoded records flattened from Produce requests and Fetch responses. Each record back-links to the frame it rode on so you jump from the message to the wire in one click.
- **Filter DSL.** Wireshark-style: `topic == "orders" && envelope.size > 1024 && headers.tenant == "acme"`. Compose, autocomplete, save.
- **Connection profiles.** Bootstrap, TLS, SASL — saved locally; passwords in the OS keychain. Last-used profile is pre-selected on launch.
- **Expert info — 25 detectors.** Kapture flags client + cluster anti-patterns on the wire as you go: overcommit, producer-per-record, tiny batches, rebalance loop, stale-leader producing, mixed `api_version`, SASL session-too-short, `acks=0`, compression-off, non-idempotent producer, producer-instance leak (PagerDuty 2025 shape), transactional zombie, auto-commit cadence, tight fetch polling, `INVALID_FETCH_SESSION_EPOCH` cascade, throttle pressure (KIP-219), metadata storm, classic rebalance on a KIP-848-ready cluster, `MESSAGE_TOO_LARGE`, `OFFSET_OUT_OF_RANGE`, cooperative-sticky churn, commit-during-rebalance, ACL deny storm, `UNKNOWN_TOPIC_OR_PARTITION` poll loop, coordinator churn.
- **Bonus: agent-driven.** A local MCP server (`http://127.0.0.1:7878/mcp`) exposes capture / filter / inspect tools so an IDE agent (Claude Code, Cursor, Windsurf) can drive Kapture for you. SASL frames redacted before they cross the boundary.

## Install

The app self-updates on each launch on every platform.

**macOS (Homebrew)** — Apple Silicon only:

```sh
brew tap conduktor/kapture https://github.com/conduktor/kapture
brew install --cask kapture
```

> macOS will refuse to open the app the first time with a _"Kapture is damaged"_ warning. The app is not yet codesigned with an Apple Developer ID, so Gatekeeper rejects the quarantine attribute. Strip it once and the app launches normally:
>
> ```sh
> xattr -cr /Applications/Kapture.app
> ```

**macOS (manual)** — download `Kapture_<version>_aarch64.dmg` from [Releases](https://github.com/conduktor/kapture/releases/latest), open it, drag `Kapture.app` to `/Applications`. Same `xattr -cr` workaround applies.

**Linux** — `Kapture_<version>_amd64.AppImage` (chmod +x and run) or `Kapture_<version>_amd64.deb` (`sudo apt install ./Kapture_*.deb`). x86_64 only.

**Windows** — `Kapture_<version>_x64-setup.exe` (NSIS installer) or `Kapture_<version>_x64_en-US.msi`. SmartScreen will warn on first run because the binary is not yet codesigned — click _More info_ → _Run anyway_.

In the Connection dialog: point Kapture's listener (default `127.0.0.1:9092`) at your upstream broker (Confluent Cloud, MSK, your local docker, …). Configure SASL/TLS if needed. Hit Start. Then point any Kafka client at `127.0.0.1:9092` and watch.

Building from source: `pnpm install && pnpm tauri dev`.

## Roadmap

- **Tap modes — observation without breaking TLS.** Read Kafka traffic from inside the client process instead of standing in front of it. JVM tap (ByteBuddy agent on `SslTransportLayer`) POC validated against SSL-enabled Apache Kafka — see [`experiments/jvm-tap/`](experiments/jvm-tap/) and [the five-part blog series](docs/blog/). librdkafka and Go static `crypto/tls` taps via eBPF uprobes are next. Goal: ~95% of the production Kafka client market observable without provisioning a cert.
- **Chaos.** Inject latency, error codes, connection drops at the proxy layer to validate client behaviour under adversarial conditions. Toxiproxy, but Kafka-aware.
- **Time-travel debugger.** Breakpoints by predicate against Kafka Streams / Flink consumers; step through messages; inspect state stores.

Full backlog with shirt sizes in [docs/ROADMAP.md](docs/ROADMAP.md).

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for the full feature inventory grouped by area, plus what's brewing on `main`.

## Feedback

- Email: [kapture@conduktor.io](mailto:kapture@conduktor.io)
- Issues: https://github.com/conduktor/kapture/issues

## License

Apache-2.0.
