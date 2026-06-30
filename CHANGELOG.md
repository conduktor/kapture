# Changelog

All notable user-facing features. Bug fixes, dependency bumps, lint/format
churn, and intra-feature refactors are intentionally omitted — read the git
log if you want every commit.

## [Unreleased]

## [0.3.0] — 2026-06-30 — JVM tap mode, anti-pattern detectors, tunable thresholds

### JVM tap mode

- **Observe a JVM Kafka client without a proxy.** Attach the Kapture
  agent to a Java process (`-javaagent`, or dynamic attach from the Tap
  dialog) and it streams plaintext Kafka wire bytes over a local Unix
  domain socket. The TLS connection to the broker stays end-to-end — real
  cert, real mTLS/SASL — so there's no second TLS session, no client
  config change, and nothing to point at `127.0.0.1`. Protocol / Messages
  / Expert tabs render identically to proxy mode.
- The Brokers tab is hidden in tap mode (no proxy topology to show).

### Expert tab — wire anti-pattern detectors

- **26 client + cluster anti-patterns detected live on the wire** —
  overcommit, producer-per-record, tiny batches, rebalance loop,
  stale-leader producing, mixed api_version, SASL drift, acks=0,
  compression-off, non-idempotent producer, producer-instance leak,
  transactional zombie, auto-commit cadence, tight fetch polling,
  fetch-session error cascade, throttle pressure, metadata storm, KIP-848
  holdouts, message-too-large, offset-out-of-range, cooperative-sticky
  churn, commit-during-rebalance, ACL deny, unknown-topic poll loop,
  coordinator churn, and **slow consumer poll stall** (a fetch stream that
  goes silent past `max.poll.interval.ms` then resumes — slow processing
  stalling the poll loop, which reads on dashboards as a scaling problem).
  Each finding links straight to the offending frame in the Protocol tab.
- **Tunable detector thresholds.** Every threshold is configurable via a
  global `detector_config.json`; the values the wire can't reveal
  (poll-stall gap ↔ `max.poll.interval.ms`, auto-commit interval, SASL
  reauth floor) are editable in a settings modal on the Expert tab, the
  rest in the file. Defaults reproduce the previous behaviour.

### Inspector

- **Copy button on every ProtoDetail layer.** Hover any section header
  (frame, decoded body, payload hex) → a `copy` chip appears. Frame
  metadata copies as plain `key: value` lines; decoded body as
  pretty-printed JSON; hex as hexdump-style rows. Built for pasting into a
  Slack thread or a GitHub issue without losing context.
- **RTT cell auto-scales.** Values like `1033.49 ms` used to overflow the
  narrow column. Now they read `1.03 s`; the formatter picks decimals so
  the result stays at 4 numeric chars (`1.41 ms`, `12.5 ms`, `999 ms`,
  `1.03 s`, …).

### Proxy

- **Client requests are still visible when the upstream broker is down.**
  The proxy now does a lazy per-frame upstream connect with a 1.5 s
  timeout. While the broker is unreachable, every client frame is decoded
  and pushed to the Protocol tab in error state, so you see exactly what
  the client emits and how it retries. The first successful reconnect
  flips the same TCP back to normal forwarding without a Kapture restart.

## [0.1.0] — 2026-05-09 — first OSS release

### Filter DSL

- **Wireshark-style filter language** (Pest grammar):
  `topic == "orders" && envelope.size > 1024 && headers.tenant == "acme"`.
  Compose, autocomplete, save.
- **Path-aware decoded body filter.**
  `MetadataResponse.brokers.host == "broker1"` walks the typed JSON tree
  with strict path matching, so a clicked `name` under one parent doesn't
  collide with a sibling `name` somewhere else in the message.
- **Hover-revealed filter chips on every value.** List rows, decoded
  leaves, JSON path nodes. Click → exact-match predicate; Alt-click →
  exclude. Pinned visible while the popover is open.
- **Filter menu split** into `Filter` (set new) / `Refine` (AND with the
  current expression). One row per literal predicate, no extra chrome.
- **Clear-filter button + Esc shortcut.**

### Protocol tab

- **Ring buffer of every Kafka frame** (request + response), 1 Hz poll
  to the renderer. Side-by-side list + detail with virtualized rendering.
- **Typed decoded body tree.** The vendored `kafka-protocol` fork
  derives `serde::Serialize` on every message struct, so requests and
  responses surface as a typed JSON tree (chevron rows, no nested cards),
  not just a Debug string. Hex view alongside, capped at 64 KiB.
- **Pair highlight.** Selecting a request highlights the matching
  response (and vice-versa) by `(connection_id, corr_id)`.
- **Brokers tab.** Active port mappings of the proxy listener fleet,
  per-listener send/recv counters folded from the proto frame stream.
- **Session Activity tab.** Persistent aggregate (clients seen, topics,
  groups, mechanisms) that survives ring buffer eviction — even after
  the originating frames scroll out you keep "this session is librdkafka
  2.x talking to topics X, Y, group Z".
- **KIP-516** (`topic_id` in `Produce` / `Fetch` v13+) and
  **KIP-932** (Share Groups: `ShareFetch` / `ShareGroupHeartbeat` /
  `ShareAcknowledge`) decoded.

### Proxy mode (the headline feature)

- **Local TCP intermediary.** Point your client at `127.0.0.1:9092`,
  Kapture forwards every byte to the upstream broker and copies a
  decoded view into the inspector. The client doesn't know it's there;
  no SDK swap, no broker plugin.
- **Multi-broker lazy listener fleet.** As the proxy sees new brokers
  in `Metadata` / `FindCoordinator` / `DescribeCluster` responses it
  binds a fresh local port for each and rewrites the response so the
  client's follow-up connections come back through Kapture instead of
  bypassing it.
- **KIP-951** broker-endpoint hints (`node_endpoints` on `Produce` /
  `Fetch` errors) rewritten alongside the legacy fields.
- **TLS upstream** (rustls, system roots + optional custom CA + per-broker
  SNI fallback when `server_name` is blank). **mTLS** with client cert /
  key paths in the dialog.
- **SASL pass-through** with credential redaction in the inspector ring
  (the broker still sees the real bytes; only the captured copy gets a
  placeholder).
- **SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512** upstream
  injection. Native RFC 5802/7677 SCRAM client (no C linkage).
- **Records extraction.** `Produce` request batches and `Fetch` response
  batches surface in the Messages tab, each back-linked to the frame
  that carried it.

### MCP server (let your agent drive Kapture)

- **HTTP MCP server on `127.0.0.1:7878/mcp`** with a copy-pasteable
  setup snippet panel for Claude Code, Cursor, Windsurf, etc.
- **Bearer-token auth**, token persisted in the user's config dir.
- **Tools:** capture lifecycle, snapshot, filter set, frame inspect,
  recent messages, proxy target / status / stop, session stats.
- **Resources:** session stats and a recent-messages window exposed as
  read-only resources for agents that prefer fetching to calling.

### Connection dialog

- **Connection profiles** persist upstream, auth, TLS paths, schema
  registry. Prefill on load, edit while connected.
- **Keychain integration** for SASL passwords.
- **Mode toggle** (client / proxy) — same dialog, different upstream
  semantics.
- **Localhost auto-detect** + connection Test.
- **CLI test snippets panel.** kcat / classic Apache Kafka command
  recipes, one click to copy.

### Schema Registry

- **Avro + JSON Schema** decode end-to-end. Async resolver with
  5-minute failure cache; falls back to the raw payload on lookup or
  decode failure so the row never goes blank.

### Distribution

- **macOS app bundle** with the Tauri auto-updater wired in (minisign
  signature verification against the embedded public key).
- **GitHub Actions release pipeline.** Tag push → build → sign → upload
  artifacts and `latest.json` to the GitHub Release. Optional Apple
  notarization when developer-id secrets are configured.
- **CI on every push and PR.** typecheck + eslint + prettier + vitest +
  cargo fmt + clippy (`-D warnings`) + cargo test.

### Removed in 0.1.0

- **Client mode** (the original `rdkafka`-FFI in-process capture path).
  Proxy mode covers every use case it did, without the C linkage and
  the patched-librdkafka fork. The `vendor/librdkafka` submodule is
  gone; only the pure-Rust `kafka-protocol-rs` fork remains.

### Path to 0.1.0 (May 5–9, 2026)

The project went from `git init` to first OSS release in five days. In
chronological order, the larger building blocks landed as:

1. Tauri app scaffolded; live Kafka capture via `rdkafka`; IPC event
   stream; dev stack.
2. Wireshark-style filter DSL (Pest); virtualized MessageList;
   follow-stream UX.
3. Schema Registry path (Avro + JSON Schema), with the failure cache
   and raw-payload fallback.
4. `proto_hook` patched into a librdkafka fork — per-message protocol
   context, FetchMetadata correlator, Messages | Protocol tabs.
5. Connection profiles + edit-while-connected; mTLS path; MCP server
   with bearer auth.
6. Auto-update + macOS bundling pipeline.
7. Proxy-mode pivot: TCP intermediary, per-connection pump, response
   rewriter, multi-broker lazy listeners, SASL pass-through, then
   SASL/PLAIN, SASL/SCRAM-SHA-256/512, TLS upstream, mTLS.
8. Records extraction; brokers tab; Session Activity aggregate;
   protocol filter unified on the textbox DSL.
9. Client mode retired — proxy mode subsumes it; the patched
   librdkafka fork is dropped.
10. CHANGELOG.
