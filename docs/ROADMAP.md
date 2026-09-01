# Kapture Roadmap

Session-based debugging for Kafka clients running locally. Mission:
_answer "what is my client actually doing?" in seconds, without
reaching for tcpdump or scrolling through 5000 frames._

This file tracks work that is not fully shipped yet. Items are grouped
by debugging scenario, not by implementation layer. Each heading carries
a status and a rough shirt size; completed features move to the
[changelog](../CHANGELOG.md) instead of lingering here as future work.

Conventions:

- **planned** — no user-facing implementation yet.
- **partial** — some protocol state, detector logic, or UI is shipped;
  the text states exactly what remains.
- **S** ≈ ≤ 1 day, single module, no IPC contract change.
- **M** ≈ a few days, touches one Tauri command + a frontend pane.
- **L** ≈ a week+, schema migration / new cross-tab concept / new
  capture lifecycle.

---

## Visibility — see more of what already happened

### Latency per apiKey [planned · M]

Per-RPC RTT histogram (p50 / p95 / max), slow-request highlight in
the Protocol list above a configurable threshold.

_Why:_ "my producer is slow" is unanswerable today. The data is
already on every Recv frame (`rtt_ms`) — just needs aggregation +
rendering. Distinguishes Produce-with-acks vs Metadata-thrash vs
Heartbeat-blocked diagnoses in one glance.

### Producer / transaction state [partial · M]

Shipped foundation: `InitProducerId`, `AddPartitionsToTxn`, `Produce`,
and `EndTxn` feed the anti-pattern fold; transactional-zombie and
producer-instance-leak findings are live. Remaining work is the durable
per-producer lifecycle model and its Session Activity pane.

Section in Session Activity tracking `producer_id`,
`producer_epoch`, transaction lifecycle (`InitProducerId →
AddPartitionsToTxn → Produce → EndTxn`). Surface stuck-open
transactions, epoch fences, repeated init events.

_Why:_ EOS / idempotent producers fail in subtle ways. Today the
user has to grep the Protocol tab for `InitProducerId` manually.

### Group lifecycle pane [partial · M]

Shipped foundation: Session Activity aggregates members, latest
generation, joins, heartbeats, commits, and errors; Expert detects several
rebalance/commit contradictions. Remaining work is the ordered lifecycle
model with active/zombie/fenced status and missing-event detection.

Mirror of Producer / transaction state on the consumer side.
Section in Session Activity tracking each group's membership
lifecycle: `FindCoordinator → JoinGroup → JoinGroupResponse(gen=N)
→ SyncGroup → Heartbeat × M → OffsetCommit → LeaveGroup`. One row
per `(group_id, member_id, generation)`, with a status chip
(active / zombie / fenced) and the last event timestamp. Flag the
contradictions: `JoinGroup` from a new member with no preceding
`LeaveGroup` from the previous one, generation gaps, missing
heartbeats past `session.timeout.ms × 0.5`.

_Why:_ Today the consumer-group lifecycle has to be reconstructed
by hand from the Protocol list. The zombie-member story
(connection closed without `LeaveGroup`, new joiner waits the full
`session.timeout.ms` for assignment) becomes a one-glance
diagnosis instead of a manual log walk.

### Per-partition error expansion [partial · S]

Per-partition `ProduceResponse` / `FetchResponse` errors are already
decoded and feed the Expert detectors. Add every non-zero result to the
Session Errors list with topic + partition context; that list still only
stores the aggregate response code.

_Why:_ Top-level error code on Produce/Fetch is almost always 0.
The interesting failures (NOT_LEADER, OFFSET_OUT_OF_RANGE) live in
the partition results.

### Partition routing audit [partial · S]

Shipped foundation: the stale-leader detector compares Produce routing
with the latest Metadata response. Remaining work is the standing
per-partition audit table and clean-state signal.

Per `(topic, partition)` table in Session Activity: current leader
according to the last `MetadataResponse`, the broker the last N
`ProduceRequest`s and `FetchRequest`s actually went to, and a
mismatch counter. Green when they agree, red when they drift.

_Why:_ The Protocol drift detector tells you when routing breaks;
this pane tells you it's clean before you ship — useful as a
pre-deploy or post-failover sanity check ("yes, my producer came
back to the right broker"). Same data the drift detector folds
over; just rendered as a standing audit instead of an alert.

### Negotiated API versions [partial · S]

Shipped foundation: API advertisements are decoded and mixed broker
versions produce an Expert finding. Remaining work is the standing
per-apiKey client/broker/selected-version table.

Per-apiKey table showing the version each side advertised and the
version actually negotiated. Highlight when client maxVersion <
broker maxVersion → "you're missing KIP-X".

_Why:_ `summary.apiVersionsRequest` already carries the client
software/version; broker-side max versions are in
`ApiVersionsResponse`. Five lines of fold logic; one extra panel.

### Per-broker capability matrix [partial · M]

Shipped foundation: the anti-pattern fold retains advertised max versions
long enough to detect advertised-version heterogeneity between brokers.
Remaining work is a persistent per-broker matrix, the per-request
contradiction check, and the optional active probe.

Step beyond Negotiated API versions: fold every `ApiVersionsResponse`
seen on the wire into a per-broker matrix (advertised api_version
range per apiKey, supported compression types, KIP-516 topic IDs,
SASL mechanisms). One row per upstream broker, keyed by the
`(upstream_host, upstream_port)` already tracked by the broker
provisioner — the per-broker listener arrangement makes the key
trivially available.

Surfaces:

- Heterogeneity flag — brokers advertise different max versions
  for the same apiKey → rolling upgrade in progress.
- Authoritative mismatch — client sent `Produce v7` to broker B,
  but B's advertised max in its `ApiVersionsResponse` is v6.
  Wire-side proof of mixed-version bugs (KafkaJS #1656 shape),
  not inference.
- Per-broker capability table at a glance — which broker supports
  what, in one view.

_Why:_ Sister to the Protocol drift detector's mixed-version
check. The current detector catches advertised differences; the
matrix gives you the standing picture and makes "rolling upgrade
in progress" a one-glance diagnosis. Pure passive — every
well-behaved client connection already starts with
`ApiVersionsRequest`/`Response`, so the data is on the wire today.

Optional extension — active probe: when the client only ever
talked to the bootstrap broker (KafkaJS #1656 shape, no
re-negotiation per broker), Kapture itself can dial the other
brokers from `MetadataResponse` using the upstream creds the proxy
already holds, send its own `ApiVersionsRequest`, and fill the
gaps in the matrix.

### Connection lifecycle [planned · S]

Track open / close / reconnect per `connection_id`. Render a small
table: when it opened, how long it lived, how many frames it
carried, why it closed (FIN vs RST vs proxy-stop).

_Why:_ "my client keeps reconnecting" is hard to answer with the
current per-frame view.

---

## Tap modes — observation without TLS termination

The proxy-only model has a ceiling: to read TLS, Kapture has to
terminate TLS, which provisions a fake cert and re-encrypts to the
upstream broker. That breaks pinning, mTLS, Confluent Cloud cert
chains, and any client environment where "swap the cert" is not
acceptable.

Tap modes observe the Kafka wire from _inside_ the client process
(or kernel-side via uprobes) instead of standing between client and
broker. The TLS connection stays end-to-end with the real broker.
Kapture sees plaintext because it reads the bytes before encrypt /
after decrypt, not because it broke the encryption.

All tap modes feed the same wire decoder as the proxy. The
Protocol / Messages / Expert tabs render the same data with a
`source: tap-jvm | tap-ebpf | proxy` badge per frame. Five blog
posts in `docs/blog/01..05-*.md` explain the motivation and the
design.

> **JVM tap** is already shipped (covers `SslTransportLayer` and
> `PlaintextTransportLayer`, agent at `agents/jvm-tap/`, listener at
> `src-tauri/src/jvm_tap.rs`, Tauri commands `start_jvm_tap` /
> `stop_jvm_tap`). The items below are the remaining tap work.

### JVM tap — follow-ups [partial · S/M each]

Hardening + UX items left on the JVM path:

- [ ] Bump ByteBuddy to a release with Java 25 support (eliminates the
      `-Dio.kapture.tap.shaded.bytebuddy.experimental` workaround).
- [ ] Add a shutdown drain hook on the agent. The current hook reports
      dropped frames but does not flush the writer queue, so JVM exit can
      still lose the tail of a capture.
- [x] Ship the JVM PID picker with Kafka-socket hints, dynamic attach,
      and one-click **Inject & tap**.
- [ ] Detect Conscrypt or BouncyCastle JSSE and either extend the hook
      target or surface a clear "use proxy mode" message.
- [ ] Ship `kapture-jvm-agent.jar` as a GitHub release asset alongside
      the desktop app so users do not have to build it from source.

### eBPF tap — librdkafka family [planned · L]

eBPF uprobes on `SSL_write` and `SSL_read` in OpenSSL / BoringSSL,
following the AgentSight (arXiv:2508.02736) and ecapture recipes.
Covers every Kafka client built on `librdkafka`:
confluent-kafka-{python, node, ruby, dotnet}, plus C/C++ apps and
`confluent-kafka-go` (which is cgo over librdkafka).

Implementation outline:

- Use libbpf-rs (already in the Rust ecosystem) for the userspace
  loader. Avoid bcc to keep the runtime light.
- Two probes per process: entry-uprobe on `SSL_write` (captures
  the plaintext buffer before encrypt), return-uprobe on
  `SSL_read` (captures the plaintext buffer after decrypt). Carry
  the `SSL*` pointer through a BPF map to correlate.
- Userspace ringbuf consumer in Kapture writes into the same
  decoder pipeline as the JVM tap. Same `source: tap-ebpf` badge.
- PID picker scans `/proc/*/maps` for `libssl*` and Kafka-shaped
  sockets (port 9092/9093/9094 or a string match against
  `__consumer_offsets` in the heap).

Constraints:

- Linux only. macOS dev falls back to proxy mode for non-JVM
  clients.
- `CAP_BPF` (or root) required. Document `setcap` setup; warn
  clearly if not available.
- Statically-linked OpenSSL needs offset-scanning fallback —
  defer until we see a real customer report.

_Why:_ Closes the Python / Node / .NET / Ruby gap in one move.
About a quarter of the production Kafka fleet uses these stacks.

### eBPF tap — Go static `crypto/tls` [planned · L]

Sister to the librdkafka tap, different attach technique. Pure-Go
Kafka clients (Sarama, segmentio/kafka-go) statically link
`crypto/tls` and do not export the SSL symbols. We follow the
Speedscale / ecapture pattern:

- Disassemble `crypto/tls.(*Conn).Read` and `.Write` at agent
  start, scan for `RET` instructions, attach uprobes at each
  offset (Go's uretprobes are unsafe because of goroutine stack
  management).
- Per-Go-version offset table for the common Go releases (1.20+).
  New Go releases require regeneration; surface a clear "Go
  version unsupported, please report" message instead of silently
  failing.

Constraints:

- Strip-resistant: requires non-stripped binaries (`-ldflags="-s"`
  removes the symbol that the disassembler needs).
- Linux only, `CAP_BPF` required (same as librdkafka tap).

_Why:_ Sarama and `confluent-kafka-go` native Go covers the
remaining production gap. Combined with the JVM and librdkafka
taps, Kapture observes roughly 95% of the production Kafka client
market without breaking TLS.

### Tap source picker UI [partial · S]

The JVM process picker is shipped. Runtime detection and routing for
OpenSSL/librdkafka and static Go clients depend on their eBPF tap modes.

Connection dialog gains a "Tap a process" entry alongside "New
proxy". The picker lists local processes that look like Kafka
clients (have a connection to a Kafka port + load a TLS library
Kapture knows how to hook), with the technique pre-selected per
runtime:

- JVM process → JVM tap path
- Process with `libssl*` mapped → eBPF librdkafka path
- Statically-linked Go binary with `crypto/tls` symbols → eBPF Go
  path
- Anything else → "Use proxy mode" link

_Why:_ Without a friendly picker, the tap modes are CLI flags only.
The picker is what makes the feature visible to users who don't
read the docs.

### Pcap / SSLKEYLOGFILE import [planned · M]

Fourth observation source: a `.pcap` file plus an
`SSLKEYLOGFILE`-format key log. Kapture decrypts offline using the
keys, then feeds the same wire decoder. Useful for forensic /
post-mortem analysis where neither a proxy nor a live tap is an
option.

Open: librdkafka does not natively emit `SSLKEYLOGFILE`. We may
need to ship a patched OpenSSL wrapper or lobby upstream
(librdkafka issue #3454, open since 2021).

_Why:_ Closes the "we can't be live on the host" case. Also makes
Kapture a credible Wireshark companion for Kafka — same input,
better decoder.

---

## Diagnosis — surface what looks wrong

### Session anomaly summary [partial · M]

The Expert tab now ships 26 live detectors, including rebalance loop,
metadata storm, compression off, throttle pressure, and slow poll stall.
Remaining work is a compact Session-level summary plus the signals not
yet modeled here, notably producer retry pressure and heartbeat gaps.

Banner at the top of Session Activity firing on heuristics:

- ≥ N rebalances in T seconds → "session.timeout.ms too low?"
- Metadata refresh rate > X / min → "metadata.max.age.ms too aggressive?"
- Compressed batch ratio < 1.1 → "compression disabled?"
- Producer retries > X → "broker rejecting acks?"
- Heartbeat gap > session_timeout × 0.5 → "consumer at risk of falling out"

_Why:_ This is what elevates Kapture above "Wireshark with a Kafka
dissector". Not just frames, but _flags_. Heuristics are cheap,
patterns are well-documented in Kafka client lore.

### Protocol drift detector [partial · M]

Sister to the Anomaly banner: same surface (Expert info / banner),
different signal class. Where the banner watches volumes and
cadences, this watches _contradictions_ — places where the wire
says one thing and the client does another. Fires on:

- [x] Stale-leader producing — client routes `Produce` to broker A
      while the latest `MetadataResponse` named broker B as leader for
      that partition.
- [x] Mixed `api_version` advertisements — brokers expose different
      `max_version` values for the same API key.
- [ ] Request/version contradiction — a request uses a version the
      target broker did not advertise in its `ApiVersionsResponse`.
- [ ] Topic-ID drift — same topic name appears with two different
      topic IDs across `Metadata` responses but `Produce` routing did
      not follow.
- [ ] Missing `LeaveGroup` on shutdown — connection closed without a
      clean group exit, leaving the coordinator to wait
      `session.timeout.ms` before reassigning.
- [ ] Stale-generation `OffsetCommit` — commit carries a `generation_id`
      older than the latest `JoinGroupResponse`.
- [ ] Idempotent producer wedge — `Produce` failures with PID errors
      after a timeout, with no `InitProducerId` re-handshake on the
      path.
- [x] Scheduled SASL re-auth break — `SaslAuthenticate` succeeds
      initially, then the next scheduled re-auth fails on the same
      connection on a clock-like cadence.

_Why:_ These are the bugs that turn into multi-day incidents
because the app symptom (timeout, wedge, no progress) is identical
to a dozen other causes. The wire tells you which one in seconds —
but only if the tool flags the contradiction. Patterns drawn from
public issues in librdkafka, KafkaJS, confluent-kafka-go,
aws-msk-iam-auth, and ClickHouse.

### Finding evidence bundles [planned · M]

An Expert finding currently points to only its most recent offending
frame. Retain a small, bounded, ordered set of contributing frame IDs and
render the causal sequence next to three stable sections: **Observed**,
**Why it matters**, and **Fix**. Repeated handshakes should read as
`ApiVersions → Metadata → InitProducerId → Produce × N`; commit storms
should show the Fetch/record/OffsetCommit relationship instead of one
isolated commit.

Fix guidance can use the client name/version already observed in
`ApiVersionsRequest` to show the relevant Java, librdkafka, KafkaJS, or
other client setting without changing capture behavior.

_Why:_ This shortens normal incident diagnosis, bug-report handoff, and
code review. It is a product feature, not presentation-only UI.

### Schema activity [planned · S]

Panel listing schema fetches: subject / id / kind (Avro/Protobuf/
JSON-Schema), cache hit rate, 404s. Currently silent in the UI.

_Why:_ "why is my message empty in the inspector" → was the schema
fetched? Did the resolver 404? Today only the WARN log knows.

---

## Reproduction — capture, share, replay

### Session export / replay [planned · L]

"Export session" button → `.kapture` file (proto frames +
`decoded_json` + captured records + session aggregate, gzipped
JSON or similar). Re-openable in a "replay" mode that disables the
live proxy and renders the file as if a capture were running.

_Why:_ Killer feature. Transforms Kapture from "watch live" to
"post-mortem + share". A bug report becomes "here's the .kapture",
not "here's the screenshot".

Open questions: format (custom JSON vs pcap-ng), encryption (SASL
creds in payload), schema registry resolution snapshot.

### Stash [planned · M]

Like `git stash` for capture sessions. While running, user hits a
shortcut → snapshot of the last N seconds (proto frames +
`decoded_json` + session aggregate) gets pinned with a label
("before deploy", "config change"). Stashes appear in a side
panel; clicking one opens it in replay mode (see export above).

_Why:_ The user's natural debug rhythm is "do the thing, watch what
happens, do the next thing". Stashes let them mark the boundaries
without a full export, and compare across stashes later.

Builds on the export format — a stash IS an export, scoped to a
window.

### Diff between sessions [planned · M]

Open two `.kapture` files side-by-side. Show what changed:

- New / removed topics
- New / removed groups
- Latency delta per apiKey
- Error count delta
- New anomaly flags

_Why:_ "Did my fix actually change anything?" → 1-click answer
instead of eyeballing two screenshots.

---

## Active testing — make things break on purpose

### Chaos / fault injection mode [planned · L]

Proxy gains an "evil mode" knob set: random response delays, random
error codes on configurable RPCs, random connection drops. Toggleable
per apiKey + intensity slider. Examples:

- 5% of `ProduceResponse` → `error_code = NOT_LEADER_OR_FOLLOWER`
- 500ms artificial delay on `FindCoordinatorResponse`
- Drop the connection 30s after open
- `FetchResponse` returns empty records (simulate consumer starvation)
- `OffsetCommitResponse` → REBALANCE_IN_PROGRESS x N then OK
- Schema registry 404 / 500
- Coordinator unavailable for T seconds

_Why:_ Local dev has no equivalent of "kill the broker, see what
happens". Letting the user _script_ failure is the difference
between catching a bug at staging vs in their seat.

Open: needs to be SAFELY off-by-default, opt-in per session, never
applied to writes the user actually wants persisted.

### Pre-built chaos scenarios [planned · S, depends on chaos mode]

Catalog of named scenarios users can trigger one-click:

- "Leader move" — return NOT_LEADER for partition X for 5s
- "Coordinator failure" — FindCoordinator unavailable for 10s
- "Slow consumer" — throttle FetchResponse to 10ms each
- "Schema registry outage" — all SR lookups return 503

_Why:_ Reduces "what should I test for" to a checkbox list. Useful
for resilience-readiness audits in CI.

### Declarative scenario tests [planned · L, depends on chaos + replay]

Test DSL: "with X chaos, replay this captured session, expect the
client to retry ≤ N times and recover within T". Run as part of CI.

_Why:_ Turn debugging-in-the-loop into regression tests. Captures
"my client survived this once" as "my client survives this every
build".

### Virtual broker fan-out [planned · L]

Tell the proxy "pretend there are N brokers" when there is actually
one upstream. Kapture spins up N local listeners with synthetic
`node_id`s (high range, e.g. 10000+, to avoid clashing with real
broker IDs), rewrites `MetadataResponse` to advertise the N
brokers, distributes partition leadership and coordinator roles
across them, and forwards every request to the single real
upstream while preserving the per-fake-broker view client-side.

Use cases:

- Parallelism testing — measure how the client fans out across
  N broker connections without standing up a real N-broker
  cluster.
- Leader-move chaos — rewrite metadata mid-session to move
  partition X from fake-broker-1 to fake-broker-2. Client
  experiences the full reconnect / re-route dance; underneath,
  nothing moved.
- Per-broker fault isolation — couple with Chaos mode to drop
  fake-broker-2 alone, reproducible and deterministic, without
  touching real infra.
- Mixed-version simulation — make fake-broker-2 advertise older
  api versions in its `ApiVersionsResponse`. Reproduce
  KafkaJS-#1656-shape bugs in a controlled environment.

_Why:_ Most clients are tested against one local broker and a
real cloud cluster. The middle ground — adversarial multi-broker
behaviour, deterministic and reproducible — is hard to stand up.
The proxy already rewrites `MetadataResponse` and provisions a
listener per upstream broker; multiplying the broker count is the
same trick run in the other direction. Plays with Chaos mode
(per-fake-broker fault injection) and the Per-broker capability
matrix (each fake broker can declare its own capabilities).

Open questions:

- `node_id` rewriting must be consistent across every response
  that carries one (replicas, ISRs, `node_endpoints[]` from
  KIP-951, `FindCoordinatorResponse`) so the client never sees a
  synthetic ID it can't resolve.
- Idempotent / transactional producers — sequence numbers per
  `(PID, partition)` should still work since partition ownership
  is consistent (one real broker underneath), but worth
  verifying with an EOS producer test before declaring done.
- Operations that require real cluster topology (controller
  election, real replica fencing, real ISR shrinks) can't be
  simulated — this is illusion, not a real cluster.

---

## Beyond — bigger / longer-term swings

### Time-travel scrub [planned · L]

Drag a slider over the session timeline. The Session Activity
panels reflect the state of the world _at that timestamp_ —
which topics existed, which groups had which members, which errors
had fired. The Protocol list rewinds to that point.

_Why:_ Today everything is "current". Debugging frequently means
"what did the client know at minute 2?".

### Connection topology view [planned · M]

Visualize: client → proxy listener → upstream broker. Each
connection is an edge. Show traffic per edge, reconnect events,
per-broker health. Replaces / extends the Brokers tab.

_Why:_ Multi-broker scenarios are hard to reason about as a flat
list. A graph is the natural shape.

### Sequence / swim-lane view [planned · L]

Alternate projection of the Protocol timeline. Same frames,
different lens: instead of one chronological list, render one row
per actor (`member_id` / `producer_id` / `connection_id`) with
events plotted along the time axis. Toggle from the Protocol tab.
Gaps where an expected next event did not arrive become actual
gaps on screen — the missing `LeaveGroup` before a `JoinGroup`
on a new member, the `Heartbeat` cadence breaking, the `Produce`
retries clustering on one broker after a leader move — instead of
"frames you have to know to look for".

_Why:_ Per-actor flow is how the protocol reads in your head
anyway. The flat list serializes interleaved conversations into
one stream; the swim-lane un-serializes them. Especially valuable
for multi-member groups, multi-broker producers, and rolling
upgrades where the contradiction sits across actors, not within
one. Sister to the Group lifecycle pane and Connection topology
view — same underlying data, third projection.

### MCP-driven diagnostics [planned · M]

Pre-canned MCP tools an LLM agent can call instead of re-deriving
the same analysis from raw frames every session.

**Tools shape (sketch):**

- `kapture_diagnose_consumer_lag(group_id)` → JSON with:
  - latest committed offset per (topic, partition) for the group
  - latest observed HWM per partition (from `ListOffsets` /
    Fetch responses)
  - estimated lag per partition + total
  - rebalance count in the last N minutes
  - suspected cause string (`"healthy"`, `"frequent rebalances"`,
    `"slow processing"`, `"stuck on partition X"`)
- `kapture_explain_rebalance(group_id, since?)` → ordered list:
  `JoinGroup → JoinGroupResponse(gen=N) → SyncGroup →
Heartbeat × M → trigger`. Each event with `frame_id` so the
  agent can ask for full bodies if it wants.
- `kapture_topic_activity(topic, since?)` → produced/consumed
  record counts, error codes seen, partition coverage.
- `kapture_session_summary()` → the persistent `SessionStats` plus
  derived flags (anomaly hits, traffic shape label).
- `kapture_compare_stashes(stash_a, stash_b)` → diff result (gated
  on the stash feature shipping first).

**Implementation:**

- New module `src-tauri/src/mcp_diagnostics.rs` that takes a
  `&AppState` and returns the JSON payloads. Pure functions over
  the existing `ProtoCorrelator` ring + `SessionStats` aggregate.
- Each tool registered with the existing MCP server alongside the
  raw `kapture_proto_frames` etc. tools.
- Schemas exposed via `JsonSchema` so the agent gets typed
  argument hints.

**Phasing:**

1. Ship `kapture_session_summary` first — it's a pass-through over
   `session_stats()` and proves the wiring.
2. `kapture_diagnose_consumer_lag` next — exercises the
   "compose multiple frames" pattern.
3. The rest as we discover what agents actually ask for.

**Open questions:**

- Should the tools include rendering hints (units, severity)? Or
  let the agent compose the prose? Probably the latter — Kapture
  ships data, the LLM ships words.
- MCP token already gates access; no extra auth needed.

_Why:_ The MCP server already exists. Wrapping diagnostic flows in
named tools turns the agent from "reads 5000 frames and writes a
summary" into "asks Kapture, gets a typed answer". Distinctive
because no other Kafka tool exposes this surface.

### Watch / alert mode [planned · S]

Declarative triggers: "flash the UI when error_code != 0 appears",
"play a sound on rebalance", "auto-stash when X happens". Persistent
across sessions.

_Why:_ Long debug sessions = boredom + miss the moment it happens.
Active alerting brings the moment to you.

### Traffic shape classification [planned · S]

Heuristics over the existing `SessionFold` that label the client
in one shot. Surfaced as a chip below the existing "Client" tile
in Session Activity (e.g. `apache-kafka-java 3.9.0` _·_
`streams app + transactional producer`).

**Decision tree (rough):**

| Signal observed                                                | Label                               |
| -------------------------------------------------------------- | ----------------------------------- |
| `InitProducerId` seen                                          | adds `transactional producer`       |
| `ProduceRequest` only, no group RPCs                           | `producer-only`                     |
| `JoinGroup` seen                                               | adds `consumer`                     |
| Topics include `*-changelog` or `*-repartition`                | adds `streams app`                  |
| Only Admin RPCs (CreateTopics, DescribeConfigs, ListGroups, …) | `admin client`                      |
| `ApiVersionsRequest` only, no follow-ups                       | `probe` (test_proxy_upstream-style) |

Labels compose: a Streams app shows
`consumer + transactional producer + streams app`.

**Implementation:**

- New `infer_traffic_shape(&SessionFold) -> Vec<TrafficLabel>`
  in `session_stats.rs`. Pure function, no new state.
- Result threaded into `SessionStats.shape: Vec<String>` (camelCase
  serde) so the frontend renders without computing.
- Re-evaluated on every `session_stats()` snapshot — cheap, no
  caching needed.

**Open questions:**

- Whether to attach confidence ("looks like a streams app
  (3 signals)" vs definitive). Probably keep it deterministic and
  simple — the user can drill into Topics/Groups to verify.
- Where to draw the line between "consumer" and "streams app"
  reliably without false-positives on changelog naming
  conventions.

_Why:_ "What is this client doing?" is the first question every
debug session asks. The data is already in the fold; we just need
the rules.

### Time correlation with app logs [planned · M]

Drop a log file onto Kapture; it aligns log lines with the proto
frame timeline and renders an interleaved view. The bug usually
happens _between_ a log line and a Kafka RPC; aligning both
timelines is what every engineer does mentally — let the tool do
it.

**Phase 1 — file drop (smallest viable):**

- New tab `Logs` (or right-pane on Protocol). Drop file → parse →
  render.
- Timestamp parser tries a fixed list of patterns in order:
  - ISO 8601 / RFC 3339 (`2026-05-08T17:14:05.156Z`) — Java
    structured logs, Go's slog, anything serious.
  - SLF4J / Logback default (`17:14:05.156`) — assume same date as
    capture.
  - Python logging default (`2026-05-08 17:14:05,156`).
  - Anything else → line discarded with a "couldn't parse N lines"
    indicator.
- Each line gets a `(timestamp, raw)` pair. Stored client-side, no
  backend involvement.
- Render: in the Protocol list, log lines appear as faint
  inter-row separators with the log text; clicking one expands.

**Phase 2 — overlay on Session Activity:**

- Errors panel shows nearby log lines per error event (within ±2s
  of `error.ts`). Direct correlation between Kafka error and what
  the app was logging at that moment.

**Phase 3 — follow stdout (later):**

- "Watch a file" mode: tail a path, append new lines to the
  timeline live. Same parser. Useful for `cargo run | tee app.log`
  workflows.

**Open questions:**

- Multi-line log entries (Java stack traces): treat as one event
  attached to the first line's timestamp.
- Clock drift between the app and Kapture's machine is irrelevant
  in local-dev (same host) but exists in container scenarios. Out
  of scope for v1.
- Privacy: log lines may contain secrets. Display-only, never
  shipped via MCP.

_Why:_ Closes the "what was the app doing when this RPC fired"
loop without leaving Kapture. Cheap interop with the user's
existing log discipline (no instrumentation required).

### Pcap-ng export [planned · M]

Export captured frames as a `.pcapng` readable by Wireshark with its
existing Kafka dissector. Gives users a fallback "open in Wireshark"
for cases where Kapture's UI doesn't fit.

_Why:_ Cheap interoperability win. Wireshark has filters Kapture
won't replicate; meeting users where they are widens adoption.

---

## Out of scope (for now)

These have come up in conversation and are deliberately _not_ on
the list:

- **Cluster-level monitoring** — Kapture is for one local app, not
  fleet observability. There are 50 better tools for that.
- **Persistent multi-session history** — nothing beyond the in-memory
  ring + opt-in stash/export. Avoid becoming "yet another data lake".
- **Schema editing** — Kapture reads schemas from the registry,
  doesn't write them.
- **Multi-cluster proxying** — one upstream cluster per session.
  Cross-cluster routing is a different product.
