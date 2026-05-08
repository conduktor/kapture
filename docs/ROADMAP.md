# Kapture Roadmap

Session-based debugging for Kafka clients running locally. Mission:
_answer "what is my client actually doing?" in seconds, without
reaching for tcpdump or scrolling through 5000 frames._

This file tracks the next round of features. Items are grouped by
debugging scenario, not by implementation layer. Each carries a rough
shirt size (S/M/L) and a "why now" line — the value proposition the
feature buys you that nothing else in Kapture does today.

Conventions:

- **S** ≈ ≤ 1 day, single module, no IPC contract change.
- **M** ≈ a few days, touches one Tauri command + a frontend pane.
- **L** ≈ a week+, schema migration / new cross-tab concept / new
  capture lifecycle.

---

## Visibility — see more of what already happened

### Latency per apiKey [M]

Per-RPC RTT histogram (p50 / p95 / max), slow-request highlight in
the Protocol list above a configurable threshold.

_Why:_ "my producer is slow" is unanswerable today. The data is
already on every Recv frame (`rtt_ms`) — just needs aggregation +
rendering. Distinguishes Produce-with-acks vs Metadata-thrash vs
Heartbeat-blocked diagnoses in one glance.

### Producer / transaction state [M]

Section in Session Activity tracking `producer_id`,
`producer_epoch`, transaction lifecycle (`InitProducerId →
AddPartitionsToTxn → Produce → EndTxn`). Surface stuck-open
transactions, epoch fences, repeated init events.

_Why:_ EOS / idempotent producers fail in subtle ways. Today the
user has to grep the Protocol tab for `InitProducerId` manually.

### Per-partition error expansion [S]

Walk the per-partition `error_code` fields nested inside
`ProduceResponse` / `FetchResponse` bodies (already in
`decoded_json`) and add them to the Errors list with topic +
partition context.

_Why:_ Top-level error code on Produce/Fetch is almost always 0.
The interesting failures (NOT_LEADER, OFFSET_OUT_OF_RANGE) live in
the partition results.

### Negotiated API versions [S]

Per-apiKey table showing the version each side advertised and the
version actually negotiated. Highlight when client maxVersion <
broker maxVersion → "you're missing KIP-X".

_Why:_ `summary.apiVersionsRequest` already carries the client
software/version; broker-side max versions are in
`ApiVersionsResponse`. Five lines of fold logic; one extra panel.

### Connection lifecycle [S]

Track open / close / reconnect per `connection_id`. Render a small
table: when it opened, how long it lived, how many frames it
carried, why it closed (FIN vs RST vs proxy-stop).

_Why:_ "my client keeps reconnecting" is hard to answer with the
current per-frame view.

---

## Diagnosis — surface what looks wrong

### Anomaly banner [M]

Banner at the top of Session Activity firing on heuristics:

- ≥ N rebalances in T seconds → "session.timeout.ms too low?"
- Metadata refresh rate > X / min → "metadata.max.age.ms too aggressive?"
- Compressed batch ratio < 1.1 → "compression disabled?"
- Producer retries > X → "broker rejecting acks?"
- Heartbeat gap > session_timeout × 0.5 → "consumer at risk of falling out"

_Why:_ This is what elevates Kapture above "Wireshark with a Kafka
dissector". Not just frames, but _flags_. Heuristics are cheap,
patterns are well-documented in Kafka client lore.

### Schema activity [S]

Panel listing schema fetches: subject / id / kind (Avro/Protobuf/
JSON-Schema), cache hit rate, 404s. Currently silent in the UI.

_Why:_ "why is my message empty in the inspector" → was the schema
fetched? Did the resolver 404? Today only the WARN log knows.

---

## Reproduction — capture, share, replay

### Session export / replay [L]

"Export session" button → `.kapture` file (proto frames +
`decoded_json` + captured records + session aggregate, gzipped
JSON or similar). Re-openable in a "replay" mode that disables the
live proxy and renders the file as if a capture were running.

_Why:_ Killer feature. Transforms Kapture from "watch live" to
"post-mortem + share". A bug report becomes "here's the .kapture",
not "here's the screenshot".

Open questions: format (custom JSON vs pcap-ng), encryption (SASL
creds in payload), schema registry resolution snapshot.

### Stash [M]

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

### Diff between sessions [M]

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

### Chaos / fault injection mode [L]

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

### Pre-built chaos scenarios [S, depends on chaos mode]

Catalog of named scenarios users can trigger one-click:

- "Leader move" — return NOT_LEADER for partition X for 5s
- "Coordinator failure" — FindCoordinator unavailable for 10s
- "Slow consumer" — throttle FetchResponse to 10ms each
- "Schema registry outage" — all SR lookups return 503

_Why:_ Reduces "what should I test for" to a checkbox list. Useful
for resilience-readiness audits in CI.

### Declarative scenario tests [L, depends on chaos + replay]

Test DSL: "with X chaos, replay this captured session, expect the
client to retry ≤ N times and recover within T". Run as part of CI.

_Why:_ Turn debugging-in-the-loop into regression tests. Captures
"my client survived this once" as "my client survives this every
build".

---

## Beyond — bigger / longer-term swings

### Time-travel scrub [L]

Drag a slider over the session timeline. The Session Activity
panels reflect the state of the world _at that timestamp_ —
which topics existed, which groups had which members, which errors
had fired. The Protocol list rewinds to that point.

_Why:_ Today everything is "current". Debugging frequently means
"what did the client know at minute 2?".

### Connection topology view [M]

Visualize: client → proxy listener → upstream broker. Each
connection is an edge. Show traffic per edge, reconnect events,
per-broker health. Replaces / extends the Brokers tab.

_Why:_ Multi-broker scenarios are hard to reason about as a flat
list. A graph is the natural shape.

### MCP-driven diagnostics [M]

Pre-canned MCP tools an LLM agent can call:

- `kapture_diagnose_consumer_lag(group_id)` → returns commits over
  time, current generation, members, suspected cause
- `kapture_explain_rebalance(group_id, time_window)` → reconstructs
  the rebalance from frames
- `kapture_compare_runs(stash_a, stash_b)` → diff summary

_Why:_ The MCP server already exists. Wrapping diagnostic flows in
named tools means the agent doesn't have to re-derive the same
analysis from raw frames every time.

### Watch / alert mode [S]

Declarative triggers: "flash the UI when error_code != 0 appears",
"play a sound on rebalance", "auto-stash when X happens". Persistent
across sessions.

_Why:_ Long debug sessions = boredom + miss the moment it happens.
Active alerting brings the moment to you.

### Traffic shape classification [M]

Heuristics that label the client: "producer-only", "single-consumer",
"streams app", "admin client", "transactional producer", etc. Surface
in Session Activity client tile.

_Why:_ "What is this client doing?" is the first question every
debug session asks. Today the user has to deduce it from frame
patterns; the tool can do it for them.

### Time correlation with app logs [M]

Paste / drop in an app log file (stdout or a structured log). Kapture
aligns its timeline with log timestamps and overlays log lines on
the Protocol list at matching timestamps.

_Why:_ The bug usually happens _between_ a log line and a Kafka RPC.
Aligning the two timelines is what most engineers do mentally; let
the tool do it.

### Pcap-ng export [M]

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
