# Pillar 2 — Debugger (parked, not started)

Status: **parked**. Not on the roadmap until Pillar 1 (Inspector) is shipped publicly and we have feedback. Revisit when there's a concrete user story that the Inspector + tcpdump-style replay can't already cover.

## What it would be

Time-travel debugger for stream processing apps (Kafka Streams, Flink), inspired by Chrome DevTools' "step through call stack" but applied to event-driven topology.

Core idea: developer attaches Kapture to a running Streams/Flink job; Kapture records inputs, intermediate state changes (state stores / operator state), and outputs. Developer can then **rewind** to any point and inspect the topology's state.

## Differentiator vs. Pillar 1

Pillar 1 (Inspector) is **passive observation** of the wire. It sees Kafka traffic; it does not see operator state, windowed aggregations mid-window, or repartition shuffles inside the JVM.

Pillar 2 would be **inside-the-app observability**:

- Snapshot of state stores (RocksDB) at point-in-time
- Replay a single message through a single processor with the matching state restored
- Predicate breakpoints: pause when `topic == "orders" && payload.amount > 1000` flows through processor `aggregateByCustomer`
- Step-into for sub-topologies / Flink operators

## Why parked

1. **Inspector still has to ship and prove demand.** Distribution, auto-update, and a real user base come first. A debugger no one runs is dead code.
2. **Surface area is enormous.** Kafka Streams alone needs JVM agent / instrumentation, RocksDB snapshot/restore, sub-topology mapping. Flink adds checkpoint integration. Each is a multi-month effort.
3. **Open question: is this an IDE plugin or a desktop app feature?** Step-debugging traditionally lives in the IDE. Kapture is the "external observer" tool — we may be the wrong shape for this. Worth thinking through before committing.
4. **Risk of scope creep.** Inspector + Debugger in one app dilutes both. Better to consider a separate companion product if the demand is real.

## Open questions to revisit later

- Is the audience Streams developers OR streaming SREs? They want different things (debugging vs. operational forensics).
- Can we get 80% of the value with passive replay (Pillar 1 already does this for messages)? If the hard part is JVM state, maybe a much cheaper "snapshot exporter" library that the Inspector reads is enough.
- Flink and Streams have very different runtimes. Pick one to start, OR design around a generic "state-store snapshot" protocol both can adopt.
- Does the AI-agent angle (MCP server) change the game? Maybe the debugger is "ask the agent to bisect this bug" rather than a manual step-debugger UI.

## What to do if revisited

1. Re-validate the user story with at least 3 conversations with active Streams/Flink users.
2. Prototype JVM-side instrumentation **outside** Kapture, as a standalone library, to avoid coupling.
3. Decide IDE plugin vs. desktop feature based on prototype feedback.
4. If kept inside Kapture, design the wire format between the JVM agent and Kapture as a protocol so it survives runtime swaps.

Last touched: 2026-05-05.
