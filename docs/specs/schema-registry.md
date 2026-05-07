# Schema Registry integration (proxy mode)

## Status

Plan. `ConfluentEnvelope::try_parse` already populates
`CapturedMessage.schema_id` when the value carries the `0x00 |
u32_be schema_id | …` envelope. `schema_name` is still `None`
because no `SchemaRegistryClient` is wired into the proxy session.
This doc spec's that wiring.

## Why

Kapture is a Wireshark-grade Kafka inspector. A user-facing
"schema: none (raw payload)" on a frame that's clearly Avro reads
as a bug — it's not, but the registry-backed name (`OrderCreated`,
`UserSignup`, …) is the metadata users actually scan for.

## Out of scope

- Schema _evolution_ viewer (compatibility, version diff).
- Decoding the payload bytes against the schema (Avro/JSON-Schema/
  Protobuf decode pipelines). The current `decode_payload` already
  emits a useful structured view from raw bytes; binding
  schema-aware decoding is a follow-up after the registry name is
  surfaced.

## UX

- Connection dialog gains an optional **Schema Registry URL** input
  alongside Bootstrap / TLS / SASL. Empty = disabled = current
  behaviour.
- The seed compose advertises one URL per cluster
  (`http://localhost:18081` for redpanda, `http://localhost:28081`
  for apache/kafka); the field is plain text, no auto-detection.
- Persisted on the connection profile next to TLS/SASL.
- Detail panel: when a `schema_id` is decoded but the name resolves
  to `None`, label "schema id N (resolving…)" while in flight,
  then patch to "schema: NAME (id N) — KIND" when the registry
  responds. Failure fall-through: "schema id N (registry error)".

## Architecture

### Backend

1. **`ProxyArgs`** — new optional `registry_url: Option<String>`.
   Plumbed through `start_proxy` → proxy session.
2. **`AppState`** — owns an `Option<Arc<SchemaRegistryClient>>`.
   Reset on `start_proxy`/`stop_proxy`.
3. **Resolution is async + patch-based**, not sync-blocking.
   Sync-blocking the per-frame pump for an HTTP call would spike
   p99 RTT under load. Existing `pending_produce` pattern (offsets
   back-filled from response) is the model.
   - On record extract, populate `schema_id` synchronously (already
     done).
   - Enqueue `(message_id, schema_id)` on a bounded mpsc to a
     **resolver task** spawned per session.
   - Resolver task `await`s `client.fetch(schema_id)`; on
     resolution, locates the record via `state.buffer.find_by_id`,
     patches `schema_name` + new field `schema_kind: Option<String>`
     ("AVRO"/"JSON"/"PROTOBUF"), and emits
     `kapture:message-schema-resolved` (batched).
4. **Cache** — `SchemaRegistryClient` already LRU-caches by id
   (capacity 1024). Cache hits avoid network. Misses are ~one-shot
   per id.
5. **Errors** — registry 404/timeout: stash `(id → Failed)` in a
   small failure cache (5-min TTL) so we don't retry-storm. Patch
   the record with `schema_kind: Some("UNRESOLVED")` so the UI can
   render "registry error" once.

### Ring-buffer mutation

`RingBuffer::find_by_id` is read-only today. Add
`update_message_with(&self, id: &str, mut f: impl FnMut(&mut
CapturedMessage))` with the same lock pattern as `find_by_id`.
Failure to find (record evicted before resolution) is a no-op.

### IPC patch event

```ts
// kapture:message-schema-resolved
type SchemaResolvedPatch = {
  id: string;
  schemaName: string | null; // null if registry returned 404
  schemaKind: "AVRO" | "JSON" | "PROTOBUF" | null;
};
```

Batched same as `kapture:messages` (rAF-friendly cadence).

### Frontend

1. New event subscription in `App.tsx`. Patches merge into
   `messagesRef.current` and `selectedDetail` (if currently
   selected).
2. `KafkaMessage` + `KafkaMessageDetail` gain `schemaKind`.
3. `LayerTree` schema layer renders the three-state tree:
   resolved (name + kind) / pending (id only) / failed (id +
   "registry error").
4. Filter DSL: extend `envelope.schema_kind` (string) so users
   can chip-filter "AVRO only".

## Risks / unknowns

- **Auth on the registry** (Confluent Cloud uses Basic auth).
  Defer; current scope is local dev clusters. Add `auth: { user,
pass }` to `registry_url` when needed; profile field grows.
- **TLS on the registry** — same. `https://` URLs work via
  `reqwest` defaults; cert customisation is the follow-up.
- **Schema-id collisions across clusters** — IDs are scoped per
  registry, so a profile change must reset the resolver state.
  Tied to `start_proxy` lifecycle — resolver task drops with the
  session.

## Milestones

1. Plumb `registry_url` through `ProxyArgs` and persist on profile.
2. Wire `SchemaRegistryClient` instantiation; resolver task with
   bounded mpsc; ring-buffer update path.
3. IPC patch event + frontend subscription + state merge.
4. UI: pending/resolved/failed states in the detail panel +
   filter DSL extension.

Order is strict — each step is testable in isolation against the
local seed (Avro + JSON-Schema topics already produced by
`tools/seed.mjs`).
