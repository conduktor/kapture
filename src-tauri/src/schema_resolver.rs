//! Schema-Registry resolver task.
//!
//! Drains `(message_id, schema_id)` enqueued by the proxy sink (one
//! per record carrying a Confluent envelope), fetches the schema
//! through `SchemaRegistryClient` (LRU-cached), patches the
//! ring-buffer record in place, and emits a batched
//! `kapture:message-schema-resolved` IPC event so the live UI
//! updates without re-fetching the message.
//!
//! Lifetime is per proxy session: spawned by `start_proxy_impl`
//! when the session has a registry URL; the receiver closes when
//! the sink's last `Sender` drops on `stop_proxy`.
//!
//! Failures (404 / timeout / non-2xx) feed a 5-min TTL cache so a
//! missing schema id doesn't retry-storm; the matching record is
//! still patched with `schema_kind = "UNRESOLVED"` so the UI
//! distinguishes "not yet resolved" from "registry said no".

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tauri::{AppHandle, Emitter};

use crate::avro;
use crate::decode::{decode_payload, DecodedValue};
use crate::message::CapturedMessage;
use crate::ring_buffer::RingBuffer;
use crate::schema_registry::{ResolvedSchema, SchemaKind, SchemaRegistryClient};
use crate::state::AppState;

/// One row of `kapture:message-schema-resolved`. Sent batched in a
/// `Vec` so the UI gets one rAF-friendly emit per `FLUSH_INTERVAL`
/// (or sooner when the buffer fills) even when many records resolve
/// in tight succession.
///
/// `payload` is `Some` only when the resolver successfully decoded
/// the value bytes against the resolved schema (Avro / JSON-Schema).
/// The frontend hook merges this into a currently-selected detail
/// so the user sees the structured tree without a re-fetch.
/// Subsequent `inspect_message_by_id` calls already see the patched
/// payload because the resolver writes it back into the ring buffer.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaResolvedPatch {
    pub id: String,
    pub schema_name: Option<String>,
    pub schema_kind: Option<String>,
    pub payload: Option<DecodedValue>,
}

const FAIL_TTL: Duration = Duration::from_secs(300);
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const FLUSH_LEN: usize = 64;
const UNRESOLVED_KIND: &str = "UNRESOLVED";

/// Schema reference path — payload-prefix int32 id (legacy) vs.
/// header-stored 16-byte UUID GUID (Confluent CP 8.1.1+). The
/// resolver dispatches each variant to its matching registry
/// endpoint (`/schemas/ids/{id}` vs `/schemas/guids/{guid}`).
#[derive(Debug, Clone)]
pub enum SchemaRef {
    Id(u32),
    Guid(String),
}

impl SchemaRef {
    fn cache_key(&self) -> u64 {
        // Lightweight identity used by the failure cache so a single
        // `HashMap<u64, Instant>` can key on either variant. Hash is
        // good enough — collisions just retry-storm on a stale id at
        // worst, which is what the TTL exists for.
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        match self {
            Self::Id(n) => {
                0u8.hash(&mut h);
                n.hash(&mut h);
            }
            Self::Guid(g) => {
                1u8.hash(&mut h);
                g.hash(&mut h);
            }
        }
        h.finish()
    }
}

pub fn spawn(
    client: Arc<SchemaRegistryClient>,
    buffer: Arc<RingBuffer>,
    app: AppHandle,
    mut rx: tokio::sync::mpsc::Receiver<(String, SchemaRef)>,
) {
    tauri::async_runtime::spawn(async move {
        let mut failed: HashMap<u64, Instant> = HashMap::new();
        let mut pending: Vec<SchemaResolvedPatch> = Vec::with_capacity(FLUSH_LEN);
        let mut flush = tokio::time::interval(FLUSH_INTERVAL);
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                msg = rx.recv() => {
                    let Some((message_id, schema_ref)) = msg else {
                        if !pending.is_empty() {
                            let _ = app.emit("kapture:message-schema-resolved", &pending);
                        }
                        break;
                    };
                    handle_one(&client, &buffer, &mut failed, &mut pending, message_id, schema_ref).await;
                    if pending.len() >= FLUSH_LEN {
                        let _ = app.emit("kapture:message-schema-resolved", &pending);
                        pending.clear();
                    }
                }
                _ = flush.tick() => {
                    if !pending.is_empty() {
                        let _ = app.emit("kapture:message-schema-resolved", &pending);
                        pending.clear();
                    }
                }
            }
        }
    });
}

async fn handle_one(
    client: &Arc<SchemaRegistryClient>,
    buffer: &Arc<RingBuffer>,
    failed: &mut HashMap<u64, Instant>,
    pending: &mut Vec<SchemaResolvedPatch>,
    message_id: String,
    schema_ref: SchemaRef,
) {
    let fail_key = schema_ref.cache_key();
    if let Some(t) = failed.get(&fail_key) {
        if t.elapsed() < FAIL_TTL {
            push_unresolved(buffer, pending, message_id);
            return;
        }
        failed.remove(&fail_key);
    }
    let result = match &schema_ref {
        SchemaRef::Id(id) => client.fetch(*id).await,
        SchemaRef::Guid(g) => client.fetch_by_guid(g).await,
    };
    match result {
        Ok(resolved) => {
            let name = resolved.subject.clone();
            let kind = resolved.kind.label().to_owned();
            // Smart-lazy: only patch the schema metadata (name +
            // kind) eagerly. Decoding the value bytes happens on
            // selection (see `decode_on_inspect`) so we avoid the
            // parse + decode cost on records that never get
            // inspected — common at >1k msg/s.
            if buffer.update_message_with(&message_id, |m| {
                m.schema_name.clone_from(&name);
                m.schema_kind = Some(kind.clone());
            }) {
                pending.push(SchemaResolvedPatch {
                    id: message_id,
                    schema_name: name,
                    schema_kind: Some(kind),
                    payload: None,
                });
            }
        }
        Err(err) => {
            tracing::debug!(?schema_ref, error = %err, "schema-registry fetch failed");
            failed.insert(fail_key, Instant::now());
            push_unresolved(buffer, pending, message_id);
        }
    }
}

/// Smart-lazy decode path: called by `inspect_message_by_id` when
/// the user selects a row. If the record carries a Confluent
/// envelope and the session has a registry client, fetch the schema
/// (LRU-cached → typically zero HTTP) and decode the payload bytes.
/// The decoded tree is written back into the ring buffer so a
/// re-inspect short-circuits.
///
/// Eager decode (every captured record) was the previous design; the
/// switch to lazy avoids paying parse + decode cost on the >99% of
/// records that never get inspected. Filter DSL targeting `payload.*`
/// continues to see the raw-bytes view on un-inspected records — a
/// rare-enough case to accept the trade-off.
pub async fn decode_on_inspect(
    state: &AppState,
    message: Option<CapturedMessage>,
) -> Option<CapturedMessage> {
    let mut message = message?;
    if !matches!(message.payload, DecodedValue::Bytes { .. }) {
        // Already decoded by an earlier inspect — nothing to do.
        return Some(message);
    }
    let Some(client) = state.schema_registry() else {
        return Some(message);
    };
    // Header path takes precedence: it's the newer Confluent
    // format (CP 8.1.1+). Both paths cannot coexist in a single
    // record per the producer contract.
    let (resolved, has_envelope) = if let Some(guid) = message.schema_guid.as_deref() {
        (client.fetch_by_guid(guid).await.ok()?, false)
    } else if let Some(id) = message.schema_id {
        (client.fetch(id).await.ok()?, true)
    } else {
        return Some(message);
    };
    let decoded = decode_with_schema(&resolved, &message.raw_hex, has_envelope)?;
    state.buffer.update_message_with(&message.id, |stored| {
        stored.payload = decoded.clone();
    });
    message.payload = decoded;
    Some(message)
}

/// Decode the value bytes against the resolved schema. Strips the
/// 5-byte Confluent envelope on the legacy id path
/// (`has_envelope = true`); the header-GUID path leaves the value
/// untouched (the schema reference lives on a Kafka header, not on
/// the value prefix).
///
/// Returns `None` on hex-parse failure, schema-parse failure, or an
/// unsupported kind (Protobuf — we'd need the descriptor compiled
/// from the .proto, not just the registry's text).
fn decode_with_schema(
    resolved: &ResolvedSchema,
    raw_hex: &str,
    has_envelope: bool,
) -> Option<DecodedValue> {
    let bytes = parse_render_hex(raw_hex)?;
    let body: &[u8] = if has_envelope {
        if bytes.len() <= 5 {
            return None;
        }
        &bytes[5..]
    } else {
        &bytes
    };
    match resolved.kind {
        SchemaKind::Avro => {
            let schema = avro::parse_schema(&resolved.raw).ok()?;
            avro::decode(&schema, body).ok()
        }
        SchemaKind::JsonSchema => {
            // The body is plain JSON for JSON-Schema-encoded records;
            // `decode_payload` already handles the JSON-or-bytes
            // dispatch, including nested objects / arrays.
            Some(decode_payload(Some(body)))
        }
        SchemaKind::Protobuf => None,
    }
}

/// Parse the space-separated hex stored on `CapturedMessage.raw_hex`
/// (output of `decode::render_hex`) back into raw bytes.
fn parse_render_hex(s: &str) -> Option<Vec<u8>> {
    let cleaned: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    hex::decode(&cleaned).ok()
}

fn push_unresolved(
    buffer: &Arc<RingBuffer>,
    pending: &mut Vec<SchemaResolvedPatch>,
    message_id: String,
) {
    if buffer.update_message_with(&message_id, |m| {
        m.schema_kind = Some(UNRESOLVED_KIND.to_owned());
    }) {
        pending.push(SchemaResolvedPatch {
            id: message_id,
            schema_name: None,
            schema_kind: Some(UNRESOLVED_KIND.to_owned()),
            payload: None,
        });
    }
}
