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

use crate::ring_buffer::RingBuffer;
use crate::schema_registry::SchemaRegistryClient;

/// One row of `kapture:message-schema-resolved`. Sent batched in a
/// `Vec` so the UI gets one rAF-friendly emit per `FLUSH_INTERVAL`
/// (or sooner when the buffer fills) even when many records resolve
/// in tight succession.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaResolvedPatch {
    pub id: String,
    pub schema_name: Option<String>,
    pub schema_kind: Option<String>,
}

const FAIL_TTL: Duration = Duration::from_secs(300);
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const FLUSH_LEN: usize = 64;
const UNRESOLVED_KIND: &str = "UNRESOLVED";

pub fn spawn(
    client: Arc<SchemaRegistryClient>,
    buffer: Arc<RingBuffer>,
    app: AppHandle,
    mut rx: tokio::sync::mpsc::Receiver<(String, u32)>,
) {
    tauri::async_runtime::spawn(async move {
        let mut failed: HashMap<u32, Instant> = HashMap::new();
        let mut pending: Vec<SchemaResolvedPatch> = Vec::with_capacity(FLUSH_LEN);
        let mut flush = tokio::time::interval(FLUSH_INTERVAL);
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                msg = rx.recv() => {
                    let Some((message_id, schema_id)) = msg else {
                        if !pending.is_empty() {
                            let _ = app.emit("kapture:message-schema-resolved", &pending);
                        }
                        break;
                    };
                    handle_one(&client, &buffer, &mut failed, &mut pending, message_id, schema_id).await;
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
    failed: &mut HashMap<u32, Instant>,
    pending: &mut Vec<SchemaResolvedPatch>,
    message_id: String,
    schema_id: u32,
) {
    if let Some(t) = failed.get(&schema_id) {
        if t.elapsed() < FAIL_TTL {
            // Still patch with UNRESOLVED so the UI doesn't get
            // stuck on "resolving…" indefinitely.
            push_unresolved(buffer, pending, message_id);
            return;
        }
        failed.remove(&schema_id);
    }
    match client.fetch(schema_id).await {
        Ok(resolved) => {
            let name = resolved.subject.clone();
            let kind = resolved.kind.label().to_owned();
            if buffer.update_message_with(&message_id, |m| {
                m.schema_name.clone_from(&name);
                m.schema_kind = Some(kind.clone());
            }) {
                pending.push(SchemaResolvedPatch {
                    id: message_id,
                    schema_name: name,
                    schema_kind: Some(kind),
                });
            }
        }
        Err(err) => {
            tracing::debug!(schema_id, error = %err, "schema-registry fetch failed");
            failed.insert(schema_id, Instant::now());
            push_unresolved(buffer, pending, message_id);
        }
    }
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
        });
    }
}
