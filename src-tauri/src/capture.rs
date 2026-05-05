use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Headers;
use rdkafka::Message;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::avro;
use crate::decode::{decode_payload, render_hex, DecodedValue};
use crate::error::{KaptureError, Result};
use crate::message::{CapturedMessage, KafkaHeader};
use crate::schema_registry::{ConfluentEnvelope, ResolvedSchema, SchemaKind, SchemaRegistryClient};

/// Default poll interval. Kafka consumers naturally batch via librdkafka.
const POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// A running capture, owning the consumer task and its stop signal.
#[derive(Debug)]
pub struct CaptureHandle {
    stop_tx: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl CaptureHandle {
    pub async fn stop(mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

/// Configuration for a capture session.
#[derive(Debug, Clone)]
pub struct CaptureConfig {
    pub bootstrap_servers: String,
    pub topics: Vec<String>,
    pub group_id: String,
    pub from_beginning: bool,
}

impl CaptureConfig {
    pub fn new(bootstrap_servers: String, topics: Vec<String>, from_beginning: bool) -> Self {
        Self {
            bootstrap_servers,
            topics,
            group_id: format!("kapture-{}", Uuid::new_v4().simple()),
            from_beginning,
        }
    }
}

/// Spawn a capture task. Each delivered message is decoded (with optional
/// schema-registry resolution) and then handed to `on_message`.
pub fn start<F>(
    config: CaptureConfig,
    sr_client: Option<Arc<SchemaRegistryClient>>,
    on_message: F,
) -> Result<CaptureHandle>
where
    F: Fn(CapturedMessage) + Send + Sync + 'static,
{
    if config.topics.is_empty() {
        return Err(KaptureError::Config(
            "at least one topic is required".to_owned(),
        ));
    }

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false")
        .set(
            "auto.offset.reset",
            if config.from_beginning {
                "earliest"
            } else {
                "latest"
            },
        )
        .set("session.timeout.ms", "10000")
        .set("fetch.min.bytes", "1")
        .set("client.id", "kapture-inspector");

    let consumer: StreamConsumer = client_config.create()?;
    let topic_refs: Vec<&str> = config.topics.iter().map(String::as_str).collect();
    consumer.subscribe(&topic_refs)?;

    let (stop_tx, mut stop_rx) = watch::channel(false);
    let on_message = Arc::new(on_message);

    let task = tokio::spawn(async move {
        info!(
            topics = ?config.topics,
            bootstrap = %config.bootstrap_servers,
            sr = sr_client.is_some(),
            "capture task started"
        );
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_ok() && *stop_rx.borrow() {
                        info!("capture task stopping");
                        break;
                    }
                }
                received = tokio::time::timeout(POLL_TIMEOUT, consumer.recv()) => {
                    match received {
                        Ok(Ok(msg)) => {
                            let captured = to_captured(&msg, sr_client.as_deref()).await;
                            on_message(captured);
                        }
                        Ok(Err(err)) => {
                            warn!(error = %err, "kafka consumer error");
                        }
                        Err(_) => {
                            // poll timeout, just loop
                        }
                    }
                }
            }
        }
    });

    Ok(CaptureHandle {
        stop_tx,
        task: Some(task),
    })
}

async fn to_captured<M: Message + Sync>(
    msg: &M,
    sr_client: Option<&SchemaRegistryClient>,
) -> CapturedMessage {
    let payload = msg.payload();
    let key = msg
        .key()
        .and_then(|bytes| std::str::from_utf8(bytes).ok().map(ToOwned::to_owned));
    let raw_hex = payload.map_or_else(String::new, render_hex);
    let size_bytes = payload.map_or(0, <[u8]>::len);
    let timestamp = msg.timestamp().to_millis().map_or_else(
        || Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        |millis| {
            Utc.timestamp_millis_opt(millis).single().map_or_else(
                || String::from("invalid"),
                |dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            )
        },
    );

    let headers = msg.headers().map_or_else(Vec::new, |hs| {
        let mut out = Vec::with_capacity(hs.count());
        for header in hs.iter() {
            let value = header
                .value
                .and_then(|bytes| std::str::from_utf8(bytes).ok().map(ToOwned::to_owned))
                .unwrap_or_default();
            out.push(KafkaHeader {
                key: header.key.to_owned(),
                value,
            });
        }
        out
    });

    let DecodedPayload {
        value: decoded,
        schema_name,
        schema_id,
    } = decode_with_registry(payload, sr_client).await;

    CapturedMessage {
        id: Uuid::new_v4().to_string(),
        timestamp,
        topic: msg.topic().to_owned(),
        partition: msg.partition(),
        offset: msg.offset(),
        key,
        schema_name,
        schema_id,
        size_bytes,
        headers,
        payload: decoded,
        raw_hex,
    }
}

struct DecodedPayload {
    value: DecodedValue,
    schema_name: Option<String>,
    schema_id: Option<u32>,
}

/// Decode payload bytes. When a schema-registry client is configured and
/// the payload starts with the Confluent magic byte, we resolve the schema
/// and decode by kind (Avro, JSON Schema). On any registry / decode failure,
/// we degrade gracefully to the raw `decode_payload` heuristic so capture
/// never blocks on registry availability.
async fn decode_with_registry(
    payload: Option<&[u8]>,
    sr_client: Option<&SchemaRegistryClient>,
) -> DecodedPayload {
    let Some(bytes) = payload else {
        return DecodedPayload {
            value: decode_payload(None),
            schema_name: None,
            schema_id: None,
        };
    };
    let Some(client) = sr_client else {
        return DecodedPayload {
            value: decode_payload(Some(bytes)),
            schema_name: None,
            schema_id: None,
        };
    };
    let Some(envelope) = ConfluentEnvelope::try_parse(bytes) else {
        return DecodedPayload {
            value: decode_payload(Some(bytes)),
            schema_name: None,
            schema_id: None,
        };
    };
    match client.fetch(envelope.schema_id).await {
        Ok(schema) => DecodedPayload {
            value: decode_with_schema(&schema, envelope.payload),
            schema_name: Some(friendly_schema_name(&schema)),
            schema_id: Some(envelope.schema_id),
        },
        Err(err) => {
            warn!(id = envelope.schema_id, error = %err, "schema registry lookup failed");
            DecodedPayload {
                value: decode_payload(Some(bytes)),
                schema_name: None,
                schema_id: Some(envelope.schema_id),
            }
        }
    }
}

fn friendly_schema_name(schema: &ResolvedSchema) -> String {
    schema.subject.as_ref().map_or_else(
        || schema.kind.label().to_owned(),
        |subject| format!("{subject} ({})", schema.kind.label()),
    )
}

fn decode_with_schema(schema: &ResolvedSchema, payload: &[u8]) -> DecodedValue {
    match schema.kind {
        SchemaKind::Avro => decode_avro(&schema.raw, payload),
        SchemaKind::JsonSchema => decode_payload(Some(payload)),
        SchemaKind::Protobuf => {
            // Protobuf decoding requires the FileDescriptor; deferred to a
            // later milestone. Keep the raw bytes visible.
            debug!("protobuf decode not implemented; falling back to bytes");
            DecodedValue::Bytes {
                hex: render_hex(payload),
                length: payload.len(),
            }
        }
    }
}

fn decode_avro(schema_text: &str, payload: &[u8]) -> DecodedValue {
    match avro::parse_schema(schema_text) {
        Ok(schema) => match avro::decode(&schema, payload) {
            Ok(decoded) => decoded,
            Err(err) => {
                warn!(error = %err, "avro decode failed; falling back to bytes");
                DecodedValue::Bytes {
                    hex: render_hex(payload),
                    length: payload.len(),
                }
            }
        },
        Err(err) => {
            warn!(error = %err, "avro schema parse failed");
            DecodedValue::Bytes {
                hex: render_hex(payload),
                length: payload.len(),
            }
        }
    }
}
