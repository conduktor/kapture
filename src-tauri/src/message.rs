use schemars::JsonSchema;
use serde::Serialize;

use crate::correlator::FetchMetadata;
use crate::decode::DecodedValue;

#[derive(Debug, Serialize, Clone, JsonSchema)]
pub struct KafkaHeader {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CapturedMessage {
    pub id: String,
    /// ISO-8601 timestamp.
    pub timestamp: String,
    pub topic: String,
    /// KIP-516 topic UUID. `None` on legacy wire formats (Produce/Fetch
    /// v0-12) or when the source frame did not carry one.
    pub topic_id: Option<String>,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub schema_name: Option<String>,
    pub schema_id: Option<u32>,
    /// Confluent schema kind label ("AVRO" / "JSON" / "PROTOBUF").
    /// `None` until the resolver task has answered (or when no
    /// registry is configured).
    pub schema_kind: Option<String>,
    /// Total of `key_size` + `value_size`. The label "size" historically
    /// meant just the value bytes; we keep the field name for backward
    /// compatibility with downstream consumers (filter DSL, MCP) but
    /// redefine it here to "what the user thinks of as message size" =
    /// key + value. Wire framing (varints, attrs, header k/v lengths)
    /// is not included — useful but rarely actionable in debug.
    pub size_bytes: usize,
    /// Raw bytes in the record key (0 when null/absent).
    pub key_size: usize,
    /// Raw bytes in the record value (0 when null/absent).
    pub value_size: usize,
    pub headers: Vec<KafkaHeader>,
    pub payload: DecodedValue,
    /// Raw bytes rendered as space-separated hex.
    pub raw_hex: String,
    /// Approximate Fetch response that brought this message — populated
    /// when the consumer was created against the Kapture-patched
    /// librdkafka and the proto correlator is wired in.
    pub fetch: Option<FetchMetadata>,
    /// Opaque identifier for the protocol channel that carried this
    /// record. In proxy mode it's the per-TCP-connection id (truncated
    /// to i32). In client (rdkafka) mode it's the librdkafka `broker_id`
    /// resolved via the `FetchMetadata` correlator. `None` when neither
    /// path could supply a value.
    pub connection_id: Option<i32>,
}

/// Lightweight projection of `CapturedMessage` that goes over the live
/// `kapture:messages` event. Strips `payload`, `raw_hex`, and
/// `headers` — those carry the bulk of the bytes and the UI doesn't
/// need them to render the list row. The full message is fetched on
/// demand via `inspect_message_by_id` when the user selects a row, so
/// the live IPC path stays small even at 10 k+ msg/s.
///
/// Measured: a 4 KiB wire payload produces a 41 KiB `CapturedMessage`
/// JSON; the same as a summary lands at ~500 B. ~80× reduction.
#[derive(Debug, Serialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MessageSummary {
    pub id: String,
    pub timestamp: String,
    pub topic: String,
    pub topic_id: Option<String>,
    pub partition: i32,
    pub offset: i64,
    /// Stringified key, truncated to a UI-friendly preview length.
    /// Full key is on the `CapturedMessage` (`inspect_message_by_id`).
    pub key: Option<String>,
    pub schema_name: Option<String>,
    pub schema_id: Option<u32>,
    pub schema_kind: Option<String>,
    pub size_bytes: usize,
    pub key_size: usize,
    pub value_size: usize,
    /// Number of headers; the keys + values come back via inspect.
    pub headers_count: usize,
    pub fetch: Option<FetchMetadata>,
    pub connection_id: Option<i32>,
}

impl MessageSummary {
    /// UI-friendly cap on the key preview. Long keys (UUIDs are fine
    /// at 36; binary keys can be much longer) are trimmed; the full
    /// value is one `inspect_message_by_id` away.
    const KEY_PREVIEW_MAX: usize = 128;

    #[must_use]
    pub fn from_full(m: &CapturedMessage) -> Self {
        let key = m.key.as_ref().map(|k| {
            if k.len() <= Self::KEY_PREVIEW_MAX {
                k.clone()
            } else {
                let mut cut = Self::KEY_PREVIEW_MAX;
                while cut > 0 && !k.is_char_boundary(cut) {
                    cut -= 1;
                }
                format!("{}…", &k[..cut])
            }
        });
        Self {
            id: m.id.clone(),
            timestamp: m.timestamp.clone(),
            topic: m.topic.clone(),
            topic_id: m.topic_id.clone(),
            partition: m.partition,
            offset: m.offset,
            key,
            schema_name: m.schema_name.clone(),
            schema_id: m.schema_id,
            schema_kind: m.schema_kind.clone(),
            size_bytes: m.size_bytes,
            key_size: m.key_size,
            value_size: m.value_size,
            headers_count: m.headers.len(),
            fetch: m.fetch.clone(),
            connection_id: m.connection_id,
        }
    }
}
