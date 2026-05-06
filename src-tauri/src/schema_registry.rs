//! Confluent-style Schema Registry client.
//!
//! Caches resolved schemas in an LRU keyed by id. The registry's HTTP API
//! is `GET /schemas/ids/{id}` returning `{ schema, schemaType?, subject? }`.
//! Older registries omit `schemaType`; the protocol convention is that a
//! missing field means AVRO.
//!
//! Currently unused at runtime — client (rdkafka) capture mode owned the
//! only call site. Kept for proxy-mode wiring (the proxy could decode
//! captured records' Confluent envelopes here once we expose the SR
//! URL through `start_proxy`).
#![allow(dead_code)]

use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;
use reqwest::Client;
use serde::Deserialize;
use thiserror::Error;
use tracing::{debug, warn};

const CACHE_CAPACITY: usize = 1024;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("schema registry HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("schema registry returned status {status} for id {id}: {body}")]
    BadStatus { id: u32, status: u16, body: String },

    #[error("schema registry response missing fields")]
    Malformed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaKind {
    Avro,
    JsonSchema,
    Protobuf,
}

impl SchemaKind {
    fn from_label(label: Option<&str>) -> Self {
        match label {
            Some("JSON") => Self::JsonSchema,
            Some("PROTOBUF") => Self::Protobuf,
            // Confluent SR omits schemaType when AVRO; treat None as AVRO.
            _ => Self::Avro,
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::JsonSchema => "JSON",
            Self::Protobuf => "PROTOBUF",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    /// Schema identifier as registered in the registry. Kept for callers
    /// that want to surface it in logs / responses even if not read here.
    #[allow(dead_code)]
    pub id: u32,
    pub kind: SchemaKind,
    /// Avro JSON, JSON Schema document, or proto file descriptor (raw text).
    pub raw: String,
    pub subject: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawResponse {
    schema: Option<String>,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    subject: Option<String>,
}

#[derive(Debug)]
pub struct SchemaRegistryClient {
    base_url: String,
    http: Client,
    cache: Mutex<LruCache<u32, Arc<ResolvedSchema>>>,
}

impl SchemaRegistryClient {
    /// Build a client. The base URL is normalised by stripping trailing slashes.
    pub fn new(base_url: String) -> Self {
        let trimmed = base_url.trim_end_matches('/').to_owned();
        Self {
            base_url: trimmed,
            http: Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(CACHE_CAPACITY).unwrap_or(NonZeroUsize::MIN),
            )),
        }
    }

    /// Look up a schema by id. Hits the LRU cache before going to the network.
    pub async fn fetch(&self, id: u32) -> Result<Arc<ResolvedSchema>, RegistryError> {
        // Bind to a local so the parking_lot guard drops before any await.
        let cached = self.cache.lock().get(&id).cloned();
        if let Some(cached) = cached {
            return Ok(cached);
        }
        let url = format!("{}/schemas/ids/{id}", self.base_url);
        debug!(id, %url, "fetching schema");
        let response = self.http.get(&url).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            warn!(id, status = %status, "schema registry refused lookup");
            return Err(RegistryError::BadStatus {
                id,
                status: status.as_u16(),
                body,
            });
        }
        let parsed: RawResponse = response.json().await?;
        let raw = parsed.schema.ok_or(RegistryError::Malformed)?;
        let resolved = Arc::new(ResolvedSchema {
            id,
            kind: SchemaKind::from_label(parsed.schema_type.as_deref()),
            raw,
            subject: parsed.subject,
        });
        self.cache.lock().put(id, Arc::clone(&resolved));
        Ok(resolved)
    }
}

/// Confluent envelope: `0x00 | u32_be schema_id | payload bytes`.
#[derive(Debug, Clone, Copy)]
pub struct ConfluentEnvelope<'a> {
    pub schema_id: u32,
    pub payload: &'a [u8],
}

impl<'a> ConfluentEnvelope<'a> {
    /// Detect a Confluent magic-byte envelope. Returns `None` if the bytes
    /// don't start with `0x00` or are too short.
    pub const fn try_parse(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() < 5 || bytes[0] != 0x00 {
            return None;
        }
        let id_bytes = [bytes[1], bytes[2], bytes[3], bytes[4]];
        Some(Self {
            schema_id: u32::from_be_bytes(id_bytes),
            payload: bytes.split_at(5).1,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_envelope() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x42, 0xab, 0xcd];
        let env = ConfluentEnvelope::try_parse(&bytes).unwrap();
        assert_eq!(env.schema_id, 0x42);
        assert_eq!(env.payload, &[0xab, 0xcd]);
    }

    #[test]
    fn rejects_non_magic_byte() {
        assert!(ConfluentEnvelope::try_parse(&[0x01, 0x00, 0x00, 0x00, 0x01]).is_none());
        assert!(ConfluentEnvelope::try_parse(&[0x00, 0x00]).is_none());
        assert!(ConfluentEnvelope::try_parse(&[]).is_none());
    }

    #[test]
    fn schema_kind_from_label() {
        assert_eq!(SchemaKind::from_label(None), SchemaKind::Avro);
        assert_eq!(SchemaKind::from_label(Some("AVRO")), SchemaKind::Avro);
        assert_eq!(SchemaKind::from_label(Some("JSON")), SchemaKind::JsonSchema);
        assert_eq!(
            SchemaKind::from_label(Some("PROTOBUF")),
            SchemaKind::Protobuf
        );
        assert_eq!(SchemaKind::from_label(Some("FOO")), SchemaKind::Avro);
    }
}
