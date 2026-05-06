// `unsafe` is needed in this module to install the FFI proto hook.
#![allow(unsafe_code)]

use std::ffi::c_void;
use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::message::Headers;
use rdkafka::Message;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::avro;
use crate::correlator::ProtoCorrelator;
use crate::decode::{decode_payload, render_hex, DecodedValue};
use crate::error::{KaptureError, Result};
use crate::message::{CapturedMessage, KafkaHeader};
use crate::proto_hook::ProtoHookHandle;
use crate::schema_registry::{ConfluentEnvelope, ResolvedSchema, SchemaKind, SchemaRegistryClient};

/// Default poll interval. Kafka consumers naturally batch via librdkafka.
const POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// A running capture, owning the consumer task and its stop signal.
pub struct CaptureHandle {
    stop_tx: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
    /// Detached on Drop, must outlive the consumer task.
    proto_hook: Option<ProtoHookHandle>,
}

impl std::fmt::Debug for CaptureHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `stop_tx` is omitted as it is purely a control channel.
        f.debug_struct("CaptureHandle")
            .field("task", &self.task.is_some())
            .field("proto_hook", &self.proto_hook.is_some())
            .finish_non_exhaustive()
    }
}

impl CaptureHandle {
    pub async fn stop(mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        // Drop the hook *after* the consumer task has stopped polling.
        // The C callback is detached here; the boxed state is leaked
        // intentionally to avoid a use-after-free against in-flight
        // broker-thread invocations (see `proto_hook::HookState`).
        self.proto_hook = None;
    }
}

impl Drop for CaptureHandle {
    fn drop(&mut self) {
        // Best-effort teardown for callers that don't await `stop()`:
        //   - signal the consumer task to exit at its next iteration,
        //   - abort the task so we don't keep a detached future running
        //     forever if the watch signal hasn't been observed yet,
        //   - the `proto_hook` field then drops, detaching the C
        //     callback (state intentionally leaked, see
        //     `proto_hook::HookState`).
        // Any in-flight broker-thread hook call sees a still-valid
        // opaque pointer and updates the (possibly already-dropped)
        // `Weak<ProtoCorrelator>`, which no-ops on upgrade failure.
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Default topic pattern: every topic whose name does not begin with an
/// underscore. Skips `__consumer_offsets`, `_schemas`, `__transaction_state`,
/// and similar broker-internal topics.
pub const DEFAULT_TOPIC_PATTERN: &str = "^[^_].*";

/// Configuration for a capture session.
#[derive(Debug, Clone)]
pub struct CaptureConfig {
    pub bootstrap_servers: String,
    /// Topic regex passed verbatim to librdkafka's pattern subscribe. MUST
    /// start with `^` so librdkafka recognises it as a regex. An empty /
    /// missing pattern from the GUI is normalised to `DEFAULT_TOPIC_PATTERN`
    /// at the command boundary, so this field is always populated.
    pub topic_pattern: String,
    pub group_id: String,
    pub from_beginning: bool,
    pub auth: Option<AuthConfig>,
}

/// Supported SASL mechanisms. Built into librdkafka; no Cyrus SASL
/// (no GSSAPI/Kerberos) at the moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslMechanism {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

#[derive(Clone)]
pub struct AuthConfig {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
    /// True when the broker requires TLS (`SASL_SSL`); false for the
    /// plaintext SASL transport (`SASL_PLAINTEXT`).
    pub use_tls: bool,
    /// Optional TLS / mTLS material. Only meaningful when `use_tls`
    /// is true. Any subset of fields can be set:
    /// `ca_path` overrides the system trust store, the cert+key pair
    /// enables mutual auth, `key_password` decrypts the key file.
    pub tls: Option<TlsCreds>,
}

impl std::fmt::Debug for AuthConfig {
    /// Custom redaction so `Debug` (e.g. via `{:?}` in error
    /// contexts or future tracing instrumentation) cannot leak the
    /// SASL password.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("use_tls", &self.use_tls)
            .field("tls", &self.tls)
            .finish()
    }
}

/// TLS material used when `use_tls` is on. Each field is forwarded
/// to librdkafka as `ssl.ca.location` / `ssl.certificate.location`
/// / `ssl.key.location` / `ssl.key.password`.
#[derive(Clone)]
pub struct TlsCreds {
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub key_password: Option<String>,
}

impl TlsCreds {
    /// Validate every cert / key path: canonicalise, check the path
    /// resolves and points at a regular file. Same defence-in-depth
    /// as the Tauri command path; reused by the MCP connect-by-profile
    /// flow so an agent cannot ask librdkafka to open `/etc/shadow`.
    ///
    /// # Errors
    ///
    /// Returns a human-readable description of the offending field /
    /// path on the first failure.
    pub fn validate_paths(&self) -> std::result::Result<(), String> {
        check_tls_path("tls.caPath", self.ca_path.as_deref())?;
        check_tls_path("tls.certPath", self.cert_path.as_deref())?;
        check_tls_path("tls.keyPath", self.key_path.as_deref())?;
        if self.key_password.is_some() && self.key_path.is_none() {
            return Err("tls.keyPassword set without tls.keyPath".to_owned());
        }
        if self.cert_path.is_some() && self.key_path.is_none() {
            return Err("tls.certPath set without tls.keyPath".to_owned());
        }
        if self.key_path.is_some() && self.cert_path.is_none() {
            return Err("tls.keyPath set without tls.certPath".to_owned());
        }
        Ok(())
    }
}

fn check_tls_path(field: &str, path: Option<&str>) -> std::result::Result<(), String> {
    let Some(p) = path else {
        return Ok(());
    };
    let canonical =
        std::fs::canonicalize(p).map_err(|err| format!("{field}: cannot resolve `{p}`: {err}"))?;
    let meta = std::fs::metadata(&canonical)
        .map_err(|err| format!("{field}: stat `{p}` failed: {err}"))?;
    if !meta.is_file() {
        return Err(format!("{field}: `{p}` is not a regular file"));
    }
    Ok(())
}

impl std::fmt::Debug for TlsCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsCreds")
            .field("ca_path", &self.ca_path)
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .field(
                "key_password",
                &self.key_password.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl CaptureConfig {
    pub fn new(
        bootstrap_servers: String,
        topic_pattern: String,
        from_beginning: bool,
        auth: Option<AuthConfig>,
    ) -> Self {
        Self {
            bootstrap_servers,
            topic_pattern,
            group_id: format!("kapture-{}", Uuid::new_v4().simple()),
            from_beginning,
            auth,
        }
    }
}

/// Build the rdkafka `ClientConfig` for both live captures and the connection
/// test. Pulled out into a free function so the test-connection path goes
/// through exactly the same SASL / TLS plumbing as `start` — otherwise a
/// successful test could lull the user into trying a connect that fails for
/// auth reasons that the test never exercised.
fn build_client_config(config: &CaptureConfig) -> ClientConfig {
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

    if let Some(auth) = &config.auth {
        let security_protocol = if auth.use_tls {
            "SASL_SSL"
        } else {
            "SASL_PLAINTEXT"
        };
        client_config
            .set("security.protocol", security_protocol)
            .set("sasl.mechanism", auth.mechanism.label())
            .set("sasl.username", &auth.username)
            .set("sasl.password", &auth.password);
        if let Some(tls) = &auth.tls {
            if let Some(p) = &tls.ca_path {
                client_config.set("ssl.ca.location", p);
            }
            if let Some(p) = &tls.cert_path {
                client_config.set("ssl.certificate.location", p);
            }
            if let Some(p) = &tls.key_path {
                client_config.set("ssl.key.location", p);
            }
            if let Some(secret) = &tls.key_password {
                client_config.set("ssl.key.password", secret);
            }
        }
    }

    client_config
}

/// Try a metadata fetch against the cluster without starting a capture. Used
/// by the GUI's "Test connection" button. Returns the broker count + topic
/// count on success, or a human-readable error.
///
/// Why a separate path: `start` returns a `StreamConsumer` that runs forever
/// until dropped. Here we want a single bounded operation that holds no
/// resources and exits within `timeout`. We use `BaseConsumer::fetch_metadata`,
/// which is what librdkafka offers for a one-shot probe.
pub fn test_connection(config: &CaptureConfig, timeout: Duration) -> Result<TestConnectionReport> {
    let client_config = build_client_config(config);
    let consumer: BaseConsumer = client_config.create()?;
    let metadata = consumer.fetch_metadata(None, timeout)?;
    Ok(TestConnectionReport {
        brokers: metadata.brokers().len(),
        topics: metadata.topics().len(),
    })
}

/// Result payload of `test_connection`. Kept tight: surfacing topic names
/// here would leak schema information to a caller that hasn't yet committed
/// to the connection.
#[derive(Debug, Clone, Copy)]
pub struct TestConnectionReport {
    pub brokers: usize,
    pub topics: usize,
}

/// Normalise a user-supplied topic pattern: strip surrounding whitespace,
/// substitute the default if the result is empty, and prepend `^` if the
/// caller forgot it (librdkafka requires it to recognise regex syntax).
#[must_use]
pub fn normalise_topic_pattern(input: Option<&str>) -> String {
    let trimmed = input.map_or("", str::trim);
    if trimmed.is_empty() {
        return DEFAULT_TOPIC_PATTERN.to_owned();
    }
    if trimmed.starts_with('^') {
        trimmed.to_owned()
    } else {
        format!("^{trimmed}")
    }
}

/// Spawn a capture task. Each delivered message is decoded (with optional
/// schema-registry resolution + proto correlation) and then handed to
/// `on_message`.
pub fn start<F>(
    config: CaptureConfig,
    sr_client: Option<Arc<SchemaRegistryClient>>,
    correlator: Arc<ProtoCorrelator>,
    on_message: F,
) -> Result<CaptureHandle>
where
    F: Fn(CapturedMessage) + Send + Sync + 'static,
{
    if config.topic_pattern.is_empty() {
        return Err(KaptureError::Config(
            "topic pattern must not be empty (use the default ^[^_].*)".to_owned(),
        ));
    }

    let client_config = build_client_config(&config);
    let consumer: StreamConsumer = client_config.create()?;
    // librdkafka's regex pattern subscribe — a leading `^` is the trigger.
    // The broker delivers every topic whose name matches; our default
    // `^[^_].*` excludes internal topics. The DSL filter on the UI side
    // narrows further at view time.
    consumer.subscribe(&[config.topic_pattern.as_str()])?;

    // Wire the proto hook before subscribing to avoid missing the very
    // first ApiVersions / Metadata exchanges. The hook updates the
    // correlator synchronously on the broker thread (see
    // `proto_hook::proto_hook_trampoline`), so by the time
    // `consumer.recv()` returns a message the correlator already
    // reflects the matching Fetch RECV.
    let rk_ptr: *mut c_void = consumer.client().native_ptr().cast();
    // SAFETY: `rk_ptr` is the live `rd_kafka_t` produced just above; it
    // lives for the whole consumer lifetime, which is strictly longer
    // than the `ProtoHookHandle` (we drop the handle in `stop()` before
    // the consumer is destroyed).
    let proto_hook = unsafe { ProtoHookHandle::install(rk_ptr, Arc::clone(&correlator)) };

    let (stop_tx, mut stop_rx) = watch::channel(false);
    let on_message = Arc::new(on_message);
    let correlator_for_task = Arc::clone(&correlator);

    let task = tokio::spawn(async move {
        info!(
            pattern = %config.topic_pattern,
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
                            let captured = to_captured(
                                &msg,
                                sr_client.as_deref(),
                                correlator_for_task.as_ref(),
                            )
                            .await;
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
        proto_hook: Some(proto_hook),
    })
}

async fn to_captured<M: Message + Sync>(
    msg: &M,
    sr_client: Option<&SchemaRegistryClient>,
    correlator: &ProtoCorrelator,
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

    let fetch = correlator.lookup(msg.topic(), msg.partition());

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
        fetch,
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
