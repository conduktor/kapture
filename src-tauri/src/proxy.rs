//! Kapture proxy mode. A TCP intermediary that accepts Kafka client
//! connections, forwards every byte to a real upstream broker, and
//! taps each frame to the `ProtoCorrelator` so the Protocol tab shows
//! the wire-level traffic of the *client*, not of Kapture itself.
//! The listener fleet grows on demand
//! — the response rewriter calls `BrokerProvisioner::ensure` whenever
//! it sees a new broker in `Metadata` / `FindCoordinator` /
//! `DescribeCluster` responses, binding a local listener and spawning
//! its accept loop. Bootstrap broker is pre-seeded into `BrokerMap`
//! so its listener is reused on rewrite (no double-bind).

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
use bytes::Bytes;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::warn;

use crate::correlator::ProtoCorrelator;
use crate::proto_event::{ProtoDirection, ProtoEvent};
use crate::proxy_handle::RecordSink;
use crate::proxy_provisioner::BrokerProvisioner;
use crate::proxy_records::{
    extract_from_fetch_response_bytes, extract_produce_offsets_bytes,
    extract_produce_request_bytes, extracted_to_captured, ExtractedRecord,
};
use crate::proxy_redact::{redact_sasl_authenticate_body, API_KEY_SASL_AUTHENTICATE};
use crate::proxy_topic_ids::TopicIdMap;

/// Cap on `payload` we copy into the `ProtoEvent`. Bounds the
/// Protocol tab's hex view + decoded body across modes.
pub const PROTO_PAYLOAD_CAP: usize = 64 * 1024;

/// Max in-flight requests per TCP connection. Exceeding this closes
/// the offending proxy connection instead of growing the correlation
/// map unboundedly.
pub const MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: usize = 8192;
const MAX_PENDING_PRODUCE_RECORDS: usize = 100_000;
const MAX_PENDING_PRODUCE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// `host:port` of the upstream Kafka broker we forward to.
    pub upstream: String,
    /// TCP port we bind for clients.
    pub listen_port: u16,
    /// Bind IP. Default `127.0.0.1`. `0.0.0.0` only for bounded smokes.
    pub bind: IpAddr,
    /// Optional TLS config for the upstream connection. `None` means
    /// plaintext upstream. Same config is reused for every broker in
    /// the cluster (Kafka deployments share TLS server certs across
    /// brokers in normal setups).
    pub upstream_tls: Option<UpstreamTlsConfig>,
    /// Optional SASL credentials for the upstream connection. `None`
    /// means no SASL handshake. Same credentials are reused for every
    /// broker in the cluster.
    pub upstream_sasl: Option<UpstreamSaslConfig>,
}

impl ProxyConfig {
    #[must_use]
    pub const fn new(upstream: String, listen_port: u16) -> Self {
        Self {
            upstream,
            listen_port,
            bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
            upstream_tls: None,
            upstream_sasl: None,
        }
    }
    /// Builder-style: attach TLS config for the upstream.
    #[must_use]
    pub fn with_tls(mut self, tls: UpstreamTlsConfig) -> Self {
        self.upstream_tls = Some(tls);
        self
    }
    /// Builder-style: attach SASL credentials for the upstream.
    #[must_use]
    pub fn with_sasl(mut self, sasl: UpstreamSaslConfig) -> Self {
        self.upstream_sasl = Some(sasl);
        self
    }
    /// Builder-style: override the bind IP (default `127.0.0.1`).
    #[must_use]
    pub const fn with_bind(mut self, bind: IpAddr) -> Self {
        self.bind = bind;
        self
    }
    #[must_use]
    pub const fn listen_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind, self.listen_port)
    }
}

/// Wrap a `TcpStream` in the Kafka wire-frame codec: 4-byte big-endian
/// length prefix followed by `length` body bytes. The codec hands us
/// one `Bytes` per frame on the read side, and accepts a `Bytes` per
/// frame on the write side (it prepends the length itself).
///
/// Max frame is 100 MiB — Kafka's default `socket.request.max.bytes`
/// and the effective wire ceiling. Bounds memory against malicious
/// peers sending a 4 GiB `len` field.
pub fn framed_kafka<S: AsyncRead + AsyncWrite + Unpin>(
    socket: S,
) -> Framed<S, LengthDelimitedCodec> {
    let codec = LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .max_frame_length(100 * 1024 * 1024)
        .big_endian()
        .new_codec();
    Framed::new(socket, codec)
}

/// Minimum bytes needed to peek the (`api_key`, `api_version`, `corr_id`)
/// triple at the start of every Kafka request, regardless of header
/// version. The remainder of the header (`client_id`, tagged fields)
/// varies by version and we don't need it for routing / correlation.
const REQUEST_HEADER_PREFIX_LEN: usize = 8;

#[derive(Debug, Clone, Copy)]
pub struct RequestHeaderPeek {
    pub api_key: i16,
    pub api_version: i16,
    pub corr_id: i32,
}

/// Read the fixed-shape request header prefix without consuming the
/// buffer. Returns `None` if the buffer is too short.
#[must_use]
pub fn peek_request_header(frame: &[u8]) -> Option<RequestHeaderPeek> {
    if frame.len() < REQUEST_HEADER_PREFIX_LEN {
        return None;
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let corr_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    Some(RequestHeaderPeek {
        api_key,
        api_version,
        corr_id,
    })
}

/// One in-flight request awaiting its matching response on the same
/// TCP connection. The `sent_at` timestamp powers RTT measurement —
/// strictly per-connection, not per-broker, since `corr_id` uniqueness
/// is only guaranteed within one TCP connection (Kafka spec).
#[derive(Debug, Clone, Copy)]
pub struct PendingRequest {
    pub header: RequestHeaderPeek,
    pub sent_at: Instant,
    pub observed_nanos: Option<u64>,
}

impl PendingRequest {
    #[must_use]
    pub fn rtt_at(&self, now: Instant, observed_nanos: Option<u64>) -> f64 {
        if let (Some(request), Some(response)) = (self.observed_nanos, observed_nanos) {
            if response >= request {
                return (response - request) as f64 / 1_000_000.0;
            }
        }
        let elapsed = now.saturating_duration_since(self.sent_at);
        // ms with fractional precision, like the proto-hook path.
        elapsed.as_secs_f64() * 1000.0
    }
}

/// Per-connection map `corr_id → in-flight request`.
///
/// Bounded explicitly per TCP connection so a malicious local client
/// cannot send unlimited unique correlation IDs without reading
/// responses. If a connection drops mid-flight any leftovers are
/// released when the owning task exits and drops the map.
#[derive(Debug, Default)]
pub struct CorrelationMap {
    inner: Mutex<HashMap<i32, PendingRequest>>,
}

impl CorrelationMap {
    #[cfg(test)]
    pub fn record_request(&self, corr_id: i32, header: RequestHeaderPeek) -> io::Result<()> {
        self.record_request_at(corr_id, header, None)
    }

    pub fn record_request_at(
        &self,
        corr_id: i32,
        header: RequestHeaderPeek,
        observed_nanos: Option<u64>,
    ) -> io::Result<()> {
        let mut inner = self.inner.lock();
        if !inner.contains_key(&corr_id) && inner.len() >= MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION {
            return Err(io::Error::other(format!(
                "proxy correlation map limit reached ({MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION})"
            )));
        }
        inner.insert(
            corr_id,
            PendingRequest {
                header,
                sent_at: Instant::now(),
                observed_nanos,
            },
        );
        drop(inner);
        Ok(())
    }

    pub fn take_response(&self, corr_id: i32) -> Option<PendingRequest> {
        self.inner.lock().remove(&corr_id)
    }

    pub fn discard_request(&self, corr_id: i32) {
        self.inner.lock().remove(&corr_id);
    }
}

/// Monotonic, never-zero connection identifier. Used as the pairing
/// key for `(corr_id, connection_id)` in the inspector — same field
/// as the rdkafka-client mode's `connection_id` (which forwards the
/// librdkafka `broker_id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

/// Direction of a tapped frame, from the proxy's point of view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyDirection {
    /// Frame came in from the connecting Kafka client → going to upstream.
    ClientToUpstream,
    /// Frame came back from upstream → going to the connecting client.
    UpstreamToClient,
}

/// Atomic monotonic generator for `ConnectionId`. One global counter
/// is fine — these are session-scoped and never persisted.
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[must_use]
pub fn next_connection_id() -> ConnectionId {
    ConnectionId(NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed))
}

/// Drive both directions of one client/upstream pair. Returns when
/// either side closes its half. Errors short-circuit and propagate to
/// the caller — the spawn site logs and drops the pump.
///
/// `tap` is invoked synchronously per frame, *before* forwarding, so
/// the inspector observes frames in arrival order. The callback must
/// not block: in production it just pushes into the correlator's
/// ring-buffer mutex (~µs).
///
/// Test-only: production runs `run_pump_with_rewrite`, which also
/// drives `ProtoCorrelator` + per-API rewriting. This minimal pump is
/// retained as a wire-loop sanity fixture in `proxy_tests`.
#[cfg(test)]
pub async fn run_pump<U, F>(
    conn_id: ConnectionId,
    client: TcpStream,
    upstream: U,
    tap: F,
) -> io::Result<()>
where
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Fn(ProxyDirection, ConnectionId, &Bytes) + Send + Sync + 'static,
{
    let mut client_framed = framed_kafka(client);
    let mut upstream_framed = framed_kafka(upstream);

    loop {
        tokio::select! {
            // Client → upstream
            frame = client_framed.next() => {
                let Some(frame) = frame else { break; };
                let frame = frame?;
                let bytes = frame.freeze();
                tap(ProxyDirection::ClientToUpstream, conn_id, &bytes);
                upstream_framed.send(bytes).await?;
            }
            // Upstream → client
            frame = upstream_framed.next() => {
                let Some(frame) = frame else { break; };
                let frame = frame?;
                let bytes = frame.freeze();
                tap(ProxyDirection::UpstreamToClient, conn_id, &bytes);
                client_framed.send(bytes).await?;
            }
        }
    }
    Ok(())
}

/// Per-frame pump variant that records every event in the
/// `ProtoCorrelator` AND rewrites response payloads carrying broker /
/// coordinator addresses (`Metadata`, `FindCoordinator`,
/// `DescribeCluster`) so the client's follow-up connections come back
/// through Kapture's local listeners instead of bypassing us.
/// Correlator sees the **original** bytes (Wireshark-style); only
/// forwarded bytes are rewritten. On rewrite failure the original
/// frame is forwarded verbatim and logged at `warn!` — frames are
/// never silently dropped.
///
/// # Errors
/// Bubbles up `io::Error` from the underlying TCP read/write.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]
pub async fn run_pump_with_rewrite<U>(
    conn_id: ConnectionId,
    local_port: u16,
    mut client_framed: Framed<TcpStream, LengthDelimitedCodec>,
    upstream: U,
    correlator: Arc<ProtoCorrelator>,
    corr_map: Arc<CorrelationMap>,
    provisioner: Arc<dyn BrokerProvisioner>,
    record_sink: RecordSink,
    topic_ids: Arc<TopicIdMap>,
) -> io::Result<()>
where
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut upstream_framed = framed_kafka(upstream);

    // Per-session map of in-flight Produce requests keyed by corr_id.
    // The producer always wires `base_offset = 0` on outgoing records
    // (the broker assigns the real offset and replies in the
    // ProduceResponse). Defer emission of Produce-side records until
    // the matching response lands so the Messages tab shows the offset
    // the broker actually assigned, instead of a confusing 0.
    //
    // The bucket holds (record, partition, index_within_partition);
    // final offset = base_offset_for_(topic, partition) + index.
    //
    // `acks=0` is emitted immediately (offset unknown) and removed from
    // the correlation map because no response can arrive. Acked requests
    // are held as raw `Bytes`-backed records under explicit count + byte
    // budgets; expensive hex/JSON materialization waits for the response.
    let mut pending_produce: HashMap<i32, Vec<(ExtractedRecord, i32, usize)>> = HashMap::new();
    let mut pending_produce_records = 0usize;
    let mut pending_produce_bytes = 0usize;

    loop {
        tokio::select! {
            // Client → upstream
            frame = client_framed.next() => {
                let Some(frame) = frame else { break; };
                let frame = frame?;
                let bytes = frame.freeze();
                let event = build_proto_event(
                    ProxyDirection::ClientToUpstream,
                    conn_id,
                    local_port,
                    &bytes,
                    &corr_map,
                )?;
                let req_api_key = i16::try_from(event.api_key).unwrap_or(-1);
                let req_api_version = i16::try_from(event.api_version).unwrap_or(-1);
                let corr_id = event.corr_id;
                // Forward first. Analysis and record extraction are
                // observability work and must not inflate broker-facing
                // request latency.
                upstream_framed.send(bytes.clone()).await?;
                correlator.enqueue_event(event);
                if req_api_key == 0 {
                    // Produce request — extract records before forwarding.
                    // v13+ replaced the topic-name field with topic_id
                    // (KIP-516 phase 2), so the extractor needs the
                    // cluster-wide topic-id map to surface a name.
                    //
                    // Hold the records in `pending_produce` instead of
                    // emitting; the matching response side fills in the
                    // broker-assigned offset and pushes them downstream.
                    if let Some(request) =
                        extract_produce_request_bytes(req_api_version, bytes.clone(), &topic_ids)
                    {
                        let records_bytes = request.records.iter().fold(0usize, |total, record| {
                            total.saturating_add(record.retained_payload_bytes())
                        });
                        if request.acks == 0 {
                            corr_map.discard_request(corr_id);
                            for mut record in request.records {
                                record.offset = -1;
                                record_sink(extracted_to_captured(record, conn_id.0));
                            }
                            continue;
                        }
                        if pending_produce_records.saturating_add(request.records.len())
                            > MAX_PENDING_PRODUCE_RECORDS
                            || pending_produce_bytes.saturating_add(records_bytes)
                                > MAX_PENDING_PRODUCE_BYTES
                        {
                            correlator.record_extraction_drop(request.records.len());
                            continue;
                        }
                        let mut bucket: Vec<(ExtractedRecord, i32, usize)> =
                            Vec::with_capacity(request.records.len());
                        // Track per-partition counter so we can compute
                        // each record's index within its partition: final
                        // offset = base_offset(partition) + index.
                        let mut idx_per_partition: HashMap<i32, usize> = HashMap::new();
                        for rec in request.records {
                            let partition = rec.partition;
                            let idx = idx_per_partition.entry(partition).or_insert(0);
                            bucket.push((rec, partition, *idx));
                            *idx += 1;
                        }
                        pending_produce_records = pending_produce_records.saturating_add(bucket.len());
                        pending_produce_bytes = pending_produce_bytes.saturating_add(records_bytes);
                        if let Some(replaced) = pending_produce.insert(corr_id, bucket) {
                            pending_produce_records = pending_produce_records.saturating_sub(replaced.len());
                            pending_produce_bytes = pending_produce_bytes.saturating_sub(
                                replaced.iter().fold(0usize, |total, (record, _, _)| {
                                    total.saturating_add(record.retained_payload_bytes())
                                }),
                            );
                            correlator.record_extraction_drop(replaced.len());
                        }
                    }
                }
            }
            // Upstream → client (with rewrite)
            frame = upstream_framed.next() => {
                let Some(frame) = frame else { break; };
                let frame = frame?;
                let bytes = frame.freeze();
                let event = build_proto_event(
                    ProxyDirection::UpstreamToClient,
                    conn_id,
                    local_port,
                    &bytes,
                    &corr_map,
                )?;
                let api_key = i16::try_from(event.api_key).unwrap_or(-1);
                let api_version = i16::try_from(event.api_version).unwrap_or(-1);
                let corr_id = event.corr_id;
                let event_connection_id = event.connection_id;
                let forward = if api_key >= 0 {
                    match crate::proxy_rewrite::rewrite_response(
                        api_key,
                        api_version,
                        &bytes,
                        provisioner.as_ref(),
                        &topic_ids,
                    )
                    .await
                    {
                        Ok(Some(rewritten)) => rewritten,
                        Ok(None) => bytes.clone(),
                        Err(err) => {
                            warn!(error = %err, "rewrite failed; forwarding verbatim");
                            bytes.clone()
                        }
                    }
                } else {
                    bytes.clone()
                };
                client_framed.send(forward).await?;
                correlator.enqueue_event(event);
                if api_key == 0 {
                    // Produce response — back-fill broker-assigned
                    // offsets onto the records we held from the matching
                    // request, then emit. If no pending bucket exists
                    // (e.g. extraction failed earlier), this is a no-op.
                    if let Some(bucket) = pending_produce.remove(&corr_id) {
                        pending_produce_records = pending_produce_records.saturating_sub(bucket.len());
                        pending_produce_bytes = pending_produce_bytes.saturating_sub(
                            bucket.iter().fold(0usize, |total, (record, _, _)| {
                                total.saturating_add(record.retained_payload_bytes())
                            }),
                        );
                        let offsets =
                            extract_produce_offsets_bytes(api_version, bytes.clone(), &topic_ids)
                                .unwrap_or_default();
                        for (mut record, partition, idx) in bucket {
                            if let Some(&base) = offsets.get(&(record.topic.clone(), partition)) {
                                // base + idx fits in i64 for any realistic
                                // batch — Kafka offsets are i64 and the
                                // index is bounded by the request batch
                                // size (well below i64::MAX). try_from
                                // pacifies the (theoretical) usize→i64
                                // wrap on 64-bit targets without
                                // changing observable behaviour.
                                let idx_i64 = i64::try_from(idx).unwrap_or(i64::MAX);
                                record.offset = base.saturating_add(idx_i64);
                            }
                            record_sink(extracted_to_captured(record, conn_id.0));
                        }
                    }
                }
                if api_key == 1 {
                    // Fetch response — extract records before forwarding.
                    // `bytes` is the codec output (no wire size prefix);
                    // it starts at the ResponseHeader, which is what
                    // `extract_from_fetch_response` expects. Stamp the
                    // (corr_id, connection_id) of this very response
                    // frame onto each record so the Messages tab can
                    // jump back to it.
                    for rec in extract_from_fetch_response_bytes(
                        api_version,
                        bytes.clone(),
                        &topic_ids,
                        corr_id,
                        event_connection_id,
                    ).unwrap_or_default() {
                        record_sink(extracted_to_captured(rec, conn_id.0));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Build the `ProtoEvent` for one tapped frame. On the request path,
/// peek the header and stash it in `corr_map`; on the response path,
/// look up the matching request to recover `(api_key, api_version)`
/// and RTT. `payload` re-prepends the 4-byte BE size prefix so the
/// existing `proto_decode::decode_frame` parser keeps working
/// unchanged; `payload_size` is the WIRE size including that prefix.
/// `connection_id` is the proxy's per-TCP-connection id (u64
/// truncated to i32). `local_port` is the proxy listener port that
/// owned the pump that produced this frame — stamped on the event so
/// downstream views can aggregate per-broker without a connection→
/// listener side-table.
pub fn build_proto_event(
    dir: ProxyDirection,
    conn_id: ConnectionId,
    local_port: u16,
    frame: &[u8],
    corr_map: &CorrelationMap,
) -> io::Result<ProtoEvent> {
    build_proto_event_at(dir, conn_id, local_port, frame, corr_map, None, 0.0)
}

#[allow(clippy::too_many_arguments)]
pub fn build_proto_event_at(
    dir: ProxyDirection,
    conn_id: ConnectionId,
    local_port: u16,
    frame: &[u8],
    corr_map: &CorrelationMap,
    observed_nanos: Option<u64>,
    capture_lag_ms: f64,
) -> io::Result<ProtoEvent> {
    let body_len_i32 = i32::try_from(frame.len()).unwrap_or(i32::MAX);
    let payload_size = frame.len() + 4;
    let body_take = frame.len().min(PROTO_PAYLOAD_CAP - 4);
    let mut payload = Vec::with_capacity(body_take + 4);
    payload.extend_from_slice(&body_len_i32.to_be_bytes());
    payload.extend_from_slice(&frame[..body_take]);
    // Mask to the positive i32 range (0..=i32::MAX) so the truncation
    // is lossless within that range and `try_from` cannot fail. The
    // `unwrap_or(i32::MAX)` is unreachable but satisfies clippy's
    // `unwrap_used` deny without a panic path.
    let connection_id = i32::try_from(conn_id.0 & 0x7FFF_FFFF).unwrap_or(i32::MAX);

    let event = match dir {
        ProxyDirection::ClientToUpstream => {
            let header = peek_request_header(frame);
            if let Some(h) = header {
                corr_map.record_request_at(h.corr_id, h, observed_nanos)?;
            }
            let api_key_i32 = header.map_or(-1, |h| i32::from(h.api_key));
            // Redact SaslAuthenticate request bodies BEFORE we hand the
            // payload off to the inspector. The forwarded `Bytes` in the
            // pump is a separate buffer — it's untouched, so the broker
            // still sees the real credentials. Only the in-process
            // ring-buffer copy gets the placeholder.
            let inspector_payload = if api_key_i32 == API_KEY_SASL_AUTHENTICATE {
                redact_sasl_authenticate_body(payload)
            } else {
                payload
            };
            ProtoEvent {
                observed_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                queued_at: Instant::now(),
                direction: ProtoDirection::Send,
                api_key: api_key_i32,
                api_version: header.map_or(-1, |h| i32::from(h.api_version)),
                corr_id: header.map_or(0, |h| h.corr_id),
                connection_id,
                local_port,
                payload_size,
                rtt_ms: 0.0,
                capture_lag_ms,
                payload: inspector_payload,
                frame_error: None,
            }
        }
        ProxyDirection::UpstreamToClient => {
            // Response wire prefix is just the 4-byte correlation id.
            let corr_id = if frame.len() >= 4 {
                i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]])
            } else {
                0
            };
            let pending = corr_map.take_response(corr_id);
            let rtt_ms = pending.map_or(0.0, |p| p.rtt_at(Instant::now(), observed_nanos));
            ProtoEvent {
                observed_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                queued_at: Instant::now(),
                direction: ProtoDirection::Recv,
                api_key: pending.map_or(-1, |p| i32::from(p.header.api_key)),
                api_version: pending.map_or(-1, |p| i32::from(p.header.api_version)),
                corr_id,
                connection_id,
                local_port,
                payload_size,
                rtt_ms,
                capture_lag_ms,
                payload,
                frame_error: None,
            }
        }
    };
    Ok(event)
}

pub use crate::proxy_broker_map::BrokerMap;
pub use crate::proxy_handle::ProxyHandle;
pub use crate::proxy_upstream::{UpstreamSaslConfig, UpstreamSaslMechanism, UpstreamTlsConfig};

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[path = "proxy_tests.rs"]
mod tests;
