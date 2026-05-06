//! Kapture proxy mode.
//!
//! A TCP intermediary that accepts Kafka client connections, forwards
//! every byte to a real upstream broker, and taps each frame to the
//! `ProtoCorrelator` so the Protocol tab shows the wire-level traffic
//! of the *client*, not of Kapture itself. See `docs/specs/proxy-mode.md`.
//!
//! Phase 2 (in progress): the listener fleet grows on demand — the
//! response rewriter calls `BrokerProvisioner::ensure` whenever it
//! sees a new broker in a `Metadata` / `FindCoordinator` /
//! `DescribeCluster` response, which in turn binds a local listener
//! and spawns its accept loop. Bootstrap broker is pre-seeded into
//! `BrokerMap` so its listener is reused on rewrite (no double-bind).

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::warn;

use crate::correlator::ProtoCorrelator;
use crate::proto_hook::{ProtoDirection, ProtoEvent};
use crate::proxy_handle::RecordSink;
use crate::proxy_provisioner::BrokerProvisioner;
use crate::proxy_records::{
    extract_from_fetch_response, extract_from_produce_request, extracted_to_captured,
};
use crate::proxy_topic_ids::TopicIdMap;

/// Cap on `payload` we copy into the `ProtoEvent`. Mirrors the C-side
/// `RD_KAFKA_PROTO_HOOK_PAYLOAD_MAX` so the Protocol tab's hex view +
/// decoded body stays bounded across both client and proxy modes.
pub const PROTO_PAYLOAD_CAP: usize = 64 * 1024;

/// Maximum in-flight requests tracked per client TCP connection.
/// Exceeding this closes the offending proxy connection instead of
/// allowing an unbounded correlation map to grow until memory pressure.
pub const MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: usize = 8192;

// Skeleton type for Phase 1 — wired into AppState/commands in later tasks
// of the proxy-mode plan (Tasks 6–8). Allowing dead_code locally so the
// `-D warnings` gate passes while the module is still inert.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// `host:port` of the upstream Kafka broker we forward to.
    pub upstream: String,
    /// TCP port we bind on `127.0.0.1` for clients to connect to.
    pub listen_port: u16,
}

#[allow(dead_code)] // see note on `ProxyConfig`
impl ProxyConfig {
    #[must_use]
    pub const fn new(upstream: String, listen_port: u16) -> Self {
        Self {
            upstream,
            listen_port,
        }
    }

    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.listen_port))
    }
}

/// Wrap a `TcpStream` in the Kafka wire-frame codec: 4-byte big-endian
/// length prefix followed by `length` body bytes. The codec hands us
/// one `Bytes` per frame on the read side, and accepts a `Bytes` per
/// frame on the write side (it prepends the length itself).
///
/// Max frame size is 100 MiB. The Kafka default `socket.request.max.bytes`
/// is 100 MiB, and Kafka brokers reject anything larger, so this is the
/// effective wire ceiling. Anything bigger than that and a `kafkacat -L`
/// against a 10k-topic cluster would still parse, while a malicious peer
/// can't OOM us with a 4 GiB `len` field.
#[allow(dead_code)] // see note on `ProxyConfig`
pub fn framed_kafka(socket: TcpStream) -> Framed<TcpStream, LengthDelimitedCodec> {
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

#[allow(dead_code)] // see note on `ProxyConfig`
#[derive(Debug, Clone, Copy)]
pub struct RequestHeaderPeek {
    pub api_key: i16,
    pub api_version: i16,
    pub corr_id: i32,
}

/// Read the fixed-shape request header prefix without consuming the
/// buffer. Returns `None` if the buffer is too short.
#[allow(dead_code)] // see note on `ProxyConfig`
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
#[allow(dead_code)] // see note on `ProxyConfig`
#[derive(Debug, Clone, Copy)]
pub struct PendingRequest {
    pub header: RequestHeaderPeek,
    pub sent_at: Instant,
}

#[allow(dead_code)] // see note on `ProxyConfig`
impl PendingRequest {
    #[must_use]
    pub fn rtt_at(&self, now: Instant) -> f64 {
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
#[allow(dead_code)] // see note on `ProxyConfig`
#[derive(Debug, Default)]
pub struct CorrelationMap {
    inner: Mutex<HashMap<i32, PendingRequest>>,
}

#[allow(dead_code)] // see note on `ProxyConfig`
impl CorrelationMap {
    pub fn record_request(&self, corr_id: i32, header: RequestHeaderPeek) -> io::Result<()> {
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
            },
        );
        drop(inner);
        Ok(())
    }

    pub fn take_response(&self, corr_id: i32) -> Option<PendingRequest> {
        self.inner.lock().remove(&corr_id)
    }
}

/// Monotonic, never-zero connection identifier. Used as the pairing
/// key for `(corr_id, connection_id)` in the inspector — replaces
/// the `broker_id` semantics from the rdkafka-client mode.
#[allow(dead_code)] // see note on `ProxyConfig`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

/// Direction of a tapped frame, from the proxy's point of view.
#[allow(dead_code)] // see note on `ProxyConfig`
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

#[allow(dead_code)] // see note on `ProxyConfig`
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
#[allow(dead_code)] // see note on `ProxyConfig`
pub async fn run_pump<F>(
    conn_id: ConnectionId,
    client: TcpStream,
    upstream: TcpStream,
    tap: F,
) -> io::Result<()>
where
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
///
/// The correlator records the **original** bytes (Wireshark-style:
/// "show me what was on the wire") — only the bytes forwarded to the
/// client are rewritten.
///
/// On rewrite failure the original frame is forwarded verbatim and
/// the error is logged at `warn!` — we never silently drop frames.
///
/// # Errors
/// Bubbles up `io::Error` from the underlying TCP read/write.
#[allow(dead_code)] // wired into ProxyHandle::start in Task 16
#[allow(clippy::too_many_arguments)]
pub async fn run_pump_with_rewrite(
    conn_id: ConnectionId,
    client: TcpStream,
    upstream: TcpStream,
    correlator: Arc<ProtoCorrelator>,
    corr_map: Arc<CorrelationMap>,
    provisioner: Arc<dyn BrokerProvisioner>,
    record_sink: RecordSink,
    topic_ids: Arc<TopicIdMap>,
) -> io::Result<()> {
    let mut client_framed = framed_kafka(client);
    let mut upstream_framed = framed_kafka(upstream);

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
                    &bytes,
                    &corr_map,
                )?;
                let req_api_key = i16::try_from(event.api_key).unwrap_or(-1);
                let req_api_version = i16::try_from(event.api_version).unwrap_or(-1);
                correlator.record_event(&event);
                if req_api_key == 0 {
                    // Produce request — extract records before forwarding.
                    for rec in extract_from_produce_request(req_api_version, &bytes) {
                        record_sink(extracted_to_captured(rec, conn_id.0));
                    }
                }
                upstream_framed.send(bytes).await?;
            }
            // Upstream → client (with rewrite)
            frame = upstream_framed.next() => {
                let Some(frame) = frame else { break; };
                let frame = frame?;
                let bytes = frame.freeze();
                let event = build_proto_event(
                    ProxyDirection::UpstreamToClient,
                    conn_id,
                    &bytes,
                    &corr_map,
                )?;
                let api_key = i16::try_from(event.api_key).unwrap_or(-1);
                let api_version = i16::try_from(event.api_version).unwrap_or(-1);
                correlator.record_event(&event);
                if api_key == 1 {
                    // Fetch response — extract records before forwarding.
                    // `bytes` is the codec output (no wire size prefix);
                    // it starts at the ResponseHeader, which is what
                    // `extract_from_fetch_response` expects.
                    for rec in extract_from_fetch_response(api_version, &bytes, &topic_ids) {
                        record_sink(extracted_to_captured(rec, conn_id.0));
                    }
                }

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
            }
        }
    }
    Ok(())
}

/// Build the `ProtoEvent` for one tapped frame. On the request path,
/// peek the header and stash it in `corr_map`. On the response path,
/// look up the matching request to recover `(api_key, api_version)`
/// and RTT.
///
/// The `payload` field re-prepends the 4-byte big-endian size prefix
/// (encoding the body length, i.e. `frame.len()`) so the existing
/// `proto_decode::decode_frame` parser keeps working unchanged. The
/// `payload_size` is the WIRE size including that prefix, matching
/// the librdkafka FFI semantics.
#[allow(dead_code)] // wired into the pump tap in Task 6
pub fn build_proto_event(
    dir: ProxyDirection,
    conn_id: ConnectionId,
    frame: &[u8],
    corr_map: &CorrelationMap,
) -> io::Result<ProtoEvent> {
    let body_len_i32 = i32::try_from(frame.len()).unwrap_or(i32::MAX);
    let payload_size = frame.len() + 4;
    let body_take = frame.len().min(PROTO_PAYLOAD_CAP - 4);
    let mut payload = Vec::with_capacity(body_take + 4);
    payload.extend_from_slice(&body_len_i32.to_be_bytes());
    payload.extend_from_slice(&frame[..body_take]);
    let broker_id = i32::try_from(conn_id.0 & 0x7FFF_FFFF).unwrap_or(i32::MAX);

    let event = match dir {
        ProxyDirection::ClientToUpstream => {
            let header = peek_request_header(frame);
            if let Some(h) = header {
                corr_map.record_request(h.corr_id, h)?;
            }
            ProtoEvent {
                direction: ProtoDirection::Send,
                api_key: header.map_or(-1, |h| i32::from(h.api_key)),
                api_version: header.map_or(-1, |h| i32::from(h.api_version)),
                corr_id: header.map_or(0, |h| h.corr_id),
                broker_id,
                payload_size,
                rtt_ms: 0.0,
                payload,
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
            let rtt_ms = pending.map_or(0.0, |p| p.rtt_at(Instant::now()));
            ProtoEvent {
                direction: ProtoDirection::Recv,
                api_key: pending.map_or(-1, |p| i32::from(p.header.api_key)),
                api_version: pending.map_or(-1, |p| i32::from(p.header.api_version)),
                corr_id,
                broker_id,
                payload_size,
                rtt_ms,
                payload,
            }
        }
    };
    Ok(event)
}

pub use crate::proxy_broker_map::BrokerMap;
pub use crate::proxy_handle::ProxyHandle;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
    use kafka_protocol::messages::{ApiKey, BrokerId, MetadataResponse, ResponseHeader};
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
    use parking_lot::Mutex as PMutex;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// Local copy of the `proxy_rewrite::tests` helper. Duplicated
    /// rather than re-exported so production code stays free of test
    /// fixtures.
    fn build_metadata_response_bytes(version: i16, brokers: Vec<(i32, &str, i32)>) -> Vec<u8> {
        let mut resp = MetadataResponse::default();
        resp.brokers = brokers
            .into_iter()
            .map(|(node_id, host, port)| {
                let mut b = MetadataResponseBroker::default();
                b.node_id = BrokerId(node_id);
                b.host = StrBytes::from_string(host.to_owned());
                b.port = port;
                b
            })
            .collect();

        let header_version = ApiKey::Metadata.response_header_version(version);
        let mut out = BytesMut::new();
        ResponseHeader::default()
            .encode(&mut out, header_version)
            .unwrap();
        resp.encode(&mut out, version).unwrap();
        out.to_vec()
    }

    #[test]
    fn proxy_config_normalises_listen_addr() {
        let cfg = ProxyConfig::new("upstream:9092".to_owned(), 9092);
        assert_eq!(cfg.upstream, "upstream:9092");
        assert_eq!(cfg.listen_addr().to_string(), "127.0.0.1:9092");
    }

    #[tokio::test]
    async fn frame_codec_decodes_length_prefixed_payloads() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut framed = framed_kafka(sock);
            let frame = framed.next().await.unwrap().unwrap();
            assert_eq!(frame.as_ref(), b"hello");
            let frame = framed.next().await.unwrap().unwrap();
            assert_eq!(frame.as_ref(), b"world!");
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        // Two frames back-to-back: 4-byte BE length + body.
        client.write_all(&5u32.to_be_bytes()).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        client.write_all(&6u32.to_be_bytes()).await.unwrap();
        client.write_all(b"world!").await.unwrap();
        client.shutdown().await.unwrap();

        server.await.unwrap();
    }

    #[test]
    fn peek_request_header_reads_api_key_version_corr_id() {
        // Wire shape (size prefix already stripped by the codec):
        //   api_key (i16 BE) | api_version (i16 BE) | corr_id (i32 BE) | rest...
        let mut buf = Vec::new();
        buf.extend_from_slice(&3i16.to_be_bytes()); // Metadata
        buf.extend_from_slice(&12i16.to_be_bytes()); // v12
        buf.extend_from_slice(&777i32.to_be_bytes()); // corr id
        buf.extend_from_slice(b"...remaining header + body...");

        let header = peek_request_header(&buf).unwrap();
        assert_eq!(header.api_key, 3);
        assert_eq!(header.api_version, 12);
        assert_eq!(header.corr_id, 777);
    }

    #[test]
    fn peek_request_header_rejects_short_buffer() {
        assert!(peek_request_header(&[0u8; 7]).is_none());
    }

    #[test]
    fn correlation_map_pairs_request_and_response() {
        let map = CorrelationMap::default();
        map.record_request(
            42,
            RequestHeaderPeek {
                api_key: 1,
                api_version: 13,
                corr_id: 42,
            },
        )
        .unwrap();
        let pending = map.take_response(42).unwrap();
        assert_eq!(pending.header.api_key, 1);
        assert_eq!(pending.header.api_version, 13);
        // RTT is positive (some elapsed time, even if tiny).
        let rtt = pending.rtt_at(std::time::Instant::now());
        assert!(rtt >= 0.0);
        // Subsequent take returns None — entries are consumed.
        assert!(map.take_response(42).is_none());
    }

    #[test]
    fn correlation_map_returns_none_for_unknown_corr_id() {
        let map = CorrelationMap::default();
        assert!(map.take_response(999).is_none());
    }

    #[test]
    fn correlation_map_rejects_unbounded_in_flight_requests() {
        let map = CorrelationMap::default();
        for corr_id in 0..MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION {
            map.record_request(
                i32::try_from(corr_id).unwrap(),
                RequestHeaderPeek {
                    api_key: 3,
                    api_version: 12,
                    corr_id: i32::try_from(corr_id).unwrap(),
                },
            )
            .unwrap();
        }

        let result = map.record_request(
            99_999,
            RequestHeaderPeek {
                api_key: 3,
                api_version: 12,
                corr_id: 99_999,
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn build_proto_event_for_request_uses_peeked_header() {
        let map = CorrelationMap::default();
        // 8-byte header prefix: api_key=18 (ApiVersions), api_ver=3, corr_id=99
        let mut frame = Vec::new();
        frame.extend_from_slice(&18i16.to_be_bytes());
        frame.extend_from_slice(&3i16.to_be_bytes());
        frame.extend_from_slice(&99i32.to_be_bytes());
        frame.extend_from_slice(b"....rest....");

        let event = build_proto_event(
            ProxyDirection::ClientToUpstream,
            ConnectionId(7),
            &frame,
            &map,
        )
        .unwrap();

        assert!(matches!(
            event.direction,
            crate::proto_hook::ProtoDirection::Send
        ));
        assert_eq!(event.api_key, 18);
        assert_eq!(event.api_version, 3);
        assert_eq!(event.corr_id, 99);
        assert_eq!(event.broker_id, 7);
        assert_eq!(event.payload_size, frame.len() + 4);
        let body_len = i32::try_from(frame.len()).unwrap();
        assert_eq!(&event.payload[..4], &body_len.to_be_bytes());
        assert_eq!(&event.payload[4..], &frame[..]);
        assert!(event.rtt_ms == 0.0);
        // Map now holds an entry for corr_id 99.
        assert!(map.take_response(99).is_some());
    }

    #[test]
    fn build_proto_event_for_response_resolves_from_map() {
        let map = CorrelationMap::default();
        map.record_request(
            42,
            RequestHeaderPeek {
                api_key: 1,
                api_version: 13,
                corr_id: 42,
            },
        )
        .unwrap();
        // Response wire prefix: corr_id (i32 BE) at offset 0.
        let mut frame = Vec::new();
        frame.extend_from_slice(&42i32.to_be_bytes());
        frame.extend_from_slice(b"....body....");

        let event = build_proto_event(
            ProxyDirection::UpstreamToClient,
            ConnectionId(7),
            &frame,
            &map,
        )
        .unwrap();

        assert!(matches!(
            event.direction,
            crate::proto_hook::ProtoDirection::Recv
        ));
        assert_eq!(event.api_key, 1);
        assert_eq!(event.api_version, 13);
        assert_eq!(event.corr_id, 42);
        assert_eq!(event.broker_id, 7);
        assert_eq!(event.payload_size, frame.len() + 4);
        let body_len = i32::try_from(frame.len()).unwrap();
        assert_eq!(&event.payload[..4], &body_len.to_be_bytes());
        assert_eq!(&event.payload[4..], &frame[..]);
        assert!(event.rtt_ms >= 0.0);
    }

    #[test]
    fn build_proto_event_for_unknown_response_is_marked_unknown() {
        let map = CorrelationMap::default();
        // Response with no matching request in the map.
        let mut frame = Vec::new();
        frame.extend_from_slice(&404i32.to_be_bytes());
        frame.extend_from_slice(b"....body....");

        let event = build_proto_event(
            ProxyDirection::UpstreamToClient,
            ConnectionId(7),
            &frame,
            &map,
        )
        .unwrap();

        assert_eq!(event.api_key, -1);
        assert_eq!(event.api_version, -1);
        assert_eq!(event.corr_id, 404);
        assert_eq!(event.payload_size, frame.len() + 4);
    }

    /// End-to-end: spin up a fake upstream broker that echoes each
    /// frame with its bytes reversed, run the per-connection pump
    /// against it, send a frame from the "client" side, and assert
    /// (a) the client gets the reversed echo and (b) the inspector
    /// tap saw both frames with the right direction.
    #[tokio::test]
    async fn per_connection_pump_taps_both_directions() {
        type Tap = Arc<PMutex<Vec<(ProxyDirection, Vec<u8>)>>>;

        // Fake upstream — accepts one connection, reads one frame,
        // writes back the reversed bytes (still as a length-prefixed
        // frame), then closes.
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (sock, _) = upstream.accept().await.unwrap();
            let mut framed = framed_kafka(sock);
            let frame = framed.next().await.unwrap().unwrap();
            let mut reply = frame.to_vec();
            reply.reverse();
            framed.send(reply.into()).await.unwrap();
        });

        // Tap collector.
        let tap: Tap = Arc::new(PMutex::new(Vec::new()));
        let tap_for_pump = Arc::clone(&tap);

        // Client side of the pump: a paired in-memory socket would be
        // ideal but we use a real loopback TCP for simplicity.
        let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client_addr = client_listener.local_addr().unwrap();
        let upstream_target = upstream_addr.to_string();
        let pump_task = tokio::spawn(async move {
            let (client_sock, _) = client_listener.accept().await.unwrap();
            let upstream_sock = TcpStream::connect(upstream_target).await.unwrap();
            run_pump(
                ConnectionId(1),
                client_sock,
                upstream_sock,
                move |dir, conn, payload| {
                    assert_eq!(conn, ConnectionId(1));
                    tap_for_pump.lock().push((dir, payload.to_vec()));
                },
            )
            .await
            .unwrap();
        });

        // Drive the client.
        let mut client = TcpStream::connect(client_addr).await.unwrap();
        client.write_all(&8u32.to_be_bytes()).await.unwrap();
        client.write_all(b"helloKKK").await.unwrap();
        // Read the echoed reply.
        let mut framed_client = framed_kafka(client);
        let reply = framed_client.next().await.unwrap().unwrap();
        assert_eq!(reply.as_ref(), b"KKKolleh");

        upstream_task.await.unwrap();
        pump_task.await.unwrap();

        let captured = tap.lock().clone();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].0, ProxyDirection::ClientToUpstream);
        assert_eq!(captured[0].1, b"helloKKK");
        assert_eq!(captured[1].0, ProxyDirection::UpstreamToClient);
        assert_eq!(captured[1].1, b"KKKolleh");
    }

    #[tokio::test]
    async fn proxy_handle_accepts_one_client_and_forwards_to_upstream() {
        // Fake upstream — accepts ONE connection, echoes one frame.
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (sock, _) = upstream.accept().await.unwrap();
            let mut framed = framed_kafka(sock);
            let frame = framed.next().await.unwrap().unwrap();
            framed.send(frame.freeze()).await.unwrap();
        });

        let correlator = Arc::new(crate::correlator::ProtoCorrelator::new());
        let cfg = ProxyConfig {
            upstream: upstream_addr.to_string(),
            listen_port: 0, // OS assigns
        };
        let no_op_sink: crate::proxy_handle::RecordSink = Arc::new(|_msg| {});
        let handle = ProxyHandle::start(cfg, Arc::clone(&correlator), no_op_sink)
            .await
            .unwrap();
        let listen_addr = handle.local_addr();

        // Drive a fake client.
        let mut client = TcpStream::connect(listen_addr).await.unwrap();
        client.write_all(&5u32.to_be_bytes()).await.unwrap();
        // Use a 4-byte header prefix worth of data so peek doesn't reject.
        client.write_all(b"\x00\x12\x00\x03X").await.unwrap();
        let mut framed = framed_kafka(client);
        let echoed = framed.next().await.unwrap().unwrap();
        assert_eq!(echoed.as_ref(), b"\x00\x12\x00\x03X");

        upstream_task.await.unwrap();

        // Correlator should have observed at least 2 frames (send + recv).
        let summaries = correlator.summaries(100);
        assert!(summaries.len() >= 2);

        handle.stop().await;
    }

    #[tokio::test]
    async fn pump_rewrites_metadata_response_brokers_to_local() {
        // Fake upstream: when a client sends ANY frame, reply with a
        // pre-built Metadata response that advertises 2 distant brokers.
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();

        let upstream_task = tokio::spawn(async move {
            let (sock, _) = upstream.accept().await.unwrap();
            let mut framed = framed_kafka(sock);
            // Read one request frame from the client.
            let _req = framed.next().await.unwrap().unwrap();
            // Send a Metadata v12 response.
            let body = build_metadata_response_bytes(
                12,
                vec![(1, "kafka-mb-1", 39092), (2, "kafka-mb-2", 39093)],
            );
            // Splice the corr_id=42 from the (fake) request.
            let mut buf = BytesMut::from(&body[..]);
            buf[0..4].copy_from_slice(&42i32.to_be_bytes());
            framed.send(buf.freeze()).await.unwrap();
        });

        // Client side: connect through our pump.
        let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client_addr = client_listener.local_addr().unwrap();
        let upstream_target = upstream_addr.to_string();
        let correlator = Arc::new(crate::correlator::ProtoCorrelator::new());
        let corr_map = Arc::new(CorrelationMap::default());
        let broker_map = Arc::new(BrokerMap::new());
        let correlator_for_test = Arc::clone(&correlator);
        let broker_map_for_test = Arc::clone(&broker_map);

        let provisioner: Arc<dyn BrokerProvisioner> = broker_map;
        let no_op_sink: crate::proxy_handle::RecordSink = Arc::new(|_msg| {});
        let topic_id_map = Arc::new(crate::proxy_topic_ids::TopicIdMap::new());
        let pump_task = tokio::spawn(async move {
            let (client_sock, _) = client_listener.accept().await.unwrap();
            let upstream_sock = TcpStream::connect(upstream_target).await.unwrap();
            run_pump_with_rewrite(
                ConnectionId(1),
                client_sock,
                upstream_sock,
                correlator,
                corr_map,
                provisioner,
                no_op_sink,
                topic_id_map,
            )
            .await
            .unwrap();
        });

        // Drive the client. Send a Metadata v12 request (api_key=3,
        // api_ver=12, corr_id=42, then dummy header tail).
        let mut client = TcpStream::connect(client_addr).await.unwrap();
        let mut req = Vec::new();
        req.extend_from_slice(&3i16.to_be_bytes());
        req.extend_from_slice(&12i16.to_be_bytes());
        req.extend_from_slice(&42i32.to_be_bytes());
        // client_id (nullable string, length=-1) + tagged fields=0
        req.extend_from_slice(&(-1i16).to_be_bytes());
        req.push(0); // tagged fields count = 0
                     // Empty MetadataRequest body (topics array null + tagged fields).
        req.push(0xFF); // null array marker for v12 flexible
        req.push(0); // tagged fields
        let len = u32::try_from(req.len()).unwrap();
        client.write_all(&len.to_be_bytes()).await.unwrap();
        client.write_all(&req).await.unwrap();

        // Read the rewritten response.
        let mut framed_client = framed_kafka(client);
        let resp = framed_client.next().await.unwrap().unwrap();
        let mut buf = resp.freeze();
        // First 4 bytes should be corr_id=42.
        let corr_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(corr_id, 42);
        // Decode and verify brokers were rewritten.
        let header_version = ApiKey::Metadata.response_header_version(12);
        let _hdr = ResponseHeader::decode(&mut buf, header_version).unwrap();
        let decoded = MetadataResponse::decode(&mut buf, 12).unwrap();
        for b in &decoded.brokers {
            assert_eq!(b.host.to_string(), "127.0.0.1");
            assert!(b.port > 0 && b.port < 65536);
        }
        // BrokerMap should now hold both upstream entries.
        assert_eq!(broker_map_for_test.snapshot().len(), 2);
        // Correlator should have recorded request + response.
        assert!(correlator_for_test.summaries(10).len() >= 2);

        upstream_task.await.unwrap();
        pump_task.abort();
    }
}
