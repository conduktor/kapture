//! Kapture proxy mode.
//!
//! A TCP intermediary that accepts Kafka client connections, forwards
//! every byte to a real upstream broker, and taps each frame to the
//! `ProtoCorrelator` so the Protocol tab shows the wire-level traffic
//! of the *client*, not of Kapture itself. See `docs/specs/proxy-mode.md`.
//!
//! Phase 1: single broker, plain TCP, no SASL, no TLS.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

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
/// Bounded implicitly by the number of in-flight Kafka requests on
/// one TCP connection — Kafka clients pipeline but cap at a few
/// hundred. We rely on the response take to drain entries; if a
/// connection drops mid-flight any leftovers are released when the
/// owning task exits and drops the map.
#[allow(dead_code)] // see note on `ProxyConfig`
#[derive(Debug, Default)]
pub struct CorrelationMap {
    inner: Mutex<HashMap<i32, PendingRequest>>,
}

#[allow(dead_code)] // see note on `ProxyConfig`
impl CorrelationMap {
    pub fn record_request(&self, corr_id: i32, header: RequestHeaderPeek) {
        self.inner.lock().insert(
            corr_id,
            PendingRequest {
                header,
                sent_at: Instant::now(),
            },
        );
    }

    pub fn take_response(&self, corr_id: i32) -> Option<PendingRequest> {
        self.inner.lock().remove(&corr_id)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    #[test]
    fn proxy_config_normalises_listen_addr() {
        let cfg = ProxyConfig::new("upstream:9092".to_owned(), 9092);
        assert_eq!(cfg.upstream, "upstream:9092");
        assert_eq!(cfg.listen_addr().to_string(), "127.0.0.1:9092");
    }

    #[tokio::test]
    async fn frame_codec_decodes_length_prefixed_payloads() {
        use futures::StreamExt;

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
        );
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
}
