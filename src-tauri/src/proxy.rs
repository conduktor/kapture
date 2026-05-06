//! Kapture proxy mode.
//!
//! A TCP intermediary that accepts Kafka client connections, forwards
//! every byte to a real upstream broker, and taps each frame to the
//! `ProtoCorrelator` so the Protocol tab shows the wire-level traffic
//! of the *client*, not of Kapture itself. See `docs/specs/proxy-mode.md`.
//!
//! Phase 1: single broker, plain TCP, no SASL, no TLS.

use std::net::SocketAddr;

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
}
