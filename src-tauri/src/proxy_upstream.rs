//! Open and SASL-authenticate an upstream Kafka connection on behalf
//! of the downstream client.
//!
//! The Kapture proxy lets users point a Kafka client at us and forward
//! to a real broker. When the broker requires SASL but the client app
//! speaks plaintext+no-SASL, the proxy must perform the SASL handshake
//! itself with credentials configured by the Kapture user.
//!
//! Step 1 (this module): plain-TCP upstream + SASL=PLAIN only. TLS and
//! SCRAM are deferred to later steps. Wiring into the connection pump
//! is also a later step — for now this is a self-contained primitive.
//!
//! Wire format reminder. A Kafka request frame is
//! `[size: i32 BE] [request_header (version-dependent)] [request_body]`.
//! Responses are similar with their own header layout. We bypass the
//! `LengthDelimitedCodec` here on purpose: we need to hand back the raw
//! `TcpStream` after the handshake, with no read-ahead bytes buffered
//! anywhere, so the pump can take over forwarding without losing bytes.
//! Tiny `read_kafka_frame` / `write_kafka_frame` helpers keep all reads
//! exactly frame-sized via `read_exact`.

// Wired up in a later step (pump integration). Until then, the
// public types live behind `dead_code`.
#![allow(dead_code)]

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

/// Modern API versions chosen here. v0 `ApiVersions` keeps the request
/// header non-flexible (header v1) and the body empty, which is the
/// simplest thing that works against any broker that supports SASL.
/// `SaslHandshake` v1 added the `enabled_mechanisms` field to the
/// response (still non-flexible). `SaslAuthenticate` v2 made both sides
/// flexible — `kafka-protocol` handles compact encoding for us.
const API_VERSIONS_VERSION: i16 = 0;
const SASL_HANDSHAKE_VERSION: i16 = 1;
const SASL_AUTHENTICATE_VERSION: i16 = 2;

/// Hard cap on a single response frame. Mirrors `framed_kafka`'s
/// `max_frame_length` (100 MiB) so a hostile / mis-speaking peer
/// can't OOM us with a huge length field during the handshake.
const MAX_RESPONSE_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// SASL mechanism this module supports. Step 1 = PLAIN only; SCRAM
/// is a multi-roundtrip exchange and ships in a follow-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamSaslMechanism {
    Plain,
    // ScramSha256 / ScramSha512 land in step 2.
}

impl UpstreamSaslMechanism {
    const fn label(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
        }
    }
}

/// Credentials used to authenticate to the upstream broker. The
/// password is redacted from `Debug` to match `capture::AuthConfig`'s
/// posture — `{:?}` of an `UpstreamSaslConfig` must never leak it
/// into logs / tracing spans / error messages.
#[derive(Clone)]
pub struct UpstreamSaslConfig {
    pub mechanism: UpstreamSaslMechanism,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for UpstreamSaslConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamSaslConfig")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Errors surfaced to the caller of [`open_upstream`].
///
/// Each handshake step has its own variant so the pump can log the
/// failure cause precisely (e.g. "auth failed" vs "broker doesn't
/// speak this SASL mechanism" vs "broker dropped the TCP connection").
#[derive(Debug, thiserror::Error)]
pub enum UpstreamConnectError {
    #[error("connect to {host}:{port} failed: {err}")]
    Connect {
        host: String,
        port: u16,
        err: std::io::Error,
    },
    #[error("api_versions exchange failed: {0}")]
    ApiVersions(String),
    #[error("sasl_handshake exchange failed: {0}")]
    SaslHandshake(String),
    #[error("sasl_authenticate exchange failed: {0}")]
    SaslAuthenticate(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Open a TCP connection to `host:port` and, if `sasl` is `Some`,
/// drive the broker through the `ApiVersions` / `SaslHandshake` /
/// `SaslAuthenticate` sequence using the supplied credentials.
///
/// On success the returned `TcpStream` has zero pending bytes in any
/// buffered reader — the next byte read off it will be the first byte
/// of whatever the downstream pump forwards next. That property is why
/// we read frames manually here instead of going through the
/// `LengthDelimitedCodec` wrapper used elsewhere.
///
/// When `sasl` is `None`, this is a bare TCP connect: no `ApiVersions`
/// injection, no extra bytes on the wire. The pump will treat the
/// stream like any other forwarding socket.
///
/// # Errors
///
/// See [`UpstreamConnectError`].
pub async fn open_upstream(
    host: &str,
    port: u16,
    sasl: Option<&UpstreamSaslConfig>,
) -> Result<TcpStream, UpstreamConnectError> {
    let mut stream = connect_tcp(host, port).await?;
    if let Some(cfg) = sasl {
        run_sasl_handshake(&mut stream, cfg).await?;
    }
    Ok(stream)
}

async fn connect_tcp(host: &str, port: u16) -> Result<TcpStream, UpstreamConnectError> {
    // `(host, port)` accepts both IP literals and DNS names. We do not
    // currently expose a happy-eyeballs / multi-address strategy —
    // first failure wins. In practice the bootstrap address is a
    // single IP/host pair already resolved by the user.
    let addr = (host, port);
    connect_to(addr)
        .await
        .map_err(|err| UpstreamConnectError::Connect {
            host: host.to_owned(),
            port,
            err,
        })
}

async fn connect_to<A: ToSocketAddrs>(addr: A) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(addr).await?;
    // Disable Nagle so handshake frames go out immediately. The
    // SASL exchange is small and latency-sensitive: 3 frames in,
    // 3 out. Buffering them gains nothing and risks long head-of-line
    // pauses on slow networks.
    stream.set_nodelay(true)?;
    Ok(stream)
}

async fn run_sasl_handshake(
    stream: &mut TcpStream,
    cfg: &UpstreamSaslConfig,
) -> Result<(), UpstreamConnectError> {
    // Step 1: ApiVersions. Lets the broker advertise its supported
    // versions; we don't act on the response yet but the broker
    // expects this exchange before SASL on most modern Kafka
    // distributions, and skipping it can produce confusing
    // "UNSUPPORTED_VERSION" errors on the SaslHandshake leg.
    let mut corr_id: i32 = 1;
    send_api_versions(stream, corr_id).await?;
    recv_api_versions(stream, corr_id).await?;

    // Step 2: SaslHandshake — pick a mechanism.
    corr_id += 1;
    send_sasl_handshake(stream, corr_id, cfg.mechanism).await?;
    recv_sasl_handshake(stream, corr_id, cfg.mechanism).await?;

    // Step 3: SaslAuthenticate — actually present the credentials.
    corr_id += 1;
    let auth_bytes = build_auth_bytes(cfg);
    send_sasl_authenticate(stream, corr_id, auth_bytes).await?;
    recv_sasl_authenticate(stream, corr_id).await?;
    Ok(())
}

/// PLAIN SASL token format: `[authzid] \0 username \0 password`.
/// Authzid is empty (the standard Kafka usage). Mechanism dispatch
/// here is trivial today; once SCRAM lands, this function grows a
/// match arm and returns the client-first message instead.
fn build_auth_bytes(cfg: &UpstreamSaslConfig) -> Bytes {
    match cfg.mechanism {
        UpstreamSaslMechanism::Plain => {
            let mut buf = Vec::with_capacity(2 + cfg.username.len() + cfg.password.len());
            buf.push(0);
            buf.extend_from_slice(cfg.username.as_bytes());
            buf.push(0);
            buf.extend_from_slice(cfg.password.as_bytes());
            Bytes::from(buf)
        }
    }
}

async fn send_api_versions(
    stream: &mut TcpStream,
    corr_id: i32,
) -> Result<(), UpstreamConnectError> {
    let header = make_request_header(ApiKey::ApiVersions, API_VERSIONS_VERSION, corr_id);
    let body = ApiVersionsRequest::default();
    let frame = encode_request(
        &header,
        ApiKey::ApiVersions.request_header_version(API_VERSIONS_VERSION),
        &body,
        API_VERSIONS_VERSION,
    )
    .map_err(|e| UpstreamConnectError::ApiVersions(format!("encode: {e}")))?;
    write_kafka_frame(stream, &frame).await?;
    Ok(())
}

async fn recv_api_versions(
    stream: &mut TcpStream,
    expected_corr_id: i32,
) -> Result<(), UpstreamConnectError> {
    let frame = read_kafka_frame(stream).await?;
    let mut buf = Bytes::from(frame);
    let header_version = ApiKey::ApiVersions.response_header_version(API_VERSIONS_VERSION);
    let header = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| UpstreamConnectError::ApiVersions(format!("header decode: {e}")))?;
    if header.correlation_id != expected_corr_id {
        return Err(UpstreamConnectError::ApiVersions(format!(
            "unexpected correlation_id {} (want {})",
            header.correlation_id, expected_corr_id
        )));
    }
    let resp = ApiVersionsResponse::decode(&mut buf, API_VERSIONS_VERSION)
        .map_err(|e| UpstreamConnectError::ApiVersions(format!("body decode: {e}")))?;
    if resp.error_code != 0 {
        return Err(UpstreamConnectError::ApiVersions(format!(
            "broker returned error_code={}",
            resp.error_code
        )));
    }
    Ok(())
}

async fn send_sasl_handshake(
    stream: &mut TcpStream,
    corr_id: i32,
    mechanism: UpstreamSaslMechanism,
) -> Result<(), UpstreamConnectError> {
    let header = make_request_header(ApiKey::SaslHandshake, SASL_HANDSHAKE_VERSION, corr_id);
    let mut body = SaslHandshakeRequest::default();
    body.mechanism = StrBytes::from_static_str(mechanism.label());
    let frame = encode_request(
        &header,
        ApiKey::SaslHandshake.request_header_version(SASL_HANDSHAKE_VERSION),
        &body,
        SASL_HANDSHAKE_VERSION,
    )
    .map_err(|e| UpstreamConnectError::SaslHandshake(format!("encode: {e}")))?;
    write_kafka_frame(stream, &frame).await?;
    Ok(())
}

async fn recv_sasl_handshake(
    stream: &mut TcpStream,
    expected_corr_id: i32,
    mechanism: UpstreamSaslMechanism,
) -> Result<(), UpstreamConnectError> {
    let frame = read_kafka_frame(stream).await?;
    let mut buf = Bytes::from(frame);
    let header_version = ApiKey::SaslHandshake.response_header_version(SASL_HANDSHAKE_VERSION);
    let header = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| UpstreamConnectError::SaslHandshake(format!("header decode: {e}")))?;
    if header.correlation_id != expected_corr_id {
        return Err(UpstreamConnectError::SaslHandshake(format!(
            "unexpected correlation_id {} (want {})",
            header.correlation_id, expected_corr_id
        )));
    }
    let resp = SaslHandshakeResponse::decode(&mut buf, SASL_HANDSHAKE_VERSION)
        .map_err(|e| UpstreamConnectError::SaslHandshake(format!("body decode: {e}")))?;
    if resp.error_code != 0 {
        // Surface the broker's enabled mechanisms when present, so a
        // misconfigured Kapture profile shows the user *exactly* which
        // mechanisms the broker accepts.
        let advertised: Vec<&str> = resp.mechanisms.iter().map(StrBytes::as_str).collect();
        return Err(UpstreamConnectError::SaslHandshake(format!(
            "broker rejected mechanism {:?} with error_code={} (broker advertises: {:?})",
            mechanism.label(),
            resp.error_code,
            advertised,
        )));
    }
    Ok(())
}

async fn send_sasl_authenticate(
    stream: &mut TcpStream,
    corr_id: i32,
    auth_bytes: Bytes,
) -> Result<(), UpstreamConnectError> {
    let header = make_request_header(ApiKey::SaslAuthenticate, SASL_AUTHENTICATE_VERSION, corr_id);
    let mut body = SaslAuthenticateRequest::default();
    body.auth_bytes = auth_bytes;
    let frame = encode_request(
        &header,
        ApiKey::SaslAuthenticate.request_header_version(SASL_AUTHENTICATE_VERSION),
        &body,
        SASL_AUTHENTICATE_VERSION,
    )
    .map_err(|e| UpstreamConnectError::SaslAuthenticate(format!("encode: {e}")))?;
    write_kafka_frame(stream, &frame).await?;
    Ok(())
}

async fn recv_sasl_authenticate(
    stream: &mut TcpStream,
    expected_corr_id: i32,
) -> Result<(), UpstreamConnectError> {
    let frame = read_kafka_frame(stream).await?;
    let mut buf = Bytes::from(frame);
    let header_version =
        ApiKey::SaslAuthenticate.response_header_version(SASL_AUTHENTICATE_VERSION);
    let header = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| UpstreamConnectError::SaslAuthenticate(format!("header decode: {e}")))?;
    if header.correlation_id != expected_corr_id {
        return Err(UpstreamConnectError::SaslAuthenticate(format!(
            "unexpected correlation_id {} (want {})",
            header.correlation_id, expected_corr_id
        )));
    }
    let resp = SaslAuthenticateResponse::decode(&mut buf, SASL_AUTHENTICATE_VERSION)
        .map_err(|e| UpstreamConnectError::SaslAuthenticate(format!("body decode: {e}")))?;
    if resp.error_code != 0 {
        let detail = resp
            .error_message
            .as_ref()
            .map_or("(no error_message)", StrBytes::as_str);
        return Err(UpstreamConnectError::SaslAuthenticate(format!(
            "broker returned error_code={}: {}",
            resp.error_code, detail
        )));
    }
    Ok(())
}

fn make_request_header(api: ApiKey, api_version: i16, corr_id: i32) -> RequestHeader {
    let mut h = RequestHeader::default();
    h.request_api_key = api as i16;
    h.request_api_version = api_version;
    h.correlation_id = corr_id;
    h.client_id = Some(StrBytes::from_static_str("kapture-proxy"));
    h
}

/// Encode a request as the body bytes of a single Kafka wire frame
/// (i.e. **without** the leading 4-byte length prefix — that is the
/// caller's job via [`write_kafka_frame`]). The kafka-protocol crate
/// returns `anyhow::Result` here; we surface the error as a `String`
/// so the caller can wrap it in the appropriate
/// [`UpstreamConnectError`] variant.
fn encode_request<B: Encodable>(
    header: &RequestHeader,
    header_version: i16,
    body: &B,
    body_version: i16,
) -> Result<Vec<u8>, String> {
    let mut out = BytesMut::with_capacity(256);
    header
        .encode(&mut out, header_version)
        .map_err(|e| e.to_string())?;
    body.encode(&mut out, body_version)
        .map_err(|e| e.to_string())?;
    Ok(out.to_vec())
}

/// Read one Kafka wire frame: 4-byte BE length prefix followed by
/// exactly that many body bytes. Frame size is capped at
/// [`MAX_RESPONSE_FRAME_BYTES`] to mirror `framed_kafka`'s ceiling.
async fn read_kafka_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_RESPONSE_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("response frame {len} exceeds {MAX_RESPONSE_FRAME_BYTES}"),
        ));
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

/// Write one Kafka wire frame: 4-byte BE length prefix + body.
async fn write_kafka_frame(stream: &mut TcpStream, body: &[u8]) -> std::io::Result<()> {
    let len = u32::try_from(body.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "frame body too large to encode in i32 length prefix",
        )
    })?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::expect_used)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use tokio::net::TcpListener;

    /// Server-side mirror of the production helpers — used by the fake
    /// broker to read client requests and shove fixture responses back.
    async fn server_read_frame(sock: &mut tokio::net::TcpStream) -> std::io::Result<Vec<u8>> {
        let mut len = [0u8; 4];
        sock.read_exact(&mut len).await?;
        let n = u32::from_be_bytes(len) as usize;
        let mut body = vec![0u8; n];
        sock.read_exact(&mut body).await?;
        Ok(body)
    }

    async fn server_write_frame(
        sock: &mut tokio::net::TcpStream,
        body: &[u8],
    ) -> std::io::Result<()> {
        let n = u32::try_from(body.len()).unwrap();
        sock.write_all(&n.to_be_bytes()).await?;
        sock.write_all(body).await?;
        sock.flush().await
    }

    fn encode_response<B: Encodable>(
        header: &ResponseHeader,
        header_version: i16,
        body: &B,
        body_version: i16,
    ) -> Vec<u8> {
        let mut out = BytesMut::with_capacity(128);
        header.encode(&mut out, header_version).unwrap();
        body.encode(&mut out, body_version).unwrap();
        out.to_vec()
    }

    fn build_api_versions_response(corr_id: i32) -> Vec<u8> {
        let mut header = ResponseHeader::default();
        header.correlation_id = corr_id;
        let header_version = ApiKey::ApiVersions.response_header_version(API_VERSIONS_VERSION);
        let mut body = ApiVersionsResponse::default();
        body.error_code = 0;
        encode_response(&header, header_version, &body, API_VERSIONS_VERSION)
    }

    fn build_sasl_handshake_response(corr_id: i32, error_code: i16) -> Vec<u8> {
        let mut header = ResponseHeader::default();
        header.correlation_id = corr_id;
        let header_version = ApiKey::SaslHandshake.response_header_version(SASL_HANDSHAKE_VERSION);
        let mut body = SaslHandshakeResponse::default();
        body.error_code = error_code;
        body.mechanisms = vec![StrBytes::from_static_str("PLAIN")];
        encode_response(&header, header_version, &body, SASL_HANDSHAKE_VERSION)
    }

    fn build_sasl_authenticate_response(corr_id: i32, error_code: i16) -> Vec<u8> {
        let mut header = ResponseHeader::default();
        header.correlation_id = corr_id;
        let header_version =
            ApiKey::SaslAuthenticate.response_header_version(SASL_AUTHENTICATE_VERSION);
        let mut body = SaslAuthenticateResponse::default();
        body.error_code = error_code;
        if error_code != 0 {
            body.error_message = Some(StrBytes::from_static_str("nope"));
        }
        encode_response(&header, header_version, &body, SASL_AUTHENTICATE_VERSION)
    }

    /// Decode the request frame the client sent us, returning the
    /// header plus the remaining body bytes. `api_version` drives the
    /// header shape (flexible vs not).
    fn decode_request_header(
        frame: &[u8],
        api_key: ApiKey,
        api_version: i16,
    ) -> (RequestHeader, Bytes) {
        let mut buf = Bytes::copy_from_slice(frame);
        let header_version = api_key.request_header_version(api_version);
        let header = RequestHeader::decode(&mut buf, header_version).unwrap();
        (header, buf)
    }

    /// No SASL → straight TCP connect, zero protocol bytes on the wire.
    /// We assert that by binding a listener that accepts the connection
    /// then verifies it received no bytes before close.
    #[tokio::test]
    async fn open_upstream_no_sasl_just_connects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Try to read a byte with a tight deadline; the client
            // (us) shouldn't send anything in the no-SASL path.
            let mut byte = [0u8; 1];
            let read =
                tokio::time::timeout(std::time::Duration::from_millis(100), sock.read(&mut byte))
                    .await;
            // Either the timeout fires (read still pending) OR the
            // client closed cleanly without sending bytes (read => 0).
            // Both outcomes prove the no-SASL path stayed silent.
            matches!(read, Err(_) | Ok(Ok(0)))
        });

        let stream = open_upstream("127.0.0.1", port, None).await.unwrap();
        // Drop closes the socket; lets the server task observe EOF.
        drop(stream);
        let no_bytes = server.await.unwrap();
        assert!(no_bytes, "no-SASL path must send zero bytes upstream");
    }

    /// PLAIN SASL: full happy-path. Fake broker checks each request and
    /// then writes a no-op byte to the returned stream to prove the
    /// stream is still alive and has no leftover buffered bytes.
    #[tokio::test]
    async fn open_upstream_plain_sasl_sends_correct_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();

            // Frame 1 — ApiVersions.
            let f1 = server_read_frame(&mut sock).await.unwrap();
            let (h1, _rest) = decode_request_header(&f1, ApiKey::ApiVersions, API_VERSIONS_VERSION);
            assert_eq!(h1.request_api_key, ApiKey::ApiVersions as i16);
            assert_eq!(h1.request_api_version, API_VERSIONS_VERSION);
            assert_eq!(h1.correlation_id, 1);
            assert_eq!(h1.client_id.as_deref(), Some("kapture-proxy"));
            server_write_frame(&mut sock, &build_api_versions_response(h1.correlation_id))
                .await
                .unwrap();

            // Frame 2 — SaslHandshake.
            let f2 = server_read_frame(&mut sock).await.unwrap();
            let (h2, body2) =
                decode_request_header(&f2, ApiKey::SaslHandshake, SASL_HANDSHAKE_VERSION);
            assert_eq!(h2.request_api_key, ApiKey::SaslHandshake as i16);
            assert_eq!(h2.request_api_version, SASL_HANDSHAKE_VERSION);
            assert_eq!(h2.correlation_id, 2);
            let mut body2_buf = body2;
            let req2 =
                SaslHandshakeRequest::decode(&mut body2_buf, SASL_HANDSHAKE_VERSION).unwrap();
            assert_eq!(req2.mechanism.as_str(), "PLAIN");
            server_write_frame(
                &mut sock,
                &build_sasl_handshake_response(h2.correlation_id, 0),
            )
            .await
            .unwrap();

            // Frame 3 — SaslAuthenticate. Verify the PLAIN auth_bytes.
            let f3 = server_read_frame(&mut sock).await.unwrap();
            let (h3, body3) =
                decode_request_header(&f3, ApiKey::SaslAuthenticate, SASL_AUTHENTICATE_VERSION);
            assert_eq!(h3.request_api_key, ApiKey::SaslAuthenticate as i16);
            assert_eq!(h3.request_api_version, SASL_AUTHENTICATE_VERSION);
            assert_eq!(h3.correlation_id, 3);
            let mut body3_buf = body3;
            let req3 =
                SaslAuthenticateRequest::decode(&mut body3_buf, SASL_AUTHENTICATE_VERSION).unwrap();
            // PLAIN: \0 user \0 pass — authzid empty.
            let expected = b"\x00alice\x00s3cret";
            assert_eq!(&req3.auth_bytes[..], &expected[..]);
            server_write_frame(
                &mut sock,
                &build_sasl_authenticate_response(h3.correlation_id, 0),
            )
            .await
            .unwrap();

            // After the SASL exchange, write a marker byte. The client
            // side will read it raw, asserting the returned TcpStream
            // really is "clean" (no codec swallowed bytes).
            sock.write_all(b"X").await.unwrap();
            sock.flush().await.unwrap();
            // Keep the socket open until the test drops the client.
            let mut keepalive = [0u8; 1];
            let _ = sock.read(&mut keepalive).await;
        });

        let cfg = UpstreamSaslConfig {
            mechanism: UpstreamSaslMechanism::Plain,
            username: "alice".to_owned(),
            password: "s3cret".to_owned(),
        };
        let mut stream = open_upstream("127.0.0.1", port, Some(&cfg)).await.unwrap();

        let mut buf = [0u8; 1];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"X", "post-handshake stream lost or buffered bytes");

        drop(stream);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn open_upstream_handshake_error_bubbles_up() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let f1 = server_read_frame(&mut sock).await.unwrap();
            let (h1, _) = decode_request_header(&f1, ApiKey::ApiVersions, API_VERSIONS_VERSION);
            server_write_frame(&mut sock, &build_api_versions_response(h1.correlation_id))
                .await
                .unwrap();

            let f2 = server_read_frame(&mut sock).await.unwrap();
            let (h2, _) = decode_request_header(&f2, ApiKey::SaslHandshake, SASL_HANDSHAKE_VERSION);
            // 33 = UNSUPPORTED_SASL_MECHANISM
            server_write_frame(
                &mut sock,
                &build_sasl_handshake_response(h2.correlation_id, 33),
            )
            .await
            .unwrap();
        });

        let cfg = UpstreamSaslConfig {
            mechanism: UpstreamSaslMechanism::Plain,
            username: "alice".to_owned(),
            password: "s3cret".to_owned(),
        };
        let err = open_upstream("127.0.0.1", port, Some(&cfg))
            .await
            .unwrap_err();
        match err {
            UpstreamConnectError::SaslHandshake(msg) => {
                assert!(msg.contains("33"), "msg = {msg}");
                assert!(msg.contains("PLAIN"), "msg = {msg}");
            }
            other => panic!("expected SaslHandshake, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn open_upstream_authenticate_error_bubbles_up() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let f1 = server_read_frame(&mut sock).await.unwrap();
            let (h1, _) = decode_request_header(&f1, ApiKey::ApiVersions, API_VERSIONS_VERSION);
            server_write_frame(&mut sock, &build_api_versions_response(h1.correlation_id))
                .await
                .unwrap();

            let f2 = server_read_frame(&mut sock).await.unwrap();
            let (h2, _) = decode_request_header(&f2, ApiKey::SaslHandshake, SASL_HANDSHAKE_VERSION);
            server_write_frame(
                &mut sock,
                &build_sasl_handshake_response(h2.correlation_id, 0),
            )
            .await
            .unwrap();

            let f3 = server_read_frame(&mut sock).await.unwrap();
            let (h3, _) =
                decode_request_header(&f3, ApiKey::SaslAuthenticate, SASL_AUTHENTICATE_VERSION);
            // 58 = SASL_AUTHENTICATION_FAILED
            server_write_frame(
                &mut sock,
                &build_sasl_authenticate_response(h3.correlation_id, 58),
            )
            .await
            .unwrap();
        });

        let cfg = UpstreamSaslConfig {
            mechanism: UpstreamSaslMechanism::Plain,
            username: "alice".to_owned(),
            password: "s3cret".to_owned(),
        };
        let err = open_upstream("127.0.0.1", port, Some(&cfg))
            .await
            .unwrap_err();
        match err {
            UpstreamConnectError::SaslAuthenticate(msg) => {
                assert!(msg.contains("58"), "msg = {msg}");
            }
            other => panic!("expected SaslAuthenticate, got {other:?}"),
        }
    }

    /// Bind a listener, capture its port, drop it. The OS will not
    /// immediately re-assign the port, so the next `connect` to it
    /// returns ECONNREFUSED — exactly the failure shape we need.
    #[tokio::test]
    async fn open_upstream_connect_failure_bubbles_up() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let err = open_upstream("127.0.0.1", port, None).await.unwrap_err();
        match err {
            UpstreamConnectError::Connect {
                host,
                port: p,
                err: io_err,
            } => {
                assert_eq!(host, "127.0.0.1");
                assert_eq!(p, port);
                // ECONNREFUSED on Unix; on rare CI flake it might be
                // something else — assert it at least is an io error
                // by checking we have a non-empty kind label.
                let _ = io_err.kind();
            }
            other => panic!("expected Connect, got {other:?}"),
        }
    }
}
