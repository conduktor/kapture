//! Server-side test helpers shared by `mod.rs` and `tls.rs` tests.
//!
//! All of this is `#[cfg(test)]` only and exists so the TLS tests in
//! `tls.rs` can drive the same fake-broker SASL exchange that the
//! plain-TCP tests use, without duplicating the encoder logic.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsResponse, RequestHeader, ResponseHeader, SaslAuthenticateResponse,
    SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const API_VERSIONS_VERSION: i16 = 0;
pub const SASL_HANDSHAKE_VERSION: i16 = 1;
pub const SASL_AUTHENTICATE_VERSION: i16 = 2;

pub async fn server_read_frame<S>(sock: &mut S) -> std::io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut len = [0u8; 4];
    sock.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    let mut body = vec![0u8; n];
    sock.read_exact(&mut body).await?;
    Ok(body)
}

pub async fn server_write_frame<S>(sock: &mut S, body: &[u8]) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let n = u32::try_from(body.len()).unwrap();
    sock.write_all(&n.to_be_bytes()).await?;
    sock.write_all(body).await?;
    sock.flush().await
}

pub fn encode_response<B: Encodable>(
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

pub fn build_api_versions_response(corr_id: i32) -> Vec<u8> {
    let mut header = ResponseHeader::default();
    header.correlation_id = corr_id;
    let header_version = ApiKey::ApiVersions.response_header_version(API_VERSIONS_VERSION);
    let mut body = ApiVersionsResponse::default();
    body.error_code = 0;
    encode_response(&header, header_version, &body, API_VERSIONS_VERSION)
}

pub fn build_sasl_handshake_response(corr_id: i32, error_code: i16) -> Vec<u8> {
    let mut header = ResponseHeader::default();
    header.correlation_id = corr_id;
    let header_version = ApiKey::SaslHandshake.response_header_version(SASL_HANDSHAKE_VERSION);
    let mut body = SaslHandshakeResponse::default();
    body.error_code = error_code;
    body.mechanisms = vec![StrBytes::from_static_str("PLAIN")];
    encode_response(&header, header_version, &body, SASL_HANDSHAKE_VERSION)
}

pub fn build_sasl_authenticate_response(corr_id: i32, error_code: i16) -> Vec<u8> {
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

pub fn decode_request_header(
    frame: &[u8],
    api_key: ApiKey,
    api_version: i16,
) -> (RequestHeader, Bytes) {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = api_key.request_header_version(api_version);
    let header = RequestHeader::decode(&mut buf, header_version).unwrap();
    (header, buf)
}

/// SASL fake-broker logic (PLAIN happy path). Generic over the stream
/// so it works on either a raw `TcpStream` or a `TlsStream<TcpStream>`.
/// Writes a marker byte `b"X"` after the handshake completes to let
/// the client verify the post-handshake stream isn't buffered.
pub async fn fake_broker_plain_sasl<S>(sock: &mut S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let f1 = server_read_frame(sock).await.unwrap();
    let (h1, _) = decode_request_header(&f1, ApiKey::ApiVersions, API_VERSIONS_VERSION);
    server_write_frame(sock, &build_api_versions_response(h1.correlation_id))
        .await
        .unwrap();

    let f2 = server_read_frame(sock).await.unwrap();
    let (h2, _) = decode_request_header(&f2, ApiKey::SaslHandshake, SASL_HANDSHAKE_VERSION);
    server_write_frame(sock, &build_sasl_handshake_response(h2.correlation_id, 0))
        .await
        .unwrap();

    let f3 = server_read_frame(sock).await.unwrap();
    let (h3, _) = decode_request_header(&f3, ApiKey::SaslAuthenticate, SASL_AUTHENTICATE_VERSION);
    server_write_frame(
        sock,
        &build_sasl_authenticate_response(h3.correlation_id, 0),
    )
    .await
    .unwrap();

    sock.write_all(b"X").await.unwrap();
    sock.flush().await.unwrap();
}
