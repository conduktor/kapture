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
    ApiKey, ApiVersionsResponse, RequestHeader, ResponseHeader, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeResponse,
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

/// Same as `build_sasl_authenticate_response` but with a populated
/// `auth_bytes` payload — needed for SCRAM where each SCRAM message
/// is carried in this field.
pub fn build_sasl_authenticate_response_with_payload(corr_id: i32, auth_bytes: &[u8]) -> Vec<u8> {
    let mut header = ResponseHeader::default();
    header.correlation_id = corr_id;
    let header_version =
        ApiKey::SaslAuthenticate.response_header_version(SASL_AUTHENTICATE_VERSION);
    let mut body = SaslAuthenticateResponse::default();
    body.error_code = 0;
    body.auth_bytes = Bytes::copy_from_slice(auth_bytes);
    encode_response(&header, header_version, &body, SASL_AUTHENTICATE_VERSION)
}

/// Decode the `auth_bytes` payload of a `SaslAuthenticateRequest` frame.
pub fn decode_sasl_authenticate_request(frame: &[u8]) -> (RequestHeader, Bytes) {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::SaslAuthenticate.request_header_version(SASL_AUTHENTICATE_VERSION);
    let header = RequestHeader::decode(&mut buf, header_version).unwrap();
    let req = SaslAuthenticateRequest::decode(&mut buf, SASL_AUTHENTICATE_VERSION).unwrap();
    (header, req.auth_bytes)
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

/// SASL fake-broker logic (SCRAM-SHA-256 happy path). Generic over
/// the stream so it works on either a raw `TcpStream` or a
/// `TlsStream<TcpStream>`. Uses the production-side SCRAM hash impl
/// (`Sha256Hash`) to verify the client's proof — if the proof
/// doesn't match, the test fails. Writes a marker `b"X"` byte after
/// the handshake completes to let the client verify the
/// post-handshake stream isn't buffered.
pub async fn fake_broker_scram_sha256<S>(
    sock: &mut S,
    expected_username: &str,
    expected_password: &str,
    salt: &[u8],
    iterations: u32,
    server_nonce_appendix: &str,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use base64::Engine;

    use super::scram::{ScramHash, Sha256Hash};

    // ApiVersions.
    let f1 = server_read_frame(sock).await.unwrap();
    let (h1, _) = decode_request_header(&f1, ApiKey::ApiVersions, API_VERSIONS_VERSION);
    server_write_frame(sock, &build_api_versions_response(h1.correlation_id))
        .await
        .unwrap();

    // SaslHandshake.
    let f2 = server_read_frame(sock).await.unwrap();
    let (h2, _) = decode_request_header(&f2, ApiKey::SaslHandshake, SASL_HANDSHAKE_VERSION);
    server_write_frame(sock, &build_sasl_handshake_response(h2.correlation_id, 0))
        .await
        .unwrap();

    // SaslAuthenticate roundtrip 1: read client-first, build server-first.
    let f3 = server_read_frame(sock).await.unwrap();
    let (h3, auth3) = decode_sasl_authenticate_request(&f3);
    let client_first = std::str::from_utf8(&auth3).expect("client-first must be UTF-8");
    assert!(
        client_first.starts_with("n,,n="),
        "expected GS2 header n,,n= got: {client_first}"
    );
    let client_first_bare = client_first.trim_start_matches("n,,");
    let client_nonce = client_first_bare
        .split(',')
        .find_map(|kv| kv.strip_prefix("r="))
        .expect("client-first must carry r=");
    let combined = format!("{client_nonce}{server_nonce_appendix}");
    let salt_b64 = base64::engine::general_purpose::STANDARD.encode(salt);
    let server_first = format!("r={combined},s={salt_b64},i={iterations}");
    server_write_frame(
        sock,
        &build_sasl_authenticate_response_with_payload(h3.correlation_id, server_first.as_bytes()),
    )
    .await
    .unwrap();

    // SaslAuthenticate roundtrip 2: verify proof, build server-final.
    let f4 = server_read_frame(sock).await.unwrap();
    let (h4, auth4) = decode_sasl_authenticate_request(&f4);
    let client_final = std::str::from_utf8(&auth4).expect("client-final must be UTF-8");
    let salted = Sha256Hash::pbkdf2(expected_password.as_bytes(), salt, iterations);
    let client_key = Sha256Hash::hmac(&salted, b"Client Key");
    let stored_key = Sha256Hash::hash(&client_key);
    let auth_message =
        format!("n={expected_username},r={client_nonce},{server_first},c=biws,r={combined}");
    let client_sig = Sha256Hash::hmac(&stored_key, auth_message.as_bytes());
    let expected_proof: Vec<u8> = client_key
        .iter()
        .zip(client_sig.iter())
        .map(|(x, y)| x ^ y)
        .collect();
    let expected_proof_b64 = base64::engine::general_purpose::STANDARD.encode(&expected_proof);
    assert!(
        client_final.contains(&format!("p={expected_proof_b64}")),
        "client-proof mismatch.\n  got: {client_final}\n want p={expected_proof_b64}"
    );

    let server_key = Sha256Hash::hmac(&salted, b"Server Key");
    let server_sig = Sha256Hash::hmac(&server_key, auth_message.as_bytes());
    let server_final = format!(
        "v={}",
        base64::engine::general_purpose::STANDARD.encode(&server_sig)
    );
    server_write_frame(
        sock,
        &build_sasl_authenticate_response_with_payload(h4.correlation_id, server_final.as_bytes()),
    )
    .await
    .unwrap();

    sock.write_all(b"X").await.unwrap();
    sock.flush().await.unwrap();
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
