//! Read-only upstream probe used by the GUI's "Test" button and the
//! matching MCP `kapture_test_upstream` tool.
//!
//! Distinct from [`super::open_upstream`]: the probe runs the same
//! TLS / SASL handshake the proxy would, then sends a single
//! `ApiVersionsRequest` v3 (matches modern librdkafka) so we can
//! report something more useful than "TCP connect succeeded" — the
//! count of API keys the broker advertises proves both the
//! handshake and the protocol layer round-trip cleanly.
//!
//! Fully ephemeral: does not claim the proxy slot, opens no
//! listening sockets, mutates no `AppState`. The caller is expected
//! to apply the connect + handshake timeout (5 s today).

use bytes::Bytes;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, ResponseHeader};
use kafka_protocol::protocol::{Decodable, StrBytes};
use tokio::io::{AsyncRead, AsyncWrite};

use super::{
    encode_request, make_request_header, open_upstream, read_kafka_frame, write_kafka_frame,
    UpstreamConnectError, UpstreamSaslConfig, UpstreamTlsConfig,
};

/// Outcome of [`test_upstream`]. `api_versions_count` is the size of
/// `ApiVersionsResponse.api_keys` returned by the broker — only
/// populated when the broker responded `error_code == 0`.
#[derive(Debug)]
pub struct UpstreamTestOutcome {
    pub api_versions_count: usize,
    pub api_versions_version: i16,
}

/// `ApiVersionsRequest` version used for the probe. v3 matches what
/// modern librdkafka sends; it added `client_software_name` /
/// `client_software_version` and switched both header and body to
/// flexible (compact) encoding.
const PROBE_API_VERSIONS_VERSION: i16 = 3;

/// Open a fresh upstream connection, drive the same handshake as
/// [`open_upstream`] (TLS if `tls`, SASL if `sasl`), exchange a single
/// `ApiVersionsRequest` v3, then close.
///
/// # Errors
/// See [`UpstreamConnectError`].
pub async fn test_upstream(
    host: &str,
    port: u16,
    tls: Option<&UpstreamTlsConfig>,
    sasl: Option<&UpstreamSaslConfig>,
) -> Result<UpstreamTestOutcome, UpstreamConnectError> {
    let mut stream = open_upstream(host, port, tls, sasl).await?;
    // High correlation id so it can't collide with the SASL exchange's
    // 1..=4. The handshake uses corr_ids starting at 1.
    let corr_id: i32 = 1000;
    send_probe(&mut stream, corr_id).await?;
    let count = recv_probe(&mut stream, corr_id).await?;
    drop(stream);
    Ok(UpstreamTestOutcome {
        api_versions_count: count,
        api_versions_version: PROBE_API_VERSIONS_VERSION,
    })
}

async fn send_probe<S>(stream: &mut S, corr_id: i32) -> Result<(), UpstreamConnectError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let header = make_request_header(ApiKey::ApiVersions, PROBE_API_VERSIONS_VERSION, corr_id);
    let mut body = ApiVersionsRequest::default();
    body.client_software_name = StrBytes::from_static_str("kapture");
    body.client_software_version = StrBytes::from_static_str(env!("CARGO_PKG_VERSION"));
    let frame = encode_request(
        &header,
        ApiKey::ApiVersions.request_header_version(PROBE_API_VERSIONS_VERSION),
        &body,
        PROBE_API_VERSIONS_VERSION,
    )
    .map_err(|e| UpstreamConnectError::ApiVersions(format!("encode v3: {e}")))?;
    write_kafka_frame(stream, &frame).await?;
    Ok(())
}

async fn recv_probe<S>(stream: &mut S, expected_corr_id: i32) -> Result<usize, UpstreamConnectError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = read_kafka_frame(stream).await?;
    let mut buf = Bytes::from(frame);
    let header_version = ApiKey::ApiVersions.response_header_version(PROBE_API_VERSIONS_VERSION);
    let header = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| UpstreamConnectError::ApiVersions(format!("header decode v3: {e}")))?;
    if header.correlation_id != expected_corr_id {
        return Err(UpstreamConnectError::ApiVersions(format!(
            "unexpected correlation_id {} (want {})",
            header.correlation_id, expected_corr_id
        )));
    }
    let resp = ApiVersionsResponse::decode(&mut buf, PROBE_API_VERSIONS_VERSION)
        .map_err(|e| UpstreamConnectError::ApiVersions(format!("body decode v3: {e}")))?;
    if resp.error_code != 0 {
        return Err(UpstreamConnectError::ApiVersions(format!(
            "broker returned error_code={}",
            resp.error_code
        )));
    }
    Ok(resp.api_keys.len())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::proxy_upstream::test_support::{
        decode_request_header, server_read_frame, server_write_frame,
    };
    use bytes::BytesMut;
    use kafka_protocol::messages::api_versions_response::ApiVersion;
    use kafka_protocol::protocol::Encodable;
    use tokio::net::TcpListener;

    /// Happy path: fake broker accepts the v3 request and replies with
    /// two API keys. We assert the count round-trips.
    #[tokio::test]
    async fn test_upstream_returns_api_versions_count() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let frame = server_read_frame(&mut sock).await.unwrap();
            let (header, _rest) = decode_request_header(&frame, ApiKey::ApiVersions, 3);
            assert_eq!(header.request_api_version, 3);

            let mut resp_header = ResponseHeader::default();
            resp_header.correlation_id = header.correlation_id;
            let mut body = ApiVersionsResponse::default();
            body.error_code = 0;
            let mut k1 = ApiVersion::default();
            k1.api_key = 0;
            k1.min_version = 0;
            k1.max_version = 9;
            let mut k2 = ApiVersion::default();
            k2.api_key = 1;
            k2.min_version = 0;
            k2.max_version = 13;
            body.api_keys = vec![k1, k2];
            // ApiVersions response header is v0 (non-flexible) even at
            // body version 3 — known Kafka quirk; the kafka-protocol
            // crate's `response_header_version` returns 0.
            let resp_header_version = ApiKey::ApiVersions.response_header_version(3);
            let mut out = BytesMut::with_capacity(64);
            resp_header.encode(&mut out, resp_header_version).unwrap();
            body.encode(&mut out, 3).unwrap();
            server_write_frame(&mut sock, &out).await.unwrap();
        });

        let outcome = test_upstream("127.0.0.1", port, None, None).await.unwrap();
        assert_eq!(outcome.api_versions_count, 2);
        assert_eq!(outcome.api_versions_version, 3);
        server.await.unwrap();
    }

    /// Connect failure surfaces as `UpstreamConnectError::Connect`.
    #[tokio::test]
    async fn test_upstream_connect_failure_bubbles_up() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        match test_upstream("127.0.0.1", port, None, None).await {
            Err(UpstreamConnectError::Connect { .. }) => {}
            other => panic!("expected Connect error, got {other:?}"),
        }
    }
}
