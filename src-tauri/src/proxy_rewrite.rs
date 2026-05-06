//! Rewrite Kafka responses that carry broker / coordinator host:port
//! so a client routing on those addresses comes back through Kapture
//! instead of bypassing us.
//!
//! Three verbs need rewriting (see `docs/specs/proxy-mode.md` and
//! the plan for Phase 2):
//!   - `MetadataResponse`        (api key 3)
//!   - `FindCoordinatorResponse` (api key 10)
//!   - `DescribeClusterResponse` (api key 60)
//!
//! All other responses are forwarded verbatim — they reference
//! brokers by `node_id` only and the client resolves the address
//! via the (already-rewritten) Metadata cache.

#![allow(clippy::wildcard_imports)]
// Wired up by Task 15 (pump integration). Until then, the module
// is reachable only from its own tests.
#![allow(dead_code)]

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, DescribeClusterResponse, FindCoordinatorResponse, MetadataResponse, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

use crate::proxy::BrokerMap;

/// Try to rewrite a response frame body so its broker / coordinator
/// addresses point at our local proxy listeners.
///
/// `frame` is the raw response *body* as it came off the wire,
/// **without** the 4-byte length prefix (the codec already stripped
/// it). The first 4 bytes are the `correlation_id`, followed by the
/// optional response-header tagged fields, followed by the response
/// payload.
///
/// Returns `Ok(Some(rewritten_bytes))` on a successful rewrite,
/// `Ok(None)` if the API doesn't need rewriting or the buffer was
/// untouched, `Err(_)` on decode/encode failure (caller logs and
/// forwards verbatim — never silently drop frames).
pub async fn rewrite_response(
    api_key: i16,
    api_version: i16,
    frame: &[u8],
    broker_map: &BrokerMap,
) -> Result<Option<Bytes>, RewriteError> {
    let Ok(api) = ApiKey::try_from(api_key) else {
        return Ok(None);
    };
    match api {
        ApiKey::Metadata => rewrite_metadata(api_version, frame, broker_map).await,
        ApiKey::FindCoordinator => rewrite_find_coordinator(api_version, frame, broker_map).await,
        ApiKey::DescribeCluster => rewrite_describe_cluster(api_version, frame, broker_map).await,
        _ => Ok(None),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RewriteError {
    #[error("decode failed: {0}")]
    Decode(String),
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("listener bind failed for {host}:{port}: {err}")]
    Bind {
        host: String,
        port: u16,
        err: std::io::Error,
    },
}

async fn rewrite_metadata(
    version: i16,
    frame: &[u8],
    broker_map: &BrokerMap,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::Metadata.response_header_version(version);
    let _hdr = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| RewriteError::Decode(format!("metadata header: {e}")))?;
    let mut resp = MetadataResponse::decode(&mut buf, version)
        .map_err(|e| RewriteError::Decode(format!("metadata body: {e}")))?;

    for broker in &mut resp.brokers {
        let host = broker.host.to_string();
        let Ok(port) = u16::try_from(broker.port) else {
            continue;
        };
        if port == 0 {
            continue;
        }
        let local = broker_map
            .ensure_listener(&host, port)
            .await
            .map_err(|err| RewriteError::Bind {
                host: host.clone(),
                port,
                err,
            })?;
        broker.host = StrBytes::from_string("127.0.0.1".to_owned());
        broker.port = i32::from(local);
    }

    encode_response(version, &resp, ApiKey::Metadata)
}

async fn rewrite_find_coordinator(
    version: i16,
    frame: &[u8],
    broker_map: &BrokerMap,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::FindCoordinator.response_header_version(version);
    let _hdr = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| RewriteError::Decode(format!("find_coord header: {e}")))?;
    let mut resp = FindCoordinatorResponse::decode(&mut buf, version)
        .map_err(|e| RewriteError::Decode(format!("find_coord body: {e}")))?;

    if version <= 3 {
        // Single coordinator at top level.
        let host = resp.host.to_string();
        if let Ok(port) = u16::try_from(resp.port) {
            if port != 0 {
                let local = broker_map
                    .ensure_listener(&host, port)
                    .await
                    .map_err(|err| RewriteError::Bind {
                        host: host.clone(),
                        port,
                        err,
                    })?;
                resp.host = StrBytes::from_string("127.0.0.1".to_owned());
                resp.port = i32::from(local);
            }
        }
    } else {
        for c in &mut resp.coordinators {
            let host = c.host.to_string();
            let Ok(port) = u16::try_from(c.port) else {
                continue;
            };
            if port == 0 {
                continue;
            }
            let local = broker_map
                .ensure_listener(&host, port)
                .await
                .map_err(|err| RewriteError::Bind {
                    host: host.clone(),
                    port,
                    err,
                })?;
            c.host = StrBytes::from_string("127.0.0.1".to_owned());
            c.port = i32::from(local);
        }
    }

    encode_response(version, &resp, ApiKey::FindCoordinator)
}

async fn rewrite_describe_cluster(
    version: i16,
    frame: &[u8],
    broker_map: &BrokerMap,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::DescribeCluster.response_header_version(version);
    let _hdr = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| RewriteError::Decode(format!("describe_cluster header: {e}")))?;
    let mut resp = DescribeClusterResponse::decode(&mut buf, version)
        .map_err(|e| RewriteError::Decode(format!("describe_cluster body: {e}")))?;

    for b in &mut resp.brokers {
        let host = b.host.to_string();
        let Ok(port) = u16::try_from(b.port) else {
            continue;
        };
        if port == 0 {
            continue;
        }
        let local = broker_map
            .ensure_listener(&host, port)
            .await
            .map_err(|err| RewriteError::Bind {
                host: host.clone(),
                port,
                err,
            })?;
        b.host = StrBytes::from_string("127.0.0.1".to_owned());
        b.port = i32::from(local);
    }

    encode_response(version, &resp, ApiKey::DescribeCluster)
}

fn encode_response<T: Encodable>(
    version: i16,
    msg: &T,
    api: ApiKey,
) -> Result<Option<Bytes>, RewriteError> {
    let header_version = api.response_header_version(version);
    let mut out = BytesMut::with_capacity(256);
    // ResponseHeader: just the correlation_id and (in flexible
    // versions) tagged fields. We zero corr_id here because the
    // caller will overwrite the first 4 bytes with the real
    // correlation_id before forwarding — that way we don't have to
    // round-trip the header through the body decode.
    let header = ResponseHeader::default();
    header
        .encode(&mut out, header_version)
        .map_err(|e| RewriteError::Encode(format!("header: {e}")))?;
    msg.encode(&mut out, version)
        .map_err(|e| RewriteError::Encode(format!("body: {e}")))?;
    Ok(Some(out.freeze()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
    use kafka_protocol::messages::BrokerId;

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

    #[tokio::test]
    async fn rewrites_metadata_brokers_to_local_listeners() {
        let map = BrokerMap::new();
        // v12 is a flexible version, exercises tagged fields.
        let bytes = build_metadata_response_bytes(
            12,
            vec![
                (1, "kafka-mb-1.local", 39092),
                (2, "kafka-mb-2.local", 39093),
                (3, "kafka-mb-3.local", 39094),
            ],
        );
        let rewritten = rewrite_response(3, 12, &bytes, &map)
            .await
            .unwrap()
            .unwrap();

        // Decode the rewritten bytes and verify each broker host is now 127.0.0.1.
        let mut buf = rewritten;
        let header_version = ApiKey::Metadata.response_header_version(12);
        let _hdr = ResponseHeader::decode(&mut buf, header_version).unwrap();
        let resp = MetadataResponse::decode(&mut buf, 12).unwrap();
        for b in &resp.brokers {
            assert_eq!(b.host.to_string(), "127.0.0.1");
            // Port is the local listener port — must be non-zero
            // and present in the broker map under the original.
            assert!(b.port > 0);
        }
        // Map must contain 3 distinct entries.
        let snapshot = map.snapshot();
        assert_eq!(snapshot.len(), 3);
    }

    #[tokio::test]
    async fn passes_unknown_api_through() {
        let map = BrokerMap::new();
        // Produce response (api key 0) — not in our rewrite set.
        let result = rewrite_response(0, 9, &[0u8; 16], &map).await.unwrap();
        assert!(result.is_none());
    }
}
