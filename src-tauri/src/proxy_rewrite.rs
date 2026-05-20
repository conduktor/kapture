//! Rewrite Kafka responses that carry broker / coordinator host:port
//! so a client routing on those addresses comes back through Kapture
//! instead of bypassing us.
//!
//! Five verbs need rewriting:
//!   - `MetadataResponse`        (api key 3)
//!   - `FindCoordinatorResponse` (api key 10)
//!   - `DescribeClusterResponse` (api key 60)
//!   - `ProduceResponse` v10+    (api key 0,  KIP-951 `node_endpoints[]`)
//!   - `FetchResponse`   v16+    (api key 1,  KIP-951 `node_endpoints[]`)
//!
//! Produce/Fetch in v10+/v16+ embed an optional `node_endpoints[]`
//! array carrying redirect targets (follower fetch, tiered storage,
//! leader moves). Without rewriting those, modern Java clients —
//! Kafka Streams in particular — reconnect DIRECTLY to the upstream
//! broker advertised in the redirect, silently bypassing the proxy
//! for that partition's traffic.
//!
//! All other responses are forwarded verbatim — they reference
//! brokers by `node_id` only and the client resolves the address
//! via the (already-rewritten) Metadata cache.

#![allow(clippy::wildcard_imports)]

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, DescribeClusterResponse, FetchResponse, FindCoordinatorResponse, MetadataResponse,
    ProduceResponse, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

use crate::proxy_provisioner::BrokerProvisioner;
use crate::proxy_topic_ids::TopicIdMap;

/// Try to rewrite a response frame body so its broker / coordinator
/// addresses point at our local proxy listeners.
///
/// `frame` is the raw response *body* as it came off the wire,
/// **without** the 4-byte length prefix (the codec already stripped
/// it). The first 4 bytes are the `correlation_id`, followed by the
/// optional response-header tagged fields, followed by the response
/// payload. The original response header is preserved when encoding so
/// flexible-version unknown tagged fields are not silently stripped.
///
/// Returns `Ok(Some(rewritten_bytes))` on a successful rewrite,
/// `Ok(None)` if the API doesn't need rewriting or the buffer was
/// untouched, `Err(_)` on decode/encode failure (caller logs and
/// forwards verbatim — never silently drop frames).
pub async fn rewrite_response(
    api_key: i16,
    api_version: i16,
    frame: &[u8],
    provisioner: &dyn BrokerProvisioner,
    topic_ids: &TopicIdMap,
) -> Result<Option<Bytes>, RewriteError> {
    let Ok(api) = ApiKey::try_from(api_key) else {
        return Ok(None);
    };
    match api {
        ApiKey::Metadata => rewrite_metadata(api_version, frame, provisioner, topic_ids).await,
        ApiKey::FindCoordinator => rewrite_find_coordinator(api_version, frame, provisioner).await,
        ApiKey::DescribeCluster => rewrite_describe_cluster(api_version, frame, provisioner).await,
        // KIP-951 (Apache Kafka 3.6+): ProduceResponse v10+ and
        // FetchResponse v16+ carry `node_endpoints[]` so the broker
        // can redirect the client to a different node for that
        // partition (follower fetch, tiered storage, leader
        // moves). If we don't rewrite these to local listener
        // ports, the client reconnects DIRECTLY to the upstream and
        // bypasses Kapture for that partition — silently. Kafka
        // Streams hits this whenever its internal consumer follows
        // a redirect for streams-output / changelog topics.
        ApiKey::Produce if api_version >= 10 => {
            rewrite_produce(api_version, frame, provisioner).await
        }
        ApiKey::Fetch if api_version >= 16 => rewrite_fetch(api_version, frame, provisioner).await,
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
    provisioner: &dyn BrokerProvisioner,
    topic_ids: &TopicIdMap,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::Metadata.response_header_version(version);
    let header = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| RewriteError::Decode(format!("metadata header: {e}")))?;
    let mut resp = MetadataResponse::decode(&mut buf, version)
        .map_err(|e| RewriteError::Decode(format!("metadata body: {e}")))?;

    // Stash any (topic_id, name) pairs for later Fetch v13+ resolution.
    // Pre-v10 Metadata leaves topic_id at Uuid::nil() and TopicIdMap::record
    // skips that, so this is safe across all versions.
    for topic in &resp.topics {
        if let Some(name) = topic.name.as_ref() {
            topic_ids.record(topic.topic_id, name.0.to_string());
        }
    }

    for broker in &mut resp.brokers {
        let host = broker.host.to_string();
        let Ok(port) = u16::try_from(broker.port) else {
            continue;
        };
        if port == 0 {
            continue;
        }
        let local = provisioner
            .ensure(&host, port)
            .await
            .map_err(|err| RewriteError::Bind {
                host: host.clone(),
                port,
                err,
            })?;
        broker.host = StrBytes::from_string("127.0.0.1".to_owned());
        broker.port = i32::from(local);
    }

    encode_response(version, &resp, ApiKey::Metadata, &header)
}

async fn rewrite_find_coordinator(
    version: i16,
    frame: &[u8],
    provisioner: &dyn BrokerProvisioner,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::FindCoordinator.response_header_version(version);
    let header = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| RewriteError::Decode(format!("find_coord header: {e}")))?;
    let mut resp = FindCoordinatorResponse::decode(&mut buf, version)
        .map_err(|e| RewriteError::Decode(format!("find_coord body: {e}")))?;

    if version <= 3 {
        // Single coordinator at top level.
        let host = resp.host.to_string();
        if let Ok(port) = u16::try_from(resp.port) {
            if port != 0 {
                let local =
                    provisioner
                        .ensure(&host, port)
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
            let local =
                provisioner
                    .ensure(&host, port)
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

    encode_response(version, &resp, ApiKey::FindCoordinator, &header)
}

/// Rewrite KIP-951 `node_endpoints[]` redirects in a
/// `ProduceResponse` (v10+). The `records` payload itself is left
/// untouched — only the small endpoints array is decoded /
/// re-encoded. Returns `Ok(None)` when the response carries no
/// endpoints, so the caller forwards the original bytes verbatim
/// (no decode round-trip cost).
async fn rewrite_produce(
    version: i16,
    frame: &[u8],
    provisioner: &dyn BrokerProvisioner,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::Produce.response_header_version(version);
    let header = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| RewriteError::Decode(format!("produce header: {e}")))?;
    let mut resp = ProduceResponse::decode(&mut buf, version)
        .map_err(|e| RewriteError::Decode(format!("produce body: {e}")))?;
    if resp.node_endpoints.is_empty() {
        return Ok(None);
    }
    for n in &mut resp.node_endpoints {
        let host = n.host.to_string();
        let Ok(port) = u16::try_from(n.port) else {
            continue;
        };
        if port == 0 {
            continue;
        }
        let local = provisioner
            .ensure(&host, port)
            .await
            .map_err(|err| RewriteError::Bind {
                host: host.clone(),
                port,
                err,
            })?;
        n.host = StrBytes::from_string("127.0.0.1".to_owned());
        n.port = i32::from(local);
    }
    encode_response(version, &resp, ApiKey::Produce, &header)
}

/// Same shape as [`rewrite_produce`] but for `FetchResponse` v16+.
/// The records-bearing fields are kept as opaque `Bytes` by the
/// `kafka-protocol` crate, so re-encoding is cheap.
async fn rewrite_fetch(
    version: i16,
    frame: &[u8],
    provisioner: &dyn BrokerProvisioner,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::Fetch.response_header_version(version);
    let header = ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| RewriteError::Decode(format!("fetch header: {e}")))?;
    let mut resp = FetchResponse::decode(&mut buf, version)
        .map_err(|e| RewriteError::Decode(format!("fetch body: {e}")))?;
    if resp.node_endpoints.is_empty() {
        return Ok(None);
    }
    for n in &mut resp.node_endpoints {
        let host = n.host.to_string();
        let Ok(port) = u16::try_from(n.port) else {
            continue;
        };
        if port == 0 {
            continue;
        }
        let local = provisioner
            .ensure(&host, port)
            .await
            .map_err(|err| RewriteError::Bind {
                host: host.clone(),
                port,
                err,
            })?;
        n.host = StrBytes::from_string("127.0.0.1".to_owned());
        n.port = i32::from(local);
    }
    encode_response(version, &resp, ApiKey::Fetch, &header)
}

async fn rewrite_describe_cluster(
    version: i16,
    frame: &[u8],
    provisioner: &dyn BrokerProvisioner,
) -> Result<Option<Bytes>, RewriteError> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ApiKey::DescribeCluster.response_header_version(version);
    let header = ResponseHeader::decode(&mut buf, header_version)
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
        let local = provisioner
            .ensure(&host, port)
            .await
            .map_err(|err| RewriteError::Bind {
                host: host.clone(),
                port,
                err,
            })?;
        b.host = StrBytes::from_string("127.0.0.1".to_owned());
        b.port = i32::from(local);
    }

    encode_response(version, &resp, ApiKey::DescribeCluster, &header)
}

fn encode_response<T: Encodable>(
    version: i16,
    msg: &T,
    api: ApiKey,
    header: &ResponseHeader,
) -> Result<Option<Bytes>, RewriteError> {
    let header_version = api.response_header_version(version);
    let mut out = BytesMut::with_capacity(256);
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
    use crate::proxy_broker_map::BrokerMap;
    use async_trait::async_trait;
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponseTopic,
    };
    use kafka_protocol::messages::{BrokerId, TopicName};
    use std::io;
    use uuid::Uuid;

    struct StaticProvisioner;

    #[async_trait]
    impl BrokerProvisioner for StaticProvisioner {
        async fn ensure(&self, _host: &str, _port: u16) -> io::Result<u16> {
            Ok(19092)
        }
    }

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
        let topic_ids = TopicIdMap::new();
        let rewritten = rewrite_response(3, 12, &bytes, &map, &topic_ids)
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
        let topic_ids = TopicIdMap::new();
        let result = rewrite_response(0, 9, &[0u8; 16], &map, &topic_ids)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn rewrite_metadata_records_topic_ids() {
        // Build a Metadata v12 response with two topics, each with a
        // distinct topic_id + name. After rewrite, both must land in
        // the TopicIdMap.
        let provisioner = StaticProvisioner;
        let mut resp = MetadataResponse::default();
        // Need at least one broker for the rewriter to walk; rewriting
        // brokers is incidental to this test but exercises the full path.
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(1);
        broker.host = StrBytes::from_string("kafka-mb-1.local".to_owned());
        broker.port = 39092;
        resp.brokers.push(broker);

        let id_a = Uuid::from_u128(0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210);
        let id_b = Uuid::from_u128(0xDEAD_BEEF_CAFE_BABE_0000_1111_2222_3333);

        let mut t_a = MetadataResponseTopic::default();
        t_a.name = Some(TopicName(StrBytes::from_string("alpha".to_owned())));
        t_a.topic_id = id_a;
        let mut t_b = MetadataResponseTopic::default();
        t_b.name = Some(TopicName(StrBytes::from_string("beta".to_owned())));
        t_b.topic_id = id_b;
        resp.topics = vec![t_a, t_b];

        let mut out = BytesMut::new();
        ResponseHeader::default()
            .encode(&mut out, ApiKey::Metadata.response_header_version(12))
            .unwrap();
        resp.encode(&mut out, 12).unwrap();

        let topic_ids = TopicIdMap::new();
        let rewritten = rewrite_response(3, 12, &out, &provisioner, &topic_ids)
            .await
            .unwrap();
        assert!(rewritten.is_some(), "metadata rewrite should produce bytes");

        assert_eq!(topic_ids.lookup(id_a).as_deref(), Some("alpha"));
        assert_eq!(topic_ids.lookup(id_b).as_deref(), Some("beta"));
        assert_eq!(topic_ids.len(), 2);
    }

    #[tokio::test]
    async fn preserves_flexible_response_header_unknown_tags() {
        let provisioner = StaticProvisioner;
        let mut resp = MetadataResponse::default();
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(1);
        broker.host = StrBytes::from_string("kafka-mb-1.local".to_owned());
        broker.port = 39092;
        resp.brokers.push(broker);

        let mut out = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(777)
            .with_unknown_tagged_field(42, Bytes::from_static(b"tagged"))
            .encode(&mut out, ApiKey::Metadata.response_header_version(12))
            .unwrap();
        resp.encode(&mut out, 12).unwrap();

        let topic_ids = TopicIdMap::new();
        let rewritten = rewrite_response(3, 12, &out, &provisioner, &topic_ids)
            .await
            .unwrap()
            .unwrap();
        let mut buf = rewritten;
        let header =
            ResponseHeader::decode(&mut buf, ApiKey::Metadata.response_header_version(12)).unwrap();

        assert_eq!(header.correlation_id, 777);
        assert_eq!(
            header.unknown_tagged_fields.get(&42).map(Bytes::as_ref),
            Some(b"tagged".as_slice()),
        );
    }
}
