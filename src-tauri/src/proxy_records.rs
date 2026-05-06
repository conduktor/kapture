//! Extract Kafka records out of Produce request bodies and Fetch
//! response bodies tapped by the proxy pump, and turn them into
//! `CapturedMessage` instances suitable for the Messages tab.
//!
//! Phase 1.5 of proxy mode: Wireshark-style capture only — never
//! mutates the wire bytes, never blocks the pump on parse errors,
//! and silently drops anything it can't decode (returns an empty
//! `Vec`). The forwarded frame is always the original.
//!
//! The `frame` slices fed to these functions are the codec's per-frame
//! output: i.e. the body bytes WITHOUT the 4-byte length prefix the
//! length-delimited codec already stripped on the read side. The
//! request/response header is still in the slice and gets decoded out
//! before reaching the typed message body.

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use kafka_protocol::messages::{
    ApiKey, FetchResponse, ProduceRequest, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, HeaderVersion};
use kafka_protocol::records::RecordBatchDecoder;
use uuid::Uuid;

use crate::correlator::FetchMetadata;
use crate::decode::{decode_payload, render_hex};
use crate::message::{CapturedMessage, KafkaHeader};
use crate::proto_event::ProtoEvent;
use crate::proxy_topic_ids::TopicIdMap;

/// One Kafka record extracted from a Produce request or a Fetch
/// response. Topic + partition come from the surrounding wire envelope;
/// `offset` / `timestamp_ms` come from the record batch itself when
/// available (Fetch responses always have real offsets; Produce
/// requests always have `-1` because the broker hasn't assigned one
/// yet).
#[derive(Debug, Clone)]
pub struct ExtractedRecord {
    pub topic: String,
    pub partition: i32,
    /// `-1` for Produce-side records — the broker assigns it.
    pub offset: i64,
    /// `0` if the record carries no timestamp (legacy / unset).
    pub timestamp_ms: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Option<Bytes>)>,
    /// Fetch response API version that brought this record. `None` for
    /// Produce-side records (sent by the client — no Fetch frame to
    /// link back to).
    pub fetch_api_version: Option<i16>,
    /// Correlation id of the originating Fetch response. `None` for
    /// Produce-side records.
    pub fetch_corr_id: Option<i32>,
    /// Per-TCP-connection id (truncated u64 → i32, mirrors the
    /// `ProtoFrame.connection_id` field) of the originating Fetch
    /// response. `None` for Produce-side records.
    pub fetch_connection_id: Option<i32>,
}

/// Decode the Produce request body sitting in `frame` and pull every
/// record out. Returns an empty `Vec` on any parse / decode failure —
/// we never want a malformed Produce frame to bubble up and kill the
/// proxy pump.
#[must_use]
pub fn extract_from_produce_request(version: i16, frame: &[u8]) -> Vec<ExtractedRecord> {
    extract_from_produce_request_inner(version, frame).unwrap_or_default()
}

fn extract_from_produce_request_inner(version: i16, frame: &[u8]) -> Option<Vec<ExtractedRecord>> {
    let mut buf = Bytes::copy_from_slice(frame);
    let header_version = ProduceRequest::header_version(version);
    let _hdr = RequestHeader::decode(&mut buf, header_version).ok()?;
    let req = ProduceRequest::decode(&mut buf, version).ok()?;

    let mut out = Vec::new();
    for topic in &req.topic_data {
        let topic_name = topic.name.0.to_string();
        for partition in &topic.partition_data {
            let Some(records_bytes) = &partition.records else {
                continue;
            };
            push_records(
                &topic_name,
                partition.index,
                records_bytes.clone(),
                &mut out,
            );
        }
    }
    Some(out)
}

/// Decode the Fetch response body sitting in `frame` and pull every
/// record out. Same error semantics as `extract_from_produce_request`.
///
/// `topic_ids` is the cluster-wide `topic_id → name` map populated
/// from `MetadataResponse` traffic. Only consulted on Fetch v13+ where
/// the wire dropped the topic name in favour of a UUID.
///
/// `fetch_corr_id` and `fetch_connection_id` are stamped onto every
/// returned `ExtractedRecord` so the Messages tab can backlink each
/// record to the originating `ProtoFrame` (same `(connection_id,
/// corr_id)` pair as the response frame in the proto correlator's ring
/// buffer). Pass them straight from the `ProtoEvent` raised on the
/// pump's response side.
#[must_use]
pub fn extract_from_fetch_response(
    version: i16,
    frame: &[u8],
    topic_ids: &TopicIdMap,
    fetch_corr_id: i32,
    fetch_connection_id: i32,
) -> Vec<ExtractedRecord> {
    extract_from_fetch_response_inner(
        version,
        frame,
        topic_ids,
        fetch_corr_id,
        fetch_connection_id,
    )
    .unwrap_or_default()
}

fn extract_from_fetch_response_inner(
    version: i16,
    frame: &[u8],
    topic_ids: &TopicIdMap,
    fetch_corr_id: i32,
    fetch_connection_id: i32,
) -> Option<Vec<ExtractedRecord>> {
    let mut buf = Bytes::copy_from_slice(frame);
    // Fetch v0-v11: header v0 (no tagged fields). v12+: header v1.
    let header_version = ApiKey::Fetch.response_header_version(version);
    let _hdr = ResponseHeader::decode(&mut buf, header_version).ok()?;
    let resp = FetchResponse::decode(&mut buf, version).ok()?;

    let mut out = Vec::new();
    for topic in &resp.responses {
        let topic_name = resolve_topic_name(
            version,
            &topic.topic.0.to_string(),
            topic.topic_id,
            topic_ids,
        );
        for partition in &topic.partitions {
            let Some(records_bytes) = &partition.records else {
                continue;
            };
            if records_bytes.is_empty() {
                continue;
            }
            push_records_fetch(
                &topic_name,
                partition.partition_index,
                records_bytes.clone(),
                version,
                fetch_corr_id,
                fetch_connection_id,
                &mut out,
            );
        }
    }
    Some(out)
}

/// Pick the best name we can: on v0..=v12 the wire carries it; on v13+
/// we consult `topic_ids`; if the lookup misses, we surface a visually
/// distinct placeholder so the Messages tab still groups by topic.
fn resolve_topic_name(
    version: i16,
    wire_name: &str,
    topic_id: Uuid,
    topic_ids: &TopicIdMap,
) -> String {
    if !wire_name.is_empty() {
        return wire_name.to_owned();
    }
    if let Some(name) = topic_ids.lookup(topic_id) {
        return name;
    }
    if topic_id.is_nil() {
        // Truly nothing to go on (very early Fetch versions, or a
        // malformed response). Empty string preserves prior behaviour.
        return String::new();
    }
    let _ = version; // version is kept in the signature for future hooks.
    format!("[topic-id {topic_id}]")
}

/// Decode every `RecordBatch` in `records_bytes` and push the
/// flattened `ExtractedRecord` list onto `out`. Decode failures here
/// are swallowed — the wire bytes are always forwarded verbatim by the
/// caller, so partial extraction (some records, then a malformed
/// batch) is acceptable.
///
/// Produce-side variant: `fetch_*` fields are all `None` because there
/// is no Fetch frame to backlink to.
fn push_records(topic: &str, partition: i32, records_bytes: Bytes, out: &mut Vec<ExtractedRecord>) {
    let mut buf = records_bytes;
    let Ok(batches) = RecordBatchDecoder::decode_all(&mut buf) else {
        return;
    };
    for batch in batches {
        for rec in batch.records {
            let headers = rec
                .headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            out.push(ExtractedRecord {
                topic: topic.to_owned(),
                partition,
                offset: rec.offset,
                timestamp_ms: rec.timestamp,
                key: rec.key,
                value: rec.value,
                headers,
                fetch_api_version: None,
                fetch_corr_id: None,
                fetch_connection_id: None,
            });
        }
    }
}

/// Fetch-side variant: stamps the originating Fetch frame's coordinates
/// onto every record so the UI can backlink Messages → Protocol.
fn push_records_fetch(
    topic: &str,
    partition: i32,
    records_bytes: Bytes,
    fetch_api_version: i16,
    fetch_corr_id: i32,
    fetch_connection_id: i32,
    out: &mut Vec<ExtractedRecord>,
) {
    let mut buf = records_bytes;
    let Ok(batches) = RecordBatchDecoder::decode_all(&mut buf) else {
        return;
    };
    for batch in batches {
        for rec in batch.records {
            let headers = rec
                .headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            out.push(ExtractedRecord {
                topic: topic.to_owned(),
                partition,
                offset: rec.offset,
                timestamp_ms: rec.timestamp,
                key: rec.key,
                value: rec.value,
                headers,
                fetch_api_version: Some(fetch_api_version),
                fetch_corr_id: Some(fetch_corr_id),
                fetch_connection_id: Some(fetch_connection_id),
            });
        }
    }
}

/// Build a `CapturedMessage` from one `ExtractedRecord`. The `conn_id`
/// is forwarded into `CapturedMessage::connection_id` so the Messages
/// tab can group records by which proxy TCP connection carried them.
/// The id is a fresh v4 UUID.
#[must_use]
pub fn extracted_to_captured(rec: ExtractedRecord, conn_id: u64) -> CapturedMessage {
    let value_bytes: Option<&[u8]> = rec.value.as_deref();
    let key = rec
        .key
        .as_deref()
        .and_then(|bytes| std::str::from_utf8(bytes).ok().map(ToOwned::to_owned));
    let raw_hex = value_bytes.map_or_else(String::new, render_hex);
    let size_bytes = value_bytes.map_or(0, <[u8]>::len);
    let timestamp = if rec.timestamp_ms > 0 {
        Utc.timestamp_millis_opt(rec.timestamp_ms)
            .single()
            .map_or_else(
                || Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                |dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            )
    } else {
        Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    };

    let headers = rec
        .headers
        .into_iter()
        .map(|(k, v)| KafkaHeader {
            key: k,
            value: v
                .as_deref()
                .and_then(|bytes| std::str::from_utf8(bytes).ok().map(ToOwned::to_owned))
                .unwrap_or_default(),
        })
        .collect();

    // Truncate u64 → i32 the same way `build_proto_event` does so the
    // `connection_id` on the ProtoFrame and on the CapturedMessage
    // line up for the same TCP connection. The mask bounds the result
    // to 0..=i32::MAX, so the try_from cannot fail in practice.
    let connection_id = i32::try_from(conn_id & 0x7FFF_FFFF).unwrap_or(i32::MAX);

    // Fetch-side records carry a backlink to the originating Fetch
    // protocol frame so the Messages tab can switch to the Protocol
    // tab and select that frame. We populate the existing
    // `FetchMetadata` field used by the (now-removed-soon) rdkafka
    // path so the UI doesn't need a parallel field. `response_size`
    // and `rtt_ms` aren't measured here — the proto frame itself has
    // them; setting `0` / `0.0` keeps the row compact and the
    // backlink is the only thing the UI actually consumes from this.
    let fetch = match (
        rec.fetch_api_version,
        rec.fetch_corr_id,
        rec.fetch_connection_id,
    ) {
        (Some(api_version), Some(corr_id), Some(fetch_conn_id)) => Some(FetchMetadata {
            api_key: 1,
            api_name: ProtoEvent::api_name(1),
            api_version: i32::from(api_version),
            connection_id: fetch_conn_id,
            corr_id,
            response_size: 0,
            rtt_ms: 0.0,
        }),
        _ => None,
    };

    CapturedMessage {
        id: Uuid::new_v4().to_string(),
        timestamp,
        topic: rec.topic,
        partition: rec.partition,
        offset: rec.offset,
        key,
        schema_name: None,
        schema_id: None,
        size_bytes,
        headers,
        payload: decode_payload(value_bytes),
        raw_hex,
        fetch,
        connection_id: Some(connection_id),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::{
        ApiKey, FetchResponse, ProduceRequest, RequestHeader, ResponseHeader, TopicName,
    };
    use kafka_protocol::protocol::{Encodable, HeaderVersion, StrBytes};
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    fn make_record(offset: i64, key: &[u8], value: &[u8]) -> Record {
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: i32::try_from(offset).unwrap_or(0),
            timestamp: 1_700_000_000_000,
            key: Some(Bytes::copy_from_slice(key)),
            value: Some(Bytes::copy_from_slice(value)),
            headers: IndexMap::new(),
        }
    }

    fn encode_record_batch(records: &[Record]) -> Bytes {
        let mut buf = BytesMut::new();
        let opts = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        RecordBatchEncoder::encode(&mut buf, records.iter(), &opts).unwrap();
        buf.freeze()
    }

    fn build_produce_request_bytes(
        version: i16,
        topic: &str,
        partition_records: &[(i32, Vec<Record>)],
    ) -> Vec<u8> {
        let mut req = ProduceRequest::default();
        req.transactional_id = None;
        req.acks = -1;
        req.timeout_ms = 30_000;
        let mut topic_data = TopicProduceData::default();
        topic_data.name = TopicName(StrBytes::from_string(topic.to_owned()));
        topic_data.partition_data = partition_records
            .iter()
            .map(|(idx, records)| {
                let mut p = PartitionProduceData::default();
                p.index = *idx;
                p.records = Some(encode_record_batch(records));
                p
            })
            .collect();
        req.topic_data = vec![topic_data];

        let mut out = BytesMut::new();
        let header_version = ProduceRequest::header_version(version);
        let mut header = RequestHeader::default();
        header.request_api_key = ApiKey::Produce as i16;
        header.request_api_version = version;
        header.correlation_id = 7;
        header.client_id = Some(StrBytes::from_static_str("test"));
        header.encode(&mut out, header_version).unwrap();
        req.encode(&mut out, version).unwrap();
        out.to_vec()
    }

    fn build_fetch_response_bytes_v13(
        version: i16,
        topic_id: Uuid,
        partition: i32,
        records: &[Record],
    ) -> Vec<u8> {
        let mut resp = FetchResponse::default();
        resp.throttle_time_ms = 0;
        resp.error_code = 0;
        resp.session_id = 0;
        let mut topic_resp = FetchableTopicResponse::default();
        // v13+ leaves `topic` empty on the wire; only topic_id is sent.
        topic_resp.topic = TopicName(StrBytes::from_static_str(""));
        topic_resp.topic_id = topic_id;
        let mut p = PartitionData::default();
        p.partition_index = partition;
        p.error_code = 0;
        p.high_watermark = i64::try_from(records.len()).unwrap_or(0);
        p.last_stable_offset = p.high_watermark;
        p.log_start_offset = 0;
        p.records = Some(encode_record_batch(records));
        topic_resp.partitions = vec![p];
        resp.responses = vec![topic_resp];

        let header_version = ApiKey::Fetch.response_header_version(version);
        let mut out = BytesMut::new();
        ResponseHeader::default()
            .encode(&mut out, header_version)
            .unwrap();
        resp.encode(&mut out, version).unwrap();
        out.to_vec()
    }

    fn build_fetch_response_bytes(
        version: i16,
        topic: &str,
        partition: i32,
        records: &[Record],
    ) -> Vec<u8> {
        let mut resp = FetchResponse::default();
        resp.throttle_time_ms = 0;
        resp.error_code = 0;
        resp.session_id = 0;
        let mut topic_resp = FetchableTopicResponse::default();
        topic_resp.topic = TopicName(StrBytes::from_string(topic.to_owned()));
        let mut p = PartitionData::default();
        p.partition_index = partition;
        p.error_code = 0;
        p.high_watermark = i64::try_from(records.len()).unwrap_or(0);
        p.last_stable_offset = p.high_watermark;
        p.log_start_offset = 0;
        p.records = Some(encode_record_batch(records));
        topic_resp.partitions = vec![p];
        resp.responses = vec![topic_resp];

        let header_version = ApiKey::Fetch.response_header_version(version);
        let mut out = BytesMut::new();
        ResponseHeader::default()
            .encode(&mut out, header_version)
            .unwrap();
        resp.encode(&mut out, version).unwrap();
        out.to_vec()
    }

    #[test]
    fn extract_from_produce_request_returns_records_per_topic_partition() {
        let p0_records = vec![
            make_record(0, b"k0", b"v0"),
            make_record(1, b"k1", b"v1"),
            make_record(2, b"k2", b"v2"),
        ];
        let p1_records = vec![
            make_record(0, b"k3", b"v3"),
            make_record(1, b"k4", b"v4"),
            make_record(2, b"k5", b"v5"),
        ];
        let bytes =
            build_produce_request_bytes(9, "records-test", &[(0, p0_records), (1, p1_records)]);

        let extracted = extract_from_produce_request(9, &bytes);
        assert_eq!(extracted.len(), 6);
        let p0: Vec<_> = extracted.iter().filter(|r| r.partition == 0).collect();
        let p1: Vec<_> = extracted.iter().filter(|r| r.partition == 1).collect();
        assert_eq!(p0.len(), 3);
        assert_eq!(p1.len(), 3);
        assert_eq!(p0[0].topic, "records-test");
        assert_eq!(p0[0].key.as_deref(), Some(&b"k0"[..]));
        assert_eq!(p0[0].value.as_deref(), Some(&b"v0"[..]));
        assert_eq!(p1[2].key.as_deref(), Some(&b"k5"[..]));
    }

    #[test]
    fn extract_from_fetch_response_returns_records() {
        let records: Vec<_> = (0u8..5)
            .map(|i| make_record(i64::from(i), &[b'k', b'0' + i], &[b'v', b'0' + i]))
            .collect();
        // Fetch v13+ replaces the topic-name field with topic_id (UUID)
        // so the topic-name surfaced from a v13 response would be empty.
        // We use v12 here: last version where the topic name is on the
        // wire. The proxy still works on v13 — it just emits records
        // with an empty topic field.
        let bytes = build_fetch_response_bytes(12, "records-test", 0, &records);
        // v12 carries the topic name on the wire; the topic_id_map is
        // unused on this path. Pass an empty one to prove that.
        let topic_ids = TopicIdMap::new();
        let extracted = extract_from_fetch_response(12, &bytes, &topic_ids, 555, 7);
        assert_eq!(extracted.len(), 5);
        for (i, rec) in extracted.iter().enumerate() {
            assert_eq!(rec.topic, "records-test");
            assert_eq!(rec.partition, 0);
            assert_eq!(rec.offset, i64::try_from(i).unwrap());
            assert_eq!(rec.fetch_corr_id, Some(555));
            assert_eq!(rec.fetch_connection_id, Some(7));
            assert_eq!(rec.fetch_api_version, Some(12));
        }
    }

    #[test]
    fn extract_from_fetch_response_v13_resolves_topic_name_from_map() {
        // Fetch v13 omits the topic name; the extractor must consult the
        // topic_id_map to surface the actual name.
        let records: Vec<_> = (0u8..3)
            .map(|i| make_record(i64::from(i), &[b'k', b'0' + i], &[b'v', b'0' + i]))
            .collect();
        let id = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
        let bytes = build_fetch_response_bytes_v13(13, id, 0, &records);

        let topic_ids = TopicIdMap::new();
        topic_ids.record(id, "records-test".to_owned());

        let extracted = extract_from_fetch_response(13, &bytes, &topic_ids, 0, 0);
        assert_eq!(extracted.len(), 3);
        for rec in &extracted {
            assert_eq!(rec.topic, "records-test");
            assert_eq!(rec.partition, 0);
        }
    }

    #[test]
    fn extract_from_fetch_response_v13_unresolved_topic_id_uses_placeholder() {
        let records = vec![make_record(0, b"k", b"v")];
        let id = Uuid::from_u128(0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_1111);
        let bytes = build_fetch_response_bytes_v13(13, id, 0, &records);

        // Empty map — id was never observed.
        let topic_ids = TopicIdMap::new();
        let extracted = extract_from_fetch_response(13, &bytes, &topic_ids, 0, 0);
        assert_eq!(extracted.len(), 1);
        let expected = format!("[topic-id {id}]");
        assert_eq!(extracted[0].topic, expected);
    }

    #[test]
    fn extract_from_produce_request_handles_missing_records() {
        // ProduceRequest with one topic-partition whose records field is None.
        let mut req = ProduceRequest::default();
        req.transactional_id = None;
        req.acks = -1;
        req.timeout_ms = 30_000;
        let mut t = TopicProduceData::default();
        t.name = TopicName(StrBytes::from_string("missing".to_owned()));
        let mut p = PartitionProduceData::default();
        p.index = 0;
        p.records = None;
        t.partition_data = vec![p];
        req.topic_data = vec![t];

        let version = 9_i16;
        let mut out = BytesMut::new();
        let header_version = ProduceRequest::header_version(version);
        let mut header = RequestHeader::default();
        header.request_api_key = ApiKey::Produce as i16;
        header.request_api_version = version;
        header.correlation_id = 1;
        header.client_id = Some(StrBytes::from_static_str("t"));
        header.encode(&mut out, header_version).unwrap();
        req.encode(&mut out, version).unwrap();

        let extracted = extract_from_produce_request(version, &out);
        assert!(extracted.is_empty());
    }

    #[test]
    fn extract_garbage_returns_empty() {
        // Must not panic on bogus input.
        let topic_ids = TopicIdMap::new();
        assert!(extract_from_produce_request(9, &[0u8; 4]).is_empty());
        assert!(extract_from_fetch_response(13, &[0u8; 4], &topic_ids, 0, 0).is_empty());
        assert!(extract_from_produce_request(9, &[]).is_empty());
        assert!(extract_from_fetch_response(13, &[], &topic_ids, 0, 0).is_empty());
    }

    #[test]
    fn extracted_to_captured_preserves_offset_and_headers() {
        let headers = vec![
            ("h1".to_owned(), Some(Bytes::from_static(b"v1"))),
            ("h2".to_owned(), None),
        ];
        let rec = ExtractedRecord {
            topic: "t".to_owned(),
            partition: 7,
            offset: 42,
            timestamp_ms: 1_700_000_000_000,
            key: Some(Bytes::from_static(b"my-key")),
            value: Some(Bytes::from_static(b"\"hello\"")),
            headers,
            fetch_api_version: None,
            fetch_corr_id: None,
            fetch_connection_id: None,
        };
        let captured = extracted_to_captured(rec, 99);
        assert_eq!(captured.topic, "t");
        assert_eq!(captured.partition, 7);
        assert_eq!(captured.offset, 42);
        assert_eq!(captured.key.as_deref(), Some("my-key"));
        assert_eq!(captured.size_bytes, 7);
        assert_eq!(captured.headers.len(), 2);
        assert_eq!(captured.headers[0].key, "h1");
        assert_eq!(captured.headers[0].value, "v1");
        assert_eq!(captured.headers[1].key, "h2");
        assert_eq!(captured.headers[1].value, "");
        assert!(captured.fetch.is_none());
        assert!(!captured.id.is_empty());
        assert!(!captured.timestamp.is_empty());
    }

    #[test]
    fn extracted_to_captured_populates_fetch_backlink() {
        let rec = ExtractedRecord {
            topic: "t".to_owned(),
            partition: 0,
            offset: 1,
            timestamp_ms: 0,
            key: None,
            value: Some(Bytes::from_static(b"hi")),
            headers: vec![],
            fetch_api_version: Some(16),
            fetch_corr_id: Some(123),
            fetch_connection_id: Some(42),
        };
        let captured = extracted_to_captured(rec, 42);
        let fetch = captured.fetch.unwrap();
        assert_eq!(fetch.api_key, 1);
        assert_eq!(fetch.api_name, "Fetch");
        assert_eq!(fetch.api_version, 16);
        assert_eq!(fetch.corr_id, 123);
        assert_eq!(fetch.connection_id, 42);
        // CapturedMessage.connection_id always reflects the proxy
        // connection regardless of fetch backlink.
        assert_eq!(captured.connection_id, Some(42));
    }

    /// End-to-end pump test: drive a real `ProduceRequest v9` (with one
    /// topic, one partition, two records) through `run_pump_with_rewrite`
    /// and assert the `record_sink` received both records.
    #[tokio::test]
    async fn pump_extracts_records_from_produce_request_to_sink() {
        use std::sync::Arc;

        use bytes::Bytes;
        use futures::{SinkExt, StreamExt};
        use parking_lot::Mutex as PMutex;
        use tokio::io::AsyncWriteExt;
        use tokio::net::{TcpListener, TcpStream};

        use crate::proxy::{
            framed_kafka, run_pump_with_rewrite, BrokerMap, ConnectionId, CorrelationMap,
        };
        use crate::proxy_handle::RecordSink;
        use crate::proxy_provisioner::BrokerProvisioner;

        // Fake upstream: drains one frame and replies with a stub
        // (4-byte corr_id, then a single zero byte) so the pump's
        // response side has something to forward.
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (sock, _) = upstream.accept().await.unwrap();
            let mut framed = framed_kafka(sock);
            let _req = framed.next().await.unwrap().unwrap();
            let mut reply = BytesMut::new();
            reply.extend_from_slice(&7i32.to_be_bytes());
            reply.extend_from_slice(&[0u8]);
            framed.send(reply.freeze()).await.unwrap();
        });

        let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client_addr = client_listener.local_addr().unwrap();
        let upstream_target = upstream_addr.to_string();

        let correlator = Arc::new(crate::correlator::ProtoCorrelator::new());
        let corr_map = Arc::new(CorrelationMap::default());
        let broker_map = Arc::new(BrokerMap::new());
        let provisioner: Arc<dyn BrokerProvisioner> = broker_map;

        let captured: Arc<PMutex<Vec<CapturedMessage>>> = Arc::new(PMutex::new(Vec::new()));
        let captured_for_sink = Arc::clone(&captured);
        let sink: RecordSink = Arc::new(move |msg: CapturedMessage| {
            captured_for_sink.lock().push(msg);
        });

        let topic_id_map = Arc::new(TopicIdMap::new());
        let pump_task = tokio::spawn(async move {
            let (client_sock, _) = client_listener.accept().await.unwrap();
            let upstream_sock = TcpStream::connect(upstream_target).await.unwrap();
            run_pump_with_rewrite(
                ConnectionId(42),
                client_sock,
                upstream_sock,
                correlator,
                corr_map,
                provisioner,
                sink,
                topic_id_map,
            )
            .await
            .unwrap();
        });

        // Build a Produce v9 request with one topic / one partition /
        // two records.
        let records = vec![
            Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: 0,
                timestamp: 1_700_000_000_000,
                key: Some(Bytes::from_static(b"k0")),
                value: Some(Bytes::from_static(b"v0")),
                headers: IndexMap::new(),
            },
            Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: 1,
                sequence: 1,
                timestamp: 1_700_000_000_001,
                key: Some(Bytes::from_static(b"k1")),
                value: Some(Bytes::from_static(b"v1")),
                headers: IndexMap::new(),
            },
        ];
        let frame_body = build_produce_request_bytes(9, "pump-test", &[(0, records)]);

        let mut client = TcpStream::connect(client_addr).await.unwrap();
        let len = u32::try_from(frame_body.len()).unwrap();
        client.write_all(&len.to_be_bytes()).await.unwrap();
        client.write_all(&frame_body).await.unwrap();

        upstream_task.await.unwrap();
        // Drain the upstream reply so the pump's response-side select
        // arm observes the frame and (for non-Fetch) just forwards it.
        let mut framed_client = framed_kafka(client);
        let _ = framed_client.next().await;

        // Give the sink a tick to flush.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let snap = captured.lock().clone();
        assert_eq!(snap.len(), 2, "expected 2 records, got {snap:?}");
        for (i, msg) in snap.iter().enumerate() {
            assert_eq!(msg.topic, "pump-test");
            assert_eq!(msg.partition, 0);
            assert_eq!(msg.key.as_deref(), Some(if i == 0 { "k0" } else { "k1" }));
        }

        pump_task.abort();
    }
}
