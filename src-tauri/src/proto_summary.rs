//! Structured projection of a decoded protocol body — a *small*,
//! purpose-built subset captured eagerly at frame ingestion time and
//! shipped over IPC alongside the lightweight frame envelope.
//!
//! Why a separate enum and not just enrich `decoded`? The
//! `kafka-protocol` crate types do not derive `Serialize`, so the
//! existing `decoded: String` is a `format!("{:#?}", typed_body)` —
//! great for the human inspector pane, useless for aggregation:
//! regex-parsing a Debug representation is brittle and silently
//! breaks on crate version bumps. This enum picks only the fields
//! that drive the Session Activity tab (topics seen, groups touched,
//! top-level errors, client identity) and ships them typed.
//!
//! Coverage is intentionally narrow:
//!  * control-plane request/response pairs that carry consumer-group
//!    state (Join/Sync/Heartbeat/LeaveGroup/OffsetCommit/FindCoordinator);
//!  * Produce/Fetch *requests* — topic names only; the embedded
//!    `RecordBatch` payloads stay opaque (record-value decoding lives
//!    in the schema-registry path and is much more expensive);
//!  * `ApiVersionsRequest` v3+ for the client lib name + version.
//!
//! Per-partition errors nested inside Produce/Fetch responses are out
//! of scope for v1: walking them adds significant per-frame work and
//! the local-dev debug session needs aren't there yet. Top-level
//! `error_code`s on the small group RPCs cover the common cases.

#![allow(clippy::wildcard_imports, clippy::doc_markdown)]

use bytes::{Buf, Bytes};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::Decodable;
use schemars::JsonSchema;
use serde::Serialize;

use crate::proto_event::ProtoDirection;

/// Typed projection of a decoded Kafka protocol body. `kind` is the
/// serde tag — the frontend matches on it to fold each frame into the
/// session-level aggregate state.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum FrameSummary {
    /// Client lib name + version. Available on `ApiVersionsRequest`
    /// v3 and up; v0..=2 don't carry these fields and yield `None`.
    ApiVersionsRequest {
        client_software_name: String,
        client_software_version: String,
    },
    /// Broker-advertised max version per api key — drives the
    /// "mixed `api_version` across brokers in rolling upgrade"
    /// anti-pattern. We surface every (key, `max_version`) pair so
    /// the detector can spot two upstream brokers advertising
    /// different `max_version` for the same key.
    ApiVersionsResponse {
        error_code: i16,
        /// `(api_key, max_version)` pairs.
        max_versions: Vec<(i16, i16)>,
    },
    /// Topics + broker count advertised by a `MetadataResponse`,
    /// plus the per-partition leader map needed for stale-leader
    /// detection.
    MetadataResponse {
        topics: Vec<String>,
        brokers: u32,
        /// `topic → [(partition_index, leader_id)]`. Empty when no
        /// topics are returned. Used by the stale-leader detector
        /// to compare leader hints against where Produce/Fetch
        /// actually go.
        leaders: Vec<TopicLeaders>,
        /// `broker_id → "host:port"`. Lets the UI render meaningful
        /// broker identities and the detector cross-check upstream
        /// host attribution.
        brokers_map: Vec<BrokerEndpoint>,
        /// `throttle_time_ms > 0` indicates the broker delayed this
        /// response due to a request-time quota.
        throttle_time_ms: i32,
    },
    /// Topic names + record-batch shape a client wrote to. Record
    /// values stay opaque — we only crack the `RecordBatch` v2 header
    /// to lift `record_count` (offset 57), `attributes` (offset 21) and
    /// `producerId` (offset 43) for the first batch in the request.
    ProduceRequest {
        topics: Vec<String>,
        /// `(topic, partition_index)` pairs that the request
        /// targets — feeds the stale-leader detector.
        partitions: Vec<TopicPartition>,
        /// Total number of records summed across every partition in
        /// the request. `0` when records were truncated past the
        /// captured prefix.
        record_count: u32,
        /// Total bytes of record payloads (`PartitionProduceData.records`
        /// length, summed). Approximates `batch.size` consumption.
        batch_bytes: u64,
        /// Number of partition entries in the request. With
        /// `record_count` this gives "records per batch", the key
        /// signal for the tiny-batch detector.
        batch_count: u32,
        /// `true` when the request carries a non-empty
        /// `transactional_id` — informational, not a detector input.
        transactional: bool,
        /// Durability ack mode requested by the client. `0` = fire-and-forget
        /// (silent data loss on broker crash), `1` = leader-only, `-1`/`all`
        /// = full ISR. Feeds the `acks=0` detector.
        acks: i16,
        /// Compression codec from the first `RecordBatch` v2 attributes
        /// byte (lower 3 bits): 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd.
        /// `None` when no record batch was captured. Feeds the
        /// compression-off detector.
        first_batch_compression: Option<u8>,
        /// `producerId` from the first captured RecordBatch v2 header.
        /// `-1` indicates a non-idempotent producer (no exactly-once
        /// guarantee on retries). Feeds the non-idempotent detector.
        first_batch_producer_id: Option<i64>,
    },
    /// Per-partition error codes from a `ProduceResponse`. We surface
    /// only entries with non-zero `error_code` and an optional
    /// `current_leader` hint — those are the signals for the
    /// stale-leader detector.
    ProduceResponse {
        errors: Vec<ProducePartitionError>,
        /// `throttle_time_ms > 0` indicates the broker delayed this
        /// response due to a quota (byte-rate / request-time). KIP-219.
        throttle_time_ms: i32,
    },
    /// Re-init handshake. The producer-per-record anti-pattern is
    /// detectable from `InitProducerIdRequest` rate vs `ProduceRequest`
    /// rate — every fresh-producer instance issues one.
    InitProducerIdRequest {
        transactional: bool,
        /// Empty when the request is non-transactional. Used by the
        /// transactional-zombie detector to track txn lifecycle.
        transactional_id: Option<String>,
    },
    /// Begin a transactional segment by registering partitions with the
    /// coordinator. Detector uses presence + transactional_id to
    /// distinguish in-flight vs abandoned txns.
    AddPartitionsToTxnRequest {
        transactional_id: String,
    },
    /// Commit / abort a txn. `committed=true` is commit, `false` is abort.
    EndTxnRequest {
        transactional_id: String,
        committed: bool,
    },
    /// Topic + fetch-shape parameters a client read from. `min_bytes`
    /// and `max_wait_ms` drive the tight-polling detector; `session_*`
    /// fields feed the incremental-fetch-session-error detector.
    FetchRequest {
        topics: Vec<String>,
        min_bytes: i32,
        max_wait_ms: i32,
        /// `0` = read_uncommitted, `1` = read_committed.
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
    },
    /// Top-level fields from a `FetchResponse`. `error_code` covers
    /// `INVALID_FETCH_SESSION_EPOCH` (70) and `INVALID_SESSION_ID` (71)
    /// for the cascade detector; `throttle_time_ms` for quota; `size`
    /// for the tight-polling detector.
    FetchResponse {
        error_code: i16,
        session_id: i32,
        throttle_time_ms: i32,
        /// Total payload bytes returned across all topics + partitions.
        /// Lets the tight-polling detector compute avg response size
        /// per connection.
        response_size: u64,
    },
    /// Topics + flags advertised in a `MetadataRequest`. Feeds the
    /// metadata-storm detector (rate per connection).
    MetadataRequest {
        topics: Vec<String>,
        allow_auto_topic_creation: bool,
    },
    /// Group / transactional-id keys whose coordinator the client is
    /// asking for. v0..=3 carry a single `key`; v4+ a `coordinator_keys`
    /// array — we union both into one `Vec` for simplicity.
    FindCoordinatorRequest {
        keys: Vec<String>,
    },
    /// Coordinator broker resolved by the broker. v0..=3 carries a
    /// top-level `(error_code, node_id)`; v4+ a `coordinators` array.
    /// We surface the first entry of the array, falling back to the
    /// top-level pair.
    FindCoordinatorResponse {
        error_code: i16,
        node_id: i32,
    },
    JoinGroupRequest {
        group_id: String,
        member_id: String,
    },
    JoinGroupResponse {
        error_code: i16,
        generation_id: i32,
        member_id: String,
    },
    SyncGroupRequest {
        group_id: String,
        member_id: String,
        generation_id: i32,
    },
    SyncGroupResponse {
        error_code: i16,
    },
    HeartbeatRequest {
        group_id: String,
        member_id: String,
        generation_id: i32,
    },
    HeartbeatResponse {
        error_code: i16,
    },
    LeaveGroupRequest {
        group_id: String,
    },
    LeaveGroupResponse {
        error_code: i16,
    },
    OffsetCommitRequest {
        group_id: String,
        member_id: String,
        topics: Vec<String>,
    },
    OffsetCommitResponse {
        /// Worst (highest, where 0 = OK) per-partition error code
        /// observed across all entries — gives a single yes/no signal
        /// for the Errors list without forcing the frontend to walk
        /// nested partition results.
        max_error_code: i16,
        throttle_time_ms: i32,
    },
    /// SASL re-auth result. `session_lifetime_ms` is the broker-grant
    /// window the client is expected to schedule its next
    /// `SaslAuthenticate` within. A sudden drop on re-auth ("Session
    /// too short" in MSK IAM auth) shows up here as a tiny lifetime
    /// or a non-zero `error_code`.
    SaslAuthenticateResponse {
        error_code: i16,
        error_message: Option<String>,
        session_lifetime_ms: i64,
    },
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicLeaders {
    pub topic: String,
    /// `(partition_index, leader_id)`.
    pub partitions: Vec<(i32, i32)>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrokerEndpoint {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProducePartitionError {
    pub topic: String,
    pub partition: i32,
    pub error_code: i16,
    /// Set on v10+ Produce when the broker hints at the *new* leader
    /// the producer should retry against. Useful context for the
    /// stale-leader detector — the broker is telling us exactly
    /// where the truth lies.
    pub current_leader_id: Option<i32>,
}

/// Extract a [`FrameSummary`] from the raw wire bytes of a single
/// Kafka protocol frame. Returns `None` when the api isn't projected,
/// when the bytes are truncated, or when the header / body parse
/// fails. Callers fall through to the existing `decoded: String` for
/// frames without a structured projection.
pub fn extract_summary(
    api_key: i32,
    api_version: i16,
    direction: ProtoDirection,
    payload: &[u8],
) -> Option<FrameSummary> {
    if payload.len() < 8 {
        return None;
    }
    let api = ApiKey::try_from(i16::try_from(api_key).ok()?).ok()?;
    let mut buf = Bytes::copy_from_slice(payload);
    // Wire layout is `size_prefix : i32 || header || body`. The size
    // prefix is informational — `Decodable` consumes the header and
    // body off the post-prefix slice.
    let _size = buf.get_i32();
    match direction {
        ProtoDirection::Send => {
            let header_version = api.request_header_version(api_version);
            RequestHeader::decode(&mut buf, header_version).ok()?;
            extract_request(api, api_version, &mut buf)
        }
        ProtoDirection::Recv => {
            let header_version = api.response_header_version(api_version);
            ResponseHeader::decode(&mut buf, header_version).ok()?;
            extract_response(api, api_version, &mut buf)
        }
    }
}

#[allow(clippy::too_many_lines)]
fn extract_request(api: ApiKey, version: i16, buf: &mut Bytes) -> Option<FrameSummary> {
    match api {
        ApiKey::ApiVersions => {
            // Fields only exist v3+. Decoding earlier versions yields
            // the default empty strings — we'd report an empty client
            // identity, which is misleading. Skip projection instead.
            if version < 3 {
                return None;
            }
            let req = ApiVersionsRequest::decode(buf, version).ok()?;
            Some(FrameSummary::ApiVersionsRequest {
                client_software_name: req.client_software_name.to_string(),
                client_software_version: req.client_software_version.to_string(),
            })
        }
        ApiKey::Produce => {
            let req = ProduceRequest::decode(buf, version).ok()?;
            let transactional = req
                .transactional_id
                .as_ref()
                .is_some_and(|tid| !tid.0.is_empty());
            let acks = req.acks;
            let mut topics: Vec<String> = Vec::with_capacity(req.topic_data.len());
            let mut partitions: Vec<TopicPartition> = Vec::new();
            let mut record_count: u32 = 0;
            let mut batch_bytes: u64 = 0;
            let mut batch_count: u32 = 0;
            let mut first_batch_compression: Option<u8> = None;
            let mut first_batch_producer_id: Option<i64> = None;
            for t in &req.topic_data {
                let name = t.name.0.to_string();
                if !name.is_empty() {
                    topics.push(name.clone());
                }
                for p in &t.partition_data {
                    partitions.push(TopicPartition {
                        topic: name.clone(),
                        partition: p.index,
                    });
                    batch_count = batch_count.saturating_add(1);
                    if let Some(records) = &p.records {
                        batch_bytes = batch_bytes.saturating_add(records.len() as u64);
                        let (rc, attr, pid) = first_batch_meta(records);
                        record_count = record_count.saturating_add(rc);
                        if first_batch_compression.is_none() {
                            first_batch_compression = attr;
                        }
                        if first_batch_producer_id.is_none() {
                            first_batch_producer_id = pid;
                        }
                    }
                }
            }
            Some(FrameSummary::ProduceRequest {
                topics,
                partitions,
                record_count,
                batch_bytes,
                batch_count,
                transactional,
                acks,
                first_batch_compression,
                first_batch_producer_id,
            })
        }
        ApiKey::InitProducerId => {
            let req = InitProducerIdRequest::decode(buf, version).ok()?;
            let transactional_id = req
                .transactional_id
                .as_ref()
                .map(|tid| tid.0.to_string())
                .filter(|s| !s.is_empty());
            let transactional = transactional_id.is_some();
            Some(FrameSummary::InitProducerIdRequest {
                transactional,
                transactional_id,
            })
        }
        ApiKey::AddPartitionsToTxn => {
            let req = AddPartitionsToTxnRequest::decode(buf, version).ok()?;
            // v0..=3 carries a top-level `transactional_id`; v4+ uses
            // a `transactions` array. Prefer the first transaction's
            // id when the array is non-empty.
            let from_array = req
                .transactions
                .first()
                .map(|t| t.transactional_id.0.to_string())
                .filter(|s| !s.is_empty());
            let transactional_id =
                from_array.unwrap_or_else(|| req.v3_and_below_transactional_id.0.to_string());
            if transactional_id.is_empty() {
                return None;
            }
            Some(FrameSummary::AddPartitionsToTxnRequest { transactional_id })
        }
        ApiKey::EndTxn => {
            let req = EndTxnRequest::decode(buf, version).ok()?;
            let transactional_id = req.transactional_id.0.to_string();
            Some(FrameSummary::EndTxnRequest {
                transactional_id,
                committed: req.committed,
            })
        }
        ApiKey::Fetch => {
            let req = FetchRequest::decode(buf, version).ok()?;
            Some(FrameSummary::FetchRequest {
                topics: req
                    .topics
                    .iter()
                    .map(|t| t.topic.0.to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
                min_bytes: req.min_bytes,
                max_wait_ms: req.max_wait_ms,
                isolation_level: req.isolation_level,
                session_id: req.session_id,
                session_epoch: req.session_epoch,
            })
        }
        ApiKey::Metadata => {
            let req = MetadataRequest::decode(buf, version).ok()?;
            let topics: Vec<String> = req
                .topics
                .unwrap_or_default()
                .into_iter()
                .filter_map(|t| t.name.map(|n| n.0.to_string()))
                .filter(|s| !s.is_empty())
                .collect();
            Some(FrameSummary::MetadataRequest {
                topics,
                allow_auto_topic_creation: req.allow_auto_topic_creation,
            })
        }
        ApiKey::FindCoordinator => {
            let req = FindCoordinatorRequest::decode(buf, version).ok()?;
            let mut keys: Vec<String> = req
                .coordinator_keys
                .iter()
                .map(std::string::ToString::to_string)
                .filter(|s| !s.is_empty())
                .collect();
            if !req.key.is_empty() {
                keys.push(req.key.to_string());
            }
            Some(FrameSummary::FindCoordinatorRequest { keys })
        }
        ApiKey::JoinGroup => {
            let req = JoinGroupRequest::decode(buf, version).ok()?;
            Some(FrameSummary::JoinGroupRequest {
                group_id: req.group_id.0.to_string(),
                member_id: req.member_id.to_string(),
            })
        }
        ApiKey::SyncGroup => {
            let req = SyncGroupRequest::decode(buf, version).ok()?;
            Some(FrameSummary::SyncGroupRequest {
                group_id: req.group_id.0.to_string(),
                member_id: req.member_id.to_string(),
                generation_id: req.generation_id,
            })
        }
        ApiKey::Heartbeat => {
            let req = HeartbeatRequest::decode(buf, version).ok()?;
            Some(FrameSummary::HeartbeatRequest {
                group_id: req.group_id.0.to_string(),
                member_id: req.member_id.to_string(),
                generation_id: req.generation_id,
            })
        }
        ApiKey::LeaveGroup => {
            let req = LeaveGroupRequest::decode(buf, version).ok()?;
            Some(FrameSummary::LeaveGroupRequest {
                group_id: req.group_id.0.to_string(),
            })
        }
        ApiKey::OffsetCommit => {
            let req = OffsetCommitRequest::decode(buf, version).ok()?;
            Some(FrameSummary::OffsetCommitRequest {
                group_id: req.group_id.0.to_string(),
                member_id: req.member_id.to_string(),
                topics: req
                    .topics
                    .into_iter()
                    .map(|t| t.name.0.to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            })
        }
        _ => None,
    }
}

#[allow(clippy::too_many_lines)]
fn extract_response(api: ApiKey, version: i16, buf: &mut Bytes) -> Option<FrameSummary> {
    match api {
        ApiKey::Metadata => {
            let resp = MetadataResponse::decode(buf, version).ok()?;
            let brokers = u32::try_from(resp.brokers.len()).unwrap_or(0);
            let brokers_map: Vec<BrokerEndpoint> = resp
                .brokers
                .iter()
                .map(|b| BrokerEndpoint {
                    node_id: b.node_id.0,
                    host: b.host.to_string(),
                    port: b.port,
                })
                .collect();
            let mut topic_names: Vec<String> = Vec::with_capacity(resp.topics.len());
            let mut leaders: Vec<TopicLeaders> = Vec::with_capacity(resp.topics.len());
            for t in resp.topics {
                let Some(name) = t.name.map(|n| n.0.to_string()) else {
                    continue;
                };
                if name.is_empty() {
                    continue;
                }
                topic_names.push(name.clone());
                let partitions: Vec<(i32, i32)> = t
                    .partitions
                    .iter()
                    .map(|p| (p.partition_index, p.leader_id.0))
                    .collect();
                if !partitions.is_empty() {
                    leaders.push(TopicLeaders {
                        topic: name,
                        partitions,
                    });
                }
            }
            Some(FrameSummary::MetadataResponse {
                topics: topic_names,
                brokers,
                leaders,
                brokers_map,
                throttle_time_ms: resp.throttle_time_ms,
            })
        }
        ApiKey::Fetch => {
            let resp = FetchResponse::decode(buf, version).ok()?;
            let response_size: u64 = resp
                .responses
                .iter()
                .flat_map(|t| t.partitions.iter())
                .map(|p| p.records.as_ref().map_or(0_u64, |r| r.len() as u64))
                .sum();
            Some(FrameSummary::FetchResponse {
                error_code: resp.error_code,
                session_id: resp.session_id,
                throttle_time_ms: resp.throttle_time_ms,
                response_size,
            })
        }
        ApiKey::ApiVersions => {
            let resp = ApiVersionsResponse::decode(buf, version).ok()?;
            let max_versions: Vec<(i16, i16)> = resp
                .api_keys
                .iter()
                .map(|k| (k.api_key, k.max_version))
                .collect();
            Some(FrameSummary::ApiVersionsResponse {
                error_code: resp.error_code,
                max_versions,
            })
        }
        ApiKey::Produce => {
            let resp = ProduceResponse::decode(buf, version).ok()?;
            let mut errors: Vec<ProducePartitionError> = Vec::new();
            for t in &resp.responses {
                let topic = t.name.0.to_string();
                for p in &t.partition_responses {
                    if p.error_code == 0 {
                        continue;
                    }
                    let current_leader_id = if p.current_leader.leader_id.0 >= 0 {
                        Some(p.current_leader.leader_id.0)
                    } else {
                        None
                    };
                    errors.push(ProducePartitionError {
                        topic: topic.clone(),
                        partition: p.index,
                        error_code: p.error_code,
                        current_leader_id,
                    });
                }
            }
            Some(FrameSummary::ProduceResponse {
                errors,
                throttle_time_ms: resp.throttle_time_ms,
            })
        }
        ApiKey::SaslAuthenticate => {
            let resp = SaslAuthenticateResponse::decode(buf, version).ok()?;
            let error_message = resp
                .error_message
                .as_ref()
                .map(std::string::ToString::to_string)
                .filter(|s| !s.is_empty());
            Some(FrameSummary::SaslAuthenticateResponse {
                error_code: resp.error_code,
                error_message,
                session_lifetime_ms: resp.session_lifetime_ms,
            })
        }
        ApiKey::FindCoordinator => {
            let resp = FindCoordinatorResponse::decode(buf, version).ok()?;
            // v4+ uses `coordinators[]`; older versions populate the
            // top-level fields. Prefer the array when non-empty so
            // multi-key requests surface the first coordinator.
            let (error_code, node_id) = resp
                .coordinators
                .first()
                .map_or((resp.error_code, resp.node_id.0), |c| {
                    (c.error_code, c.node_id.0)
                });
            Some(FrameSummary::FindCoordinatorResponse {
                error_code,
                node_id,
            })
        }
        ApiKey::JoinGroup => {
            let resp = JoinGroupResponse::decode(buf, version).ok()?;
            Some(FrameSummary::JoinGroupResponse {
                error_code: resp.error_code,
                generation_id: resp.generation_id,
                member_id: resp.member_id.to_string(),
            })
        }
        ApiKey::SyncGroup => {
            let resp = SyncGroupResponse::decode(buf, version).ok()?;
            Some(FrameSummary::SyncGroupResponse {
                error_code: resp.error_code,
            })
        }
        ApiKey::Heartbeat => {
            let resp = HeartbeatResponse::decode(buf, version).ok()?;
            Some(FrameSummary::HeartbeatResponse {
                error_code: resp.error_code,
            })
        }
        ApiKey::LeaveGroup => {
            let resp = LeaveGroupResponse::decode(buf, version).ok()?;
            Some(FrameSummary::LeaveGroupResponse {
                error_code: resp.error_code,
            })
        }
        ApiKey::OffsetCommit => {
            let resp = OffsetCommitResponse::decode(buf, version).ok()?;
            // Walk topic→partition results once and keep the worst
            // error. 0 = OK; any non-zero is a commit failure worth
            // surfacing.
            let max_error_code = resp
                .topics
                .iter()
                .flat_map(|t| t.partitions.iter())
                .map(|p| p.error_code)
                .max()
                .unwrap_or(0);
            Some(FrameSummary::OffsetCommitResponse {
                max_error_code,
                throttle_time_ms: resp.throttle_time_ms,
            })
        }
        _ => None,
    }
}

/// Walk one or more concatenated Kafka `RecordBatch` v2 frames and sum
/// their `record_count` fields. Stops cleanly on truncation — the
/// captured prefix may slice through a batch and we'd rather under-count
/// than panic.
///
/// `RecordBatch` v2 wire layout (per Kafka protocol):
/// ```text
///   0..8    baseOffset                 i64
///   8..12   batchLength                i32  (bytes after this field)
///  12..16   partitionLeaderEpoch       i32
///  16..17   magic                      i8   (== 2 for v2)
///  17..21   crc                        i32
///  21..23   attributes                 i16
///  23..27   lastOffsetDelta            i32
///  27..35   baseTimestamp              i64
///  35..43   maxTimestamp               i64
///  43..51   producerId                 i64
///  51..53   producerEpoch              i16
///  53..57   baseSequence               i32
///  57..61   recordCount                i32
///  61..     records[]
/// ```
/// Walk concatenated `RecordBatch` v2 frames and extract:
///  * total `record_count` summed across all batches in the prefix;
///  * `attributes & 0b111` (compression codec) of the *first* batch;
///  * `producerId` of the *first* batch (for idempotence detection).
///
/// Returns `(0, None, None)` if no v2 batch is parseable (e.g. v0/v1
/// message set, truncated buffer, or empty).
fn first_batch_meta(bytes: &[u8]) -> (u32, Option<u8>, Option<i64>) {
    const HEADER_LEN: usize = 61;
    const BATCH_LENGTH_OFFSET: usize = 8;
    const MAGIC_OFFSET: usize = 16;
    const ATTRIBUTES_OFFSET: usize = 21;
    const PRODUCER_ID_OFFSET: usize = 43;
    const RECORD_COUNT_OFFSET: usize = 57;

    let mut total: u32 = 0;
    let mut first_attr: Option<u8> = None;
    let mut first_pid: Option<i64> = None;
    let mut cursor = 0_usize;
    while cursor + HEADER_LEN <= bytes.len() {
        let batch_length = u32::from_be_bytes([
            bytes[cursor + BATCH_LENGTH_OFFSET],
            bytes[cursor + BATCH_LENGTH_OFFSET + 1],
            bytes[cursor + BATCH_LENGTH_OFFSET + 2],
            bytes[cursor + BATCH_LENGTH_OFFSET + 3],
        ]);
        let magic = i8::from_le_bytes([bytes[cursor + MAGIC_OFFSET]]);
        if magic != 2 {
            break;
        }
        if first_attr.is_none() {
            // attributes is i16 big-endian; we only need the compression
            // codec in the lower 3 bits.
            first_attr = Some(bytes[cursor + ATTRIBUTES_OFFSET + 1] & 0b0000_0111);
        }
        if first_pid.is_none() {
            let mut pid_bytes = [0_u8; 8];
            pid_bytes.copy_from_slice(
                &bytes[cursor + PRODUCER_ID_OFFSET..cursor + PRODUCER_ID_OFFSET + 8],
            );
            first_pid = Some(i64::from_be_bytes(pid_bytes));
        }
        let count = i32::from_be_bytes([
            bytes[cursor + RECORD_COUNT_OFFSET],
            bytes[cursor + RECORD_COUNT_OFFSET + 1],
            bytes[cursor + RECORD_COUNT_OFFSET + 2],
            bytes[cursor + RECORD_COUNT_OFFSET + 3],
        ]);
        if count > 0 {
            total = total.saturating_add(u32::try_from(count).unwrap_or(0));
        }
        let advance = 12_usize.saturating_add(batch_length as usize);
        if advance == 0 {
            break;
        }
        cursor = cursor.saturating_add(advance);
    }
    (total, first_attr, first_pid)
}
