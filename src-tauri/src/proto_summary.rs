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

#![allow(clippy::wildcard_imports)]

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
    /// Topics + broker count advertised by a `MetadataResponse`. Used
    /// to populate the "topics seen" set from the cluster's own view.
    MetadataResponse {
        topics: Vec<String>,
        brokers: u32,
    },
    /// Topic names a client wrote to. Record batches are *not*
    /// decoded — payload values stay opaque.
    ProduceRequest {
        topics: Vec<String>,
    },
    /// Topic names a client read from. Same opacity rule for records.
    FetchRequest {
        topics: Vec<String>,
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
    },
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
            Some(FrameSummary::ProduceRequest {
                topics: req
                    .topic_data
                    .into_iter()
                    .map(|t| t.name.0.to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            })
        }
        ApiKey::Fetch => {
            let req = FetchRequest::decode(buf, version).ok()?;
            Some(FrameSummary::FetchRequest {
                topics: req
                    .topics
                    .into_iter()
                    .map(|t| t.topic.0.to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
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

fn extract_response(api: ApiKey, version: i16, buf: &mut Bytes) -> Option<FrameSummary> {
    match api {
        ApiKey::Metadata => {
            let resp = MetadataResponse::decode(buf, version).ok()?;
            Some(FrameSummary::MetadataResponse {
                topics: resp
                    .topics
                    .into_iter()
                    .filter_map(|t| t.name.map(|n| n.0.to_string()))
                    .filter(|s| !s.is_empty())
                    .collect(),
                brokers: u32::try_from(resp.brokers.len()).unwrap_or(0),
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
            Some(FrameSummary::OffsetCommitResponse { max_error_code })
        }
        _ => None,
    }
}
