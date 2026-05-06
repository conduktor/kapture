//! Decode the wire bytes captured by the proto-hook into a typed,
//! human-readable Kafka protocol structure.
//!
//! Strategy v1: dispatch on `(ApiKey, version, direction)` to the
//! matching `kafka-protocol` crate type, decode it, and return the
//! `Debug`-formatted output. The crate's types do not derive
//! `Serialize`, so a pretty-printed `{:#?}` is the lowest-friction way
//! to surface every field. Later iterations can replace this with a
//! structured `DecodedValue` mapping per type.
//!
//! Anything we don't have an arm for falls through to `None`, and the
//! UI continues to show the raw hex view. Adding a new API is one line
//! in `decode_request` / `decode_response`.

// Wildcard import is intentional here: the dispatch tables below name
// every supported message type by its short name, and listing them
// individually would dwarf the table itself. Confined to this one
// module so it can't bleed into the rest of the crate.
#![allow(clippy::wildcard_imports)]

use bytes::{Buf, Bytes};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::Decodable;

use crate::proto_event::ProtoDirection;

/// Max `ApiVersions` request/response version the bundled
/// kafka-protocol 0.16 crate knows how to decode. Newer librdkafka
/// builds ask for v5+ which is wire-compatible with v4 for the fields
/// we surface (v5+ only adds further optional tagged fields). When
/// the primary decode at `v_requested` fails, we retry at this
/// version so the user still sees a decoded view.
const MAX_KNOWN_API_VERSIONS_VERSION: i16 = 4;

/// Decode the captured wire bytes (size prefix + header + body) of a
/// single Kafka protocol frame. Returns `None` when the api isn't yet
/// supported, when the bytes are truncated past the body, or when the
/// header parse fails.
pub fn decode_frame(
    api_key: i32,
    api_version: i16,
    direction: ProtoDirection,
    payload: &[u8],
) -> Option<String> {
    if payload.len() < 8 {
        return None;
    }
    let buf = Bytes::copy_from_slice(payload);
    let api = ApiKey::try_from(i16::try_from(api_key).ok()?).ok()?;

    // Primary attempt: decode at the version the wire actually
    // advertises. Header version mapping comes from the crate.
    if let Some(out) = try_decode_at(api, api_version, direction, &buf) {
        return Some(out);
    }

    // ApiVersions has TWO documented fallback paths once the primary
    // fails:
    //
    //   1. Crate-version cap: kafka-protocol 0.16 knows ApiVersions
    //      0..=4. Modern librdkafka asks for v5; the wire layout for
    //      the fields we surface is identical, so retry at v4.
    //
    //   2. KIP-511 downgrade (response only): when a broker doesn't
    //      support the requested version, it MUST frame the response
    //      as v0 (non-flexible header, non-compact arrays) with
    //      error_code = UNSUPPORTED_VERSION (35), regardless of the
    //      version the client asked for. Retry as v0.
    if api == ApiKey::ApiVersions {
        if api_version > MAX_KNOWN_API_VERSIONS_VERSION {
            if let Some(out) = try_decode_at(api, MAX_KNOWN_API_VERSIONS_VERSION, direction, &buf) {
                return Some(annotate_apiversions_fallback(
                    &out,
                    api_version,
                    direction,
                    "crate-version cap",
                ));
            }
        }
        if matches!(direction, ProtoDirection::Recv) {
            if let Some(out) = try_decode_at(api, 0, direction, &buf) {
                return Some(annotate_apiversions_fallback(
                    &out,
                    api_version,
                    direction,
                    "KIP-511 downgrade",
                ));
            }
        }
    }

    None
}

/// Attempt one (api, version, direction) decode pass over a fresh
/// clone of `buf`. Returns `None` on any header / body decode error.
fn try_decode_at(
    api: ApiKey,
    version: i16,
    direction: ProtoDirection,
    buf: &Bytes,
) -> Option<String> {
    let mut local = buf.clone();
    let _size = local.get_i32();
    match direction {
        ProtoDirection::Send => {
            let header_version = api.request_header_version(version);
            RequestHeader::decode(&mut local, header_version).ok()?;
            decode_request(api, version, &mut local)
        }
        ProtoDirection::Recv => {
            let header_version = api.response_header_version(version);
            ResponseHeader::decode(&mut local, header_version).ok()?;
            decode_response(api, version, &mut local)
        }
    }
}

/// Wrap the kafka-protocol Debug output with a short comment header
/// explaining why we fell back. The body parses cleanly through the
/// frontend's debugTree parser because the comment lives INSIDE the
/// outer struct's `{ ... }` and looks like a tagged-fields entry; the
/// parser tolerates extra `// ...` style noise inside braces.
fn annotate_apiversions_fallback(
    body: &str,
    requested_version: i16,
    direction: ProtoDirection,
    reason: &str,
) -> String {
    let kind = match direction {
        ProtoDirection::Send => "ApiVersionsRequest",
        ProtoDirection::Recv => "ApiVersionsResponse",
    };
    let inner = body
        .trim_start_matches(kind)
        .trim_start()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .trim();
    format!(
        "{kind} {{\n    // Fallback decode ({reason}). Requested wire version: v{requested_version}.\n    // Body shown below was decoded at the highest version this build understands.\n    {inner}\n}}",
    )
}

fn decode_one<T: Decodable + std::fmt::Debug>(buf: &mut Bytes, version: i16) -> Option<String> {
    let msg = T::decode(buf, version).ok()?;
    Some(format!("{msg:#?}"))
}

fn decode_request(api: ApiKey, version: i16, buf: &mut Bytes) -> Option<String> {
    match api {
        ApiKey::Produce => decode_one::<ProduceRequest>(buf, version),
        ApiKey::Fetch => decode_one::<FetchRequest>(buf, version),
        ApiKey::ListOffsets => decode_one::<ListOffsetsRequest>(buf, version),
        ApiKey::Metadata => decode_one::<MetadataRequest>(buf, version),
        ApiKey::OffsetCommit => decode_one::<OffsetCommitRequest>(buf, version),
        ApiKey::OffsetFetch => decode_one::<OffsetFetchRequest>(buf, version),
        ApiKey::FindCoordinator => decode_one::<FindCoordinatorRequest>(buf, version),
        ApiKey::JoinGroup => decode_one::<JoinGroupRequest>(buf, version),
        ApiKey::Heartbeat => decode_one::<HeartbeatRequest>(buf, version),
        ApiKey::LeaveGroup => decode_one::<LeaveGroupRequest>(buf, version),
        ApiKey::SyncGroup => decode_one::<SyncGroupRequest>(buf, version),
        ApiKey::DescribeGroups => decode_one::<DescribeGroupsRequest>(buf, version),
        ApiKey::ListGroups => decode_one::<ListGroupsRequest>(buf, version),
        ApiKey::SaslHandshake => decode_one::<SaslHandshakeRequest>(buf, version),
        ApiKey::ApiVersions => decode_one::<ApiVersionsRequest>(buf, version),
        ApiKey::CreateTopics => decode_one::<CreateTopicsRequest>(buf, version),
        ApiKey::InitProducerId => decode_one::<InitProducerIdRequest>(buf, version),
        ApiKey::DescribeConfigs => decode_one::<DescribeConfigsRequest>(buf, version),
        ApiKey::SaslAuthenticate => decode_one::<SaslAuthenticateRequest>(buf, version),
        ApiKey::DescribeCluster => decode_one::<DescribeClusterRequest>(buf, version),
        _ => None,
    }
}

fn decode_response(api: ApiKey, version: i16, buf: &mut Bytes) -> Option<String> {
    match api {
        ApiKey::Produce => decode_one::<ProduceResponse>(buf, version),
        ApiKey::Fetch => decode_one::<FetchResponse>(buf, version),
        ApiKey::ListOffsets => decode_one::<ListOffsetsResponse>(buf, version),
        ApiKey::Metadata => decode_one::<MetadataResponse>(buf, version),
        ApiKey::OffsetCommit => decode_one::<OffsetCommitResponse>(buf, version),
        ApiKey::OffsetFetch => decode_one::<OffsetFetchResponse>(buf, version),
        ApiKey::FindCoordinator => decode_one::<FindCoordinatorResponse>(buf, version),
        ApiKey::JoinGroup => decode_one::<JoinGroupResponse>(buf, version),
        ApiKey::Heartbeat => decode_one::<HeartbeatResponse>(buf, version),
        ApiKey::LeaveGroup => decode_one::<LeaveGroupResponse>(buf, version),
        ApiKey::SyncGroup => decode_one::<SyncGroupResponse>(buf, version),
        ApiKey::DescribeGroups => decode_one::<DescribeGroupsResponse>(buf, version),
        ApiKey::ListGroups => decode_one::<ListGroupsResponse>(buf, version),
        ApiKey::SaslHandshake => decode_one::<SaslHandshakeResponse>(buf, version),
        ApiKey::ApiVersions => decode_one::<ApiVersionsResponse>(buf, version),
        ApiKey::CreateTopics => decode_one::<CreateTopicsResponse>(buf, version),
        ApiKey::InitProducerId => decode_one::<InitProducerIdResponse>(buf, version),
        ApiKey::DescribeConfigs => decode_one::<DescribeConfigsResponse>(buf, version),
        ApiKey::SaslAuthenticate => decode_one::<SaslAuthenticateResponse>(buf, version),
        ApiKey::DescribeCluster => decode_one::<DescribeClusterResponse>(buf, version),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::doc_markdown)]
mod tests {
    use super::*;

    /// Exact bytes captured from a Confluent Cloud broker rejecting an
    /// ApiVersionsRequest v5: response is framed as v0 with
    /// error_code = 35 (UNSUPPORTED_VERSION) and a single-entry
    /// api_keys array advertising ApiVersions v0..=4.
    #[test]
    fn api_versions_v5_response_falls_back_to_v0_decode() {
        let bytes: &[u8] = &[
            0x00, 0x00, 0x00, 0x10, // size = 16
            0x00, 0x00, 0x00, 0x00, // corr_id = 0
            0x00, 0x23, // error_code = 35
            0x00, 0x00, 0x00, 0x01, // api_keys array length = 1 (v0, non-compact)
            0x00, 0x12, // api_key = 18 (ApiVersions)
            0x00, 0x00, // min_version = 0
            0x00, 0x04, // max_version = 4
        ];

        // Direct v5 decode would have failed silently (returned None
        // and the UI would show no decoded panel) — verify the
        // fallback now produces a useful debug string.
        let out = decode_frame(18, 5, ProtoDirection::Recv, bytes).expect("v5 fallback decoded");
        assert!(
            out.contains("KIP-511 downgrade"),
            "should annotate the fallback"
        );
        assert!(
            out.contains("error_code"),
            "should include error_code field"
        );
        assert!(out.contains("35") || out.contains("UnsupportedVersion"));
        assert!(out.contains("api_key: ApiKey(18)") || out.contains("18"));
    }
}
