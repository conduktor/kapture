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

use crate::proto_hook::ProtoDirection;

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
    let mut buf = Bytes::copy_from_slice(payload);
    // First 4 bytes are the wire size header (length of the rest of
    // the frame). Drop it.
    let _size = buf.get_i32();

    let api = ApiKey::try_from(i16::try_from(api_key).ok()?).ok()?;

    // Header version varies by api+version; the crate gives us the
    // mapping. Decode-and-drop the header — its contents are already
    // surfaced in ProtoFrame metadata.
    match direction {
        ProtoDirection::Send => {
            let header_version = api.request_header_version(api_version);
            let _hdr = RequestHeader::decode(&mut buf, header_version).ok()?;
            decode_request(api, api_version, &mut buf)
        }
        ProtoDirection::Recv => {
            let header_version = api.response_header_version(api_version);
            let _hdr = ResponseHeader::decode(&mut buf, header_version).ok()?;
            decode_response(api, api_version, &mut buf)
        }
    }
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
