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

/// Pass-through. Earlier versions of this fn injected `// comment`
/// lines inside the struct body to explain the fallback; turns out
/// the frontend's debugTree parser doesn't speak `//` so the tree
/// view fell back to a raw `<pre>` block. The fallback is still
/// observable from the data itself: `error_code: 35` in the body
/// (KIP-511) or a `max_version` lower than what was requested
/// (crate-version cap). No need to add ceremony.
fn annotate_apiversions_fallback(
    body: &str,
    _requested_version: i16,
    _direction: ProtoDirection,
    _reason: &str,
) -> String {
    body.to_owned()
}

fn decode_one<T: Decodable + std::fmt::Debug>(buf: &mut Bytes, version: i16) -> Option<String> {
    let msg = T::decode(buf, version).ok()?;
    Some(format!("{msg:#?}"))
}

// Both dispatch tables below are EXHAUSTIVE on `ApiKey` by design:
// the catch-all `_ => None` arm is intentionally absent so that
// regenerating the kafka-protocol fork against a newer apache/kafka
// schema (which may add new ApiKey variants) breaks the build until a
// human classifies each new variant as either:
//
//   * decode_one::<XxxRequest/Response>(buf, version) — a real
//     client-facing API that proxies see on the wire.
//   * `=> None` with a `// broker-internal` comment — a control-plane
//     RPC (KRaft consensus, broker registration, share-coordinator
//     state replication, etc.) that never crosses a client proxy and
//     so has no useful decoded view to surface.
//
// Arms are sorted by ApiKey numeric value to make schema diffs trivial.
//
// The two clippy lints disabled below would otherwise undermine the
// scheme:
//
//   * `too_many_lines`: the whole point is one explicit arm per
//     variant; the function is "too long" by design.
//   * `match_same_arms`: collapsing the broker-internal `=> None`
//     arms via `|`-patterns would still preserve exhaustiveness, but
//     it would obscure which specific variant belongs to which
//     control-plane subsystem — defeating the human-review value.

#[allow(clippy::too_many_lines, clippy::match_same_arms)]
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
        ApiKey::DeleteTopics => decode_one::<DeleteTopicsRequest>(buf, version),
        ApiKey::DeleteRecords => decode_one::<DeleteRecordsRequest>(buf, version),
        ApiKey::InitProducerId => decode_one::<InitProducerIdRequest>(buf, version),
        ApiKey::OffsetForLeaderEpoch => decode_one::<OffsetForLeaderEpochRequest>(buf, version),
        ApiKey::AddPartitionsToTxn => decode_one::<AddPartitionsToTxnRequest>(buf, version),
        ApiKey::AddOffsetsToTxn => decode_one::<AddOffsetsToTxnRequest>(buf, version),
        ApiKey::EndTxn => decode_one::<EndTxnRequest>(buf, version),
        ApiKey::WriteTxnMarkers => decode_one::<WriteTxnMarkersRequest>(buf, version),
        ApiKey::TxnOffsetCommit => decode_one::<TxnOffsetCommitRequest>(buf, version),
        ApiKey::DescribeAcls => decode_one::<DescribeAclsRequest>(buf, version),
        ApiKey::CreateAcls => decode_one::<CreateAclsRequest>(buf, version),
        ApiKey::DeleteAcls => decode_one::<DeleteAclsRequest>(buf, version),
        ApiKey::DescribeConfigs => decode_one::<DescribeConfigsRequest>(buf, version),
        ApiKey::AlterConfigs => decode_one::<AlterConfigsRequest>(buf, version),
        ApiKey::AlterReplicaLogDirs => decode_one::<AlterReplicaLogDirsRequest>(buf, version),
        ApiKey::DescribeLogDirs => decode_one::<DescribeLogDirsRequest>(buf, version),
        ApiKey::SaslAuthenticate => decode_one::<SaslAuthenticateRequest>(buf, version),
        ApiKey::CreatePartitions => decode_one::<CreatePartitionsRequest>(buf, version),
        ApiKey::CreateDelegationToken => decode_one::<CreateDelegationTokenRequest>(buf, version),
        ApiKey::RenewDelegationToken => decode_one::<RenewDelegationTokenRequest>(buf, version),
        ApiKey::ExpireDelegationToken => decode_one::<ExpireDelegationTokenRequest>(buf, version),
        ApiKey::DescribeDelegationToken => {
            decode_one::<DescribeDelegationTokenRequest>(buf, version)
        }
        ApiKey::DeleteGroups => decode_one::<DeleteGroupsRequest>(buf, version),
        ApiKey::ElectLeaders => decode_one::<ElectLeadersRequest>(buf, version),
        ApiKey::IncrementalAlterConfigs => {
            decode_one::<IncrementalAlterConfigsRequest>(buf, version)
        }
        ApiKey::AlterPartitionReassignments => {
            decode_one::<AlterPartitionReassignmentsRequest>(buf, version)
        }
        ApiKey::ListPartitionReassignments => {
            decode_one::<ListPartitionReassignmentsRequest>(buf, version)
        }
        ApiKey::OffsetDelete => decode_one::<OffsetDeleteRequest>(buf, version),
        ApiKey::DescribeClientQuotas => decode_one::<DescribeClientQuotasRequest>(buf, version),
        ApiKey::AlterClientQuotas => decode_one::<AlterClientQuotasRequest>(buf, version),
        ApiKey::DescribeUserScramCredentials => {
            decode_one::<DescribeUserScramCredentialsRequest>(buf, version)
        }
        ApiKey::AlterUserScramCredentials => {
            decode_one::<AlterUserScramCredentialsRequest>(buf, version)
        }
        ApiKey::Vote => None,             // broker-internal: KRaft consensus
        ApiKey::BeginQuorumEpoch => None, // broker-internal: KRaft consensus
        ApiKey::EndQuorumEpoch => None,   // broker-internal: KRaft consensus
        ApiKey::DescribeQuorum => None,   // broker-internal: KRaft consensus
        ApiKey::AlterPartition => None,   // broker-internal: broker -> controller
        ApiKey::UpdateFeatures => decode_one::<UpdateFeaturesRequest>(buf, version),
        ApiKey::Envelope => None, // broker-internal: KRaft envelope routing
        ApiKey::FetchSnapshot => None, // broker-internal: KRaft snapshot replication
        ApiKey::DescribeCluster => decode_one::<DescribeClusterRequest>(buf, version),
        ApiKey::DescribeProducers => decode_one::<DescribeProducersRequest>(buf, version),
        ApiKey::BrokerRegistration => None, // broker-internal: KRaft cluster mgmt
        ApiKey::BrokerHeartbeat => None,    // broker-internal: KRaft cluster mgmt
        ApiKey::UnregisterBroker => None,   // broker-internal: KRaft cluster mgmt
        ApiKey::DescribeTransactions => decode_one::<DescribeTransactionsRequest>(buf, version),
        ApiKey::ListTransactions => decode_one::<ListTransactionsRequest>(buf, version),
        ApiKey::AllocateProducerIds => None, // broker-internal: broker -> controller
        ApiKey::ConsumerGroupHeartbeat => decode_one::<ConsumerGroupHeartbeatRequest>(buf, version),
        ApiKey::ConsumerGroupDescribe => decode_one::<ConsumerGroupDescribeRequest>(buf, version),
        ApiKey::ControllerRegistration => None, // broker-internal: KRaft cluster mgmt
        ApiKey::GetTelemetrySubscriptions => {
            decode_one::<GetTelemetrySubscriptionsRequest>(buf, version)
        }
        ApiKey::PushTelemetry => decode_one::<PushTelemetryRequest>(buf, version),
        ApiKey::AssignReplicasToDirs => None, // broker-internal: broker storage
        ApiKey::ListConfigResources => decode_one::<ListConfigResourcesRequest>(buf, version),
        ApiKey::DescribeTopicPartitions => {
            decode_one::<DescribeTopicPartitionsRequest>(buf, version)
        }
        ApiKey::ShareGroupHeartbeat => decode_one::<ShareGroupHeartbeatRequest>(buf, version),
        ApiKey::ShareGroupDescribe => decode_one::<ShareGroupDescribeRequest>(buf, version),
        ApiKey::ShareFetch => decode_one::<ShareFetchRequest>(buf, version),
        ApiKey::ShareAcknowledge => decode_one::<ShareAcknowledgeRequest>(buf, version),
        ApiKey::AddRaftVoter => None, // broker-internal: KRaft membership
        ApiKey::RemoveRaftVoter => None, // broker-internal: KRaft membership
        ApiKey::UpdateRaftVoter => None, // broker-internal: KRaft membership
        ApiKey::InitializeShareGroupState => None, // broker-internal: share coordinator state
        ApiKey::ReadShareGroupState => None, // broker-internal: share coordinator state
        ApiKey::WriteShareGroupState => None, // broker-internal: share coordinator state
        ApiKey::DeleteShareGroupState => None, // broker-internal: share coordinator state
        ApiKey::ReadShareGroupStateSummary => None, // broker-internal: share coordinator state
        ApiKey::StreamsGroupHeartbeat => decode_one::<StreamsGroupHeartbeatRequest>(buf, version),
        ApiKey::StreamsGroupDescribe => decode_one::<StreamsGroupDescribeRequest>(buf, version),
        ApiKey::DescribeShareGroupOffsets => {
            decode_one::<DescribeShareGroupOffsetsRequest>(buf, version)
        }
        ApiKey::AlterShareGroupOffsets => decode_one::<AlterShareGroupOffsetsRequest>(buf, version),
        ApiKey::DeleteShareGroupOffsets => {
            decode_one::<DeleteShareGroupOffsetsRequest>(buf, version)
        }
    }
}

#[allow(clippy::too_many_lines, clippy::match_same_arms)]
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
        ApiKey::DeleteTopics => decode_one::<DeleteTopicsResponse>(buf, version),
        ApiKey::DeleteRecords => decode_one::<DeleteRecordsResponse>(buf, version),
        ApiKey::InitProducerId => decode_one::<InitProducerIdResponse>(buf, version),
        ApiKey::OffsetForLeaderEpoch => decode_one::<OffsetForLeaderEpochResponse>(buf, version),
        ApiKey::AddPartitionsToTxn => decode_one::<AddPartitionsToTxnResponse>(buf, version),
        ApiKey::AddOffsetsToTxn => decode_one::<AddOffsetsToTxnResponse>(buf, version),
        ApiKey::EndTxn => decode_one::<EndTxnResponse>(buf, version),
        ApiKey::WriteTxnMarkers => decode_one::<WriteTxnMarkersResponse>(buf, version),
        ApiKey::TxnOffsetCommit => decode_one::<TxnOffsetCommitResponse>(buf, version),
        ApiKey::DescribeAcls => decode_one::<DescribeAclsResponse>(buf, version),
        ApiKey::CreateAcls => decode_one::<CreateAclsResponse>(buf, version),
        ApiKey::DeleteAcls => decode_one::<DeleteAclsResponse>(buf, version),
        ApiKey::DescribeConfigs => decode_one::<DescribeConfigsResponse>(buf, version),
        ApiKey::AlterConfigs => decode_one::<AlterConfigsResponse>(buf, version),
        ApiKey::AlterReplicaLogDirs => decode_one::<AlterReplicaLogDirsResponse>(buf, version),
        ApiKey::DescribeLogDirs => decode_one::<DescribeLogDirsResponse>(buf, version),
        ApiKey::SaslAuthenticate => decode_one::<SaslAuthenticateResponse>(buf, version),
        ApiKey::CreatePartitions => decode_one::<CreatePartitionsResponse>(buf, version),
        ApiKey::CreateDelegationToken => decode_one::<CreateDelegationTokenResponse>(buf, version),
        ApiKey::RenewDelegationToken => decode_one::<RenewDelegationTokenResponse>(buf, version),
        ApiKey::ExpireDelegationToken => decode_one::<ExpireDelegationTokenResponse>(buf, version),
        ApiKey::DescribeDelegationToken => {
            decode_one::<DescribeDelegationTokenResponse>(buf, version)
        }
        ApiKey::DeleteGroups => decode_one::<DeleteGroupsResponse>(buf, version),
        ApiKey::ElectLeaders => decode_one::<ElectLeadersResponse>(buf, version),
        ApiKey::IncrementalAlterConfigs => {
            decode_one::<IncrementalAlterConfigsResponse>(buf, version)
        }
        ApiKey::AlterPartitionReassignments => {
            decode_one::<AlterPartitionReassignmentsResponse>(buf, version)
        }
        ApiKey::ListPartitionReassignments => {
            decode_one::<ListPartitionReassignmentsResponse>(buf, version)
        }
        ApiKey::OffsetDelete => decode_one::<OffsetDeleteResponse>(buf, version),
        ApiKey::DescribeClientQuotas => decode_one::<DescribeClientQuotasResponse>(buf, version),
        ApiKey::AlterClientQuotas => decode_one::<AlterClientQuotasResponse>(buf, version),
        ApiKey::DescribeUserScramCredentials => {
            decode_one::<DescribeUserScramCredentialsResponse>(buf, version)
        }
        ApiKey::AlterUserScramCredentials => {
            decode_one::<AlterUserScramCredentialsResponse>(buf, version)
        }
        ApiKey::Vote => None,             // broker-internal: KRaft consensus
        ApiKey::BeginQuorumEpoch => None, // broker-internal: KRaft consensus
        ApiKey::EndQuorumEpoch => None,   // broker-internal: KRaft consensus
        ApiKey::DescribeQuorum => None,   // broker-internal: KRaft consensus
        ApiKey::AlterPartition => None,   // broker-internal: broker -> controller
        ApiKey::UpdateFeatures => decode_one::<UpdateFeaturesResponse>(buf, version),
        ApiKey::Envelope => None, // broker-internal: KRaft envelope routing
        ApiKey::FetchSnapshot => None, // broker-internal: KRaft snapshot replication
        ApiKey::DescribeCluster => decode_one::<DescribeClusterResponse>(buf, version),
        ApiKey::DescribeProducers => decode_one::<DescribeProducersResponse>(buf, version),
        ApiKey::BrokerRegistration => None, // broker-internal: KRaft cluster mgmt
        ApiKey::BrokerHeartbeat => None,    // broker-internal: KRaft cluster mgmt
        ApiKey::UnregisterBroker => None,   // broker-internal: KRaft cluster mgmt
        ApiKey::DescribeTransactions => decode_one::<DescribeTransactionsResponse>(buf, version),
        ApiKey::ListTransactions => decode_one::<ListTransactionsResponse>(buf, version),
        ApiKey::AllocateProducerIds => None, // broker-internal: broker -> controller
        ApiKey::ConsumerGroupHeartbeat => {
            decode_one::<ConsumerGroupHeartbeatResponse>(buf, version)
        }
        ApiKey::ConsumerGroupDescribe => decode_one::<ConsumerGroupDescribeResponse>(buf, version),
        ApiKey::ControllerRegistration => None, // broker-internal: KRaft cluster mgmt
        ApiKey::GetTelemetrySubscriptions => {
            decode_one::<GetTelemetrySubscriptionsResponse>(buf, version)
        }
        ApiKey::PushTelemetry => decode_one::<PushTelemetryResponse>(buf, version),
        ApiKey::AssignReplicasToDirs => None, // broker-internal: broker storage
        ApiKey::ListConfigResources => decode_one::<ListConfigResourcesResponse>(buf, version),
        ApiKey::DescribeTopicPartitions => {
            decode_one::<DescribeTopicPartitionsResponse>(buf, version)
        }
        ApiKey::ShareGroupHeartbeat => decode_one::<ShareGroupHeartbeatResponse>(buf, version),
        ApiKey::ShareGroupDescribe => decode_one::<ShareGroupDescribeResponse>(buf, version),
        ApiKey::ShareFetch => decode_one::<ShareFetchResponse>(buf, version),
        ApiKey::ShareAcknowledge => decode_one::<ShareAcknowledgeResponse>(buf, version),
        ApiKey::AddRaftVoter => None, // broker-internal: KRaft membership
        ApiKey::RemoveRaftVoter => None, // broker-internal: KRaft membership
        ApiKey::UpdateRaftVoter => None, // broker-internal: KRaft membership
        ApiKey::InitializeShareGroupState => None, // broker-internal: share coordinator state
        ApiKey::ReadShareGroupState => None, // broker-internal: share coordinator state
        ApiKey::WriteShareGroupState => None, // broker-internal: share coordinator state
        ApiKey::DeleteShareGroupState => None, // broker-internal: share coordinator state
        ApiKey::ReadShareGroupStateSummary => None, // broker-internal: share coordinator state
        ApiKey::StreamsGroupHeartbeat => decode_one::<StreamsGroupHeartbeatResponse>(buf, version),
        ApiKey::StreamsGroupDescribe => decode_one::<StreamsGroupDescribeResponse>(buf, version),
        ApiKey::DescribeShareGroupOffsets => {
            decode_one::<DescribeShareGroupOffsetsResponse>(buf, version)
        }
        ApiKey::AlterShareGroupOffsets => {
            decode_one::<AlterShareGroupOffsetsResponse>(buf, version)
        }
        ApiKey::DeleteShareGroupOffsets => {
            decode_one::<DeleteShareGroupOffsetsResponse>(buf, version)
        }
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
        // and the UI would show no decoded panel). The fallback retries
        // as v0 so the user sees the broker's downgrade-error response.
        // Annotation comments were dropped because the frontend tree
        // parser couldn't handle injected `// ...` lines; the value of
        // the fallback is purely that the body decodes at all.
        let out = decode_frame(18, 5, ProtoDirection::Recv, bytes).expect("v5 fallback decoded");
        assert!(
            out.contains("error_code"),
            "should include error_code field"
        );
        assert!(out.contains("35") || out.contains("UnsupportedVersion"));
        assert!(out.contains("api_key: ApiKey(18)") || out.contains("18"));
    }
}
