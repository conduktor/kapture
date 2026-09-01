/**
 * Wire types for the anti-pattern detector fold computed by the Rust
 * backend (`src-tauri/src/anti_patterns.rs`). The backend exposes the
 * snapshot via the `anti_patterns` Tauri command; the Expert tab
 * polls it on the same 1 Hz schedule as session stats.
 *
 * Each detection is keyed by `(kind, scope)` server-side, so successive
 * contributing frames update the same row instead of fanning out.
 */

export type AntiPatternKind =
  | "overcommit"
  | "producerPerRecord"
  | "tinyBatches"
  | "rebalanceLoop"
  | "staleLeaderProducing"
  | "mixedApiVersion"
  | "saslSessionTooShort"
  | "acks0"
  | "compressionOff"
  | "nonIdempotentProducer"
  | "producerInstanceLeak"
  | "transactionalZombie"
  | "autoCommitCadence"
  | "tightFetchPolling"
  | "fetchSessionErrorCascade"
  | "throttlePressure"
  | "metadataStorm"
  | "classicRebalanceOnModernCluster"
  | "messageTooLargeRejected"
  | "offsetOutOfRangeOnFetch"
  | "cooperativeStickyChurn"
  | "commitDuringRebalance"
  | "aclDeny"
  | "unknownTopicPollLoop"
  | "coordinatorChurn"
  | "slowConsumerPollStall"
  | "hungRequests"
  | "inFlightSaturation"
  | "excessiveIdempotentInFlight"
  | "readUncommittedTransactional"
  | "partitionSkew"
  | "retryStorm";

export type Severity = "warn" | "note";

export interface Detection {
  kind: AntiPatternKind;
  severity: Severity;
  title: string;
  detail: string;
  scope: string;
  firstSeen: string;
  lastSeen: string;
  occurrences: number;
  frameId: string | null;
}

export interface AntiPatternsSnapshot {
  detections: Detection[];
}

export const EMPTY_ANTI_PATTERNS: AntiPatternsSnapshot = {
  detections: [],
};

/** Stable copy for empty / sentinel rendering. */
export function kindLabel(kind: AntiPatternKind): string {
  switch (kind) {
    case "overcommit":
      return "Overcommit";
    case "producerPerRecord":
      return "Producer per record";
    case "tinyBatches":
      return "Tiny Produce batches";
    case "rebalanceLoop":
      return "Rebalance loop";
    case "staleLeaderProducing":
      return "Stale-leader producing";
    case "mixedApiVersion":
      return "Mixed api_version across brokers";
    case "saslSessionTooShort":
      return "SASL session too short on re-auth";
    case "acks0":
      return "acks=0 (silent durability loss)";
    case "compressionOff":
      return "Compression off on bursty producer";
    case "nonIdempotentProducer":
      return "Non-idempotent producer";
    case "producerInstanceLeak":
      return "Producer-instance leak";
    case "transactionalZombie":
      return "Transactional zombie";
    case "autoCommitCadence":
      return "Auto-commit cadence";
    case "tightFetchPolling":
      return "Tight fetch polling";
    case "fetchSessionErrorCascade":
      return "Fetch-session error cascade";
    case "throttlePressure":
      return "Throttle pressure";
    case "metadataStorm":
      return "Metadata storm";
    case "classicRebalanceOnModernCluster":
      return "Classic rebalance on KIP-848 cluster";
    case "messageTooLargeRejected":
      return "Message too large rejected by broker";
    case "offsetOutOfRangeOnFetch":
      return "Offset out of range on Fetch";
    case "cooperativeStickyChurn":
      return "Cooperative-sticky rebalance churn";
    case "commitDuringRebalance":
      return "Offset commit during rebalance";
    case "aclDeny":
      return "ACL deny";
    case "unknownTopicPollLoop":
      return "Unknown-topic poll loop";
    case "coordinatorChurn":
      return "Coordinator churn";
    case "slowConsumerPollStall":
      return "Slow consumer poll stall";
    case "hungRequests":
      return "Hung requests";
    case "inFlightSaturation":
      return "In-flight request saturation";
    case "excessiveIdempotentInFlight":
      return "Excessive idempotent Produce in-flight";
    case "readUncommittedTransactional":
      return "read_uncommitted on transactional traffic";
    case "partitionSkew":
      return "Partition traffic skew";
    case "retryStorm":
      return "Retriable-error storm";
  }
}
