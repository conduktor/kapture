/**
 * Wire types for the persistent session aggregate computed by the
 * Rust backend (`src-tauri/src/session_stats.rs`).
 *
 * The aggregate is folded incrementally on every captured event so
 * it survives ring-buffer eviction — a `MetadataResponse`
 * advertising `streams-output` keeps that topic visible in the
 * Session Activity tab even after the originating frame scrolls
 * out. Frontend just renders; no folding here.
 */

export interface ClientInfo {
  software: string;
  version: string;
}

export interface ConnectionInfo {
  localPort: number;
  frameCount: number;
}

export interface TopicStats {
  name: string;
  metadata: boolean;
  produced: boolean;
  consumed: boolean;
  errorCount: number;
}

export interface GroupStats {
  groupId: string;
  members: string[];
  generation: number | null;
  joinCount: number;
  heartbeatCount: number;
  commitCount: number;
  errorCount: number;
}

export interface ErrorEvent {
  ts: string;
  frameId: string;
  apiName: string;
  errorCode: number;
  groupId?: string;
}

export interface LatencyStats {
  localPort: number;
  apiKey: number;
  apiName: string;
  count: number;
  minMs: number;
  maxMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  p999Ms: number;
}

export interface SessionStats {
  client: ClientInfo | null;
  connections: ConnectionInfo[];
  topics: TopicStats[];
  groups: GroupStats[];
  errors: ErrorEvent[];
  latencies: LatencyStats[];
}

export const EMPTY_SESSION_STATS: SessionStats = {
  client: null,
  connections: [],
  topics: [],
  groups: [],
  errors: [],
  latencies: [],
};

/**
 * Map a Kafka error code to its canonical short name. The backend
 * ships the numeric code; we translate at render time so adding a
 * code is a frontend-only change. Source:
 * https://kafka.apache.org/protocol#protocol_error_codes
 */
export function errorName(code: number): string {
  return ERROR_NAMES[code] ?? `ERROR_${String(code)}`;
}

const ERROR_NAMES: Record<number, string> = {
  [-1]: "UNKNOWN_SERVER_ERROR",
  1: "OFFSET_OUT_OF_RANGE",
  2: "CORRUPT_MESSAGE",
  3: "UNKNOWN_TOPIC_OR_PARTITION",
  5: "LEADER_NOT_AVAILABLE",
  6: "NOT_LEADER_OR_FOLLOWER",
  7: "REQUEST_TIMED_OUT",
  9: "REPLICA_NOT_AVAILABLE",
  10: "MESSAGE_TOO_LARGE",
  13: "NETWORK_EXCEPTION",
  14: "COORDINATOR_LOAD_IN_PROGRESS",
  15: "COORDINATOR_NOT_AVAILABLE",
  16: "NOT_COORDINATOR",
  17: "INVALID_TOPIC_EXCEPTION",
  19: "NOT_ENOUGH_REPLICAS",
  20: "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
  22: "ILLEGAL_GENERATION",
  23: "INCONSISTENT_GROUP_PROTOCOL",
  24: "INVALID_GROUP_ID",
  25: "UNKNOWN_MEMBER_ID",
  26: "INVALID_SESSION_TIMEOUT",
  27: "REBALANCE_IN_PROGRESS",
  28: "INVALID_COMMIT_OFFSET_SIZE",
  29: "TOPIC_AUTHORIZATION_FAILED",
  30: "GROUP_AUTHORIZATION_FAILED",
  31: "CLUSTER_AUTHORIZATION_FAILED",
  33: "UNSUPPORTED_SASL_MECHANISM",
  34: "ILLEGAL_SASL_STATE",
  35: "UNSUPPORTED_VERSION",
  36: "TOPIC_ALREADY_EXISTS",
  37: "INVALID_PARTITIONS",
  38: "INVALID_REPLICATION_FACTOR",
  41: "NOT_CONTROLLER",
  42: "INVALID_REQUEST",
  44: "POLICY_VIOLATION",
  47: "FENCED_LEADER_EPOCH",
  48: "UNKNOWN_LEADER_EPOCH",
  49: "UNSUPPORTED_COMPRESSION_TYPE",
  50: "STALE_BROKER_EPOCH",
  51: "OFFSET_NOT_AVAILABLE",
  55: "OPERATION_NOT_ATTEMPTED",
  58: "INVALID_PRODUCER_EPOCH",
  62: "INVALID_TXN_STATE",
  74: "MEMBER_ID_REQUIRED",
  75: "PREFERRED_LEADER_NOT_AVAILABLE",
  76: "GROUP_MAX_SIZE_REACHED",
  77: "FENCED_INSTANCE_ID",
  79: "STALE_MEMBER_EPOCH",
  87: "TRANSACTION_ABORTED",
};
