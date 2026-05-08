/**
 * Aggregate session-level state from a `ProtoFrame[]` snapshot.
 *
 * Pure function — same shape as `aggregateByBroker` (walks the ring,
 * folds into a typed `SessionState`). Walked from the typed
 * `frame.summary` projection emitted by the Rust backend, never from
 * the Debug-formatted `decoded` string. New protocol surface = new
 * `FrameSummary` variant + a fold arm here. No regex.
 *
 * Intended consumer: the Session Activity tab. Recomputed on each
 * 1 Hz `proto_frames` poll via `useMemo([protoFrames])`.
 */
import type { FrameSummary, ProtoFrame } from "../types";

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
  /** Seen as the target of at least one `ProduceRequest`. */
  produced: boolean;
  /** Seen as the target of at least one `FetchRequest`. */
  consumed: boolean;
  /** Seen in at least one `MetadataResponse` topic list. */
  metadata: boolean;
  errorCount: number;
}

export interface GroupStats {
  groupId: string;
  members: Set<string>;
  /** Latest generation observed (from JoinGroupResponse / commits / heartbeats). */
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
  errorName: string;
  context: { topic?: string; group?: string };
}

export interface SessionState {
  client: ClientInfo | null;
  connections: ConnectionInfo[];
  topics: Map<string, TopicStats>;
  groups: Map<string, GroupStats>;
  errors: ErrorEvent[];
}

const ERRORS_CAP = 200;

export function aggregateSession(frames: ProtoFrame[]): SessionState {
  const state: SessionState = {
    client: null,
    connections: [],
    topics: new Map(),
    groups: new Map(),
    errors: [],
  };
  const connByPort = new Map<number, ConnectionInfo>();

  for (const f of frames) {
    let conn = connByPort.get(f.localPort);
    if (conn === undefined) {
      conn = { localPort: f.localPort, frameCount: 0 };
      connByPort.set(f.localPort, conn);
    }
    conn.frameCount += 1;
    if (f.summary !== undefined) {
      foldSummary(state, f, f.summary);
    }
  }

  state.connections = [...connByPort.values()].sort((a, b) => a.localPort - b.localPort);
  // Cap errors window: keep the most recent N. The full list lives
  // in the Protocol tab anyway; this view is a lossy summary.
  if (state.errors.length > ERRORS_CAP) {
    state.errors.splice(0, state.errors.length - ERRORS_CAP);
  }
  return state;
}

function foldSummary(state: SessionState, frame: ProtoFrame, s: FrameSummary): void {
  switch (s.kind) {
    case "apiVersionsRequest":
      // Last-write-wins. In practice the client sends one per
      // connection, so the latest reflects the active library.
      if (s.clientSoftwareName.length > 0) {
        state.client = {
          software: s.clientSoftwareName,
          version: s.clientSoftwareVersion,
        };
      }
      break;
    case "metadataResponse":
      for (const name of s.topics) {
        topicOf(state, name).metadata = true;
      }
      break;
    case "produceRequest":
      for (const name of s.topics) {
        topicOf(state, name).produced = true;
      }
      break;
    case "fetchRequest":
      for (const name of s.topics) {
        topicOf(state, name).consumed = true;
      }
      break;
    case "findCoordinatorRequest":
      for (const key of s.keys) {
        groupOf(state, key);
      }
      break;
    case "findCoordinatorResponse":
      if (s.errorCode !== 0) {
        pushError(state, frame, s.errorCode, {});
      }
      break;
    case "joinGroupRequest":
      groupOf(state, s.groupId).joinCount += 1;
      break;
    case "joinGroupResponse": {
      // No groupId on the response — the request immediately
      // preceding it on the same connection has it. We don't
      // correlate here; we instead attach the assigned memberId +
      // generation onto whichever group the frame's connection has
      // been touching. Cheap heuristic: the most-recently-touched
      // group on this connection. To stay pure-functional we skip
      // the connection-aware bit and rely on subsequent
      // Heartbeat/SyncGroup/OffsetCommit RPCs (which DO carry
      // groupId) to register member + generation.
      if (s.errorCode !== 0) {
        pushError(state, frame, s.errorCode, {});
      }
      break;
    }
    case "syncGroupRequest": {
      const g = groupOf(state, s.groupId);
      if (s.memberId.length > 0) {
        g.members.add(s.memberId);
      }
      g.generation = s.generationId;
      break;
    }
    case "syncGroupResponse":
      if (s.errorCode !== 0) {
        pushError(state, frame, s.errorCode, {});
      }
      break;
    case "heartbeatRequest": {
      const g = groupOf(state, s.groupId);
      g.heartbeatCount += 1;
      if (s.memberId.length > 0) {
        g.members.add(s.memberId);
      }
      g.generation = s.generationId;
      break;
    }
    case "heartbeatResponse":
      if (s.errorCode !== 0) {
        pushError(state, frame, s.errorCode, {});
      }
      break;
    case "leaveGroupRequest":
      groupOf(state, s.groupId);
      break;
    case "leaveGroupResponse":
      if (s.errorCode !== 0) {
        pushError(state, frame, s.errorCode, {});
      }
      break;
    case "offsetCommitRequest": {
      const g = groupOf(state, s.groupId);
      g.commitCount += 1;
      if (s.memberId.length > 0) {
        g.members.add(s.memberId);
      }
      for (const t of s.topics) {
        topicOf(state, t);
      }
      break;
    }
    case "offsetCommitResponse":
      if (s.maxErrorCode !== 0) {
        pushError(state, frame, s.maxErrorCode, {});
      }
      break;
  }
}

function topicOf(state: SessionState, name: string): TopicStats {
  let t = state.topics.get(name);
  if (t === undefined) {
    t = {
      name,
      produced: false,
      consumed: false,
      metadata: false,
      errorCount: 0,
    };
    state.topics.set(name, t);
  }
  return t;
}

function groupOf(state: SessionState, groupId: string): GroupStats {
  let g = state.groups.get(groupId);
  if (g === undefined) {
    g = {
      groupId,
      members: new Set(),
      generation: null,
      joinCount: 0,
      heartbeatCount: 0,
      commitCount: 0,
      errorCount: 0,
    };
    state.groups.set(groupId, g);
  }
  return g;
}

function pushError(
  state: SessionState,
  frame: ProtoFrame,
  errorCode: number,
  context: { topic?: string; group?: string },
): void {
  state.errors.push({
    ts: frame.timestamp,
    frameId: frame.id,
    apiName: frame.apiName,
    errorCode,
    errorName: errorName(errorCode),
    context,
  });
  if (context.topic !== undefined) {
    const t = state.topics.get(context.topic);
    if (t !== undefined) {
      t.errorCount += 1;
    }
  }
  if (context.group !== undefined) {
    const g = state.groups.get(context.group);
    if (g !== undefined) {
      g.errorCount += 1;
    }
  }
}

/**
 * Map the Kafka error code to its canonical short name. Covers the
 * codes a local-dev session realistically encounters; falls back to
 * `ERROR_<code>` for the long tail. Source:
 * https://kafka.apache.org/protocol#protocol_error_codes
 */
function errorName(code: number): string {
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
