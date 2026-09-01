export type DecodedValue =
  | { kind: "primitive"; type: "string" | "number" | "boolean" | "null"; value: string }
  | { kind: "bytes"; hex: string; length: number }
  | { kind: "object"; fields: { name: string; value: DecodedValue }[] }
  | { kind: "array"; items: DecodedValue[] };

export interface KafkaHeader {
  key: string;
  value: string;
}

export interface FetchMetadata {
  apiKey: number;
  apiName: string;
  apiVersion: number;
  connectionId: number;
  corrId: number;
  responseSize: number;
  rttMs: number;
}

export type ProtoDirection = "send" | "recv";

/**
 * `kapture:message-schema-resolved` payload row. The resolver task
 * (Rust `schema_resolver.rs`) mints one per record after the registry
 * has answered, so the live UI can patch the cached summary in place
 * without re-fetching the full message via `inspect_message_by_id`.
 *
 * `schemaKind === "UNRESOLVED"` means the registry rejected the id
 * (404 / timeout / non-2xx). The 5-min failure cache backstops
 * retry-storms; the row stays patched as UNRESOLVED until the TTL
 * expires and a fresh record carrying the same id arrives.
 */
export interface SchemaResolvedPatch {
  id: string;
  schemaName: string | null;
  schemaKind: string | null;
  /** Decoded payload tree once the resolver has parsed the value
   *  bytes against the schema (Avro / JSON-Schema). `null` when
   *  decoding wasn't attempted (Protobuf, hex-parse failure) or
   *  when only the schema metadata changed. */
  payload: DecodedValue | null;
}

/**
 * Lightweight projection of a protocol frame — everything the list
 * row needs and nothing more. The 1 Hz proto_frames poll returns
 * these to keep the IPC payload small even when the ring buffer is
 * full of large Fetch responses.
 */
export interface ProtoFrame {
  id: string;
  timestamp: string;
  direction: ProtoDirection;
  apiKey: number;
  apiName: string;
  apiVersion: number;
  connectionId: number;
  /**
   * Local proxy listener port that owned the pump that emitted this
   * frame. `0` when not attributable to a proxy listener (defensive
   * default; no such code path exists today). Used by the BrokersTab
   * to aggregate send/recv counters per broker.
   */
  localPort: number;
  corrId: number;
  /** True wire size. */
  size: number;
  /** Bytes captured for inspection. `≤ size` (capped at 64 KiB). */
  captured: number;
  /** Round-trip time in ms. Only meaningful when `direction === "recv"`. */
  rttMs: number;
  /** Observation-to-Rust delay for an external tap. */
  captureLagMs: number;
  /** Capture-to-bounded-analyzer delay, including external tap queueing. */
  analysisLagMs: number;
  /**
   * Typed projection of the decoded body for APIs the Session
   * Activity tab aggregates. `undefined` for non-projected APIs or
   * when decoding failed. The discriminator is `kind` — see
   * `FrameSummary` below.
   */
  summary?: FrameSummary;
  /**
   * Set when the proxy accepted the client TCP but couldn't reach
   * upstream — the frame was decoded from the client send but never
   * forwarded. Surfaces in the Protocol tab as an error-state row so
   * the user can see what the client emitted and how it retried.
   */
  frameError?: string;
}

/**
 * Structured projection of a decoded protocol body. Eagerly extracted
 * at frame ingestion (alongside the existing `decoded` Debug string)
 * so the Session Activity tab can fold each frame into session-level
 * aggregates without re-parsing the Debug representation.
 *
 * Coverage is intentionally narrow: control-plane RPCs, plus
 * Produce/Fetch *requests* (topic names only — record batches stay
 * opaque). Per-partition errors nested in Produce/Fetch responses
 * are out of scope.
 */
export type FrameSummary =
  | {
      kind: "apiVersionsRequest";
      clientSoftwareName: string;
      clientSoftwareVersion: string;
    }
  | { kind: "metadataResponse"; topics: string[]; brokers: number }
  | { kind: "produceRequest"; topics: string[] }
  | { kind: "fetchRequest"; topics: string[] }
  | { kind: "findCoordinatorRequest"; keys: string[] }
  | { kind: "findCoordinatorResponse"; errorCode: number; nodeId: number }
  | { kind: "joinGroupRequest"; groupId: string; memberId: string }
  | {
      kind: "joinGroupResponse";
      errorCode: number;
      generationId: number;
      memberId: string;
    }
  | {
      kind: "syncGroupRequest";
      groupId: string;
      memberId: string;
      generationId: number;
    }
  | { kind: "syncGroupResponse"; errorCode: number }
  | {
      kind: "heartbeatRequest";
      groupId: string;
      memberId: string;
      generationId: number;
    }
  | { kind: "heartbeatResponse"; errorCode: number }
  | { kind: "leaveGroupRequest"; groupId: string }
  | { kind: "leaveGroupResponse"; errorCode: number }
  | {
      kind: "offsetCommitRequest";
      groupId: string;
      memberId: string;
      topics: string[];
    }
  | { kind: "offsetCommitResponse"; maxErrorCode: number };

/**
 * Full frame including the captured bytes (lowercase hex) and the
 * decoded body. Fetched on demand via `proto_frame_detail(id)` only
 * when the user selects a row.
 */
export interface ProtoFrameDetail extends ProtoFrame {
  /** Lowercase hex of the captured prefix. Empty when capture was 0 bytes. */
  payloadHex: string;
  /**
   * Typed JSON of the decoded request/response body. Emitted by the
   * Kapture fork of `kafka-protocol` (which derives `serde::Serialize`
   * on every message struct). Newtype wrappers like `GroupId`,
   * `TopicName` flatten transparently to strings; `unknownTaggedFields`
   * surface as objects keyed by tag id. `undefined` for APIs we don't
   * decode yet, when the bytes were truncated past the body, or when
   * the header parse failed — the UI then falls back to the raw hex
   * view.
   */
  decodedJson?: unknown;
}

export interface ProtoFramesDelta {
  frames: ProtoFrame[];
  reset: boolean;
  nextCursor: string | null;
}

export interface DecodedBodyResult {
  id: string;
  decodedJson: unknown;
}

/**
 * Live wire format for the Messages tab.
 *
 * The backend ring buffer holds the full `CapturedMessage`, but the
 * `kapture:messages` event and the `snapshot` command both transmit
 * this lightweight projection — no payload, no rawHex, no headers.
 * That's what keeps the Messages tab responsive at high throughput
 * (measured: ~80× IPC reduction vs full message). When the user
 * selects a row, the full body is fetched lazily via
 * `inspect_message_by_id`.
 */
export interface KafkaMessage {
  id: string;
  timestamp: string;
  topic: string;
  /** KIP-516 topic UUID. Null on legacy wire formats (Produce/Fetch v0-12). */
  topicId: string | null;
  partition: number;
  offset: number;
  /** Stringified key, truncated to ~128 chars for the live preview. */
  key: string | null;
  schemaName: string | null;
  /** Legacy magic-byte schema id (`0x00 | u32_be id | …` payload
   *  prefix). Mutually exclusive with `schemaGuid` in practice. */
  schemaId: number | null;
  /** Confluent CP 8.1.1+ header-stored 16-byte UUID GUID. Resolved
   *  via the registry's `/schemas/guids/{guid}` endpoint. */
  schemaGuid: string | null;
  /** Confluent schema kind label ("AVRO" / "JSON" / "PROTOBUF"), or
   *  "UNRESOLVED" when the registry rejected the id. `null` while a
   *  resolution is pending or when no registry is configured. */
  schemaKind: string | null;
  /** Total of `keySize` + `valueSize` (the user-meaningful bytes).
   *  Wire framing (varints, attrs, header k/v lengths) is not counted. */
  sizeBytes: number;
  /** Raw bytes in the record key (0 when null/absent). */
  keySize: number;
  /** Raw bytes in the record value (0 when null/absent). */
  valueSize: number;
  /** Number of headers; full keys+values are on `KafkaMessageDetail`. */
  headersCount: number;
  /** Originating Fetch frame for backlinks; null on extraction failure. */
  fetch: FetchMetadata | null;
  /**
   * Identifier for the proxy TCP connection that carried this record.
   * `null` when the record couldn't be attributed to a connection.
   */
  connectionId: number | null;
}

/**
 * Full body — fetched lazily when the user selects a message via
 * `inspect_message_by_id`. Mirrors the backend `CapturedMessage`.
 * Distinct from `KafkaMessage` (the summary) because the wire shape
 * differs: this one carries `headers` (the full vec), the summary
 * carries `headersCount`.
 */
export interface KafkaMessageDetail {
  id: string;
  timestamp: string;
  topic: string;
  topicId: string | null;
  partition: number;
  offset: number;
  /** Full key, untruncated. */
  key: string | null;
  schemaName: string | null;
  schemaId: number | null;
  schemaGuid: string | null;
  schemaKind: string | null;
  sizeBytes: number;
  keySize: number;
  valueSize: number;
  headers: KafkaHeader[];
  payload: DecodedValue;
  rawHex: string;
  fetch: FetchMetadata | null;
  connectionId: number | null;
}

export interface AppInfo {
  name: string;
  version: string;
  status: string;
}

export type ConnectionStatus = "disconnected" | "connecting" | "connected" | "error";

export interface ProxyConfig {
  upstream: string;
  listenPort: number;
}

export interface ProxyStatus {
  listenAddr: string;
  upstream: string;
}

/**
 * One Java process listed by `list_local_jvms` for the tap picker.
 * `looksKafkaActive` is best-effort: `true` when we detected a TCP
 * connection to a Kafka-shaped port. Absence does not mean "not a
 * Kafka client", only "no live socket observed".
 */
export interface JvmProcess {
  pid: number;
  command: string;
  looksKafkaActive: boolean;
}

/** Result of `attach_jvm_tap_agent`. `log` is the verbatim stdout +
 * stderr from the JDK attacher — surfaced unmodified on failure so
 * the user sees the real cause (DisableAttachMechanism, JRE-only,
 * wrong UID, target uses Conscrypt, etc.). */
export interface AttachResult {
  success: boolean;
  log: string;
}

/**
 * Result of `test_proxy_upstream` — a one-shot probe of the upstream
 * broker that runs the same handshake as `start_proxy` (TLS + SASL +
 * ApiVersionsRequest v3) and closes. `apiVersionsCount` is populated
 * only on success.
 */
export interface TestUpstreamResult {
  ok: boolean;
  latencyMs: number;
  message: string;
  apiVersionsCount: number | null;
}

/**
 * TLS args for the upstream hop in proxy mode. Mirrors the Rust
 * `ProxyTlsArgs` struct (camelCase serde). `serverName` empty string
 * means "use the bootstrap host parsed from `upstream`".
 */
export interface ProxyTlsArgs {
  serverName: string;
  caPath: string | null;
  skipHostnameVerification: boolean;
}

/**
 * SASL credentials for the upstream hop. Mirrors `ProxySaslArgs`.
 * Backend currently accepts `mechanism === "PLAIN"` only.
 */
export interface ProxySaslArgs {
  mechanism: SaslMechanism;
  username: string;
  password: string;
}

/**
 * Snapshot of the running proxy. Returned by the `proxy_status`
 * command (polled by the StatusBar) and the `kapture_proxy_status`
 * MCP tool. `listening: false` when no proxy is active — the rest
 * of the fields are zeroed in that case.
 */
export interface ProxyStatusSummary {
  listening: boolean;
  listenAddr: string | null;
  upstream: string | null;
  activeConnections: number;
  /** `[[upstreamHost, upstreamPort], localPort]` sorted by localPort. */
  brokerMappings: [[string, number], number][];
}

export interface ConnectionState {
  status: ConnectionStatus;
  /** Proxy upstream remembered locally so the edit dialog can prefill. */
  upstream: string | null;
  error: string | null;
  /** Populated only when the listener is up. */
  proxyStatus: ProxyStatus | null;
  /** When in tap mode, identifies which JVM is being observed. The
   * cluster pill shows `tap PID X` instead of `proxy <addr> → <up>`.
   * Mutually exclusive with `proxyStatus` — at most one is non-null
   * because the backend's capture slot is shared. */
  tapStatus: { pid: number; command: string; socketPath: string } | null;
}

export interface CaptureStats {
  totalReceived: number;
  inBuffer: number;
  bufferCapacity: number;
  bufferBytes: number;
  bufferByteCapacity: number;
  drops: number;
  bufferEvictions: number;
  oversizedDrops: number;
  uiSummaryDrops: number;
  analyzerDrops: number;
  recordExtractionDrops: number;
  agentDrops: number;
  throughputPerSec: number;
  /** Drops/sec over the last stats tick. Sustained > 0 = hemorrhage. */
  dropsPerSec: number;
}

export interface EbpfTarget {
  pid: number;
  command: string;
  libraryPath: string;
}

export interface EbpfTapStatus extends EbpfTarget {
  socketPath: string;
}

export type SaslMechanism = "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";

export interface ProbeResult {
  bootstrapServers: string | null;
  schemaRegistryUrl: string | null;
  /** Friendly cluster name (e.g. "Redpanda", "Apache Kafka"). */
  flavour: string | null;
}

export interface ProfileTlsMetadata {
  caPath: string | null;
  certPath: string | null;
  keyPath: string | null;
  /** True when a TLS key password is stored in the OS keychain. */
  hasKeyPassword: boolean;
}

export interface ProfileAuthMetadata {
  mechanism: SaslMechanism;
  username: string;
  useTls: boolean;
  /** True when a password is stored in the OS keychain for this profile. */
  hasPassword: boolean;
  tls: ProfileTlsMetadata | null;
}

/**
 * Proxy-mode upstream TLS settings persisted in a profile. Mirrors the
 * Rust `UpstreamTlsMetadata` (camelCase serde). Distinct from
 * `ProfileTlsMetadata`, which tracks legacy client-mode mTLS material.
 */
export interface UpstreamTlsMetadata {
  /** SNI / cert hostname. Empty string = derive from the bootstrap host. */
  serverName: string;
  caPath: string | null;
  skipHostnameVerification: boolean;
}

/**
 * Proxy-mode upstream SASL settings persisted in a profile. The
 * password lives in the OS keychain (slot `<name>::proxy-sasl`); on
 * load the dialog re-prompts the user.
 */
export interface UpstreamSaslMetadata {
  mechanism: SaslMechanism;
  username: string;
  /** True when a password is stored in the OS keychain for this profile. */
  hasPassword: boolean;
}

export interface ProfileMetadata {
  name: string;
  bootstrapServers: string;
  /** `null` means "use the default pattern" (every non-internal topic). */
  topicPattern: string | null;
  schemaRegistryUrl: string | null;
  auth: ProfileAuthMetadata | null;
  fromBeginning: boolean;
  upstreamTls: UpstreamTlsMetadata | null;
  upstreamSasl: UpstreamSaslMetadata | null;
}

export interface LoadedProfile extends ProfileMetadata {
  password: string | null;
  keyPassword: string | null;
  /** Proxy-mode upstream SASL password resolved from the keychain. */
  upstreamSaslPassword: string | null;
}

export interface SaveProfileTls {
  caPath: string | null;
  certPath: string | null;
  keyPath: string | null;
  /** `null` to leave any existing keychain TLS key password untouched. */
  keyPassword: string | null;
}

export interface SaveProfileAuth {
  mechanism: SaslMechanism;
  username: string;
  useTls: boolean;
  /** `null` to leave any existing keychain password untouched. */
  password: string | null;
  tls: SaveProfileTls | null;
}

/**
 * Proxy-mode upstream TLS args sent on save. Paths are stored in
 * cleartext on disk; there is no key file in proxy mode (the proxy
 * does not present a client cert), so no keychain entry is involved.
 */
export interface SaveProfileUpstreamTls {
  /** SNI / cert hostname. Empty string = derive from the bootstrap host. */
  serverName: string;
  caPath: string | null;
  skipHostnameVerification: boolean;
}

/**
 * Proxy-mode upstream SASL args sent on save. `password` follows the
 * same `Some(secret) | Some("") | null` semantics as `SaveProfileAuth`:
 * `null` leaves the keychain entry untouched.
 */
export interface SaveProfileUpstreamSasl {
  mechanism: SaslMechanism;
  username: string;
  /** `null` to leave any existing keychain proxy-SASL password untouched. */
  password: string | null;
}

export interface SaveProfileArgs {
  name: string;
  bootstrapServers: string;
  /** `null` records the default-pattern intent (every non-internal topic). */
  topicPattern: string | null;
  schemaRegistryUrl: string | null;
  auth: SaveProfileAuth | null;
  fromBeginning: boolean;
  upstreamTls: SaveProfileUpstreamTls | null;
  upstreamSasl: SaveProfileUpstreamSasl | null;
}
