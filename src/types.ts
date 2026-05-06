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
  corrId: number;
  /** True wire size. */
  size: number;
  /** Bytes captured for inspection. `≤ size` (capped at 64 KiB). */
  captured: number;
  /** Round-trip time in ms. Only meaningful when `direction === "recv"`. */
  rttMs: number;
}

/**
 * Full frame including the captured bytes (lowercase hex) and the
 * decoded body. Fetched on demand via `proto_frame_detail(id)` only
 * when the user selects a row.
 */
export interface ProtoFrameDetail extends ProtoFrame {
  /** Lowercase hex of the captured prefix. Empty when capture was 0 bytes. */
  payloadHex: string;
  /**
   * Pretty-printed Debug of the decoded request/response body via the
   * `kafka-protocol` crate, when the apiKey is in our supported set.
   * `null` for APIs we don't decode yet — the UI then falls back to
   * the raw hex view.
   */
  decoded: string | null;
}

export interface KafkaMessage {
  id: string;
  timestamp: string;
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  schemaName: string | null;
  schemaId: number | null;
  sizeBytes: number;
  headers: KafkaHeader[];
  payload: DecodedValue;
  rawHex: string;
  fetch: FetchMetadata | null;
  /**
   * Identifier for the protocol channel that carried this record.
   * In proxy mode it's the per-TCP-connection id; in client (rdkafka)
   * mode it's the librdkafka broker_id. `null` when neither path
   * could attribute the record to a connection.
   */
  connectionId: number | null;
}

export interface AppInfo {
  name: string;
  version: string;
  status: string;
}

export type ConnectionStatus = "disconnected" | "connecting" | "connected" | "error";

export type ConnectionMode = "client" | "proxy";

export interface ProxyConfig {
  upstream: string;
  listenPort: number;
}

export interface ProxyStatus {
  listenAddr: string;
  upstream: string;
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
 * command (polled by the SidePanel) and the `kapture_proxy_status`
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
  /** "client" = Kapture connects as a consumer; "proxy" = apps point at us. */
  mode: ConnectionMode;
  cluster: string | null;
  /** Topic regex actually in effect on the broker side (default ^[^_].*). */
  topicPattern: string | null;
  error: string | null;
  // Non-secret connection params remembered locally so the edit dialog
  // can prefill them. Secrets (auth.password, tls.keyPassword) are NOT
  // round-tripped — the user re-enters them in edit mode.
  schemaRegistryUrl: string | null;
  fromBeginning: boolean;
  authPrefill: ConnectionAuthPrefill | null;
  /** Populated only when mode === "proxy" and the listener is up. */
  proxyStatus: ProxyStatus | null;
}

export interface ConnectionAuthPrefill {
  mechanism: SaslMechanism;
  username: string;
  useTls: boolean;
  caPath: string | null;
  certPath: string | null;
  keyPath: string | null;
}

export interface CaptureStats {
  totalReceived: number;
  inBuffer: number;
  bufferCapacity: number;
  bufferBytes: number;
  bufferByteCapacity: number;
  drops: number;
  throughputPerSec: number;
}

export type SaslMechanism = "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";

export interface TlsArgs {
  caPath: string | null;
  certPath: string | null;
  keyPath: string | null;
  keyPassword: string | null;
}

export interface AuthArgs {
  mechanism: SaslMechanism;
  username: string;
  password: string;
  /** `true` for `SASL_SSL`, `false` for `SASL_PLAINTEXT`. */
  useTls: boolean;
  tls: TlsArgs | null;
}

export interface ConnectArgs {
  bootstrapServers: string;
  /** `null` to subscribe to every non-internal topic. */
  topicPattern: string | null;
  fromBeginning: boolean;
  schemaRegistryUrl: string | null;
  auth: AuthArgs | null;
}

export interface TestConnectionResponse {
  ok: boolean;
  brokers: number;
  topics: number;
  message: string;
}

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

export interface ProfileMetadata {
  name: string;
  bootstrapServers: string;
  /** `null` means "use the default pattern" (every non-internal topic). */
  topicPattern: string | null;
  schemaRegistryUrl: string | null;
  auth: ProfileAuthMetadata | null;
  fromBeginning: boolean;
}

export interface LoadedProfile extends ProfileMetadata {
  password: string | null;
  keyPassword: string | null;
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

export interface SaveProfileArgs {
  name: string;
  bootstrapServers: string;
  /** `null` records the default-pattern intent (every non-internal topic). */
  topicPattern: string | null;
  schemaRegistryUrl: string | null;
  auth: SaveProfileAuth | null;
  fromBeginning: boolean;
}
