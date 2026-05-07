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
  schemaId: number | null;
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
}

export interface CaptureStats {
  totalReceived: number;
  inBuffer: number;
  bufferCapacity: number;
  bufferBytes: number;
  bufferByteCapacity: number;
  drops: number;
  throughputPerSec: number;
  /** Drops/sec over the last stats tick. Sustained > 0 = hemorrhage. */
  dropsPerSec: number;
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
