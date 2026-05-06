import { useEffect, useRef, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import type {
  AuthArgs,
  ConnectionMode,
  LoadedProfile,
  ProbeResult,
  ProfileMetadata,
  ProxyStatus,
  SaslMechanism,
  SaveProfileArgs,
  SaveProfileAuth,
  SaveProfileTls,
  TestConnectionResponse,
  TlsArgs,
} from "../types";

type AuthMethod = "none" | SaslMechanism;

const SASL_MECHANISMS: SaslMechanism[] = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];

interface Initial {
  bootstrap: string;
  topicPattern: string;
  registry: string;
  authMethod: AuthMethod;
  username: string;
  useTls: boolean;
  fromBeginning: boolean;
  // TLS paths prefill in edit mode. Passwords intentionally excluded:
  // they live only in the OS keychain and the user must re-enter them.
  caPath: string;
  certPath: string;
  keyPath: string;
}

interface Props {
  defaultBootstrap: string;
  defaultRegistry: string;
  initial?: Partial<Initial> | undefined;
  isEditing: boolean;
  onConnect: (
    bootstrap: string,
    topicPattern: string | null,
    fromBeginning: boolean,
    schemaRegistryUrl: string | null,
    auth: AuthArgs | null,
  ) => void;
  /**
   * Called when the user starts the proxy listener. The parent stores the
   * resulting `ProxyStatus` so the cluster pill can show
   * `proxy {listenAddr} → {upstream}`.
   */
  onProxyStarted: (status: ProxyStatus) => void;
  /** Surfaces a proxy start failure to the parent's `error` slot. */
  onProxyError: (message: string) => void;
  /** Optimistic flip to "connecting" while the proxy listener boots. */
  onProxyStarting: () => void;
  onCancel?: (() => void) | undefined;
  pending: boolean;
  error: string | null;
}

export function ConnectionDialog({
  defaultBootstrap,
  defaultRegistry,
  initial,
  isEditing,
  onConnect,
  onProxyStarted,
  onProxyError,
  onProxyStarting,
  onCancel,
  pending,
  error,
}: Props): JSX.Element {
  const [mode, setMode] = useState<ConnectionMode>("client");
  const [proxyUpstream, setProxyUpstream] = useState("localhost:9092");
  const [proxyListenPort, setProxyListenPort] = useState(9092);
  const [bootstrap, setBootstrap] = useState(initial?.bootstrap ?? defaultBootstrap);
  const [topicPattern, setTopicPattern] = useState(initial?.topicPattern ?? "");
  const [showAdvanced, setShowAdvanced] = useState((initial?.topicPattern ?? "").trim() !== "");
  const [registry, setRegistry] = useState(initial?.registry ?? defaultRegistry);
  // Default off: Wireshark captures from "now" — reading from the start of
  // every topic on a busy cluster is the volume cliff users least expect.
  const [fromBeginning, setFromBeginning] = useState(initial?.fromBeginning ?? false);
  const [authMethod, setAuthMethod] = useState<AuthMethod>(initial?.authMethod ?? "none");
  const [username, setUsername] = useState(initial?.username ?? "");
  const [password, setPassword] = useState("");
  const [useTls, setUseTls] = useState(initial?.useTls ?? false);
  const [caPath, setCaPath] = useState(initial?.caPath ?? "");
  const [certPath, setCertPath] = useState(initial?.certPath ?? "");
  const [keyPath, setKeyPath] = useState(initial?.keyPath ?? "");
  const [keyPassword, setKeyPassword] = useState("");

  const [profiles, setProfiles] = useState<ProfileMetadata[]>([]);
  const [selectedProfile, setSelectedProfile] = useState<string>("");
  const [profileError, setProfileError] = useState<string | null>(null);
  const [profileBusy, setProfileBusy] = useState(false);

  // Test connection state. `null` = not tested yet; otherwise carries the
  // backend's verdict so the UI can show ✓ green / ✗ red.
  type TestState =
    | { phase: "idle" }
    | { phase: "testing" }
    | { phase: "ok"; message: string }
    | { phase: "fail"; message: string };
  const [testState, setTestState] = useState<TestState>({ phase: "idle" });

  // Auto-detect: probe localhost on mount only when the user is starting from
  // the blank "New connection" state. We don't overwrite any field the user
  // has already typed. Fires once.
  const detectedRef = useRef(false);
  useEffect(() => {
    if (detectedRef.current || isEditing || initial !== undefined) {
      return;
    }
    detectedRef.current = true;
    void (async () => {
      try {
        const probe = await invoke<ProbeResult>("probe_localhost_brokers");
        if (probe.bootstrapServers) {
          setBootstrap((current) =>
            current.trim() === defaultBootstrap || current.trim() === ""
              ? (probe.bootstrapServers ?? current)
              : current,
          );
        }
        if (probe.schemaRegistryUrl) {
          setRegistry((current) =>
            current.trim() === defaultRegistry || current.trim() === ""
              ? (probe.schemaRegistryUrl ?? current)
              : current,
          );
        }
      } catch (err) {
        // Probe is best-effort; never surface to the user.
        console.warn("probe_localhost failed", err);
      }
    })();
    // Run-once-on-mount: the deps are intentionally only read, not tracked.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    void refreshProfiles();
  }, []);

  async function refreshProfiles(): Promise<void> {
    try {
      const list = await invoke<ProfileMetadata[]>("list_profiles");
      setProfiles(list);
    } catch (err) {
      console.error("list_profiles failed", err);
    }
  }

  async function applyProfile(name: string): Promise<void> {
    setSelectedProfile(name);
    if (name === "") {
      return;
    }
    setProfileBusy(true);
    setProfileError(null);
    try {
      const profile = await invoke<LoadedProfile>("load_profile", { name });
      setBootstrap(profile.bootstrapServers);
      setTopicPattern(profile.topicPattern ?? "");
      setShowAdvanced((profile.topicPattern ?? "").trim() !== "");
      setRegistry(profile.schemaRegistryUrl ?? "");
      setFromBeginning(profile.fromBeginning);
      if (profile.auth) {
        setAuthMethod(profile.auth.mechanism);
        setUsername(profile.auth.username);
        setUseTls(profile.auth.useTls);
        setPassword(profile.password ?? "");
        setCaPath(profile.auth.tls?.caPath ?? "");
        setCertPath(profile.auth.tls?.certPath ?? "");
        setKeyPath(profile.auth.tls?.keyPath ?? "");
        setKeyPassword(profile.keyPassword ?? "");
      } else {
        setAuthMethod("none");
        setUsername("");
        setUseTls(false);
        setPassword("");
        setCaPath("");
        setCertPath("");
        setKeyPath("");
        setKeyPassword("");
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setProfileError(message);
    } finally {
      setProfileBusy(false);
    }
  }

  async function saveAsProfile(): Promise<void> {
    const name = window.prompt("Profile name", selectedProfile || bootstrap);
    if (!name) {
      return;
    }
    const trimmedPattern = topicPattern.trim();
    const args: SaveProfileArgs = {
      name,
      bootstrapServers: bootstrap.trim(),
      topicPattern: trimmedPattern === "" ? null : trimmedPattern,
      schemaRegistryUrl: registry.trim() === "" ? null : registry.trim(),
      auth: buildSaveAuth(
        authMethod,
        username,
        password,
        useTls,
        caPath,
        certPath,
        keyPath,
        keyPassword,
      ),
      fromBeginning,
    };
    setProfileBusy(true);
    setProfileError(null);
    try {
      await invoke("save_profile", { args });
      await refreshProfiles();
      setSelectedProfile(name);
    } catch (err) {
      setProfileError(err instanceof Error ? err.message : String(err));
    } finally {
      setProfileBusy(false);
    }
  }

  async function deleteSelectedProfile(): Promise<void> {
    if (selectedProfile === "") {
      return;
    }
    if (!window.confirm(`Delete profile "${selectedProfile}"?`)) {
      return;
    }
    setProfileBusy(true);
    setProfileError(null);
    try {
      await invoke("delete_profile", { name: selectedProfile });
      setSelectedProfile("");
      await refreshProfiles();
    } catch (err) {
      setProfileError(err instanceof Error ? err.message : String(err));
    } finally {
      setProfileBusy(false);
    }
  }

  async function runTest(): Promise<void> {
    setTestState({ phase: "testing" });
    const registryUrl = registry.trim();
    const auth: AuthArgs | null =
      authMethod === "none"
        ? null
        : {
            mechanism: authMethod,
            username,
            password,
            useTls,
            tls: useTls ? buildTlsArgs(caPath, certPath, keyPath, keyPassword) : null,
          };
    try {
      const result = await invoke<TestConnectionResponse>("test_connection", {
        bootstrapServers: bootstrap.trim(),
        schemaRegistryUrl: registryUrl === "" ? null : registryUrl,
        auth,
      });
      setTestState(
        result.ok
          ? { phase: "ok", message: result.message }
          : { phase: "fail", message: result.message },
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setTestState({ phase: "fail", message });
    }
  }

  const submit = (): void => {
    if (mode === "proxy") {
      const upstream = proxyUpstream.trim();
      onProxyStarting();
      void (async () => {
        try {
          const status = await invoke<ProxyStatus>("start_proxy", {
            upstream,
            listenPort: proxyListenPort,
          });
          onProxyStarted(status);
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          onProxyError(message);
        }
      })();
      return;
    }
    const registryUrl = registry.trim();
    const trimmedPattern = topicPattern.trim();
    const auth: AuthArgs | null =
      authMethod === "none"
        ? null
        : {
            mechanism: authMethod,
            username,
            password,
            useTls,
            tls: useTls ? buildTlsArgs(caPath, certPath, keyPath, keyPassword) : null,
          };
    onConnect(
      bootstrap.trim(),
      trimmedPattern === "" ? null : trimmedPattern,
      fromBeginning,
      registryUrl === "" ? null : registryUrl,
      auth,
    );
  };

  return (
    <div className="dialog-backdrop">
      <form
        className="dialog"
        onSubmit={(event) => {
          event.preventDefault();
          submit();
        }}
      >
        <h2 className="dialog__title">{isEditing ? "Edit connection" : "Connect to Kafka"}</h2>
        <fieldset className="dialog__mode">
          <legend className="dialog__label">Mode</legend>
          <label className="dialog__check">
            <input
              type="radio"
              name="mode"
              value="client"
              checked={mode === "client"}
              onChange={() => {
                setMode("client");
              }}
            />
            <span>Client — Kapture connects as a consumer</span>
          </label>
          <label className="dialog__check">
            <input
              type="radio"
              name="mode"
              value="proxy"
              checked={mode === "proxy"}
              onChange={() => {
                setMode("proxy");
              }}
            />
            <span>Proxy — point your apps at Kapture</span>
          </label>
        </fieldset>
        {mode === "proxy" ? (
          <>
            <label className="dialog__field">
              <span className="dialog__label">Upstream broker</span>
              <input
                className="dialog__input"
                value={proxyUpstream}
                onChange={(e) => {
                  setProxyUpstream(e.target.value);
                }}
                placeholder="kafka.example.com:9092"
                spellCheck={false}
                autoComplete="off"
                required
              />
            </label>
            <label className="dialog__field">
              <span className="dialog__label">Listen port (127.0.0.1)</span>
              <input
                className="dialog__input"
                type="number"
                value={proxyListenPort}
                onChange={(e) => {
                  setProxyListenPort(Number(e.target.value));
                }}
                min={1}
                max={65535}
                required
              />
            </label>
          </>
        ) : (
          <>
            <div className="dialog__profile-row">
              <select
                className="dialog__input"
                value={selectedProfile}
                onChange={(e) => {
                  void applyProfile(e.target.value);
                }}
                disabled={profileBusy}
              >
                <option value="">— New connection —</option>
                {profiles.map((p) => (
                  <option key={p.name} value={p.name}>
                    {p.name} ({p.bootstrapServers})
                  </option>
                ))}
              </select>
              <button
                type="button"
                className="btn"
                onClick={() => {
                  void deleteSelectedProfile();
                }}
                disabled={profileBusy || selectedProfile === ""}
                title="Delete this profile"
              >
                Delete
              </button>
            </div>
            {profileError ? <p className="dialog__error">{profileError}</p> : null}
            <label className="dialog__field">
              <span className="dialog__label">Bootstrap servers</span>
              <input
                className="dialog__input"
                value={bootstrap}
                onChange={(e) => {
                  setBootstrap(e.target.value);
                }}
                placeholder="host:port,host:port"
                spellCheck={false}
                autoComplete="off"
                required
              />
            </label>
            <label className="dialog__field">
              <span className="dialog__label">Schema Registry URL (optional)</span>
              <input
                className="dialog__input"
                value={registry}
                onChange={(e) => {
                  setRegistry(e.target.value);
                }}
                placeholder="http://localhost:18081"
                spellCheck={false}
                autoComplete="off"
              />
            </label>
            <button
              type="button"
              className="dialog__disclosure"
              onClick={() => {
                setShowAdvanced((s) => !s);
              }}
              aria-expanded={showAdvanced}
            >
              {showAdvanced ? "▾ Advanced" : "▸ Advanced"}
            </button>
            {showAdvanced ? (
              <label className="dialog__field">
                <span className="dialog__label">
                  Topic pattern (regex, optional)
                  <span className="dialog__hint-inline">
                    {" "}
                    e.g. <code>^orders\..*</code> — narrows broker-side subscription
                  </span>
                </span>
                <input
                  className="dialog__input"
                  value={topicPattern}
                  onChange={(e) => {
                    setTopicPattern(e.target.value);
                  }}
                  placeholder="^[^_].*"
                  spellCheck={false}
                  autoComplete="off"
                />
              </label>
            ) : null}
            <label className="dialog__field">
              <span className="dialog__label">Authentication</span>
              <select
                className="dialog__input"
                value={authMethod}
                onChange={(e) => {
                  setAuthMethod(e.target.value as AuthMethod);
                }}
              >
                <option value="none">None (PLAINTEXT)</option>
                {SASL_MECHANISMS.map((m) => (
                  <option key={m} value={m}>
                    SASL/{m}
                  </option>
                ))}
              </select>
            </label>
            {authMethod !== "none" ? (
              <>
                <label className="dialog__field">
                  <span className="dialog__label">Username</span>
                  <input
                    className="dialog__input"
                    value={username}
                    onChange={(e) => {
                      setUsername(e.target.value);
                    }}
                    spellCheck={false}
                    autoComplete="off"
                    required
                  />
                </label>
                <label className="dialog__field">
                  <span className="dialog__label">
                    Password
                    {isEditing && initial?.authMethod && initial.authMethod !== "none" ? (
                      <span className="dialog__hint-inline"> (re-enter; not stored in UI)</span>
                    ) : null}
                  </span>
                  <input
                    type="password"
                    className="dialog__input"
                    value={password}
                    onChange={(e) => {
                      setPassword(e.target.value);
                    }}
                    autoComplete="off"
                    required
                  />
                </label>
                <label className="dialog__check">
                  <input
                    type="checkbox"
                    checked={useTls}
                    onChange={(e) => {
                      setUseTls(e.target.checked);
                    }}
                  />
                  <span>TLS (SASL_SSL)</span>
                </label>
                {useTls ? (
                  <>
                    <label className="dialog__field">
                      <span className="dialog__label">CA certificate path (optional)</span>
                      <input
                        className="dialog__input"
                        value={caPath}
                        onChange={(e) => {
                          setCaPath(e.target.value);
                        }}
                        placeholder="/path/to/ca.pem"
                        spellCheck={false}
                        autoComplete="off"
                      />
                    </label>
                    <label className="dialog__field">
                      <span className="dialog__label">Client certificate path (mTLS)</span>
                      <input
                        className="dialog__input"
                        value={certPath}
                        onChange={(e) => {
                          setCertPath(e.target.value);
                        }}
                        placeholder="/path/to/client.crt"
                        spellCheck={false}
                        autoComplete="off"
                      />
                    </label>
                    <label className="dialog__field">
                      <span className="dialog__label">Client private key path (mTLS)</span>
                      <input
                        className="dialog__input"
                        value={keyPath}
                        onChange={(e) => {
                          setKeyPath(e.target.value);
                        }}
                        placeholder="/path/to/client.key"
                        spellCheck={false}
                        autoComplete="off"
                      />
                    </label>
                    <label className="dialog__field">
                      <span className="dialog__label">Key password (optional, encrypted keys)</span>
                      <input
                        type="password"
                        className="dialog__input"
                        value={keyPassword}
                        onChange={(e) => {
                          setKeyPassword(e.target.value);
                        }}
                        autoComplete="off"
                      />
                    </label>
                  </>
                ) : null}
              </>
            ) : null}
            <label className="dialog__check">
              <input
                type="checkbox"
                checked={fromBeginning}
                onChange={(e) => {
                  setFromBeginning(e.target.checked);
                }}
              />
              <span>Read from beginning</span>
            </label>
          </>
        )}
        {error ? <p className="dialog__error">{error}</p> : null}
        {mode === "client" && testState.phase !== "idle" ? (
          <p
            className={
              testState.phase === "ok"
                ? "dialog__test dialog__test--ok"
                : testState.phase === "fail"
                  ? "dialog__test dialog__test--fail"
                  : "dialog__test dialog__test--pending"
            }
            role="status"
            aria-live="polite"
          >
            {testState.phase === "testing"
              ? "Testing…"
              : testState.phase === "ok"
                ? `✓ ${testState.message}`
                : `✗ ${testState.message}`}
          </p>
        ) : null}
        <div className="dialog__actions">
          {mode === "client" ? (
            <>
              <button
                type="button"
                className="btn"
                onClick={() => {
                  void runTest();
                }}
                disabled={testState.phase === "testing"}
                title="Probe broker + Schema Registry without starting a capture"
              >
                {testState.phase === "testing" ? "Testing…" : "Test"}
              </button>
              <button
                type="button"
                className="btn"
                onClick={() => {
                  void saveAsProfile();
                }}
                disabled={profileBusy}
              >
                Save profile…
              </button>
            </>
          ) : null}
          {onCancel ? (
            <button type="button" className="btn" onClick={onCancel}>
              Cancel
            </button>
          ) : null}
          <button type="submit" className="btn btn--primary" disabled={pending}>
            {mode === "proxy"
              ? pending
                ? "Starting…"
                : "Start proxy"
              : pending
                ? "Connecting…"
                : isEditing
                  ? "Reconnect"
                  : "Connect"}
          </button>
        </div>
      </form>
    </div>
  );
}

function buildSaveAuth(
  method: AuthMethod,
  username: string,
  password: string,
  useTls: boolean,
  ca: string,
  cert: string,
  key: string,
  keyPassword: string,
): SaveProfileAuth | null {
  if (method === "none") {
    return null;
  }
  return {
    mechanism: method,
    username,
    useTls,
    password: password === "" ? null : password,
    tls: useTls ? buildSaveTls(ca, cert, key, keyPassword) : null,
  };
}

function nullIfBlank(s: string): string | null {
  const trimmed = s.trim();
  return trimmed === "" ? null : trimmed;
}

function buildTlsArgs(ca: string, cert: string, key: string, keyPassword: string): TlsArgs | null {
  const args: TlsArgs = {
    caPath: nullIfBlank(ca),
    certPath: nullIfBlank(cert),
    keyPath: nullIfBlank(key),
    keyPassword: keyPassword === "" ? null : keyPassword,
  };
  if (
    args.caPath === null &&
    args.certPath === null &&
    args.keyPath === null &&
    args.keyPassword === null
  ) {
    return null;
  }
  return args;
}

function buildSaveTls(
  ca: string,
  cert: string,
  key: string,
  keyPassword: string,
): SaveProfileTls | null {
  const ca2 = nullIfBlank(ca);
  const cert2 = nullIfBlank(cert);
  const key2 = nullIfBlank(key);
  const keyPw = keyPassword === "" ? null : keyPassword;
  if (ca2 === null && cert2 === null && key2 === null && keyPw === null) {
    return null;
  }
  return { caPath: ca2, certPath: cert2, keyPath: key2, keyPassword: keyPw };
}
