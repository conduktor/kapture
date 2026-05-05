import { useEffect, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import type {
  AuthArgs,
  LoadedProfile,
  ProfileMetadata,
  SaslMechanism,
  SaveProfileArgs,
  SaveProfileAuth,
  SaveProfileTls,
  TlsArgs,
} from "../types";

type AuthMethod = "none" | SaslMechanism;

const SASL_MECHANISMS: SaslMechanism[] = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];

interface Initial {
  bootstrap: string;
  topics: string;
  registry: string;
  authMethod: AuthMethod;
  username: string;
  useTls: boolean;
  fromBeginning: boolean;
}

interface Props {
  defaultBootstrap: string;
  defaultTopics: string;
  defaultRegistry: string;
  initial?: Partial<Initial> | undefined;
  isEditing: boolean;
  onConnect: (
    bootstrap: string,
    topics: string[],
    fromBeginning: boolean,
    schemaRegistryUrl: string | null,
    auth: AuthArgs | null,
  ) => void;
  onCancel?: (() => void) | undefined;
  pending: boolean;
  error: string | null;
}

export function ConnectionDialog({
  defaultBootstrap,
  defaultTopics,
  defaultRegistry,
  initial,
  isEditing,
  onConnect,
  onCancel,
  pending,
  error,
}: Props): JSX.Element {
  const [bootstrap, setBootstrap] = useState(initial?.bootstrap ?? defaultBootstrap);
  const [topics, setTopics] = useState(initial?.topics ?? defaultTopics);
  const [registry, setRegistry] = useState(initial?.registry ?? defaultRegistry);
  const [fromBeginning, setFromBeginning] = useState(initial?.fromBeginning ?? true);
  const [authMethod, setAuthMethod] = useState<AuthMethod>(initial?.authMethod ?? "none");
  const [username, setUsername] = useState(initial?.username ?? "");
  const [password, setPassword] = useState("");
  const [useTls, setUseTls] = useState(initial?.useTls ?? false);
  const [caPath, setCaPath] = useState("");
  const [certPath, setCertPath] = useState("");
  const [keyPath, setKeyPath] = useState("");
  const [keyPassword, setKeyPassword] = useState("");

  const [profiles, setProfiles] = useState<ProfileMetadata[]>([]);
  const [selectedProfile, setSelectedProfile] = useState<string>("");
  const [profileError, setProfileError] = useState<string | null>(null);
  const [profileBusy, setProfileBusy] = useState(false);

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
      setTopics(profile.topics.join(", "));
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
    const list = topics
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
    if (list.length === 0) {
      setProfileError("Topics must not be empty");
      return;
    }
    const args: SaveProfileArgs = {
      name,
      bootstrapServers: bootstrap.trim(),
      topics: list,
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

  const submit = (): void => {
    const list = topics
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
    if (list.length === 0) {
      return;
    }
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
    onConnect(bootstrap.trim(), list, fromBeginning, registryUrl === "" ? null : registryUrl, auth);
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
        <p className="dialog__hint">
          Local dev: Redpanda <code>localhost:19092</code> or Apache Kafka{" "}
          <code>localhost:29092</code>.
        </p>
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
          <span className="dialog__label">Topics (comma-separated)</span>
          <input
            className="dialog__input"
            value={topics}
            onChange={(e) => {
              setTopics(e.target.value);
            }}
            placeholder="orders.raw, orders.enriched"
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
              <span className="dialog__label">Password</span>
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
        {error ? <p className="dialog__error">{error}</p> : null}
        <div className="dialog__actions">
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
          {onCancel ? (
            <button type="button" className="btn" onClick={onCancel}>
              Cancel
            </button>
          ) : null}
          <button type="submit" className="btn btn--primary" disabled={pending}>
            {pending ? "Connecting…" : isEditing ? "Reconnect" : "Connect"}
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
