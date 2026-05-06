import { useEffect, useRef, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import type {
  LoadedProfile,
  ProbeResult,
  ProfileMetadata,
  ProxySaslArgs,
  ProxyStatus,
  ProxyTlsArgs,
  SaslMechanism,
  SaveProfileArgs,
} from "../types";

const DEFAULT_LISTEN_PORT = 9092;

interface Initial {
  upstream: string;
}

interface Props {
  defaultUpstream: string;
  initial?: Partial<Initial> | undefined;
  isEditing: boolean;
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

/**
 * Connection dialog — proxy-only.
 *
 * Client (rdkafka) mode was removed. The dialog binds a proxy listener
 * on `127.0.0.1:listenPort`, optionally with upstream TLS / SASL.
 * Profiles still apply: a saved profile's `bootstrapServers` field
 * pre-fills the upstream broker. Per-profile SASL credentials are
 * NOT auto-applied to the proxy form — proxy mode lets the user
 * configure upstream auth explicitly per session.
 */
export function ConnectionDialog({
  defaultUpstream,
  initial,
  isEditing,
  onProxyStarted,
  onProxyError,
  onProxyStarting,
  onCancel,
  pending,
  error,
}: Props): JSX.Element {
  const [upstream, setUpstream] = useState(initial?.upstream ?? defaultUpstream);
  const [listenPort, setListenPort] = useState(DEFAULT_LISTEN_PORT);
  // Upstream TLS. Defence in depth: secrets never round-trip through
  // localStorage / profiles, so these always start blank on edit.
  const [useTls, setUseTls] = useState(false);
  const [tlsCaPath, setTlsCaPath] = useState("");
  const [tlsSkipHostname, setTlsSkipHostname] = useState(false);
  // Upstream SASL. Backend supports PLAIN + SCRAM-SHA-{256,512}.
  const [useSasl, setUseSasl] = useState(false);
  const [saslMechanism, setSaslMechanism] = useState<SaslMechanism>("PLAIN");
  const [saslUsername, setSaslUsername] = useState("");
  const [saslPassword, setSaslPassword] = useState("");
  const [validationError, setValidationError] = useState<string | null>(null);

  const [profiles, setProfiles] = useState<ProfileMetadata[]>([]);
  const [selectedProfile, setSelectedProfile] = useState<string>("");
  const [profileError, setProfileError] = useState<string | null>(null);
  const [profileBusy, setProfileBusy] = useState(false);

  // Auto-detect: probe localhost on mount only when the user is starting
  // from the blank state. We don't overwrite any field the user has
  // already typed. Fires once.
  const detectedRef = useRef(false);
  useEffect(() => {
    if (detectedRef.current || isEditing || initial !== undefined) {
      return;
    }
    detectedRef.current = true;
    void (async () => {
      try {
        const probe = await invoke<ProbeResult>("probe_localhost_brokers");
        if (probe.bootstrapServers !== null) {
          setUpstream((current) =>
            current.trim() === defaultUpstream || current.trim() === ""
              ? (probe.bootstrapServers ?? current)
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
      setUpstream(profile.bootstrapServers);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setProfileError(message);
    } finally {
      setProfileBusy(false);
    }
  }

  async function saveAsProfile(): Promise<void> {
    const name = window.prompt("Profile name", selectedProfile || upstream);
    if (name === null || name === "") {
      return;
    }
    // Profiles store bootstrap-only metadata in proxy mode. SASL/TLS
    // forms in the dialog are intentionally session-local; the user
    // re-enters them per session.
    const args: SaveProfileArgs = {
      name,
      bootstrapServers: upstream.trim(),
      topicPattern: null,
      schemaRegistryUrl: null,
      auth: null,
      fromBeginning: false,
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
    const trimmedUpstream = upstream.trim();
    if (useSasl && (saslUsername.trim() === "" || saslPassword === "")) {
      setValidationError("Upstream SASL requires both username and password.");
      return;
    }
    setValidationError(null);
    const upstreamTls: ProxyTlsArgs | null = useTls
      ? {
          // Empty string lets the backend derive SNI from the bootstrap host.
          serverName: "",
          caPath: tlsCaPath.trim() === "" ? null : tlsCaPath.trim(),
          skipHostnameVerification: tlsSkipHostname,
        }
      : null;
    const upstreamSasl: ProxySaslArgs | null = useSasl
      ? {
          mechanism: saslMechanism,
          username: saslUsername,
          password: saslPassword,
        }
      : null;
    onProxyStarting();
    void (async () => {
      try {
        const status = await invoke<ProxyStatus>("start_proxy", {
          upstream: trimmedUpstream,
          listenPort,
          upstreamTls,
          upstreamSasl,
        });
        onProxyStarted(status);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        onProxyError(message);
      }
    })();
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
        <h2 className="dialog__title">{isEditing ? "Edit proxy" : "Start proxy"}</h2>
        <div className="dialog__profile-row">
          <select
            className="dialog__input"
            value={selectedProfile}
            onChange={(e) => {
              void applyProfile(e.target.value);
            }}
            disabled={profileBusy}
          >
            <option value="">— New proxy —</option>
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
        {profileError !== null ? <p className="dialog__error">{profileError}</p> : null}
        <label className="dialog__field">
          <span className="dialog__label">Upstream broker</span>
          <input
            className="dialog__input"
            value={upstream}
            onChange={(e) => {
              setUpstream(e.target.value);
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
            value={listenPort}
            onChange={(e) => {
              setListenPort(Number(e.target.value));
            }}
            min={1}
            max={65535}
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
          <span>Upstream uses TLS</span>
        </label>
        {useTls ? (
          <>
            <label className="dialog__field">
              <span className="dialog__label">
                CA certificate path (optional)
                <span className="dialog__hint-inline">empty = system roots</span>
              </span>
              <input
                className="dialog__input"
                value={tlsCaPath}
                onChange={(e) => {
                  setTlsCaPath(e.target.value);
                }}
                placeholder="/path/to/ca.pem"
                spellCheck={false}
                autoComplete="off"
              />
            </label>
            <label className="dialog__check">
              <input
                type="checkbox"
                checked={tlsSkipHostname}
                onChange={(e) => {
                  setTlsSkipHostname(e.target.checked);
                }}
              />
              <span>Skip hostname verification (UNSAFE)</span>
            </label>
            {tlsSkipHostname ? (
              <p className="dialog__warn" role="alert">
                WARNING: only enable for self-signed clusters with no hostname match. Defeats cert
                chain validation.
              </p>
            ) : null}
          </>
        ) : null}
        <label className="dialog__check">
          <input
            type="checkbox"
            checked={useSasl}
            onChange={(e) => {
              setUseSasl(e.target.checked);
            }}
          />
          <span>Upstream requires SASL</span>
        </label>
        {useSasl ? (
          <>
            <label className="dialog__field">
              <span className="dialog__label">Mechanism</span>
              <select
                className="dialog__input"
                value={saslMechanism}
                onChange={(e) => {
                  setSaslMechanism(e.target.value as SaslMechanism);
                }}
              >
                <option value="PLAIN">SASL/PLAIN</option>
                <option value="SCRAM-SHA-256">SASL/SCRAM-SHA-256</option>
                <option value="SCRAM-SHA-512">SASL/SCRAM-SHA-512</option>
              </select>
            </label>
            <label className="dialog__field">
              <span className="dialog__label">Username</span>
              <input
                className="dialog__input"
                value={saslUsername}
                onChange={(e) => {
                  setSaslUsername(e.target.value);
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
                value={saslPassword}
                onChange={(e) => {
                  setSaslPassword(e.target.value);
                }}
                autoComplete="off"
                required
              />
            </label>
          </>
        ) : null}
        {validationError !== null ? <p className="dialog__error">{validationError}</p> : null}
        {error !== null ? <p className="dialog__error">{error}</p> : null}
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
            {pending ? "Starting…" : isEditing ? "Restart proxy" : "Start proxy"}
          </button>
        </div>
      </form>
    </div>
  );
}
