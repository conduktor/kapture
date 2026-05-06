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
  TestUpstreamResult,
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
  // True when the loaded profile has a SASL password in the keychain
  // but we deliberately don't paste it back into the form — surfaces a
  // "re-enter password" hint so the user knows why it's blank.
  const [savedSaslPasswordHint, setSavedSaslPasswordHint] = useState(false);
  // SNI for upstream TLS. Empty = derive from upstream host. Persisted
  // as-is in profiles so a load round-trips.
  const [tlsServerName, setTlsServerName] = useState("");

  // "Test" button state. Result is shown inline below the actions row;
  // auto-clears 5 s after success or when any form field changes (so a
  // stale OK never tricks the user into trusting a broken config).
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestUpstreamResult | null>(null);
  const testClearTimerRef = useRef<number | null>(null);

  const [profiles, setProfiles] = useState<ProfileMetadata[]>([]);
  const [selectedProfile, setSelectedProfile] = useState<string>("");
  const [profileError, setProfileError] = useState<string | null>(null);
  const [profileBusy, setProfileBusy] = useState(false);
  // Inline "save profile" name-entry row. `null` = row hidden; a string
  // (even empty) = row visible. Replaces `window.prompt`, which Tauri 2's
  // webview disables silently — clicking "Save profile…" did nothing.
  const [profileNameInput, setProfileNameInput] = useState<string | null>(null);
  const [savingProfile, setSavingProfile] = useState(false);

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

  // Clear any pending auto-dismiss timer on unmount so a state-update
  // doesn't fire on a torn-down component.
  useEffect(() => {
    return () => {
      if (testClearTimerRef.current !== null) {
        window.clearTimeout(testClearTimerRef.current);
      }
    };
  }, []);

  /**
   * Drop the inline Test result. Called from every form-field setter
   * so a stale "OK" doesn't bleed across config changes the user just
   * made — the spec calls this out explicitly.
   */
  function clearTestResult(): void {
    if (testResult !== null) {
      setTestResult(null);
    }
    if (testClearTimerRef.current !== null) {
      window.clearTimeout(testClearTimerRef.current);
      testClearTimerRef.current = null;
    }
  }

  async function runTest(): Promise<void> {
    if (testClearTimerRef.current !== null) {
      window.clearTimeout(testClearTimerRef.current);
      testClearTimerRef.current = null;
    }
    if (useSasl && (saslUsername.trim() === "" || saslPassword === "")) {
      setTestResult({
        ok: false,
        latencyMs: 0,
        message: "Upstream SASL requires both username and password.",
        apiVersionsCount: null,
      });
      return;
    }
    const upstreamTls: ProxyTlsArgs | null = useTls
      ? {
          serverName: tlsServerName,
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
    setTesting(true);
    setTestResult(null);
    try {
      const result = await invoke<TestUpstreamResult>("test_proxy_upstream", {
        upstream: upstream.trim(),
        upstreamTls,
        upstreamSasl,
      });
      setTestResult(result);
      if (result.ok) {
        // Auto-clear OK results after 5s — the spec mirrors curl /
        // postman one-shot probe UX. Failures stay until the user
        // edits the form.
        testClearTimerRef.current = window.setTimeout(() => {
          setTestResult(null);
          testClearTimerRef.current = null;
        }, 5000);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setTestResult({ ok: false, latencyMs: 0, message, apiVersionsCount: null });
    } finally {
      setTesting(false);
    }
  }

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
      // Restore proxy-mode TLS state. Toggle reflects "has saved
      // upstreamTls"; field values are zeroed when the profile didn't
      // record TLS so a stale prior selection doesn't bleed in.
      if (profile.upstreamTls !== null) {
        setUseTls(true);
        setTlsServerName(profile.upstreamTls.serverName);
        setTlsCaPath(profile.upstreamTls.caPath ?? "");
        setTlsSkipHostname(profile.upstreamTls.skipHostnameVerification);
      } else {
        setUseTls(false);
        setTlsServerName("");
        setTlsCaPath("");
        setTlsSkipHostname(false);
      }
      // Restore proxy-mode SASL state. The backend resolves the
      // upstream SASL password from the OS keychain at load time
      // (`<name>::proxy-sasl`); when present we paste it back into
      // the form so a profile load is one click away from a working
      // proxy. Earlier behaviour left the field blank ("defence in
      // depth") which was painful in practice — the secret already
      // round-trips through the keychain on every save, so reading it
      // back doesn't broaden the threat surface.
      // The "re-enter" hint only surfaces in the broken case where
      // `hasPassword === true` but the keychain returned nothing
      // (entry wiped out-of-band, no keychain backend in this env).
      if (profile.upstreamSasl !== null) {
        setUseSasl(true);
        setSaslMechanism(profile.upstreamSasl.mechanism);
        setSaslUsername(profile.upstreamSasl.username);
        const resolved = profile.upstreamSaslPassword ?? "";
        setSaslPassword(resolved);
        setSavedSaslPasswordHint(profile.upstreamSasl.hasPassword && resolved === "");
      } else {
        setUseSasl(false);
        setSaslMechanism("PLAIN");
        setSaslUsername("");
        setSaslPassword("");
        setSavedSaslPasswordHint(false);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setProfileError(message);
    } finally {
      setProfileBusy(false);
    }
  }

  function openSaveProfileRow(): void {
    // Default to the current profile name if editing one, otherwise to
    // the upstream string — matches the previous prompt's default.
    setProfileNameInput(selectedProfile || upstream);
    setProfileError(null);
  }

  function cancelSaveProfile(): void {
    setProfileNameInput(null);
  }

  async function confirmSaveProfile(): Promise<void> {
    const name = (profileNameInput ?? "").trim();
    if (name === "") {
      return;
    }
    // Snapshot the proxy-mode TLS / SASL form state into the profile.
    // The password is keychain-resident: pass it along when the user
    // typed one, otherwise `null` (= leave any existing entry alone).
    // Empty string would *clear* the entry — we don't want that on a
    // round-trip save where the form simply hasn't been filled in.
    const upstreamTls = useTls
      ? {
          serverName: tlsServerName,
          caPath: tlsCaPath.trim() === "" ? null : tlsCaPath.trim(),
          skipHostnameVerification: tlsSkipHostname,
        }
      : null;
    const upstreamSasl = useSasl
      ? {
          mechanism: saslMechanism,
          username: saslUsername,
          password: saslPassword === "" ? null : saslPassword,
        }
      : null;
    const args: SaveProfileArgs = {
      name,
      bootstrapServers: upstream.trim(),
      topicPattern: null,
      schemaRegistryUrl: null,
      auth: null,
      fromBeginning: false,
      upstreamTls,
      upstreamSasl,
    };
    setSavingProfile(true);
    setProfileError(null);
    try {
      await invoke("save_profile", { args });
      await refreshProfiles();
      setSelectedProfile(name);
      setProfileNameInput(null);
    } catch (err) {
      setProfileError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingProfile(false);
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
          serverName: tlsServerName,
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
        <div className="dialog__section">
          <div className="dialog__section-title">Upstream</div>
          <label className="dialog__field">
            <span className="dialog__label">Upstream broker</span>
            <input
              className="dialog__input"
              value={upstream}
              onChange={(e) => {
                setUpstream(e.target.value);
                clearTestResult();
              }}
              placeholder="kafka.example.com:9092"
              spellCheck={false}
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
                clearTestResult();
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
                    clearTestResult();
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
                    clearTestResult();
                  }}
                />
                <span>Skip hostname verification</span>
              </label>
            </>
          ) : null}
          <label className="dialog__check">
            <input
              type="checkbox"
              checked={useSasl}
              onChange={(e) => {
                setUseSasl(e.target.checked);
                clearTestResult();
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
                    clearTestResult();
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
                    clearTestResult();
                  }}
                  spellCheck={false}
                  autoComplete="off"
                  required
                />
              </label>
              <label className="dialog__field">
                <span className="dialog__label">
                  Password
                  {savedSaslPasswordHint && saslPassword === "" ? (
                    <span className="dialog__hint-inline">not stored in profile — re-enter</span>
                  ) : null}
                </span>
                <input
                  type="password"
                  className="dialog__input"
                  value={saslPassword}
                  onChange={(e) => {
                    setSaslPassword(e.target.value);
                    if (savedSaslPasswordHint) {
                      setSavedSaslPasswordHint(false);
                    }
                    clearTestResult();
                  }}
                  autoComplete="off"
                  required
                />
              </label>
            </>
          ) : null}
        </div>
        <div className="dialog__section">
          <div className="dialog__section-title">Local</div>
          <label className="dialog__field">
            <span className="dialog__label">Listen port (127.0.0.1)</span>
            <input
              className="dialog__input"
              type="number"
              value={listenPort}
              onChange={(e) => {
                setListenPort(Number(e.target.value));
                clearTestResult();
              }}
              min={1}
              max={65535}
              required
            />
            <span className="dialog__hint">
              Bound to 127.0.0.1 — only reachable from this machine.
            </span>
          </label>
        </div>
        {validationError !== null ? <p className="dialog__error">{validationError}</p> : null}
        {error !== null ? <p className="dialog__error">{error}</p> : null}
        <div className="dialog__actions">
          <button
            type="button"
            className="btn"
            onClick={openSaveProfileRow}
            disabled={profileBusy || savingProfile || profileNameInput !== null}
          >
            Save profile…
          </button>
          {onCancel ? (
            <button type="button" className="btn" onClick={onCancel}>
              Cancel
            </button>
          ) : null}
          <button
            type="button"
            className="btn"
            onClick={() => {
              void runTest();
            }}
            disabled={testing || pending || upstream.trim() === ""}
            title="Connect, run handshake (TLS/SASL), exchange ApiVersions, then close. Doesn't start the proxy."
          >
            {testing ? "Testing…" : "Test"}
          </button>
          <button type="submit" className="btn btn--primary" disabled={pending}>
            {pending ? "Starting…" : isEditing ? "Restart proxy" : "Start proxy"}
          </button>
        </div>
        {testResult !== null ? (
          <p
            className={
              testResult.ok ? "dialog__test-result dialog__test-result--ok" : "dialog__error"
            }
          >
            {testResult.ok ? "✓ OK" : `✗ ${testResult.message}`}
          </p>
        ) : null}
        {profileNameInput !== null ? (
          <div className="dialog__profile-row">
            <input
              className="dialog__input"
              value={profileNameInput}
              onChange={(e) => {
                setProfileNameInput(e.target.value);
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault();
                  void confirmSaveProfile();
                } else if (e.key === "Escape") {
                  e.preventDefault();
                  cancelSaveProfile();
                }
              }}
              placeholder="Profile name"
              spellCheck={false}
              autoComplete="off"
              autoFocus
              disabled={savingProfile}
            />
            <button
              type="button"
              className="btn btn--primary"
              onClick={() => {
                void confirmSaveProfile();
              }}
              disabled={savingProfile || profileNameInput.trim() === ""}
            >
              {savingProfile ? "Saving…" : "Save"}
            </button>
            <button
              type="button"
              className="btn"
              onClick={cancelSaveProfile}
              disabled={savingProfile}
            >
              Cancel
            </button>
          </div>
        ) : null}
      </form>
    </div>
  );
}
