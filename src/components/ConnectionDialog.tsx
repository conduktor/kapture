import { useState, type JSX } from "react";
import type { AuthArgs, SaslMechanism } from "../types";

type AuthMethod = "none" | SaslMechanism;

const SASL_MECHANISMS: SaslMechanism[] = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];

interface Props {
  defaultBootstrap: string;
  defaultTopics: string;
  defaultRegistry: string;
  onConnect: (
    bootstrap: string,
    topics: string[],
    fromBeginning: boolean,
    schemaRegistryUrl: string | null,
    auth: AuthArgs | null,
  ) => void;
  pending: boolean;
  error: string | null;
}

export function ConnectionDialog({
  defaultBootstrap,
  defaultTopics,
  defaultRegistry,
  onConnect,
  pending,
  error,
}: Props): JSX.Element {
  const [bootstrap, setBootstrap] = useState(defaultBootstrap);
  const [topics, setTopics] = useState(defaultTopics);
  const [registry, setRegistry] = useState(defaultRegistry);
  const [fromBeginning, setFromBeginning] = useState(true);
  const [authMethod, setAuthMethod] = useState<AuthMethod>("none");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [useTls, setUseTls] = useState(false);

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
      authMethod === "none" ? null : { mechanism: authMethod, username, password, useTls };
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
        <h2 className="dialog__title">Connect to Kafka</h2>
        <p className="dialog__hint">
          Local dev: Redpanda <code>localhost:19092</code> or Apache Kafka{" "}
          <code>localhost:29092</code> via <code>docker compose up -d</code>.
        </p>
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
          <button type="submit" className="btn btn--primary" disabled={pending}>
            {pending ? "Connecting…" : "Connect"}
          </button>
        </div>
      </form>
    </div>
  );
}
