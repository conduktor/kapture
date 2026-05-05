import { useState, type JSX } from "react";

interface Props {
  defaultBootstrap: string;
  defaultTopics: string;
  defaultRegistry: string;
  onConnect: (
    bootstrap: string,
    topics: string[],
    fromBeginning: boolean,
    schemaRegistryUrl: string | null,
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

  const submit = (): void => {
    const list = topics
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
    if (list.length === 0) {
      return;
    }
    const registryUrl = registry.trim();
    onConnect(bootstrap.trim(), list, fromBeginning, registryUrl === "" ? null : registryUrl);
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
          Local Redpanda from <code>docker compose up -d</code>: bootstrap{" "}
          <code>localhost:19092</code>.
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
