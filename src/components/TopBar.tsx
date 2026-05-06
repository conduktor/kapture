import type { JSX } from "react";
import type { ConnectionMode, ProxyStatus } from "../types";

interface Props {
  filter: string;
  onFilterChange: (next: string) => void;
  filterError: string | null;
  filterPlaceholder?: string;
  capturing: boolean;
  onToggleCapture: () => void;
  onClear: () => void;
  cluster: string;
  mode: ConnectionMode;
  proxyStatus: ProxyStatus | null;
  onEdit: () => void;
}

export function TopBar({
  filter,
  onFilterChange,
  filterError,
  filterPlaceholder,
  capturing,
  onToggleCapture,
  onClear,
  cluster,
  mode,
  proxyStatus,
  onEdit,
}: Props): JSX.Element {
  // Cluster pill: in proxy mode show `proxy {listenAddr} → {upstream}` when
  // the listener is up; fall back to "proxy" while the listener is booting
  // so the pill never blanks. Client mode keeps the original bootstrap text.
  const pillLabel =
    mode === "proxy"
      ? proxyStatus
        ? `proxy ${proxyStatus.listenAddr} → ${proxyStatus.upstream}`
        : "proxy"
      : cluster;
  return (
    <header className="topbar">
      <button
        type="button"
        className="topbar__cluster"
        onClick={onEdit}
        title="Edit connection settings"
      >
        <span className="topbar__cluster-dot" data-status={capturing ? "live" : "idle"} />
        <span className="topbar__cluster-name">{pillLabel}</span>
        <span className="topbar__cluster-edit" aria-hidden="true">
          ✎
        </span>
      </button>
      <div className="topbar__filter-wrap">
        <input
          className={`topbar__filter${filterError ? " topbar__filter--invalid" : ""}`}
          spellCheck={false}
          autoComplete="off"
          placeholder={
            filterPlaceholder ??
            'topic =~ "orders.*" && headers.tenant == "acme" && payload.amount > 1000'
          }
          value={filter}
          onChange={(event) => {
            onFilterChange(event.target.value);
          }}
          aria-invalid={filterError ? "true" : "false"}
          title={filterError ?? undefined}
        />
        {filterError ? <span className="topbar__filter-error">{filterError}</span> : null}
      </div>
      <div className="topbar__controls">
        <button
          type="button"
          className="btn btn--primary"
          onClick={onToggleCapture}
          aria-pressed={capturing}
        >
          {capturing ? "Stop" : "Start"}
        </button>
        <button type="button" className="btn" onClick={onClear}>
          Clear
        </button>
      </div>
    </header>
  );
}
