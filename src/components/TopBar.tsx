import type { JSX } from "react";
import type { ProxyStatus } from "../types";

interface Props {
  filter: string;
  onFilterChange: (next: string) => void;
  filterError: string | null;
  filterPlaceholder?: string;
  capturing: boolean;
  onToggleCapture: () => void;
  onClear: () => void;
  proxyStatus: ProxyStatus | null;
  onEdit: () => void;
  /** Open the Snippets modal. Button is only rendered when connected. */
  onOpenSnippets: () => void;
}

export function TopBar({
  filter,
  onFilterChange,
  filterError,
  filterPlaceholder,
  capturing,
  onToggleCapture,
  onClear,
  proxyStatus,
  onEdit,
  onOpenSnippets,
}: Props): JSX.Element {
  // Proxy pill: show `proxy {listenAddr} → {upstream}` when the listener
  // is up; "no proxy" when nothing's running; "proxy starting…" when the
  // listener is booting so the pill never blanks.
  const pillLabel =
    proxyStatus !== null ? `proxy ${proxyStatus.listenAddr} → ${proxyStatus.upstream}` : "no proxy";
  return (
    <header className="topbar">
      <button
        type="button"
        className="topbar__cluster"
        onClick={onEdit}
        title="Edit proxy settings"
      >
        <span className="topbar__cluster-dot" data-status={capturing ? "live" : "idle"} />
        <span className="topbar__cluster-name">{pillLabel}</span>
        <span className="topbar__cluster-edit" aria-hidden="true">
          ✎
        </span>
      </button>
      <div className="topbar__filter-wrap">
        <input
          className={`topbar__filter${filterError !== null ? " topbar__filter--invalid" : ""}`}
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
          aria-invalid={filterError !== null ? "true" : "false"}
          title={filterError ?? undefined}
        />
        {filterError !== null ? <span className="topbar__filter-error">{filterError}</span> : null}
      </div>
      <div className="topbar__controls">
        {capturing ? (
          <button
            type="button"
            className="btn btn--ghost topbar__snippets"
            onClick={onOpenSnippets}
            title="Show test commands (kcat / kafka CLI)"
          >
            <span aria-hidden="true">{">_"}</span> Snippets
          </button>
        ) : null}
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
