import type { JSX } from "react";

interface Props {
  filter: string;
  onFilterChange: (next: string) => void;
  filterError: string | null;
  capturing: boolean;
  onToggleCapture: () => void;
  onClear: () => void;
  cluster: string;
  onEdit: () => void;
}

export function TopBar({
  filter,
  onFilterChange,
  filterError,
  capturing,
  onToggleCapture,
  onClear,
  cluster,
  onEdit,
}: Props): JSX.Element {
  return (
    <header className="topbar">
      <button
        type="button"
        className="topbar__cluster"
        onClick={onEdit}
        title="Edit connection settings"
      >
        <span className="topbar__cluster-dot" data-status={capturing ? "live" : "idle"} />
        <span className="topbar__cluster-name">{cluster}</span>
        <span className="topbar__cluster-edit" aria-hidden="true">
          ✎
        </span>
      </button>
      <div className="topbar__filter-wrap">
        <input
          className={`topbar__filter${filterError ? " topbar__filter--invalid" : ""}`}
          spellCheck={false}
          autoComplete="off"
          placeholder='topic =~ "orders.*" && headers.tenant == "acme" && payload.amount > 1000'
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
