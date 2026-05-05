import type { JSX } from "react";

interface Props {
  filter: string;
  onFilterChange: (next: string) => void;
  capturing: boolean;
  onToggleCapture: () => void;
  onClear: () => void;
  cluster: string;
}

export function TopBar({
  filter,
  onFilterChange,
  capturing,
  onToggleCapture,
  onClear,
  cluster,
}: Props): JSX.Element {
  return (
    <header className="topbar">
      <div className="topbar__cluster">
        <span className="topbar__cluster-dot" data-status={capturing ? "live" : "idle"} />
        <span className="topbar__cluster-name">{cluster}</span>
      </div>
      <input
        className="topbar__filter"
        spellCheck={false}
        autoComplete="off"
        placeholder='topic =~ "orders.*" && headers.tenant == "acme" && payload.amount > 1000'
        value={filter}
        onChange={(event) => {
          onFilterChange(event.target.value);
        }}
      />
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
