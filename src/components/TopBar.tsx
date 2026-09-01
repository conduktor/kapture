import type { JSX } from "react";
import type { ProxyStatus } from "../types";
import { useIsLinux, useIsWindows } from "../lib/platform";

interface Props {
  filter: string;
  onFilterChange: (next: string) => void;
  filterError: string | null;
  filterPlaceholder?: string;
  capturing: boolean;
  onToggleCapture: () => void;
  /** Open the JVM tap picker. Visible only when not capturing —
   * once a session is active, Stop terminates it regardless of mode. */
  onOpenTap: () => void;
  /** Open the Linux eBPF/OpenSSL process picker. */
  onOpenEbpf: () => void;
  onClear: () => void;
  proxyStatus: ProxyStatus | null;
  /** When a tap session is active, the cluster pill flips to
   * `tap PID X (truncated command)` instead of the proxy address. */
  tapStatus: { pid: number; command: string; socketPath: string } | null;
  onEdit: () => void;
  /** Open the Snippets modal. Button is only rendered when connected. */
  onOpenSnippets: () => void;
  /** Open the MCP integration modal. Always available — surfaces the
   * local MCP URL + bearer token regardless of proxy state. */
  onOpenMcp: () => void;
  /** UI-paused = ring buffer keeps capturing, but Messages and
   *  Protocol lists stop refreshing so the user can investigate
   *  without scroll-away. Distinct from Stop (which would tear down
   *  the proxy and disconnect clients). */
  paused: boolean;
  onTogglePaused: (next: boolean) => void;
}

export function TopBar({
  filter,
  onFilterChange,
  filterError,
  filterPlaceholder,
  capturing,
  onToggleCapture,
  onOpenTap,
  onOpenEbpf,
  onClear,
  proxyStatus,
  tapStatus,
  onEdit,
  onOpenSnippets,
  onOpenMcp,
  paused,
  onTogglePaused,
}: Props): JSX.Element {
  // JVM tap mode is Unix-only (Unix-domain-socket transport); hide its
  // entry point on Windows, where the backend command is stubbed out.
  const isWindows = useIsWindows();
  const isLinux = useIsLinux();
  // Cluster pill: show `{listenAddr} → {upstream}` when the proxy
  // is up, `tap PID X` when a JVM tap is active, "not connected"
  // otherwise. "proxy" / "tap" wording is intentionally minimal —
  // Kapture presents itself as an inspector; the capture-mode
  // plumbing is implementation detail, not a feature the user has
  // to think about.
  let pillLabel = "not connected";
  if (tapStatus !== null) {
    pillLabel = `tap PID ${tapStatus.pid}`;
  } else if (proxyStatus !== null) {
    pillLabel = `${proxyStatus.listenAddr} → ${proxyStatus.upstream}`;
  }
  return (
    <header className="topbar">
      <div className="topbar__controls">
        <button
          type="button"
          className="btn btn--ghost topbar__mcp"
          onClick={onOpenMcp}
          title="Wire Kapture into Claude Code, Cursor, etc."
        >
          MCP
        </button>
        {capturing ? (
          <>
            <button
              type="button"
              className={paused ? "btn btn--paused" : "btn btn--ghost"}
              onClick={() => {
                onTogglePaused(!paused);
              }}
              aria-pressed={paused}
              title={
                paused
                  ? "UI paused — ring buffer keeps capturing. Click to resume + sync the lists."
                  : "Freeze the live lists so you can investigate without scroll-away. Capture and forwarding continue."
              }
            >
              {paused ? "Resume UI" : "Pause UI"}
            </button>
            <button
              type="button"
              className="btn btn--ghost topbar__snippets"
              onClick={onOpenSnippets}
              title="Show test commands (kcat / kafka CLI)"
            >
              <span aria-hidden="true">{">_"}</span> Snippets
            </button>
          </>
        ) : null}
        <button
          type="button"
          className="btn btn--primary"
          onClick={onToggleCapture}
          aria-pressed={capturing}
        >
          {capturing ? "Stop" : "Start"}
        </button>
        {!capturing && !isWindows ? (
          <button
            type="button"
            className="btn"
            onClick={onOpenTap}
            title="Inject the Kapture agent into a running Java Kafka client (no proxy)"
          >
            Tap JVM
          </button>
        ) : null}
        {!capturing && isLinux ? (
          <button
            type="button"
            className="btn"
            onClick={onOpenEbpf}
            title="Attach PID-scoped OpenSSL eBPF probes (no proxy)"
          >
            Tap eBPF
          </button>
        ) : null}
        <button type="button" className="btn" onClick={onClear}>
          Clear
        </button>
      </div>
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
          onKeyDown={(event) => {
            // Esc on a non-empty filter clears it. The browser's
            // native search-input behaviour, ported to our text input
            // since we can't use type="search" without inheriting the
            // UA's pill chrome.
            if (event.key === "Escape" && filter.length > 0) {
              event.preventDefault();
              onFilterChange("");
            }
          }}
          aria-invalid={filterError !== null ? "true" : "false"}
          title={filterError ?? undefined}
        />
        {filter.length > 0 ? (
          <button
            type="button"
            className="topbar__filter-clear"
            onClick={() => {
              onFilterChange("");
            }}
            aria-label="Clear filter"
            title="Clear filter (Esc)"
          >
            ×
          </button>
        ) : null}
        {filterError !== null ? <span className="topbar__filter-error">{filterError}</span> : null}
      </div>
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
    </header>
  );
}
