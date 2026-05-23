import type { CaptureStats, ConnectionState, ProxyStatus } from "../types";

/** Bag of setters App.tsx already owns. Passing them as a struct keeps
 * the call site short and avoids polluting App.tsx with five duplicate
 * proxy-vs-tap parameter lists. The ref-touching part is wrapped in a
 * `clearMessages` callback the caller passes — lets the React refs
 * lint stay happy (we never inspect ref `.current` during render).
 *
 * The handlers themselves used to live inline at the top of `App.tsx`;
 * they were moved out so the file stays under the 1000-line budget. */
export interface TapLifecycleDeps {
  clearMessages: () => void;
  setSelectedId: (id: string | null) => void;
  setStats: (stats: CaptureStats) => void;
  setConnection: (next: ConnectionState | ((prev: ConnectionState) => ConnectionState)) => void;
  setTapDialogOpen: (open: boolean) => void;
  initialStats: CaptureStats;
}

/** Three callbacks the `TapDialog` calls during the start-tap flow:
 *  - `handleTapStarting` clears stale message state and flips the UI
 *    to "connecting" before the agent attach round-trip resolves.
 *  - `handleTapStarted` records the chosen JVM as the active capture
 *    source so the cluster pill flips to `tap PID X`.
 *  - `handleTapError` surfaces an attacher failure (DisableAttachMechanism,
 *    JRE-only, wrong UID) into the same error slot the proxy uses. */
export function buildTapHandlers(deps: TapLifecycleDeps): {
  handleTapStarting: () => void;
  handleTapStarted: (info: { pid: number; command: string; socketPath: string }) => void;
  handleTapError: (message: string) => void;
} {
  const handleTapStarting = (): void => {
    deps.clearMessages();
    deps.setSelectedId(null);
    deps.setStats(deps.initialStats);
    deps.setConnection((prev) => ({
      ...prev,
      status: "connecting",
      error: null,
      proxyStatus: null,
      tapStatus: null,
    }));
  };

  const handleTapStarted = (info: { pid: number; command: string; socketPath: string }): void => {
    deps.setConnection({
      status: "connected",
      upstream: null,
      error: null,
      proxyStatus: null,
      tapStatus: info,
    });
    deps.setTapDialogOpen(false);
  };

  const handleTapError = (message: string): void => {
    deps.setConnection((prev) => ({
      ...prev,
      status: "error",
      error: message,
      proxyStatus: null,
      tapStatus: null,
    }));
  };

  return { handleTapStarting, handleTapStarted, handleTapError };
}

/** Deps for the proxy variant — same setters but with a hook for the
 * editing-modal close + no tap-dialog setter (the proxy modal is
 * standalone, not gated by a separate dialog flag). */
export interface ProxyLifecycleDeps {
  clearMessages: () => void;
  setSelectedId: (id: string | null) => void;
  setStats: (stats: CaptureStats) => void;
  setConnection: (next: ConnectionState | ((prev: ConnectionState) => ConnectionState)) => void;
  setEditing: (editing: boolean) => void;
  initialStats: CaptureStats;
}

/** Mirror of `buildTapHandlers` for the proxy flow. Body identical
 * shape; kept symmetric so the eBPF-tap variant (next PR) slots in
 * with the same factory pattern. */
export function buildProxyHandlers(deps: ProxyLifecycleDeps): {
  handleProxyStarting: () => void;
  handleProxyStarted: (status: ProxyStatus) => void;
  handleProxyError: (message: string) => void;
} {
  const handleProxyStarting = (): void => {
    deps.clearMessages();
    deps.setSelectedId(null);
    deps.setStats(deps.initialStats);
    deps.setConnection((prev) => ({
      ...prev,
      status: "connecting",
      error: null,
      proxyStatus: null,
    }));
    deps.setEditing(false);
  };

  const handleProxyStarted = (status: ProxyStatus): void => {
    deps.setConnection({
      status: "connected",
      upstream: status.upstream,
      error: null,
      proxyStatus: status,
      tapStatus: null,
    });
  };

  const handleProxyError = (message: string): void => {
    deps.setConnection((prev) => ({
      ...prev,
      status: "error",
      error: message,
      proxyStatus: null,
    }));
  };

  return { handleProxyStarting, handleProxyStarted, handleProxyError };
}
