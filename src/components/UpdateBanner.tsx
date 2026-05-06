import { useEffect, useRef, useState, type JSX } from "react";
import { check, type Update } from "@tauri-apps/plugin-updater";
import { relaunch } from "@tauri-apps/plugin-process";

type Phase = "idle" | "available" | "installing" | "ready" | "failed";

interface State {
  phase: Phase;
  version: string | null;
  notes: string | null;
  error: string | null;
}

const INITIAL: State = { phase: "idle", version: null, notes: null, error: null };

// Probe once on startup. Failures are silent (no internet, dev build with a
// placeholder pubkey, etc.) — we never block app usage on update checks.
export function UpdateBanner(): JSX.Element | null {
  const [state, setState] = useState<State>(INITIAL);
  const [update, setUpdate] = useState<Update | null>(null);

  // useRef rather than a closed-over `let` so TS doesn't flow-narrow the
  // post-await read to `false` (which would break the strict-eslint
  // no-unnecessary-condition rule).
  const cancelledRef = useRef(false);
  useEffect(() => {
    cancelledRef.current = false;
    void (async () => {
      try {
        const next = await check();
        if (cancelledRef.current || !next) {
          return;
        }
        setUpdate(next);
        setState({
          phase: "available",
          version: next.version,
          notes: next.body ?? null,
          error: null,
        });
      } catch (err) {
        if (cancelledRef.current) {
          return;
        }
        // Codex finding [2]: differentiate transient errors (offline, dev
        // build with placeholder pubkey) from suspected security errors
        // (signature mismatch). The latter is loud — anything matching
        // "signature" surfaces in the UI as a failed banner so the user
        // can see the update channel is compromised. Network / pubkey
        // misconfig stays a silent console warning.
        const message = err instanceof Error ? err.message : String(err);
        if (/signature|verif|tamper/i.test(message)) {
          console.error("update verification failed", err);
          setState({
            phase: "failed",
            version: null,
            notes: null,
            error: `Update verification failed: ${message}`,
          });
        } else {
          console.warn("update check failed (transient)", err);
        }
      }
    })();
    return () => {
      cancelledRef.current = true;
    };
  }, []);

  // "failed" before any successful discovery means the verification check
  // tripped — show a visible error even with no update object.
  if (state.phase === "idle") {
    return null;
  }
  if (!update && state.phase !== "failed") {
    return null;
  }

  const handleInstall = (): void => {
    // The "Install & restart" button only renders when phase is "available",
    // which implies update is non-null (set together in the discovery effect).
    // Re-bind locally so TS keeps the narrow inside the async closure.
    const target = update;
    if (!target) {
      return;
    }
    setState((prev) => ({ ...prev, phase: "installing" }));
    void (async () => {
      try {
        await target.downloadAndInstall();
        setState((prev) => ({ ...prev, phase: "ready" }));
        await relaunch();
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setState((prev) => ({ ...prev, phase: "failed", error: message }));
      }
    })();
  };

  const handleDismiss = (): void => {
    setState(INITIAL);
    setUpdate(null);
  };

  return (
    <div className="update-banner" role="status" aria-live="polite">
      <span className="update-banner__icon" aria-hidden="true">
        ↑
      </span>
      <span className="update-banner__text">
        {state.phase === "installing" ? (
          <>Downloading v{state.version}…</>
        ) : state.phase === "ready" ? (
          <>v{state.version} installed — restarting…</>
        ) : state.phase === "failed" ? (
          <>Update failed: {state.error}</>
        ) : (
          <>
            Kapture v{state.version} is available.
            {state.notes ? <span className="update-banner__notes"> {state.notes}</span> : null}
          </>
        )}
      </span>
      {state.phase === "available" ? (
        <div className="update-banner__actions">
          <button type="button" className="btn btn--primary" onClick={handleInstall}>
            Install &amp; restart
          </button>
          <button type="button" className="btn" onClick={handleDismiss}>
            Later
          </button>
        </div>
      ) : null}
      {state.phase === "failed" ? (
        <div className="update-banner__actions">
          <button type="button" className="btn" onClick={handleDismiss}>
            Dismiss
          </button>
        </div>
      ) : null}
    </div>
  );
}
