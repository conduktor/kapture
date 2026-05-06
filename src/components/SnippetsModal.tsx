import { useEffect, type JSX } from "react";
import { SnippetsPanel } from "./SnippetsPanel";

interface Props {
  listenAddr: string;
  onClose: () => void;
}

/**
 * Modal hosting the SnippetsPanel. Backdrop click + Escape close it.
 * Sized as `min(640px, 90vw)` so on a wide monitor the snippet `<code>`
 * blocks don't horizontally scroll, and on a narrow window we still fit.
 */
export function SnippetsModal({ listenAddr, onClose }: Props): JSX.Element {
  useEffect(() => {
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === "Escape") {
        onClose();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("keydown", onKey);
    };
  }, [onClose]);

  return (
    <div
      className="dialog-backdrop"
      onClick={(e) => {
        // Backdrop close only — clicks bubbled up from the dialog itself
        // would otherwise dismiss it on every interaction inside.
        if (e.target === e.currentTarget) {
          onClose();
        }
      }}
    >
      <div
        className="dialog modal--snippets"
        role="dialog"
        aria-modal="true"
        aria-label="Test commands"
      >
        <div className="modal__header">
          <h2 className="dialog__title">Test commands</h2>
          <button
            type="button"
            className="modal__close"
            onClick={onClose}
            aria-label="Close snippets dialog"
          >
            ×
          </button>
        </div>
        <p className="dialog__hint">
          Point any Kafka client at <code>{listenAddr}</code> — Kapture proxies upstream.
        </p>
        <SnippetsPanel listenAddr={listenAddr} />
      </div>
    </div>
  );
}
