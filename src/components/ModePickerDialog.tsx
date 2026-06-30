import type { JSX } from "react";

import { useIsWindows } from "../lib/platform";

interface Props {
  /** User picked tap mode → parent closes this dialog and opens the
   * JVM picker (`TapDialog`). */
  onPickTap: () => void;
  /** User picked proxy mode → parent closes this dialog and opens
   * the proxy config (`ConnectionDialog`). */
  onPickProxy: () => void;
  /** Close without picking; the user lands on the empty inspector
   * and can use the TopBar buttons later. */
  onCancel: () => void;
}

/**
 * First-contact mode picker. Shown on launch and whenever the user
 * needs to choose between Kapture's two capture modes without
 * pre-loading one or the other.
 *
 * The framing on each card matches what the user actually does:
 *   * Tap   = leave the client config alone, Kapture goes to the JVM.
 *   * Proxy = change the client's bootstrap to Kapture, Kapture
 *             forwards to the real broker.
 *
 * We don't try to "recommend" one over the other — both are
 * legitimate, and which one fits depends on the user's setup
 * (JVM-only? remote client? chaos testing?). The card descriptions
 * lay out the tradeoff in a single sentence each.
 */
export function ModePickerDialog({ onPickTap, onPickProxy, onCancel }: Props): JSX.Element {
  // JVM tap mode is Unix-only; on Windows only proxy mode is offered.
  const isWindows = useIsWindows();
  return (
    <div className="dialog-backdrop" onClick={onCancel} role="presentation">
      <div
        className="dialog mode-picker"
        role="dialog"
        aria-labelledby="mode-picker-title"
        onClick={(e) => {
          e.stopPropagation();
        }}
      >
        <header className="dialog__header">
          <h2 id="mode-picker-title" className="dialog__title">
            How do you want to capture Kafka traffic?
          </h2>
          <button type="button" className="tap-dialog__close" onClick={onCancel} aria-label="Close">
            ×
          </button>
        </header>

        <p className="dialog__hint">
          Two paths, one decoder. Pick the one that fits your setup — you can switch later from the
          top bar.
        </p>

        <div className="mode-picker__cards">
          {!isWindows ? (
            <button type="button" className="mode-card" onClick={onPickTap}>
              <div className="mode-card__icon" aria-hidden="true">
                ⌖
              </div>
              <div className="mode-card__title">Connect to my existing Java apps</div>
              <div className="mode-card__desc">
                Inject the Kapture agent into a running Java Kafka client. No proxy, no cert swap.
                TLS stays end-to-end with the real broker.
              </div>
            </button>
          ) : null}

          <button type="button" className="mode-card" onClick={onPickProxy}>
            <div className="mode-card__icon" aria-hidden="true">
              ⇄
            </div>
            <div className="mode-card__title">Proxy a Kafka cluster</div>
            <div className="mode-card__desc">
              Point your clients at <code>127.0.0.1:9092</code>; Kapture forwards every byte to your
              real broker. Works with any client, any host, any language.
            </div>
          </button>
        </div>
      </div>
    </div>
  );
}
