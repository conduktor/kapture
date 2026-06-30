import { useEffect, useState, type JSX } from "react";

import {
  CLASS_B_FIELDS,
  getDetectorConfig,
  setDetectorConfig,
  type DetectorConfig,
} from "../lib/detectorConfig";

interface Props {
  onClose: () => void;
}

/**
 * Detector threshold settings. Surfaces the "Class B" thresholds — the
 * ones whose correct value the user knows but the wire can't reveal
 * (poll-stall gap ↔ max.poll.interval.ms, auto-commit interval, SASL
 * reauth floor). The remaining sensitivity knobs live in
 * detector_config.json and are edited there.
 *
 * Changes apply to the next capture session; the running correlator is
 * not rebuilt mid-flight (mirrors the backend's `set_detector_config`).
 */
export function DetectorSettingsModal({ onClose }: Props): JSX.Element {
  const [config, setConfig] = useState<DetectorConfig | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    void (async () => {
      try {
        setConfig(await getDetectorConfig());
      } catch (err) {
        setLoadError(err instanceof Error ? err.message : String(err));
      }
    })();
  }, []);

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

  const update = (key: keyof DetectorConfig, value: number): void => {
    setSaved(false);
    setConfig((prev) => (prev === null ? prev : { ...prev, [key]: value }));
  };

  const onSave = (): void => {
    if (config === null) {
      return;
    }
    setSaving(true);
    setSaveError(null);
    void setDetectorConfig(config)
      .then(() => {
        setSaved(true);
      })
      .catch((err: unknown) => {
        setSaveError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        setSaving(false);
      });
  };

  return (
    <div
      className="dialog-backdrop"
      onClick={(e) => {
        if (e.target === e.currentTarget) {
          onClose();
        }
      }}
    >
      <div
        className="dialog modal--detector"
        role="dialog"
        aria-modal="true"
        aria-label="Detector thresholds"
      >
        <div className="modal__header">
          <h2 className="dialog__title">Detector thresholds</h2>
          <button
            type="button"
            className="modal__close"
            onClick={onClose}
            aria-label="Close detector settings"
          >
            ×
          </button>
        </div>
        <p className="dialog__hint">
          Tune the thresholds whose right value the wire can&apos;t reveal — set them to your own
          client config. Changes apply to the next capture session.
        </p>

        {loadError !== null ? (
          <p className="dialog__error">Could not read detector config: {loadError}</p>
        ) : config === null ? (
          <p className="muted">Loading…</p>
        ) : (
          <>
            <div className="detector__grid">
              {CLASS_B_FIELDS.map((f) => (
                <label key={f.key} className="dialog__field detector__field">
                  <span className="dialog__label">
                    {f.label}
                    {f.mirrors !== undefined ? (
                      <code className="detector__mirrors">{f.mirrors}</code>
                    ) : null}
                  </span>
                  <span className="detector__input-row">
                    <input
                      className="dialog__input detector__input"
                      type="number"
                      min={0}
                      step={f.integer ? 1 : 0.01}
                      value={config[f.key]}
                      onChange={(e) => {
                        const n = Number(e.target.value);
                        update(f.key, Number.isFinite(n) && n >= 0 ? n : 0);
                      }}
                    />
                    <span className="detector__unit">{f.unit}</span>
                  </span>
                  <span className="detector__help muted">{f.help}</span>
                </label>
              ))}
            </div>

            <p className="detector__note muted">
              Other sensitivity knobs (rates, counts, ratios) live in{" "}
              <code>detector_config.json</code> in your config dir — edit them there.
            </p>

            {saveError !== null ? <p className="dialog__error">Save failed: {saveError}</p> : null}

            <div className="dialog__actions">
              <button type="button" className="btn btn--ghost" onClick={onClose}>
                Close
              </button>
              <button type="button" className="btn btn--primary" onClick={onSave} disabled={saving}>
                {saving ? "Saving…" : saved ? "Saved" : "Save"}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
