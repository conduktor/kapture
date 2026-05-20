import { type JSX } from "react";

import { kindLabel, type AntiPatternsSnapshot, type Detection } from "../lib/antiPatterns";

interface Props {
  /**
   * Detector snapshot polled from the Rust backend
   * (`anti_patterns` Tauri command). Already sorted server-side:
   * severity first (Warn before Note), then most-recent.
   */
  snapshot: AntiPatternsSnapshot;
  /**
   * Switch to the Protocol tab and select the offending frame so the
   * user can read its decoded body. The Expert tab doesn't apply a
   * filter — the row already names the (group / topic / partition /
   * connection) scope.
   */
  onJumpToFrame: (frameId: string) => void;
}

export function ExpertTab({ snapshot, onJumpToFrame }: Props): JSX.Element {
  const detections = snapshot.detections;
  if (detections.length === 0) {
    return (
      <div className="expert expert--empty">
        <div className="expert__empty">
          No anti-patterns detected yet. The detector watches the wire for 25 client + cluster
          patterns — overcommit, producer-per-record, tiny batches, rebalance loop, stale-leader
          producing, mixed api_version, SASL drift, acks=0, compression-off, non-idempotent
          producer, producer-instance leak, transactional zombie, auto-commit cadence, tight fetch
          polling, fetch-session error cascade, throttle pressure, metadata storm, KIP-848 holdouts,
          message-too-large, offset-out-of-range, cooperative-sticky churn, commit-during-rebalance,
          ACL deny, unknown-topic poll loop, and coordinator churn. Run some traffic through the
          proxy and findings will surface here.
        </div>
      </div>
    );
  }
  const warnCount = detections.filter((d) => d.severity === "warn").length;
  return (
    <div className="expert">
      <section className="expert__header" aria-label="Expert summary">
        <SummaryTile
          label="Active findings"
          value={String(detections.length)}
          tone={warnCount > 0 ? "warn" : "ok"}
          hint={warnCount > 0 ? `${String(warnCount)} warn` : "all clear"}
        />
      </section>
      <div className="expert__list" role="list">
        {detections.map((d) => (
          <DetectionRow key={`${d.kind}|${d.scope}`} det={d} onJumpToFrame={onJumpToFrame} />
        ))}
      </div>
    </div>
  );
}

function DetectionRow({
  det,
  onJumpToFrame,
}: {
  det: Detection;
  onJumpToFrame: (frameId: string) => void;
}): JSX.Element {
  const icon = det.severity === "warn" ? "⚠" : "●";
  return (
    <div
      className={`expert__row expert__row--${det.severity}`}
      role="listitem"
      aria-label={`${kindLabel(det.kind)} on ${det.scope}`}
    >
      <div className="expert__icon" aria-hidden="true">
        {icon}
      </div>
      <div className="expert__body">
        <div className="expert__title-line">
          <span className="expert__title">{det.title}</span>
          <span className="expert__kind">{kindLabel(det.kind)}</span>
        </div>
        <div className="expert__detail">{det.detail}</div>
        <div className="expert__meta">
          <span className="expert__scope" title="Scope of this detection">
            {det.scope || "—"}
          </span>
          <span className="expert__occurrences">
            {det.occurrences} {det.occurrences === 1 ? "occurrence" : "occurrences"}
          </span>
          <span className="expert__time">last {formatTime(det.lastSeen)}</span>
        </div>
      </div>
      {det.frameId !== null ? (
        <JumpButton frameId={det.frameId} onJumpToFrame={onJumpToFrame} />
      ) : null}
    </div>
  );
}

function JumpButton({
  frameId,
  onJumpToFrame,
}: {
  frameId: string;
  onJumpToFrame: (id: string) => void;
}): JSX.Element {
  return (
    <button
      type="button"
      className="expert__jump"
      onClick={() => {
        onJumpToFrame(frameId);
      }}
      title="Open the offending frame in the Protocol tab"
    >
      Jump to frame
    </button>
  );
}

interface SummaryTileProps {
  label: string;
  value: string;
  hint?: string;
  tone?: "ok" | "warn";
}

function SummaryTile({ label, value, hint, tone }: SummaryTileProps): JSX.Element {
  return (
    <div className={`expert__tile${tone !== undefined ? ` expert__tile--${tone}` : ""}`}>
      <div className="expert__tile-label">{label}</div>
      <div className="expert__tile-value">{value}</div>
      {hint !== undefined ? <div className="expert__tile-hint">{hint}</div> : null}
    </div>
  );
}

/** Hour:minute:second slice of an RFC3339 timestamp. */
function formatTime(ts: string): string {
  const t = ts.indexOf("T");
  if (t < 0) {
    return ts;
  }
  const dot = ts.indexOf(".", t);
  const end = dot < 0 ? ts.length : dot;
  return ts.slice(t + 1, end);
}
