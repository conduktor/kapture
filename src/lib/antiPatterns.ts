/**
 * Wire types for the anti-pattern detector fold computed by the Rust
 * backend (`src-tauri/src/anti_patterns.rs`). The backend exposes the
 * snapshot via the `anti_patterns` Tauri command; the Expert tab
 * polls it on the same 1 Hz schedule as session stats.
 *
 * Each detection is keyed by `(kind, scope)` server-side, so successive
 * contributing frames update the same row instead of fanning out.
 */

export type AntiPatternKind =
  | "overcommit"
  | "producerPerRecord"
  | "tinyBatches"
  | "rebalanceLoop"
  | "staleLeaderProducing"
  | "mixedApiVersion"
  | "saslSessionTooShort";

export type Severity = "warn" | "note";

export interface Detection {
  kind: AntiPatternKind;
  severity: Severity;
  title: string;
  detail: string;
  scope: string;
  firstSeen: string;
  lastSeen: string;
  occurrences: number;
  frameId: string | null;
}

export interface AntiPatternsSnapshot {
  detections: Detection[];
}

export const EMPTY_ANTI_PATTERNS: AntiPatternsSnapshot = {
  detections: [],
};

/** Stable copy for empty / sentinel rendering. */
export function kindLabel(kind: AntiPatternKind): string {
  switch (kind) {
    case "overcommit":
      return "Overcommit";
    case "producerPerRecord":
      return "Producer per record";
    case "tinyBatches":
      return "Tiny Produce batches";
    case "rebalanceLoop":
      return "Rebalance loop";
    case "staleLeaderProducing":
      return "Stale-leader producing";
    case "mixedApiVersion":
      return "Mixed api_version across brokers";
    case "saslSessionTooShort":
      return "SASL session too short on re-auth";
  }
}
