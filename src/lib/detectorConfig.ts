/**
 * Wire types + IPC wrappers for the anti-pattern detector thresholds
 * (`DetectorConfig` in `src-tauri/src/anti_patterns/config.rs`).
 *
 * The backend persists this to `<config_dir>/detector_config.json`.
 * `get_detector_config` returns the active config; `set_detector_config`
 * replaces + persists it (applies to the *next* capture session).
 *
 * Two field classes (see the Rust doc):
 *  - **Class B** — values the wire can't reveal; their correct value
 *    lives in the client config. Surfaced in the settings UI.
 *  - **Class A** — sensitivity knobs; edit the JSON file directly.
 */

import { invoke } from "@tauri-apps/api/core";

export interface DetectorConfig {
  // Class B — values the wire can't reveal.
  pollStallGapMs: number;
  pollStallMinFetches: number;
  autocommitIntervalMs: number;
  autocommitIntervalTolerance: number;
  saslShortSessionMs: number;
  // Class A — sensitivity knobs.
  overcommitRatePerSec: number;
  overcommitMinSamples: number;
  producerPerRecordInitRatio: number;
  producerPerRecordMinInits: number;
  tinyBatchRecordsPerProduce: number;
  tinyBatchMinProduceRate: number;
  tinyBatchMinSamples: number;
  rebalanceJoinsInWindow: number;
  compressionOffMinSamples: number;
  compressionOffMinRate: number;
  nonIdempotentMinSamples: number;
  producerInstanceLeakPerSec: number;
  producerInstanceLeakMinSamples: number;
  autocommitMinSamples: number;
  tightFetchAvgResponseBytes: number;
  tightFetchMinRate: number;
  tightFetchMinSamples: number;
  fetchSessionErrorsThreshold: number;
  metadataStormRatePerSec: number;
  metadataStormMinSamples: number;
  cooperativeStickyChurnThreshold: number;
  coordinatorChurnThreshold: number;
  unknownTopicPollThreshold: number;
  offsetOutOfRangeThreshold: number;
  aclDenyThreshold: number;
}

/** Editable Class-B field descriptor for the settings UI. */
export interface ClassBField {
  key: keyof DetectorConfig;
  label: string;
  unit: string;
  /** The Kafka client config this should mirror, if any. */
  mirrors?: string;
  help: string;
  /** Whole-number input (ms / counts) vs decimal (ratios). */
  integer: boolean;
}

/**
 * The thresholds whose *correct* value the user knows but the wire
 * doesn't. Everything else is a sensitivity knob, edited via the JSON
 * file.
 */
export const CLASS_B_FIELDS: readonly ClassBField[] = [
  {
    key: "pollStallGapMs",
    label: "Poll-stall gap",
    unit: "ms",
    mirrors: "max.poll.interval.ms",
    help: "Flag a consumer whose fetch stream goes silent for longer than this. Set it to your consumer's max.poll.interval.ms (default 300000) for a precise eviction signal; the 10000 default is an early warning.",
    integer: true,
  },
  {
    key: "pollStallMinFetches",
    label: "Poll-stall cadence gate",
    unit: "fetches",
    help: "Prior fetches required before a gap counts, so startup isn't flagged as a stall.",
    integer: true,
  },
  {
    key: "autocommitIntervalMs",
    label: "Auto-commit interval",
    unit: "ms",
    mirrors: "auto.commit.interval.ms",
    help: "Expected spacing between OffsetCommits for the auto-commit cadence detector. Set to your auto.commit.interval.ms (default 5000).",
    integer: true,
  },
  {
    key: "autocommitIntervalTolerance",
    label: "Auto-commit tolerance",
    unit: "ratio",
    help: "Allowed relative deviation around the interval before the cadence stops looking like auto-commit (0.10 = ±10%).",
    integer: false,
  },
  {
    key: "saslShortSessionMs",
    label: "SASL short-session floor",
    unit: "ms",
    mirrors: "connections.max.reauth.ms",
    help: "A re-auth session lifetime below this reads as a misconfigured reauth window. Relate to your connections.max.reauth.ms.",
    integer: true,
  },
];

export async function getDetectorConfig(): Promise<DetectorConfig> {
  return invoke<DetectorConfig>("get_detector_config");
}

export async function setDetectorConfig(config: DetectorConfig): Promise<void> {
  await invoke("set_detector_config", { config });
}
