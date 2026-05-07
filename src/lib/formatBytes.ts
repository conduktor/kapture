/**
 * Render a byte count with a unit that fits the magnitude. Uses
 * binary (1024-based) units — this is wire/buffer accounting, not
 * disk-vendor marketing. Negative values are clamped to 0; non-finite
 * values render as "—" so a stray NaN doesn't poison the UI.
 *
 * Compact form (default) is one-letter, no space — fits in tight
 * grid columns:
 *   1023      → "1023 B"
 *   1024      → "1.0 KiB"
 *   1_500_000 → "1.4 MiB"
 *   2_500_000_000 → "2.3 GiB"
 *
 * Set `precision` for the fractional digits when above 1 KiB
 * (default 1). Bytes are always integer.
 */
export function formatBytes(n: number, precision = 1): string {
  if (!Number.isFinite(n)) return "—";
  const v = Math.max(0, n);
  if (v < 1024) return `${Math.round(v)} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(precision)} KiB`;
  if (v < 1024 * 1024 * 1024) return `${(v / (1024 * 1024)).toFixed(precision)} MiB`;
  return `${(v / (1024 * 1024 * 1024)).toFixed(precision)} GiB`;
}
