/**
 * Format an RFC3339 µs timestamp emitted by the Rust backend (always
 * UTC, e.g. `2026-05-07T18:39:43.479708Z`) as the user's local
 * wall-clock time `HH:MM:SS.µs`. JS `Date` only carries millisecond
 * precision, so we splice the µs trailer from the original string
 * back in to keep sub-ms ordering visible during bursts.
 */
export function formatLocalTime(ts: string): string {
  const d = new Date(ts);
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  const dotIdx = ts.indexOf(".");
  const zIdx = ts.indexOf("Z");
  const frac = dotIdx >= 0 && zIdx > dotIdx ? ts.slice(dotIdx + 1, zIdx) : "";
  const tail = frac ? `.${frac.slice(0, 6)}` : "";
  return `${hh}:${mm}:${ss}${tail}`;
}
