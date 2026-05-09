/**
 * RTT formatter — keeps the cell width bounded by capping at 4
 * numeric chars (excluding the decimal point) and switching unit at
 * 1000 ms / 1000 s. The list cell is narrow; long values like
 * `1033.49 ms` used to truncate with an ellipsis. Now that's `1.03 s`.
 */
export interface FormattedRtt {
  value: string;
  unit: string;
}

export function formatRtt(ms: number): FormattedRtt {
  if (!Number.isFinite(ms) || ms < 0) {
    return { value: "—", unit: "" };
  }
  if (ms < 1000) {
    return { value: chooseDigits(ms), unit: "ms" };
  }
  return { value: chooseDigits(ms / 1000), unit: "s" };
}

/** Pick decimal precision so the output stays ≤ 4 numeric chars after
 *  rounding. Naively `n < 10 ? toFixed(2) : toFixed(1)` breaks when
 *  rounding pushes the value across the boundary (9.999 → "10.00").
 *  We round at the candidate precision first, then re-bucket. */
function chooseDigits(n: number): string {
  if (Math.round(n * 100) / 100 < 10) {
    return n.toFixed(2);
  }
  if (Math.round(n * 10) / 10 < 100) {
    return n.toFixed(1);
  }
  return n.toFixed(0);
}
