/** Read a clamped split ratio from `localStorage`, falling back to
 *  `fallback` when the entry is absent, malformed, or out of range.
 *  Extracted from App.tsx so the App module stays under the line cap.
 *  `localStorage` may be unavailable (private mode, file://) — the
 *  catch covers that case implicitly. */
export function readSplit(key: string, fallback: number): number {
  try {
    const raw = window.localStorage.getItem(key);
    if (raw === null) {
      return fallback;
    }
    const n = Number.parseFloat(raw);
    if (Number.isFinite(n) && n >= 0.05 && n <= 0.95) {
      return n;
    }
  } catch {
    /* ignore */
  }
  return fallback;
}
