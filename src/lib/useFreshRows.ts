import { useEffect, useRef, useState } from "react";

/**
 * Track which row ids landed in the list within the last `ttlMs` so
 * the row component can flash a brief background tint as new traffic
 * streams in. Designed for live-tail UX (Wireshark / DevTools Network).
 *
 * Triggering rules:
 *   - Detect appended rows via the previous tail id, not via length —
 *     when the ring buffer is at cap, length stops changing while
 *     content keeps rotating, and a length-only check would silently
 *     stop animating.
 *   - First tick after mount: skip (don't flash the entire initial
 *     batch). All later bursts flash regardless of size — Protocol
 *     bursts (request + response per Kafka call, plus Metadata,
 *     Heartbeat, …) easily exceed a small cap, and the user wants
 *     every new arrival visible.
 *
 * Each tick schedules its own untracked timeout — multiple in-flight
 * timers are fine, each removes only its own ids.
 */
export function useFreshRows<T>(
  items: readonly T[],
  getId: (t: T) => string,
  ttlMs = 700,
): ReadonlySet<string> {
  const [fresh, setFresh] = useState<ReadonlySet<string>>(() => new Set<string>());
  const prevTailRef = useRef<string | null>(null);

  useEffect(() => {
    const last = items[items.length - 1];
    const prevTail = prevTailRef.current;
    prevTailRef.current = last !== undefined ? getId(last) : null;
    if (last === undefined || prevTail === null) {
      // Empty list, or first observation since mount — nothing to
      // mark fresh until we have a baseline tail to diff against.
      return;
    }
    // Walk backwards to find the previous tail. Faster than findIndex
    // when the new arrivals are few (the common case under live
    // traffic) — usually 1–3 items added per tick.
    let tailIdx = -1;
    for (let i = items.length - 1; i >= 0; i -= 1) {
      const item = items[i];
      if (item !== undefined && getId(item) === prevTail) {
        tailIdx = i;
        break;
      }
    }
    if (tailIdx < 0) {
      // Previous tail evicted from the ring buffer — we can't bound
      // the new range, so skip rather than flash everything.
      return;
    }
    const newCount = items.length - 1 - tailIdx;
    if (newCount <= 0) {
      return;
    }
    const newIds: string[] = [];
    for (let i = items.length - newCount; i < items.length; i += 1) {
      const item = items[i];
      if (item !== undefined) {
        newIds.push(getId(item));
      }
    }
    setFresh((prev) => {
      const next = new Set(prev);
      for (const id of newIds) next.add(id);
      return next;
    });
    // Intentionally do not clear this timer: the flash should always fade
    // even if the items array updates again before ttlMs.
    window.setTimeout(() => {
      setFresh((prev) => {
        const next = new Set(prev);
        for (const id of newIds) next.delete(id);
        return next;
      });
    }, ttlMs);
  }, [items, getId, ttlMs]);

  return fresh;
}
