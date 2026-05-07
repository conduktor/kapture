import { useCallback, useEffect, useRef, type UIEvent } from "react";
import type { ListImperativeAPI } from "react-window";

/**
 * Wireshark-style live-tail for a virtualised react-window list.
 *
 * The hook exposes:
 *   - `listProps`  — handlers to spread onto <List>; track scroll +
 *                    user-input gestures inside the virtualiser
 *   - `armUserInput` — call from any custom user-input path (e.g. the
 *                      parent section's onKeyDown for arrow navigation)
 *
 * Key idea: a DOM `scroll` event is indistinguishable between
 * user-driven and programmatic, so we infer cause by *correlating*
 * it with a recent user-only input event (wheel, touch, mousedown,
 * keydown). Only scrolls within `USER_INPUT_GRACE_MS` of such an
 * event update the follow latch — bare scroll events from our own
 * `scrollToRow` calls are ignored.
 *
 * Why this beats the obvious "stopIndex === rowCount-1" check:
 * on the very first paint the list often renders many rows already
 * (200+ from the initial buffer flush), so stopIndex is tiny while
 * rowCount is large — the user-hasn't-touched-anything case looks
 * identical to a deliberate scroll-up. Result: follow defaulted off.
 * Time-correlation cleanly separates "user touched the surface" from
 * "list is just freshly mounted with content".
 *
 * Pass the live array (not just its length) as `signal` so in-place
 * rotation when the ring buffer is at cap still triggers the
 * follow-effect.
 */
export interface AutoFollowApi {
  listProps: {
    onScroll: (e: UIEvent<HTMLDivElement>) => void;
    onWheel: () => void;
    onTouchStart: () => void;
    onTouchMove: () => void;
    onMouseDown: () => void;
  };
  armUserInput: () => void;
}

const USER_INPUT_GRACE_MS = 300;
// Generous "near bottom" tolerance (~3-4 rows). Tight thresholds break
// re-arming under live traffic: while the user wheels back down,
// scrollHeight grows from arriving rows, so a 4px window is almost
// never satisfied — they reach "yesterday's bottom" but the goalpost
// has moved. ~100px lets the latch flip the moment they're visibly
// chasing the tail.
const BOTTOM_THRESHOLD_PX = 100;

export function useAutoFollow(
  signal: readonly unknown[],
  listRef: { current: ListImperativeAPI | null },
): AutoFollowApi {
  const followingRef = useRef(true);
  const lastUserInputAtRef = useRef(0);
  const rowCount = signal.length;

  const armUserInput = useCallback((): void => {
    lastUserInputAtRef.current = performance.now();
  }, []);

  const onScroll = useCallback((e: UIEvent<HTMLDivElement>): void => {
    const sinceInput = performance.now() - lastUserInputAtRef.current;
    if (sinceInput > USER_INPUT_GRACE_MS) {
      // No recent user gesture → this scroll came from our own
      // scrollToRow (or browser focus/anchor adjustment). Don't let
      // it flip the follow latch.
      return;
    }
    const el = e.currentTarget;
    const atBottom = el.scrollTop + el.clientHeight >= el.scrollHeight - BOTTOM_THRESHOLD_PX;
    followingRef.current = atBottom;
  }, []);

  useEffect(() => {
    if (!followingRef.current || rowCount === 0) {
      return;
    }
    listRef.current?.scrollToRow({ index: rowCount - 1, align: "end" });
    // signal carries both length growth AND in-place tail rotation
    // when the ring buffer is at cap.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [signal]);

  return {
    listProps: {
      onScroll,
      onWheel: armUserInput,
      onTouchStart: armUserInput,
      onTouchMove: armUserInput,
      onMouseDown: armUserInput,
    },
    armUserInput,
  };
}
