/**
 * Opportunistic LRU of decoded frame bodies, plus the prefetch
 * loop that warms it when a body-touching filter is active.
 *
 * Lives behind a `useRef<Map>` so cache writes don't trigger a
 * cascade of re-renders, but a RAF-coalesced `version` counter
 * pinned in `useState` ensures a single re-render per RAF tick —
 * critical under pause, where `protoFrames` polling is suspended
 * and otherwise nothing would invalidate the consumer's
 * `useMemo([…, decodedFor])` after the prefetch fills the cache.
 *
 * The prefetch walks the entire ring (newest-first) so a
 * `decodedField` hard-filter doesn't keep evicting frames invisible
 * just because their detail hasn't been fetched yet. Concurrency
 * is capped at 8 to bound IPC pressure.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import type { DecodedBodyResult, ProtoFrame } from "../types";

/** ~25 MiB worst case (≈ proto ring cap × 5 KiB per body). */
const DECODED_CACHE_MAX = 5000;

export interface DecodedCache {
  /** Cache lookup. Identity changes after every batched cache
   *  write, so a consumer's `useMemo([..., decodedFor])`
   *  invalidates the moment the prefetch fills new entries. */
  decodedFor: (id: string) => unknown;
  /** Insert into the cache + LRU-bump + schedule a re-render. */
  put: (id: string, decoded: unknown) => void;
}

export function useDecodedCache(protoFrames: ProtoFrame[], enabled: boolean): DecodedCache {
  const cacheRef = useRef<Map<string, unknown>>(new Map());
  const unavailableRef = useRef<Set<string>>(new Set());
  const [version, setVersion] = useState(0);
  const rafRef = useRef<number | null>(null);

  const bump = useCallback(() => {
    if (rafRef.current !== null) return;
    rafRef.current = requestAnimationFrame(() => {
      rafRef.current = null;
      setVersion((v) => v + 1);
    });
  }, []);

  const put = useCallback(
    (id: string, decoded: unknown): void => {
      const cache = cacheRef.current;
      cache.delete(id);
      cache.set(id, decoded);
      unavailableRef.current.delete(id);
      while (cache.size > DECODED_CACHE_MAX) {
        const oldest = cache.keys().next();
        if (oldest.done === true) break;
        cache.delete(oldest.value);
      }
      bump();
    },
    [bump],
  );

  // eslint-disable-next-line react-hooks/exhaustive-deps -- identity bump on `version` is the entire point.
  const decodedFor = useCallback((id: string): unknown => cacheRef.current.get(id), [version]);

  useEffect(() => {
    if (!enabled || protoFrames.length === 0) return undefined;
    // Holder so TS doesn't narrow `cancelled` to `false` inside the
    // async closure — the cleanup mutates via the holder.
    const state = { cancelled: false };
    const cache = cacheRef.current;
    const unavailable = unavailableRef.current;
    // Evict orphans whose frame has aged out of the ring; otherwise
    // the cache pins memory for ids the user can never reach again.
    const liveIds = new Set(protoFrames.map((f) => f.id));
    for (const id of Array.from(cache.keys())) {
      if (!liveIds.has(id)) cache.delete(id);
    }
    for (const id of Array.from(unavailable)) {
      if (!liveIds.has(id)) unavailable.delete(id);
    }
    const queue = protoFrames
      .slice()
      .reverse()
      .filter((f) => !cache.has(f.id) && !unavailable.has(f.id))
      .map((f) => f.id);
    const batchSize = 128;
    void (async () => {
      while (!state.cancelled && queue.length > 0) {
        const ids = queue.splice(0, batchSize);
        try {
          const results = await invoke<DecodedBodyResult[]>("proto_frame_decoded_batch", { ids });
          // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- mutated by cleanup
          if (state.cancelled) return;
          for (const result of results) {
            if (result.decodedJson === null || result.decodedJson === undefined) {
              unavailable.add(result.id);
            } else {
              cache.set(result.id, result.decodedJson);
            }
          }
          bump();
        } catch {
          /* best-effort: retry this batch after the next frame poll */
          return;
        }
      }
    })();
    return () => {
      state.cancelled = true;
    };
  }, [enabled, protoFrames, bump]);

  return { decodedFor, put };
}
