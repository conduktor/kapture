import { useCallback, useEffect, useMemo, useRef, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import "./App.css";
import { TopBar } from "./components/TopBar";
import { MessageList } from "./components/MessageList";
import { LayerTree } from "./components/LayerTree";
import { HexDump } from "./components/HexDump";
import { StatusBar } from "./components/StatusBar";
import { SnippetsModal } from "./components/SnippetsModal";
import { McpModal } from "./components/McpModal";
import { ConnectionDialog } from "./components/ConnectionDialog";
import { UpdateBanner } from "./components/UpdateBanner";
import { FilterMenu, type FilterTarget } from "./components/FilterMenu";
import { activeMenuKeyFor, type MenuState } from "./lib/filterMenuState";
import { ProtoList } from "./components/ProtoList";
import { ProtoDetail } from "./components/ProtoDetail";
import { Splitter } from "./components/Splitter";
import {
  appendClause as appendProtoClause,
  encodeDecodedField,
  hasBodyTouchingPredicate,
  parseExpression as parseProtoExpression,
  removePredicate as removeProtoPredicate,
  serializeFilter as serializeProtoFilter,
  type DecodedFieldPair,
  type ProtoFilterChip,
  type ProtoFilterMode,
} from "./lib/protoFilter";
import { BrokersTab } from "./components/BrokersTab";
import { SessionActivityTab } from "./components/SessionActivityTab";
import type {
  CaptureStats,
  ConnectionState,
  KafkaMessage,
  KafkaMessageDetail,
  ProtoFrame,
  ProtoFrameDetail,
  ProxyStatus,
  ProxyStatusSummary,
} from "./types";
import { useSchemaResolvedListener } from "./lib/useSchemaResolvedListener";
import { readSplit } from "./lib/readSplit";

const DEFAULT_UPSTREAM = "localhost:19092";
const UI_MAX_MESSAGES = 5_000;
const FILTER_DEBOUNCE_MS = 250;

const INITIAL_STATS: CaptureStats = {
  totalReceived: 0,
  inBuffer: 0,
  bufferCapacity: 100_000,
  bufferBytes: 0,
  bufferByteCapacity: 256 * 1024 * 1024,
  drops: 0,
  throughputPerSec: 0,
  dropsPerSec: 0,
};

const INITIAL_CONNECTION: ConnectionState = {
  status: "disconnected",
  upstream: null,
  error: null,
  proxyStatus: null,
};

function App(): JSX.Element {
  const [filter, setFilter] = useState("");
  const [filterError, setFilterError] = useState<string | null>(null);
  const [messages, setMessages] = useState<KafkaMessage[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [connection, setConnection] = useState<ConnectionState>(INITIAL_CONNECTION);
  const [stats, setStats] = useState<CaptureStats>(INITIAL_STATS);
  // Manual UI freeze. Backend keeps capturing — proxy forwards
  // bytes, ring buffer keeps growing, MCP / inspect / snapshot all
  // still work. We just stop appending to the visible lists so the
  // user can investigate a row without it scrolling away.
  // Distinct from "Stop proxy", which tears down the TCP listener
  // and would surface as broker disconnects on the client side.
  const [paused, setPaused] = useState(false);
  const pausedRef = useRef(false);
  useEffect(() => {
    pausedRef.current = paused;
  }, [paused]);

  /** Toggle UI pause. On resume, replay the snapshot so the user
   *  catches up to the ring buffer state instead of seeing only
   *  whatever happens to land next. Keeps current selection — the
   *  detail panel doesn't blank out. */
  const toggleUiPaused = useCallback(
    (next: boolean): void => {
      // Backend pinning: snapshot the rings on pause, clear on resume.
      // Best-effort — a transient IPC failure shouldn't block the UI
      // toggle; the worst case is detail panels go empty for evicted
      // rows, same as before pinning existed.
      void invoke("set_capture_paused", { paused: next }).catch((err: unknown) => {
        console.warn("set_capture_paused failed", err);
      });
      setPaused(next);
      if (!next && connection.status === "connected") {
        void (async () => {
          try {
            const snap = await invoke<KafkaMessage[]>("snapshot");
            messagesRef.current = snap.slice(-UI_MAX_MESSAGES);
            setMessages(messagesRef.current);
          } catch (err) {
            console.warn("resume snapshot failed", err);
          }
        })();
      }
    },
    [connection.status],
  );
  // Lifted: snippets modal lives at App level so the backdrop covers
  // the full viewport and Escape/backdrop close work uniformly.
  const [snippetsOpen, setSnippetsOpen] = useState(false);
  const [mcpOpen, setMcpOpen] = useState(false);
  // Open the dialog automatically on first launch when nothing is
  // connected. Cancelling the dialog flips this to false and we stay in
  // the disconnected workspace; the user re-opens via the cluster pill.
  const [editing, setEditing] = useState(true);
  const [menu, setMenu] = useState<MenuState | null>(null);
  const [tab, setTab] = useState<"messages" | "protocol" | "brokers" | "session">("messages");
  // Lifted from StatusBar so the Brokers tab can read the same snapshot
  // without a second 1Hz poll. Null when no proxy is up (or before the
  // first tick lands).
  const [proxyStatusSummary, setProxyStatusSummary] = useState<ProxyStatusSummary | null>(null);
  const [protoFrames, setProtoFrames] = useState<ProtoFrame[]>([]);
  const [selectedFrameId, setSelectedFrameId] = useState<string | null>(null);
  const [selectedFrameDetail, setSelectedFrameDetail] = useState<ProtoFrameDetail | null>(null);
  // Top-textbox-driven filter for the protocol tab. The textbox is
  // the single source of truth: typing → re-parses → ProtoFilter →
  // applied to rows. The hover ⊕ popover and chip-bar removal both
  // mutate this text via parse/serialize round-trips. Kept separate
  // from the Wireshark-style DSL filter applied to messages — proto
  // frames don't go through the message DSL.
  const [protoFilterText, setProtoFilterText] = useState("");
  // Opportunistic LRU of fetched decoded bodies keyed by frame id.
  // Used by the decodedContains and decodedField predicates; frames
  // not in the cache are REJECTED by those predicates (hard filter
  // semantics — see `applyFilter` in protoFilter.ts). Cache stores
  // the typed JSON value (parsed from the backend's `decodedJson`).
  // Bounded so the chip-based filter doesn't pin unbounded memory.
  const decodedCacheRef = useRef<Map<string, unknown>>(new Map());
  // Sized to cover the proto ring buffer (5000 frames). The earlier
  // 50-entry cap turned the decodedContains hard-filter into a
  // ghost: the prefetch warmed up to 500 entries, but the next
  // detail-fetch eviction trimmed back to 50, so >99 % of frames
  // had `decoded === undefined` and got rejected. Bumping to 5000
  // keeps memory bounded (each typed JSON body is ~0.5-5 KiB → max
  // ~25 MiB worst case, acceptable on the inspector workstation).
  const DECODED_CACHE_MAX = 5000;
  // Pane splits, expressed as fr ratios. Messages tab is stacked
  // top-to-bottom (two vertical splits between MessageList/LayerTree and
  // LayerTree/HexDump). Protocol tab is side-by-side (one horizontal
  // split between ProtoList/ProtoDetail). Adjusted via Splitter drag
  // handles. Constrained to [0.05, 0.95] to keep panes minimally
  // visible. The protocol split is persisted to localStorage so the
  // user's preferred list/detail width survives reloads.
  const PROTO_SPLIT_KEY = "kapture.proto.splitRatio";
  const MSG_SPLIT_KEY = "kapture.msg.splitRatio";
  const MSG_DETAIL_SPLIT_KEY = "kapture.msg.detailSplitRatio";
  // Messages: list on the left, [LayerTree above HexDump] on the right
  // — same shape as Protocol (mimics the Wireshark inspector layout).
  // `msgSplit` is the list/detail horizontal split; `msgDetailSplit`
  // is the LayerTree/HexDump vertical split inside the right pane.
  const [msgSplit, setMsgSplit] = useState<number>(() => readSplit(MSG_SPLIT_KEY, 0.4));
  const [msgDetailSplit, setMsgDetailSplit] = useState<number>(() =>
    readSplit(MSG_DETAIL_SPLIT_KEY, 0.55),
  );
  const [protoSplit, setProtoSplit] = useState<number>(() => readSplit(PROTO_SPLIT_KEY, 0.45));
  useEffect(() => {
    try {
      window.localStorage.setItem(PROTO_SPLIT_KEY, String(protoSplit));
    } catch {
      /* ignore quota / unavailable */
    }
  }, [protoSplit]);
  useEffect(() => {
    try {
      window.localStorage.setItem(MSG_SPLIT_KEY, String(msgSplit));
    } catch {
      /* ignore */
    }
  }, [msgSplit]);
  useEffect(() => {
    try {
      window.localStorage.setItem(MSG_DETAIL_SPLIT_KEY, String(msgDetailSplit));
    } catch {
      /* ignore */
    }
  }, [msgDetailSplit]);
  const panesRef = useRef<HTMLDivElement | null>(null);
  const messagesRef = useRef<KafkaMessage[]>([]);
  // Monotonic generation. Each filter change bumps it; only the latest
  // generation is allowed to commit set_filter / snapshot results, so a
  // slow backend round-trip from a stale filter can't overwrite the UI.
  const filterGenRef = useRef(0);

  // Subscribe to live events while connected.
  //
  // Messages arrive one-per-event. Naively calling setMessages in each
  // event handler does an O(n) concat + splice and a React reconcile
  // per message — at 10 msg/s with 5 k rows that's ~50 k array ops/s
  // and 10 list re-renders/s. We batch through rAF so the renderer
  // sees at most one update per frame regardless of event rate.
  useEffect(() => {
    if (connection.status !== "connected") {
      return;
    }
    let messageUnlisten: UnlistenFn | null = null;
    let statsUnlisten: UnlistenFn | null = null;
    let cancelled = false;
    const pending: KafkaMessage[] = [];
    let rafScheduled = false;

    const flush = (): void => {
      rafScheduled = false;
      if (cancelled || pending.length === 0) {
        return;
      }
      const drained = pending.splice(0, pending.length);
      const next = messagesRef.current.concat(drained);
      if (next.length > UI_MAX_MESSAGES) {
        next.splice(0, next.length - UI_MAX_MESSAGES);
      }
      messagesRef.current = next;
      setMessages(next);
    };

    void (async () => {
      // Backend coalesces messages into batches (50 ms / 256 max).
      // Each event payload is `KafkaMessage[]`; we still rAF-batch on
      // top because multiple batches may land in the same frame under
      // sustained load. When the user paused the UI, we drop incoming
      // events on the floor — the ring buffer keeps capturing so
      // nothing is lost; resume re-syncs via `snapshot`.
      messageUnlisten = await listen<KafkaMessage[]>("kapture:messages", (event) => {
        if (pausedRef.current) {
          return;
        }
        for (const m of event.payload) {
          pending.push(m);
        }
        if (!rafScheduled) {
          rafScheduled = true;
          window.requestAnimationFrame(flush);
        }
      });
      statsUnlisten = await listen<CaptureStats>("kapture:stats", (event) => {
        if (!cancelled) {
          setStats(event.payload);
        }
      });
    })();

    return () => {
      cancelled = true;
      if (messageUnlisten) {
        messageUnlisten();
      }
      if (statsUnlisten) {
        statsUnlisten();
      }
    };
  }, [connection.status]);

  // Poll the protocol-frame ring buffer while connected. Cheap (a single
  // command + a small JSON), no event stream because frames fire from
  // the broker thread at very high rates and a per-frame Tauri event
  // would flood the IPC channel. 1 s is plenty for a humans-eyeballing
  // view; the buffer is capped at 4000 frames backend-side.
  useEffect(() => {
    if (connection.status !== "connected") {
      // We don't reset the buffer here — that would be a setState inside
      // an effect for a state we don't subscribe to. handleDisconnect
      // clears it explicitly when the user stops the capture.
      return;
    }
    let cancelled = false;
    const tick = async (): Promise<void> => {
      if (pausedRef.current) {
        return;
      }
      try {
        const frames = await invoke<ProtoFrame[]>("proto_frames", { limit: 5000 });
        if (!cancelled) {
          setProtoFrames(frames);
        }
      } catch (err) {
        if (!cancelled) {
          console.warn("proto_frames poll failed", err);
        }
      }
    };
    void tick();
    const interval = window.setInterval(() => {
      void tick();
    }, 1000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [connection.status]);

  // 1Hz proxy_status poll. Lifted from StatusBar so both StatusBar and
  // BrokersTab consume one snapshot — no double polling. The
  // disconnect handler clears the summary explicitly (lint forbids a
  // setState call in the effect body itself).
  useEffect(() => {
    if (connection.status !== "connected") {
      return;
    }
    let cancelled = false;
    const tick = async (): Promise<void> => {
      try {
        const next = await invoke<ProxyStatusSummary>("proxy_status");
        if (!cancelled) {
          setProxyStatusSummary(next);
        }
      } catch {
        /* command may transiently fail during connect/disconnect */
      }
    };
    void tick();
    const id = window.setInterval(() => {
      void tick();
    }, 1000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [connection.status]);

  // Fetch the selected frame's full payload (hex + decoded body) lazily.
  // The list poll above only carries summaries to keep IPC small; the
  // heavy fields land here, scoped to one frame at a time.
  // useRef-based cancellation flag — strict TS would otherwise narrow a
  // closed-over `let cancelled = false` to always-false inside the
  // async closure.
  const detailCancelRef = useRef(false);
  useEffect(() => {
    if (selectedFrameId === null) {
      return;
    }
    detailCancelRef.current = false;
    const id = selectedFrameId;
    void (async () => {
      try {
        const detail = await invoke<ProtoFrameDetail | null>("proto_frame_detail", { id });
        if (!detailCancelRef.current) {
          setSelectedFrameDetail(detail);
          // Seed the decoded LRU. Map iteration order is insertion
          // order, so re-inserting (delete + set) bumps the entry to
          // the most-recent slot; oldest is the first key.
          if (detail?.decodedJson !== undefined && detail.decodedJson !== null) {
            const cache = decodedCacheRef.current;
            cache.delete(detail.id);
            cache.set(detail.id, detail.decodedJson);
            while (cache.size > DECODED_CACHE_MAX) {
              const oldest = cache.keys().next();
              if (oldest.done === true) {
                break;
              }
              cache.delete(oldest.value);
            }
          }
        }
      } catch (err) {
        if (!detailCancelRef.current) {
          console.warn("proto_frame_detail failed", err);
          setSelectedFrameDetail(null);
        }
      }
    })();
    return () => {
      detailCancelRef.current = true;
    };
  }, [selectedFrameId]);

  // Debounced filter sync to backend, with a generation guard so concurrent
  // filter edits cannot let a stale snapshot overwrite a fresher view.
  useEffect(() => {
    const handle = setTimeout(() => {
      filterGenRef.current += 1;
      const myGen = filterGenRef.current;
      void (async () => {
        try {
          await invoke("set_filter", { expression: filter });
          if (myGen !== filterGenRef.current) {
            return;
          }
          setFilterError(null);
          if (connection.status !== "connected") {
            return;
          }
          // Capture any live messages that arrived before snapshot is
          // requested — they're emitted under the new filter (already set
          // backend-side) and may be missed if the backend snapshot cut
          // before they were ring-buffered server-side.
          const inFlightBefore = messagesRef.current.slice();
          let snap: KafkaMessage[];
          try {
            snap = await invoke<KafkaMessage[]>("snapshot");
          } catch (err) {
            console.error("snapshot failed", err);
            return;
          }
          if (myGen !== filterGenRef.current) {
            return;
          }
          // Merge: the snapshot is authoritative for the past; anything
          // received locally after `inFlightBefore` was captured is
          // appended (deduplicated by id). This protects against the
          // narrow race where a live event lands between snapshot read
          // and snapshot delivery.
          const arrivedDuring = messagesRef.current.slice(inFlightBefore.length);
          const seen = new Set(snap.map((m) => m.id));
          const extras = arrivedDuring.filter((m) => !seen.has(m.id));
          const merged = snap.concat(extras).slice(-UI_MAX_MESSAGES);
          messagesRef.current = merged;
          setMessages(merged);
        } catch (err) {
          if (myGen !== filterGenRef.current) {
            return;
          }
          const message = err instanceof Error ? err.message : String(err);
          setFilterError(message);
        }
      })();
    }, FILTER_DEBOUNCE_MS);
    return () => {
      clearTimeout(handle);
    };
  }, [filter, connection.status]);

  // Lazily-fetched full body of the selected message. The live event
  // and the snapshot command both ship summaries (no payload, no
  // rawHex, no headers) — measured at ~80× IPC reduction. When the
  // user picks a row we fetch the heavy fields once via
  // `inspect_message_by_id`. `null` means "no row selected" or
  // "still loading"; the LayerTree / HexDump panels render a muted
  // placeholder in that state.
  const [selectedDetail, setSelectedDetail] = useState<KafkaMessageDetail | null>(null);
  // Mirror the latest detail in a ref so the schema-resolved listener
  // (defined upstream in another effect's closure) can patch a
  // currently-selected record without subscribing to selectedDetail
  // and causing a relisten / re-fetch storm.
  const selectedDetailRef = useRef<KafkaMessageDetail | null>(null);
  useEffect(() => {
    selectedDetailRef.current = selectedDetail;
  }, [selectedDetail]);
  const messageDetailCancelRef = useRef(false);
  useEffect(() => {
    if (selectedId === null) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- intentional: clear stale detail on deselection
      setSelectedDetail(null);
      return;
    }
    messageDetailCancelRef.current = false;
    const id = selectedId;
    void (async () => {
      try {
        const detail = await invoke<KafkaMessageDetail | null>("inspect_message_by_id", {
          id,
        });
        if (!messageDetailCancelRef.current) {
          setSelectedDetail(detail);
        }
      } catch (err) {
        if (!messageDetailCancelRef.current) {
          console.warn("inspect_message_by_id failed", err);
          setSelectedDetail(null);
        }
      }
    })();
    return () => {
      messageDetailCancelRef.current = true;
    };
  }, [selectedId]);

  useSchemaResolvedListener({
    enabled: connection.status === "connected",
    pausedRef,
    messagesRef,
    setMessages,
    selectedDetailRef,
    setSelectedDetail,
  });

  // Proxy lifecycle: ConnectionDialog drives `start_proxy` and pushes the
  // result back via these callbacks. The dialog is the only entry point
  // since client (rdkafka) mode was removed.
  const handleProxyStarting = (): void => {
    // eslint-disable-next-line react-hooks/immutability -- ref is our source of truth for the rAF batcher; state mirrors it
    messagesRef.current = [];
    setMessages([]);
    setSelectedId(null);
    setStats(INITIAL_STATS);
    setConnection((prev) => ({
      ...prev,
      status: "connecting",
      error: null,
      proxyStatus: null,
    }));
    setEditing(false);
  };

  const handleProxyStarted = (status: ProxyStatus): void => {
    setConnection({
      status: "connected",
      upstream: status.upstream,
      error: null,
      proxyStatus: status,
    });
  };

  const handleProxyError = (message: string): void => {
    setConnection((prev) => ({
      ...prev,
      status: "error",
      error: message,
      proxyStatus: null,
    }));
  };

  const handleDisconnect = (): void => {
    void (async () => {
      try {
        await invoke("stop_proxy");
      } catch (err) {
        console.error("stop_proxy failed", err);
      } finally {
        setConnection(INITIAL_CONNECTION);
        // Brokers tab is hidden when disconnected — fall back to messages
        // so the user lands on a visible tab next time they connect.
        setTab((current) => (current === "brokers" ? "messages" : current));
        // Drop the stale snapshot so a quick reconnect doesn't flash the
        // previous broker list before the next poll tick lands.
        setProxyStatusSummary(null);
      }
    })();
  };

  const handleClear = (): void => {
    // Wipes BOTH the message ring buffer AND the protocol frame ring
    // on the backend, so a "Clear" reset gives the user a fully empty
    // workspace before testing a new scenario. Local state caches
    // (messages, protoFrames, selection) reset alongside.
    void invoke("clear_capture").catch((err: unknown) => {
      console.error("clear failed", err);
    });
    // eslint-disable-next-line react-hooks/immutability -- see handleProxyStarting
    messagesRef.current = [];
    setMessages([]);
    setProtoFrames([]);
    setSelectedId(null);
    setSelectedFrameId(null);
  };

  const applyFilter = useCallback((expression: string): void => {
    setFilter(expression);
  }, []);

  const openFilterMenu = useCallback(
    (
      target: FilterTarget,
      position: { x: number; y: number },
      anchorId: string | null = null,
    ): void => {
      setMenu({ target, position, anchorId, scope: "messages" });
    },
    [],
  );
  const openProtoFilterMenu = useCallback(
    (
      target: FilterTarget,
      position: { x: number; y: number },
      anchorId: string | null = null,
    ): void => {
      setMenu({ target, position, anchorId, scope: "protocol" });
    },
    [],
  );

  // Always allow the user to escape the dialog. Real-world traps this
  // unblocks:
  //   1. start_proxy fails with "AlreadyProxying" (zombie slot from a
  //      previous run): cancel fires `stop_proxy` best-effort to clear
  //      the slot, then drops the dialog into the disconnected state.
  //   2. User just changed their mind: cancel returns them to the
  //      previous workspace (disconnected → empty, connected → still
  //      capturing).
  const cancelDialog = (): void => {
    if (connection.status === "error") {
      void invoke("stop_proxy").catch(() => {
        /* best-effort cleanup of zombie proxy slot */
      });
      setConnection(INITIAL_CONNECTION);
    }
    setEditing(false);
  };

  // Dialog is visible whenever the user is editing OR we're in the middle
  // of a connect attempt. Cancel always returns to the workspace.
  const showDialog = editing || connection.status === "connecting" || connection.status === "error";
  const isEditing = editing && connection.status === "connected";
  const initialPrefill = isEditing
    ? { upstream: connection.upstream ?? DEFAULT_UPSTREAM }
    : undefined;

  // Parse the protocol-tab text on every keystroke. Pure client-side,
  // cheap (≤ a few clauses), so no debounce. On parse error we surface
  // EMPTY_PROTO_FILTER (= no filtering, all rows visible) — the least
  // surprising outcome for a typo mid-edit. The error message renders
  // inline near the textbox.
  const protoParsed = useMemo(() => parseProtoExpression(protoFilterText), [protoFilterText]);
  const protoFilter = protoParsed.filter;
  const protoFilterError = protoParsed.error;

  // Hard-filter pre-fetch: when ANY body-touching predicate is active
  // (`decodedContains` OR a path-aware `decodedField`), walk the
  // visible frames and fetch any whose decoded body isn't cached yet.
  // Without this the list would near-empty as the predicate rejects
  // every uncached row — particularly visible while paused, since the
  // user freezes a 5000-frame snapshot they couldn't possibly have
  // clicked through individually.
  const decodedFiltersActive = hasBodyTouchingPredicate(protoFilter);
  useEffect(() => {
    if (!decodedFiltersActive || protoFrames.length === 0) {
      return;
    }
    // Holder so TS doesn't narrow `cancelled` to `false` at the
    // closure capture site (cleanup mutates it via the holder, which
    // the type checker treats as opaque).
    const state = { cancelled: false };
    const cache = decodedCacheRef.current;
    // Drop orphan cache entries: ids that have been evicted from the
    // ring buffer no longer correspond to a visible frame, so they're
    // dead memory. Keeps the cache bounded to the ring size in the
    // long run without an explicit cap.
    const liveIds = new Set(protoFrames.map((f) => f.id));
    for (const id of Array.from(cache.keys())) {
      if (!liveIds.has(id)) cache.delete(id);
    }
    // Cap concurrent inflight fetches so we don't flood IPC. Frames
    // are processed newest-first to prioritise what the user is most
    // likely looking at.
    // No upper bound: a `decodedContains` predicate is a hard
    // filter that rejects every frame without a cached decoded
    // body. Capping the prefetch (an earlier 500-frame slice)
    // left frames 501..5000 invisible no matter what — defeating
    // the filter. Walk the entire ring; concurrency caps IPC
    // pressure (~625 sequential round-trips at 8-wide → seconds).
    const queue = protoFrames
      .slice()
      .reverse()
      .filter((f) => !cache.has(f.id));
    const concurrency = 8;
    void (async () => {
      const workers = Array.from({ length: concurrency }, async () => {
        while (!state.cancelled) {
          const next = queue.shift();
          if (!next) return;
          try {
            const detail = await invoke<ProtoFrameDetail | null>("proto_frame_detail", {
              id: next.id,
            });
            // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- mutated by cleanup return
            if (state.cancelled) return;
            if (detail?.decodedJson !== undefined && detail.decodedJson !== null) {
              cache.set(detail.id, detail.decodedJson);
            }
          } catch {
            /* best effort — skip on transient errors */
          }
        }
      });
      await Promise.all(workers);
    })();
    return () => {
      state.cancelled = true;
    };
  }, [decodedFiltersActive, protoFrames]);

  // Filter bar wiring — Messages tab uses the Wireshark-style DSL
  // (validated server-side); Protocol tab uses the local DSL parsed
  // from the same input box.
  const filterValue = tab === "messages" ? filter : protoFilterText;
  const filterPlaceholder =
    tab === "messages"
      ? 'topic =~ "orders.*" && headers.tenant == "acme" && payload.amount > 1000'
      : 'apiName == "Fetch" && conn != 42 && corrId == 7';
  const onFilterChange = (next: string): void => {
    if (tab === "messages") {
      setFilter(next);
    } else {
      setProtoFilterText(next);
    }
  };
  const topFilterError = tab === "messages" ? filterError : protoFilterError;

  const decodedFor = useCallback((id: string): unknown => decodedCacheRef.current.get(id), []);

  // Click on a decoded leaf appends a `<name> == "<value>"` clause
  // (bareword field name, no `field` keyword). The matcher walks the
  // JSON body looking for any object exposing that property —
  // auto-adapts to whatever schema the frame carries.
  const onAddDecodedFilter = useCallback((pair: DecodedFieldPair, mode: ProtoFilterMode): void => {
    setProtoFilterText((prev) =>
      appendProtoClause(prev, "decodedField", encodeDecodedField(pair), mode),
    );
  }, []);

  // Hover-cell click: TOGGLE the (kind, value, mode) predicate.
  //  - Already present in this mode → remove it (the user is undoing).
  //  - Not present → add it. addPredicate() removes any opposite-mode
  //    predicate for the same value automatically (a value can't sit
  //    in both include and exclude — that would be unsatisfiable).
  // The textbox is then re-serialised from the updated filter so the
  // top-of-page DSL stays the canonical source of truth.
  // Chip removal: parse current text → drop the predicate → serialize
  // back. Keeps the canonical form intact and means the textbox always
  // mirrors the chips exactly. The chip carries `(kind, value)` with
  // the value already typed against its kind (KindMap[K]) — the cast
  // re-aligns the call back into the generic on a per-chip basis.
  const onRemoveProtoChip = useCallback((chip: ProtoFilterChip): void => {
    setProtoFilterText((prev) => {
      const parsed = parseProtoExpression(prev);
      const next = removeProtoPredicate(
        parsed.filter,
        chip.kind,
        chip.value as string & number,
        chip.mode,
      );
      return serializeProtoFilter(next);
    });
  }, []);

  const onClearProtoFilter = useCallback((): void => {
    setProtoFilterText("");
  }, []);

  // Backlink from a captured Message → its originating Fetch frame.
  // Switches to the Protocol tab and selects the matching frame in the
  // Splitter callbacks: convert pixel deltas into ratio deltas. The
  // axis depends on the pane container's split direction — vertical
  // splits use height, horizontal use width.
  const clamp = (x: number, lo: number, hi: number): number => Math.max(lo, Math.min(hi, x));
  const adjustSplit = (
    setter: (next: (prev: number) => number) => void,
    deltaPx: number,
    axis: "y" | "x" = "y",
  ): void => {
    const el = panesRef.current;
    const extent = (axis === "x" ? el?.offsetWidth : el?.offsetHeight) ?? 1;
    setter((prev) => clamp(prev + deltaPx / extent, 0.05, 0.95));
  };

  return (
    <div className="app">
      <UpdateBanner />
      <TopBar
        filter={filterValue}
        onFilterChange={onFilterChange}
        filterError={topFilterError}
        filterPlaceholder={filterPlaceholder}
        capturing={connection.status === "connected"}
        onToggleCapture={() => {
          if (connection.status === "connected") {
            handleDisconnect();
          } else {
            // Re-open the dialog, which auto-applies the last-used
            // profile from localStorage. One extra click vs. firing
            // `start_proxy` directly, but the dialog is the single
            // source of truth for profile resolution + edits-on-Start
            // — duplicating that wiring would be a fork waiting to
            // drift.
            setEditing(true);
          }
        }}
        onClear={handleClear}
        proxyStatus={connection.proxyStatus}
        onEdit={() => {
          setEditing(true);
        }}
        onOpenSnippets={() => {
          setSnippetsOpen(true);
        }}
        onOpenMcp={() => {
          setMcpOpen(true);
        }}
        paused={paused}
        onTogglePaused={toggleUiPaused}
      />
      <main className="layout">
        <div className="layout__main">
          <div className="tabs" role="tablist">
            <button
              type="button"
              role="tab"
              aria-selected={tab === "messages"}
              className={`tabs__tab${tab === "messages" ? " is-active" : ""}`}
              onClick={() => {
                setTab("messages");
              }}
            >
              Messages <span className="tabs__count">({messages.length})</span>
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={tab === "protocol"}
              className={`tabs__tab${tab === "protocol" ? " is-active" : ""}`}
              onClick={() => {
                setTab("protocol");
              }}
              title="Kafka protocol frames observed at the wire level"
            >
              Protocol <span className="tabs__count">({protoFrames.length})</span>
            </button>
            {connection.status === "connected" ? (
              <button
                type="button"
                role="tab"
                aria-selected={tab === "brokers"}
                className={`tabs__tab${tab === "brokers" ? " is-active" : ""}`}
                onClick={() => {
                  setTab("brokers");
                }}
                title="Per-broker proxy port mappings"
              >
                Brokers{" "}
                <span className="tabs__count">
                  ({proxyStatusSummary?.brokerMappings.length ?? 0})
                </span>
              </button>
            ) : null}
            <button
              type="button"
              role="tab"
              aria-selected={tab === "session"}
              className={`tabs__tab${tab === "session" ? " is-active" : ""}`}
              onClick={() => {
                setTab("session");
              }}
              title="Aggregated session activity — topics, groups, errors"
            >
              Session
            </button>
          </div>
          {tab === "session" ? (
            <SessionActivityTab
              protoFrames={protoFrames}
              onJumpToProtocol={(value, frameId) => {
                setProtoFilterText((prev) =>
                  appendProtoClause(prev, "decodedContains", value, "include"),
                );
                if (frameId !== undefined) {
                  setSelectedFrameId(frameId);
                }
                setTab("protocol");
              }}
            />
          ) : tab === "brokers" ? (
            <BrokersTab proxyStatus={proxyStatusSummary} protoFrames={protoFrames} />
          ) : tab === "messages" ? (
            <div
              ref={panesRef}
              className="layout__panes layout__panes--horizontal"
              style={{ gridTemplateColumns: `${msgSplit}fr 6px ${1 - msgSplit}fr` }}
            >
              <MessageList
                messages={messages}
                selectedId={selectedId}
                onSelect={setSelectedId}
                onOpenFilterMenu={openFilterMenu}
                activeMenuKey={activeMenuKeyFor(menu, "messages")}
              />
              <Splitter
                orientation="horizontal"
                onResize={(dx) => {
                  adjustSplit(setMsgSplit, dx, "x");
                }}
              />
              <div
                className="layout__msg-detail"
                style={{
                  gridTemplateRows: `${msgDetailSplit}fr 6px ${1 - msgDetailSplit}fr`,
                }}
              >
                <LayerTree
                  message={selectedDetail}
                  onOpenFilterMenu={openFilterMenu}
                  activeMenuKey={activeMenuKeyFor(menu, "messages")}
                />
                <Splitter
                  onResize={(dy) => {
                    adjustSplit(setMsgDetailSplit, dy);
                  }}
                />
                <HexDump message={selectedDetail} />
              </div>
            </div>
          ) : (
            <div
              ref={panesRef}
              className="layout__panes layout__panes--horizontal"
              style={{ gridTemplateColumns: `${protoSplit}fr 6px ${1 - protoSplit}fr` }}
            >
              <ProtoList
                frames={protoFrames}
                selectedId={selectedFrameId}
                onSelect={setSelectedFrameId}
                filter={protoFilter}
                onRemoveChip={onRemoveProtoChip}
                onClearFilter={onClearProtoFilter}
                decodedFor={decodedFor}
                onOpenFilterMenu={openProtoFilterMenu}
                activeMenuKey={activeMenuKeyFor(menu, "protocol")}
              />
              <Splitter
                orientation="horizontal"
                onResize={(dx) => {
                  adjustSplit(setProtoSplit, dx, "x");
                }}
              />
              <ProtoDetail
                frame={
                  // Only show the detail when it matches the current
                  // selection — avoids flashing a stale frame's bytes
                  // during the brief window before the new fetch lands.
                  selectedFrameDetail?.id === selectedFrameId ? selectedFrameDetail : null
                }
                onAddDecodedFilter={onAddDecodedFilter}
              />
            </div>
          )}
        </div>
      </main>
      <StatusBar connection={connection} stats={stats} proxy={proxyStatusSummary} />
      {snippetsOpen && connection.status === "connected" && connection.proxyStatus !== null ? (
        <SnippetsModal
          listenAddr={connection.proxyStatus.listenAddr}
          onClose={() => {
            setSnippetsOpen(false);
          }}
        />
      ) : null}
      {mcpOpen ? (
        <McpModal
          onClose={() => {
            setMcpOpen(false);
          }}
        />
      ) : null}
      {showDialog ? (
        <ConnectionDialog
          defaultUpstream={DEFAULT_UPSTREAM}
          initial={initialPrefill}
          isEditing={isEditing}
          onProxyStarting={handleProxyStarting}
          onProxyStarted={handleProxyStarted}
          onProxyError={handleProxyError}
          onCancel={cancelDialog}
          pending={connection.status === "connecting"}
          error={connection.error}
        />
      ) : null}
      {menu ? (
        <FilterMenu
          target={menu.target}
          position={menu.position}
          currentFilter={menu.scope === "protocol" ? protoFilterText : filter}
          onApply={menu.scope === "protocol" ? setProtoFilterText : applyFilter}
          onClose={() => {
            setMenu(null);
          }}
        />
      ) : null}
    </div>
  );
}

export default App;
