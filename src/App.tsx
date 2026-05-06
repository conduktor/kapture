import { useCallback, useEffect, useMemo, useRef, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import "./App.css";
import { TopBar } from "./components/TopBar";
import { MessageList } from "./components/MessageList";
import { LayerTree } from "./components/LayerTree";
import { HexDump } from "./components/HexDump";
import { SidePanel } from "./components/SidePanel";
import { ConnectionDialog } from "./components/ConnectionDialog";
import { UpdateBanner } from "./components/UpdateBanner";
import { FilterMenu, type FilterTarget } from "./components/FilterMenu";
import { ProtoList } from "./components/ProtoList";
import { ProtoDetail } from "./components/ProtoDetail";
import { Splitter } from "./components/Splitter";
import {
  EMPTY_PROTO_FILTER,
  addPredicate as addProtoPredicate,
  type ProtoFilter,
  type ProtoFilterMode,
} from "./lib/protoFilter";
import type {
  AppInfo,
  CaptureStats,
  ConnectionState,
  KafkaMessage,
  ProtoFrame,
  ProtoFrameDetail,
  ProxyStatus,
} from "./types";

interface MenuState {
  target: FilterTarget;
  position: { x: number; y: number };
}

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
  const [appInfo, setAppInfo] = useState<AppInfo | null>(null);
  const [connection, setConnection] = useState<ConnectionState>(INITIAL_CONNECTION);
  const [stats, setStats] = useState<CaptureStats>(INITIAL_STATS);
  // Open the dialog automatically on first launch when nothing is
  // connected. Cancelling the dialog flips this to false and we stay in
  // the disconnected workspace; the user re-opens via the cluster pill.
  const [editing, setEditing] = useState(true);
  const [menu, setMenu] = useState<MenuState | null>(null);
  const [tab, setTab] = useState<"messages" | "protocol">("messages");
  const [protoFrames, setProtoFrames] = useState<ProtoFrame[]>([]);
  const [selectedFrameId, setSelectedFrameId] = useState<string | null>(null);
  const [selectedFrameDetail, setSelectedFrameDetail] = useState<ProtoFrameDetail | null>(null);
  // Chip-based filter for the protocol tab. Built from hover ⊕/⊖
  // affordances on list cells and decoded leaf values. Kept separate
  // from the Wireshark-style DSL filter applied to messages — proto
  // frames don't go through the message DSL.
  const [protoFilter, setProtoFilter] = useState<ProtoFilter>(EMPTY_PROTO_FILTER);
  // Opportunistic LRU of fetched decoded bodies keyed by frame id.
  // Used by the decodedContains predicate; frames not in the cache
  // bypass the predicate (over-include rather than over-exclude).
  // Bounded so the chip-based filter doesn't pin unbounded memory.
  const decodedCacheRef = useRef<Map<string, string>>(new Map());
  const DECODED_CACHE_MAX = 50;
  // Vertical splits, expressed as fr ratios. Two splits in messages tab
  // (between MessageList/LayerTree and LayerTree/HexDump), one in
  // protocol (between ProtoList/ProtoDetail). Adjusted via Splitter
  // drag handles. Constrained to [0.05, 0.95] to keep panes minimally
  // visible.
  const [msgSplitTop, setMsgSplitTop] = useState(0.4);
  const [msgSplitMid, setMsgSplitMid] = useState(0.7);
  const [protoSplit, setProtoSplit] = useState(0.55);
  const panesRef = useRef<HTMLDivElement | null>(null);
  const messagesRef = useRef<KafkaMessage[]>([]);
  // Monotonic generation. Each filter change bumps it; only the latest
  // generation is allowed to commit set_filter / snapshot results, so a
  // slow backend round-trip from a stale filter can't overwrite the UI.
  const filterGenRef = useRef(0);

  // Initial: fetch app info
  useEffect(() => {
    void (async () => {
      try {
        const info = await invoke<AppInfo>("app_info");
        setAppInfo(info);
      } catch (error) {
        console.error("ipc app_info failed", error);
      }
    })();
  }, []);

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
      messageUnlisten = await listen<KafkaMessage>("kapture:message", (event) => {
        pending.push(event.payload);
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
      try {
        const frames = await invoke<ProtoFrame[]>("proto_frames", { limit: 2000 });
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
          if (detail?.decoded != null) {
            const cache = decodedCacheRef.current;
            cache.delete(detail.id);
            cache.set(detail.id, detail.decoded);
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

  const selected = useMemo(
    () => messages.find((m) => m.id === selectedId) ?? null,
    [messages, selectedId],
  );

  // Proxy lifecycle: ConnectionDialog drives `start_proxy` and pushes the
  // result back via these callbacks. The dialog is the only entry point
  // since client (rdkafka) mode was removed.
  const handleProxyStarting = (): void => {
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
      }
    })();
  };

  const handleClear = (): void => {
    void invoke("clear_buffer").catch((err: unknown) => {
      console.error("clear failed", err);
    });
    messagesRef.current = [];
    setMessages([]);
    setSelectedId(null);
  };

  const applyFilter = useCallback((expression: string): void => {
    setFilter(expression);
  }, []);

  const openFilterMenu = useCallback(
    (target: FilterTarget, position: { x: number; y: number }): void => {
      setMenu({ target, position });
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

  // Filter bar wiring — Messages tab uses the Wireshark-style DSL
  // (validated server-side); Protocol tab disables the top input and
  // exposes filters via in-row hover ⊕/⊖ chips instead. The chip bar
  // lives inside ProtoList.
  const filterValue = tab === "messages" ? filter : "";
  const filterPlaceholder =
    tab === "messages"
      ? 'topic =~ "orders.*" && headers.tenant == "acme" && payload.amount > 1000'
      : "Hover any cell or decoded field and click ⊕ to filter";
  const onFilterChange = (next: string): void => {
    if (tab === "messages") {
      setFilter(next);
    }
    // Protocol tab: top filter input is read-only / informational. The
    // chip-based filter is driven by hover ⊕/⊖ buttons.
  };

  const decodedFor = useCallback(
    (id: string): string | undefined => decodedCacheRef.current.get(id),
    [],
  );

  const onAddDecodedFilter = useCallback((substring: string, mode: ProtoFilterMode): void => {
    setProtoFilter((prev) => addProtoPredicate(prev, "decodedContains", substring, mode));
  }, []);

  // Backlink from a captured Message → its originating Fetch frame.
  // Switches to the Protocol tab and selects the matching frame in the
  // ring buffer summary cache. The match is `(connectionId, corrId,
  // direction === "recv")` because the Fetch *response* is what
  // actually carried the records — the request never has them.
  // Logs a console warning if the frame has aged out (4000-frame ring
  // buffer); the selection still happens (`null`) so the Protocol tab
  // surfaces an empty detail pane rather than silently doing nothing.
  const handleJumpToFetchFrame = useCallback(
    (connectionId: number, corrId: number): void => {
      setTab("protocol");
      const match = protoFrames.find(
        (f) => f.connectionId === connectionId && f.corrId === corrId && f.direction === "recv",
      );
      if (match) {
        setSelectedFrameId(match.id);
      } else {
        console.warn(
          `Fetch frame (conn=${String(connectionId)}, corr=${String(corrId)}) not found in ring buffer — likely aged out.`,
        );
        setSelectedFrameId(null);
      }
    },
    [protoFrames],
  );

  // Splitter callbacks: convert pixel deltas into ratio deltas.
  const clamp = (x: number, lo: number, hi: number): number => Math.max(lo, Math.min(hi, x));
  const adjustSplit = (setter: (next: (prev: number) => number) => void, deltaPx: number): void => {
    const h = panesRef.current?.offsetHeight ?? 1;
    setter((prev) => clamp(prev + deltaPx / h, 0.05, 0.95));
  };

  return (
    <div className="app">
      <UpdateBanner />
      <TopBar
        filter={filterValue}
        onFilterChange={onFilterChange}
        filterError={tab === "messages" ? filterError : null}
        filterPlaceholder={filterPlaceholder}
        capturing={connection.status === "connected"}
        onToggleCapture={() => {
          if (connection.status === "connected") {
            handleDisconnect();
          }
        }}
        onClear={handleClear}
        proxyStatus={connection.proxyStatus}
        onEdit={() => {
          setEditing(true);
        }}
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
          </div>
          {tab === "messages" ? (
            <div
              ref={panesRef}
              className="layout__panes"
              style={{
                gridTemplateRows: `${msgSplitTop}fr 6px ${
                  msgSplitMid - msgSplitTop
                }fr 6px ${1 - msgSplitMid}fr`,
              }}
            >
              <MessageList
                messages={messages}
                selectedId={selectedId}
                onSelect={setSelectedId}
                onOpenFilterMenu={openFilterMenu}
                onJumpToFetchFrame={handleJumpToFetchFrame}
              />
              <Splitter
                onResize={(dy) => {
                  adjustSplit(setMsgSplitTop, dy);
                }}
              />
              <LayerTree message={selected} onOpenFilterMenu={openFilterMenu} />
              <Splitter
                onResize={(dy) => {
                  adjustSplit(setMsgSplitMid, dy);
                }}
              />
              <HexDump message={selected} />
            </div>
          ) : (
            <div
              ref={panesRef}
              className="layout__panes"
              style={{ gridTemplateRows: `${protoSplit}fr 6px ${1 - protoSplit}fr` }}
            >
              <ProtoList
                frames={protoFrames}
                selectedId={selectedFrameId}
                onSelect={setSelectedFrameId}
                filter={protoFilter}
                onFilterChange={setProtoFilter}
                decodedFor={decodedFor}
              />
              <Splitter
                onResize={(dy) => {
                  adjustSplit(setProtoSplit, dy);
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
        <SidePanel appInfo={appInfo} connection={connection} stats={stats} />
      </main>
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
          currentFilter={filter}
          onApply={applyFilter}
          onClose={() => {
            setMenu(null);
          }}
        />
      ) : null}
    </div>
  );
}

export default App;
