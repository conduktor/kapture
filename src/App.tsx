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
import type {
  AppInfo,
  AuthArgs,
  CaptureStats,
  ConnectionState,
  KafkaMessage,
  ProtoFrame,
} from "./types";

interface MenuState {
  target: FilterTarget;
  position: { x: number; y: number };
}

const DEFAULT_BOOTSTRAP = "localhost:19092";
const DEFAULT_REGISTRY = "http://localhost:18081";
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
  cluster: null,
  topicPattern: null,
  error: null,
  schemaRegistryUrl: null,
  fromBeginning: false,
  authPrefill: null,
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

  // Subscribe to live events while connected
  useEffect(() => {
    if (connection.status !== "connected") {
      return;
    }
    let messageUnlisten: UnlistenFn | null = null;
    let statsUnlisten: UnlistenFn | null = null;
    let cancelled = false;

    void (async () => {
      messageUnlisten = await listen<KafkaMessage>("kapture:message", (event) => {
        const next = messagesRef.current.concat([event.payload]);
        if (next.length > UI_MAX_MESSAGES) {
          next.splice(0, next.length - UI_MAX_MESSAGES);
        }
        messagesRef.current = next;
        setMessages(next);
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

  const handleConnect = (
    bootstrap: string,
    topicPattern: string | null,
    fromBeginning: boolean,
    schemaRegistryUrl: string | null,
    auth: AuthArgs | null,
  ): void => {
    setConnection({
      status: "connecting",
      cluster: bootstrap,
      topicPattern,
      error: null,
      schemaRegistryUrl,
      fromBeginning,
      authPrefill: auth
        ? {
            mechanism: auth.mechanism,
            username: auth.username,
            useTls: auth.useTls,
            caPath: auth.tls?.caPath ?? null,
            certPath: auth.tls?.certPath ?? null,
            keyPath: auth.tls?.keyPath ?? null,
          }
        : null,
    });
    setEditing(false);
    void (async () => {
      try {
        // The backend connect command stops any previous capture
        // atomically, so the frontend doesn't disconnect first.
        await invoke("connect", {
          bootstrapServers: bootstrap,
          topicPattern,
          fromBeginning,
          schemaRegistryUrl,
          auth,
        });
        messagesRef.current = [];
        setMessages([]);
        setSelectedId(null);
        setStats(INITIAL_STATS);
        setConnection((prev) => ({
          ...prev,
          status: "connected",
          cluster: bootstrap,
          topicPattern,
          error: null,
        }));
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setConnection((prev) => ({
          ...prev,
          status: "error",
          cluster: bootstrap,
          topicPattern,
          error: message,
        }));
      }
    })();
  };

  const handleDisconnect = (): void => {
    void (async () => {
      try {
        await invoke("disconnect");
      } catch (err) {
        console.error("disconnect failed", err);
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
  //   1. Connect fails with "AlreadyCapturing" (zombie slot from a previous
  //      run): cancel fires `disconnect` best-effort to clear the slot,
  //      then drops the dialog into the disconnected state.
  //   2. User just changed their mind: cancel returns them to the previous
  //      workspace (disconnected → empty, connected → still capturing).
  const cancelDialog = (): void => {
    if (connection.status === "error") {
      void invoke("disconnect").catch(() => {
        /* best-effort cleanup of zombie capture slot */
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
    ? {
        bootstrap: connection.cluster ?? DEFAULT_BOOTSTRAP,
        topicPattern: connection.topicPattern ?? "",
        registry: connection.schemaRegistryUrl ?? "",
        fromBeginning: connection.fromBeginning,
        ...(connection.authPrefill
          ? {
              authMethod: connection.authPrefill.mechanism,
              username: connection.authPrefill.username,
              useTls: connection.authPrefill.useTls,
              caPath: connection.authPrefill.caPath ?? "",
              certPath: connection.authPrefill.certPath ?? "",
              keyPath: connection.authPrefill.keyPath ?? "",
            }
          : { authMethod: "none" as const }),
      }
    : undefined;

  return (
    <div className="app">
      <UpdateBanner />
      <TopBar
        filter={filter}
        onFilterChange={setFilter}
        filterError={filterError}
        capturing={connection.status === "connected"}
        onToggleCapture={() => {
          if (connection.status === "connected") {
            handleDisconnect();
          }
        }}
        onClear={handleClear}
        cluster={connection.cluster ?? "no cluster"}
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
            <>
              <MessageList
                messages={messages}
                selectedId={selectedId}
                onSelect={setSelectedId}
                onOpenFilterMenu={openFilterMenu}
              />
              <LayerTree message={selected} onOpenFilterMenu={openFilterMenu} />
              <HexDump message={selected} />
            </>
          ) : (
            <>
              <ProtoList
                frames={protoFrames}
                selectedId={selectedFrameId}
                onSelect={setSelectedFrameId}
              />
              <ProtoDetail frame={protoFrames.find((f) => f.id === selectedFrameId) ?? null} />
            </>
          )}
        </div>
        <SidePanel appInfo={appInfo} connection={connection} stats={stats} />
      </main>
      {showDialog ? (
        <ConnectionDialog
          defaultBootstrap={DEFAULT_BOOTSTRAP}
          defaultRegistry={DEFAULT_REGISTRY}
          initial={initialPrefill}
          isEditing={isEditing}
          onConnect={handleConnect}
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
