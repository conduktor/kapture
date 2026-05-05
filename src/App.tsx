import { useEffect, useMemo, useRef, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import "./App.css";
import { TopBar } from "./components/TopBar";
import { MessageList } from "./components/MessageList";
import { LayerTree } from "./components/LayerTree";
import { HexDump } from "./components/HexDump";
import { SidePanel } from "./components/SidePanel";
import { ConnectionDialog } from "./components/ConnectionDialog";
import type { AppInfo, CaptureStats, ConnectionState, KafkaMessage } from "./types";

const DEFAULT_BOOTSTRAP = "localhost:19092";
const DEFAULT_TOPICS = "orders.raw, orders.enriched, users.events";
const UI_MAX_MESSAGES = 5_000;
const FILTER_DEBOUNCE_MS = 250;

const INITIAL_STATS: CaptureStats = {
  totalReceived: 0,
  inBuffer: 0,
  bufferCapacity: 100_000,
  drops: 0,
  throughputPerSec: 0,
};

const INITIAL_CONNECTION: ConnectionState = {
  status: "disconnected",
  cluster: null,
  topics: [],
  error: null,
};

function App(): JSX.Element {
  const [filter, setFilter] = useState("");
  const [filterError, setFilterError] = useState<string | null>(null);
  const [messages, setMessages] = useState<KafkaMessage[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [appInfo, setAppInfo] = useState<AppInfo | null>(null);
  const [connection, setConnection] = useState<ConnectionState>(INITIAL_CONNECTION);
  const [stats, setStats] = useState<CaptureStats>(INITIAL_STATS);
  const messagesRef = useRef<KafkaMessage[]>([]);

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

  // Debounced filter sync to backend.
  useEffect(() => {
    const handle = setTimeout(() => {
      void (async () => {
        try {
          await invoke("set_filter", { expression: filter });
          setFilterError(null);
          // Refresh visible list so previously-buffered messages are
          // re-evaluated against the new filter.
          if (connection.status === "connected") {
            try {
              const snap = await invoke<KafkaMessage[]>("snapshot");
              messagesRef.current = snap.slice(-UI_MAX_MESSAGES);
              setMessages(messagesRef.current);
            } catch (err) {
              console.error("snapshot failed", err);
            }
          }
        } catch (err) {
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

  const handleConnect = (bootstrap: string, topicList: string[], fromBeginning: boolean): void => {
    setConnection({
      status: "connecting",
      cluster: bootstrap,
      topics: topicList,
      error: null,
    });
    void (async () => {
      try {
        await invoke("connect", {
          bootstrapServers: bootstrap,
          topics: topicList,
          fromBeginning,
        });
        messagesRef.current = [];
        setMessages([]);
        setSelectedId(null);
        setStats(INITIAL_STATS);
        setConnection({
          status: "connected",
          cluster: bootstrap,
          topics: topicList,
          error: null,
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setConnection({
          status: "error",
          cluster: bootstrap,
          topics: topicList,
          error: message,
        });
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

  const showDialog = connection.status === "disconnected" || connection.status === "error";

  return (
    <div className="app">
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
      />
      <main className="layout">
        <div className="layout__main">
          <MessageList messages={messages} selectedId={selectedId} onSelect={setSelectedId} />
          <LayerTree message={selected} />
          <HexDump message={selected} />
        </div>
        <SidePanel appInfo={appInfo} connection={connection} stats={stats} />
      </main>
      {showDialog ? (
        <ConnectionDialog
          defaultBootstrap={DEFAULT_BOOTSTRAP}
          defaultTopics={DEFAULT_TOPICS}
          onConnect={handleConnect}
          pending={connection.status === "connecting"}
          error={connection.error}
        />
      ) : null}
    </div>
  );
}

export default App;
