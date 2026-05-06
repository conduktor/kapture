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
import { followKeyExpr } from "./lib/filterExpr";
import type { AppInfo, AuthArgs, CaptureStats, ConnectionState, KafkaMessage } from "./types";

const DEFAULT_BOOTSTRAP = "localhost:19092";
const DEFAULT_TOPICS = "orders.raw, orders.enriched, users.events, orders.avro, orders.jsonschema";
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
  topics: [],
  error: null,
  schemaRegistryUrl: null,
  fromBeginning: true,
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
  const [editing, setEditing] = useState(false);
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
    topicList: string[],
    fromBeginning: boolean,
    schemaRegistryUrl: string | null,
    auth: AuthArgs | null,
  ): void => {
    setConnection({
      status: "connecting",
      cluster: bootstrap,
      topics: topicList,
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
        // If we're already connected, disconnect first (edit-mode reconnect).
        if (connection.status === "connected") {
          try {
            await invoke("disconnect");
          } catch (err) {
            console.error("disconnect during reconnect failed", err);
          }
        }
        await invoke("connect", {
          bootstrapServers: bootstrap,
          topics: topicList,
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
          topics: topicList,
          error: null,
        }));
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setConnection((prev) => ({
          ...prev,
          status: "error",
          cluster: bootstrap,
          topics: topicList,
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

  const followKey = useCallback((message: KafkaMessage): void => {
    // Empty-string keys are valid in Kafka and meaningfully filterable
    // (envelope.key == "").
    if (message.key !== null) {
      setFilter(followKeyExpr(message.key));
    }
  }, []);

  const showDialog =
    connection.status === "disconnected" || connection.status === "error" || editing;
  const isEditing = editing && connection.status === "connected";
  const initialPrefill = isEditing
    ? {
        bootstrap: connection.cluster ?? DEFAULT_BOOTSTRAP,
        topics: connection.topics.join(", "),
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
          <MessageList
            messages={messages}
            selectedId={selectedId}
            onSelect={setSelectedId}
            onFollow={followKey}
          />
          <LayerTree message={selected} onApplyFilter={applyFilter} />
          <HexDump message={selected} />
        </div>
        <SidePanel appInfo={appInfo} connection={connection} stats={stats} />
      </main>
      {showDialog ? (
        <ConnectionDialog
          defaultBootstrap={DEFAULT_BOOTSTRAP}
          defaultTopics={DEFAULT_TOPICS}
          defaultRegistry={DEFAULT_REGISTRY}
          initial={initialPrefill}
          isEditing={isEditing}
          onConnect={handleConnect}
          onCancel={
            isEditing
              ? () => {
                  setEditing(false);
                }
              : undefined
          }
          pending={connection.status === "connecting"}
          error={connection.error}
        />
      ) : null}
    </div>
  );
}

export default App;
