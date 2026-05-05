import { useEffect, useMemo, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";
import "./App.css";
import { TopBar } from "./components/TopBar";
import { MessageList } from "./components/MessageList";
import { LayerTree } from "./components/LayerTree";
import { HexDump } from "./components/HexDump";
import { SidePanel } from "./components/SidePanel";
import { MOCK_MESSAGES } from "./mock";
import type { AppInfo, CaptureStats, ConnectionState } from "./types";

const INITIAL_CONNECTION: ConnectionState = {
  status: "disconnected",
  cluster: "local-kafka:9092",
  error: null,
};

const INITIAL_STATS: CaptureStats = {
  totalReceived: 0,
  inBuffer: 0,
  bufferCapacity: 100_000,
  drops: 0,
  throughputPerSec: 0,
};

function App(): JSX.Element {
  const [filter, setFilter] = useState("");
  const [capturing, setCapturing] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(MOCK_MESSAGES[0]?.id ?? null);
  const [appInfo, setAppInfo] = useState<AppInfo | null>(null);
  const [connection] = useState<ConnectionState>(INITIAL_CONNECTION);
  const [stats] = useState<CaptureStats>(INITIAL_STATS);

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

  const messages = MOCK_MESSAGES;
  const selected = useMemo(
    () => messages.find((m) => m.id === selectedId) ?? null,
    [messages, selectedId],
  );

  return (
    <div className="app">
      <TopBar
        filter={filter}
        onFilterChange={setFilter}
        capturing={capturing}
        onToggleCapture={() => {
          setCapturing((prev) => !prev);
        }}
        onClear={() => {
          setSelectedId(null);
        }}
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
    </div>
  );
}

export default App;
