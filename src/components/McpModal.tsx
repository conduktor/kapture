import { useEffect, useState, type JSX } from "react";
import { invoke } from "@tauri-apps/api/core";

interface McpInfo {
  url: string;
  port: number;
  token: string;
  tokenPath: string;
}

interface Props {
  onClose: () => void;
}

type TabKey = "prompt" | "claude-cli" | "claude-json" | "cursor" | "raw";

interface TabSpec {
  key: TabKey;
  label: string;
}

const TABS: readonly TabSpec[] = [
  { key: "prompt", label: "Prompt your agent" },
  { key: "claude-cli", label: "Claude Code (CLI)" },
  { key: "claude-json", label: "Claude (JSON)" },
  { key: "cursor", label: "Cursor / Windsurf" },
  { key: "raw", label: "URL & Token" },
];

/**
 * MCP integration modal. Surfaces the local MCP server URL + bearer
 * token so the user can wire Kapture into Claude Code, Cursor, or any
 * other agentic IDE without hand-rolling the config. Each tab shows a
 * single copy-paste-ready snippet for one target.
 *
 * The token is read fresh from the backend on open — `mcp::ensure_token`
 * persists it to `<config_dir>/mcp-token` at boot, so every session
 * shows the same value.
 */
export function McpModal({ onClose }: Props): JSX.Element {
  const [info, setInfo] = useState<McpInfo | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [tab, setTab] = useState<TabKey>("prompt");

  useEffect(() => {
    void (async () => {
      try {
        const data = await invoke<McpInfo>("mcp_info");
        setInfo(data);
      } catch (err) {
        setLoadError(err instanceof Error ? err.message : String(err));
      }
    })();
  }, []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === "Escape") {
        onClose();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("keydown", onKey);
    };
  }, [onClose]);

  return (
    <div
      className="dialog-backdrop"
      onClick={(e) => {
        if (e.target === e.currentTarget) {
          onClose();
        }
      }}
    >
      <div
        className="dialog modal--mcp"
        role="dialog"
        aria-modal="true"
        aria-label="MCP integration"
      >
        <div className="modal__header">
          <h2 className="dialog__title">MCP integration</h2>
          <button
            type="button"
            className="modal__close"
            onClick={onClose}
            aria-label="Close MCP dialog"
          >
            ×
          </button>
        </div>
        <p className="dialog__hint">
          Hook Kapture into your agentic IDE. The server is local-only (<code>127.0.0.1</code>),
          authenticated by a bearer token persisted in your config dir.
        </p>
        {loadError !== null ? (
          <p className="dialog__error">Could not read MCP config: {loadError}</p>
        ) : info === null ? (
          <p className="muted">Loading…</p>
        ) : (
          <>
            <div className="mcp__tabs" role="tablist" aria-label="MCP integration targets">
              {TABS.map((t) => (
                <button
                  key={t.key}
                  type="button"
                  role="tab"
                  aria-selected={tab === t.key}
                  className={`mcp__tab${tab === t.key ? " mcp__tab--active" : ""}`}
                  onClick={() => {
                    setTab(t.key);
                  }}
                >
                  {t.label}
                </button>
              ))}
            </div>
            <div className="mcp__panel" role="tabpanel">
              <McpPanel tab={tab} info={info} />
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function McpPanel({ tab, info }: { tab: TabKey; info: McpInfo }): JSX.Element {
  if (tab === "prompt") {
    const body = `Please register Kapture as an MCP server in this workspace.

- Name: kapture
- Transport: HTTP (streamable)
- URL: ${info.url}
- Auth header: Authorization: Bearer ${info.token}

Pick the registration mechanism that fits this IDE/agent (e.g. \`claude mcp add\`, edit \`.mcp.json\`, Cursor's mcp.json, etc.) and verify the connection by listing the tools the kapture server exposes.`;
    return (
      <Snippet
        hint="Drop this in your agent chat. It'll figure out the right config file or CLI for the IDE you're in."
        body={body}
      />
    );
  }
  if (tab === "claude-cli") {
    const body = `claude mcp add kapture --transport http ${info.url} -H "Authorization: Bearer ${info.token}"`;
    return (
      <Snippet
        hint="Run once in any project. Claude Code persists the registration in its config."
        body={body}
      />
    );
  }
  if (tab === "claude-json") {
    const body = JSON.stringify(
      {
        mcpServers: {
          kapture: {
            url: info.url,
            headers: { Authorization: `Bearer ${info.token}` },
          },
        },
      },
      null,
      2,
    );
    return (
      <Snippet
        hint="Paste into ~/.claude.json (or workspace .mcp.json) under the project's root mcpServers."
        body={body}
      />
    );
  }
  if (tab === "cursor") {
    const body = JSON.stringify(
      {
        mcpServers: {
          kapture: {
            url: info.url,
            headers: { Authorization: `Bearer ${info.token}` },
          },
        },
      },
      null,
      2,
    );
    return (
      <Snippet
        hint="Cursor: ~/.cursor/mcp.json. Windsurf: ~/.codeium/windsurf/mcp_config.json."
        body={body}
      />
    );
  }
  // "raw" tab
  return (
    <div className="mcp__raw">
      <Field label="Server URL" value={info.url} />
      <Field label="Bearer token" value={info.token} mono />
      <p className="muted mcp__token-path">
        stored at <code>{info.tokenPath}</code>
      </p>
    </div>
  );
}

function Field({
  label,
  value,
  mono,
}: {
  label: string;
  value: string;
  mono?: boolean;
}): JSX.Element {
  return (
    <div className="mcp__field">
      <span className="mcp__field-label">{label}</span>
      <code
        className={mono === true ? "mcp__field-value mcp__field-value--mono" : "mcp__field-value"}
      >
        {value}
      </code>
      <CopyButton text={value} />
    </div>
  );
}

function Snippet({ hint, body }: { hint: string; body: string }): JSX.Element {
  return (
    <section className="mcp__snippet">
      <div className="mcp__snippet-head">
        <p className="mcp__snippet-hint">{hint}</p>
        <CopyButton text={body} />
      </div>
      <pre className="mcp__snippet-body">
        <code>{body}</code>
      </pre>
    </section>
  );
}

function CopyButton({ text }: { text: string }): JSX.Element {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className="btn btn--ghost mcp__copy"
      onClick={() => {
        void navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          window.setTimeout(() => {
            setCopied(false);
          }, 1500);
        });
      }}
    >
      {copied ? "Copied" : "Copy"}
    </button>
  );
}
