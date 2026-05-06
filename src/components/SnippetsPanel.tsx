import { useState, type JSX } from "react";

interface Props {
  /** The proxy's actual listen address, e.g. `127.0.0.1:9092`. Required —
   * the modal only renders when the proxy is up so we always have one. */
  listenAddr: string;
}

interface Snippet {
  label: string;
  command: string;
}

interface SnippetGroup {
  heading: string;
  snippets: Snippet[];
}

/** Render the kcat + Apache CLI groups parametrised by the live listen
 * addr. We do not pre-fill cluster-specific options (TLS, SASL) because
 * the proxy listener is plain TCP on loopback — the user's CLI talks to
 * us in clear, and we (the proxy) handle upstream auth. The snippets
 * intentionally show the SHORTEST working invocation per tool. */
function buildGroups(listenAddr: string): SnippetGroup[] {
  return [
    {
      heading: "kcat",
      snippets: [
        { label: "List metadata", command: `kcat -b ${listenAddr} -L` },
        {
          label: "Produce",
          command: `echo "key:value" | kcat -b ${listenAddr} -P -t demo -K:`,
        },
        {
          label: "Consume",
          command: `kcat -b ${listenAddr} -C -t demo -e -o beginning`,
        },
        {
          label: "Consumer group",
          command: `kcat -b ${listenAddr} -G my-group demo`,
        },
      ],
    },
    {
      heading: "Apache Kafka CLI",
      snippets: [
        {
          label: "List topics",
          command: `kafka-topics.sh --bootstrap-server ${listenAddr} --list`,
        },
        {
          label: "Produce",
          command: `kafka-console-producer.sh --bootstrap-server ${listenAddr} --topic demo`,
        },
        {
          label: "Consume",
          command: `kafka-console-consumer.sh --bootstrap-server ${listenAddr} --topic demo --from-beginning`,
        },
        {
          label: "List consumer groups",
          command: `kafka-consumer-groups.sh --bootstrap-server ${listenAddr} --list`,
        },
      ],
    },
  ];
}

/** Body-only render of the snippet groups. Used inside SnippetsModal. */
export function SnippetsPanel({ listenAddr }: Props): JSX.Element {
  const groups = buildGroups(listenAddr);
  return (
    <div className="snippets">
      {groups.map((group) => (
        <div key={group.heading} className="snippets__group">
          <h3 className="snippets__group-title">{group.heading}</h3>
          {group.snippets.map((s) => (
            <SnippetBlock key={s.label} label={s.label} command={s.command} />
          ))}
        </div>
      ))}
    </div>
  );
}

function SnippetBlock({ label, command }: Snippet): JSX.Element {
  const [copied, setCopied] = useState(false);

  const onCopy = (): void => {
    void (async () => {
      try {
        await navigator.clipboard.writeText(command);
        setCopied(true);
        window.setTimeout(() => {
          setCopied(false);
        }, 1500);
      } catch (err) {
        // Clipboard write can fail on some webview platforms; surfacing a
        // tiny UI for that edge is overkill — log only.
        console.warn("clipboard write failed", err);
      }
    })();
  };

  return (
    <div className="snippet">
      <div className="snippet__label">{label}</div>
      <div className="snippet__block">
        <code className="snippet__code">{command}</code>
        <button type="button" className="snippet__copy" onClick={onCopy} title="Copy to clipboard">
          {copied ? "Copied!" : "Copy"}
        </button>
      </div>
    </div>
  );
}
