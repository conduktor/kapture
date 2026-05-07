import { useEffect, type Dispatch, type RefObject, type SetStateAction } from "react";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";

import type { KafkaMessage, KafkaMessageDetail, SchemaResolvedPatch } from "../types";

interface Args {
  /** Subscribe only while the proxy is connected. */
  enabled: boolean;
  /** Mirror of the live "ignore events" UI flag — patches drop silently
   *  while paused, the ring buffer still holds the resolved record so
   *  a Resume re-syncs via snapshot. */
  pausedRef: RefObject<boolean>;
  /** Cached list backing `<MessageList>`. We mutate it in place (with
   *  a fresh array ref) so virtual-list row keys stay stable across
   *  patches. */
  messagesRef: RefObject<KafkaMessage[]>;
  setMessages: Dispatch<SetStateAction<KafkaMessage[]>>;
  /** Current detail copy (populated by `inspect_message_by_id` on
   *  selection). Read via ref so the listener doesn't re-subscribe on
   *  every selection change. */
  selectedDetailRef: RefObject<KafkaMessageDetail | null>;
  setSelectedDetail: Dispatch<SetStateAction<KafkaMessageDetail | null>>;
}

/**
 * Subscribe to `kapture:message-schema-resolved` and fold the patches
 * into the live UI state. The backend (Rust `schema_resolver.rs`)
 * mints these batches once the registry has answered for a message;
 * here we patch the cached summary list and, when the currently-
 * selected record is in the batch, the detail copy too.
 *
 * No-op when `enabled` is false. Tears down the listener on disable
 * or unmount.
 */
export function useSchemaResolvedListener(args: Args): void {
  const { enabled, pausedRef, messagesRef, setMessages, selectedDetailRef, setSelectedDetail } =
    args;
  useEffect(() => {
    if (!enabled) {
      return undefined;
    }
    let unlisten: UnlistenFn | null = null;
    void (async () => {
      unlisten = await listen<SchemaResolvedPatch[]>("kapture:message-schema-resolved", (event) => {
        if (pausedRef.current || event.payload.length === 0) {
          return;
        }
        const byId = new Map<string, SchemaResolvedPatch>();
        for (const p of event.payload) byId.set(p.id, p);
        let touched = false;
        const next = messagesRef.current.map((m) => {
          const patch = byId.get(m.id);
          if (!patch) return m;
          touched = true;
          return {
            ...m,
            schemaName: patch.schemaName,
            schemaKind: patch.schemaKind,
          };
        });
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- mutated inside the .map() arrow above, narrowing misses it
        if (touched) {
          messagesRef.current = next;
          setMessages(next);
        }
        const detail = selectedDetailRef.current;
        if (detail !== null) {
          const patch = byId.get(detail.id);
          if (patch) {
            setSelectedDetail({
              ...detail,
              schemaName: patch.schemaName,
              schemaKind: patch.schemaKind,
            });
          }
        }
      });
    })();
    return () => {
      if (unlisten !== null) {
        unlisten();
      }
    };
  }, [enabled, pausedRef, messagesRef, setMessages, selectedDetailRef, setSelectedDetail]);
}
