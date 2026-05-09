/**
 * Chat history shared between MainChat (right pane on /) and ChatWidget
 * (floating button on every other route).
 *
 * Lives in a module-level subscription store + localStorage so:
 *  - Navigating /  ->  /projects  ->  / preserves the conversation (the two
 *    components subscribe to the same source of truth, so unmounting MainChat
 *    doesn't drop messages).
 *  - Page reload preserves it too.
 *  - Both surfaces show the SAME conversation, so what you typed in the
 *    floating widget on /coverage is still there when you come back to /.
 */
import { useEffect, useState } from "react";
import type { Statute } from "./statutes";

export type ChatRole = "user" | "assistant";

export type ChatMessage = {
  id: string;
  role: ChatRole;
  text: string;
  /** Statute cards rendered under an assistant message (search hits). */
  statutes?: Statute[];
  /** Filenames the user attached when sending this message. */
  attachments?: string[];
  /** Placeholder while the request is in flight. */
  pending?: boolean;
};

const STORAGE_KEY = "lex-harvester:chat-history-v1";

function load(): ChatMessage[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    // Drop any messages still flagged pending (orphaned by a reload mid-request).
    return parsed.filter((m) => !m?.pending);
  } catch {
    return [];
  }
}

let state: ChatMessage[] = load();
const listeners = new Set<(s: ChatMessage[]) => void>();

function persist() {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    // ignore quota / disabled-storage errors
  }
}

function notify() {
  for (const fn of listeners) fn(state);
}

export const chatStore = {
  get(): ChatMessage[] {
    return state;
  },
  set(next: ChatMessage[]) {
    state = next;
    persist();
    notify();
  },
  update(updater: (prev: ChatMessage[]) => ChatMessage[]) {
    chatStore.set(updater(state));
  },
  reset() {
    chatStore.set([]);
  },
  subscribe(fn: (s: ChatMessage[]) => void): () => void {
    listeners.add(fn);
    return () => {
      listeners.delete(fn);
    };
  },
};

/** React hook — components re-render when the shared store changes. */
export function useChatHistory(): [
  ChatMessage[],
  (updater: (prev: ChatMessage[]) => ChatMessage[]) => void,
] {
  const [messages, setMessages] = useState<ChatMessage[]>(() => chatStore.get());

  useEffect(() => {
    const unsub = chatStore.subscribe(setMessages);
    return unsub;
  }, []);

  return [messages, chatStore.update];
}
