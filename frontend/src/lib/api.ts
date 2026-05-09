import type { Statute } from "./statutes";

// Same-origin in dev (Vite proxy forwards /api → :8000) and in any deploy
// where the API is mounted at /api. Override via VITE_API_BASE if needed.
const API_BASE = import.meta.env.VITE_API_BASE ?? "";

async function jsonFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "content-type": "application/json", ...(init?.headers ?? {}) },
    ...init,
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText} — ${body}`);
  }
  return res.json() as Promise<T>;
}

export type SearchHit = {
  id: string;
  doc_id: number;
  chunk_idx: number;
  score: number;
  text: string;
  char_start: number;
  char_end: number;
  source_url: string;
  metadata: Record<string, unknown>;
};

export type ChatMessage = { role: "user" | "assistant"; text: string };

export const api = {
  stats: () =>
    jsonFetch<{ documents: number; chunks: number; extractions: number }>(
      "/api/stats",
    ),
  statutes: () => jsonFetch<Statute[]>("/api/statutes"),
  search: (query: string, top_k = 10) =>
    jsonFetch<SearchHit[]>("/api/search", {
      method: "POST",
      body: JSON.stringify({ query, top_k }),
    }),
  chat: (params: {
    message: string;
    history: ChatMessage[];
    matter_name?: string;
    matter_caption?: string;
  }) =>
    jsonFetch<{ text: string }>("/api/chat", {
      method: "POST",
      body: JSON.stringify(params),
    }),
};
