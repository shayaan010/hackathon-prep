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
export type AttachedFile = { filename: string; text: string };

export type UploadResponse = {
  filename: string;
  size: number;
  char_count: number;
  text: string;
  ingested: boolean;
  doc_id: number | null;
  chunks: number;
};

export type StatutesQuery = {
  q?: string;
  // CSV of 2-letter codes, e.g. "CA,NY,TX". Backend caps results at 50.
  jurisdiction?: string;
  // CSV of contributing-factor strings (must match FACTOR_CATEGORIES). Any-of.
  factors?: string;
  limit?: number;
};

export type StatuteList = {
  items: Statute[];
  // Total number of statutes matching the query across the whole DB
  // (i.e. the count without LIMIT). `items.length` is capped server-side at 50.
  total: number;
};

export const api = {
  stats: () =>
    jsonFetch<{ documents: number; chunks: number; extractions: number }>(
      "/api/stats",
    ),
<<<<<<< Updated upstream
  statutes: () => jsonFetch<Statute[]>("/api/statutes"),
  comparables: () => jsonFetch<Comparable[]>("/api/comparables"),
  search: (query: string, top_k = 10, jurisdictions: string[] = []) =>
=======
  statutes: (params: StatutesQuery = {}) => {
    const sp = new URLSearchParams();
    if (params.q?.trim()) sp.set("q", params.q.trim());
    if (params.jurisdiction?.trim()) sp.set("jurisdiction", params.jurisdiction.trim());
    if (params.factors?.trim()) sp.set("factors", params.factors.trim());
    if (params.limit) sp.set("limit", String(params.limit));
    const qs = sp.toString();
    return jsonFetch<StatuteList>(`/api/statutes${qs ? `?${qs}` : ""}`);
  },
  search: (query: string, top_k = 10) =>
>>>>>>> Stashed changes
    jsonFetch<SearchHit[]>("/api/search", {
      method: "POST",
      body: JSON.stringify({ query, top_k, jurisdictions }),
    }),
  chat: (params: {
    message: string;
    history: ChatMessage[];
    matter_name?: string;
    matter_caption?: string;
    attached_files?: AttachedFile[];
  }) =>
    // `statutes` is the list of statute rows the model actually pulled this
    // turn via search_statutes / get_statute (empty when no statute tool ran).
    jsonFetch<{ text: string; statutes?: Statute[] }>("/api/chat", {
      method: "POST",
      body: JSON.stringify(params),
    }),
  upload: async (file: File, ingest = false): Promise<UploadResponse> => {
    const fd = new FormData();
    fd.append("file", file);
    fd.append("ingest", ingest ? "true" : "false");
    const res = await fetch(`${API_BASE}/api/upload`, {
      method: "POST",
      body: fd,
    });
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`${res.status} ${res.statusText} — ${body}`);
    }
    return res.json();
  },
};
