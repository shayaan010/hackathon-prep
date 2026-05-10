import { createFileRoute } from "@tanstack/react-router";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { AppHeader } from "@/components/AppHeader";
import { StatuteListItem } from "@/components/StatuteListItem";
import { StatuteDetail } from "@/components/StatuteDetail";
import { MainChat } from "@/components/MainChat";
import { Sheet, SheetContent } from "@/components/ui/sheet";
import { FACTOR_CATEGORIES, type Statute } from "@/lib/statutes";
import { api } from "@/lib/api";
import { useProjects } from "@/lib/projects";
import { Search, Sparkles } from "lucide-react";

// Map of human label → 2-letter code for the jurisdictions the curated
// Postgres covers. Keys must match what /api/statutes returns in
// `jurisdictionLabel` so the chip filter round-trips cleanly.
const JURISDICTION_LABEL_TO_CODE: Record<string, string> = {
  California: "CA",
  "New York": "NY",
  Texas: "TX",
  Colorado: "CO",
  Florida: "FL",
  Nevada: "NV",
  Oregon: "OR",
};

export const Route = createFileRoute("/")({
  head: () => ({
    meta: [
      { title: "Lex Harvester — Legal Research Workspace for PI Attorneys" },
      {
        name: "description",
        content:
          "Queryable database of motor vehicle statutes across US jurisdictions, with case law, related authorities, and an attorney-grade research workspace.",
      },
      { property: "og:title", content: "Lex Harvester — PI Legal Research Workspace" },
      { property: "og:description", content: "Search statutes, save authorities, and chase down evidence." },
    ],
  }),
  component: HarvesterPage,
});

function HarvesterPage() {
  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [activeJurisdictions, setActiveJurisdictions] = useState<string[]>([]);
  const [activeFactors, setActiveFactors] = useState<string[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  // Statutes the chat assistant pulled via tool calls — kept in a separate
  // map so the drawer can resolve a card click even when the sidebar's
  // search/filter state would have hidden the statute.
  const [chatStatutes, setChatStatutes] = useState<Map<string, Statute>>(
    () => new Map(),
  );
  const mergeChatStatutes = (incoming: Statute[]) => {
    if (incoming.length === 0) return;
    setChatStatutes((prev) => {
      const next = new Map(prev);
      for (const s of incoming) next.set(s.id, s);
      return next;
    });
  };

  // Debounce typing → backend search (avoid hitting Postgres every keystroke).
  useEffect(() => {
    const t = setTimeout(() => setDebouncedQuery(query.trim()), 350);
    return () => clearTimeout(t);
  }, [query]);

  // Translate label-based chips ("California") into the CSV of codes ("CA")
  // that the backend filter expects. Stable string → stable query key.
  const jurisdictionParam = useMemo(() => {
    const codes = activeJurisdictions
      .map((label) => JURISDICTION_LABEL_TO_CODE[label])
      .filter(Boolean);
    return codes.sort().join(",");
  }, [activeJurisdictions]);

  // Factor chips: server-side filter via `factors=CSV`. Sort for query-key
  // stability (otherwise [A,B] and [B,A] would refetch needlessly).
  const factorParam = useMemo(
    () => activeFactors.slice().sort().join(","),
    [activeFactors],
  );

  // One server call drives the list. Empty filters → top-50 default.
  // `items` is capped at 50 server-side; `total` is the full match count
  // across the table so we can show "50 of 1,234".
  const { data, isFetching: searching } = useQuery({
    queryKey: ["statutes", debouncedQuery, jurisdictionParam, factorParam],
    queryFn: () =>
      api.statutes({
        q: debouncedQuery || undefined,
        jurisdiction: jurisdictionParam || undefined,
        factors: factorParam || undefined,
        limit: 50,
      }),
    staleTime: 30_000,
    placeholderData: keepPreviousData,
  });
  const statutes = data?.items ?? [];
  const totalStatutes = data?.total ?? 0;

  // Jurisdictions chip list: union of (a) the static codes the backend covers
  // and (b) anything the current result set actually contains. Keeping (a)
  // visible means the chips don't disappear when the user types into a
  // narrowing query and the result set drops to a single state.
  const jurisdictions = useMemo(() => {
    const set = new Set<string>(Object.keys(JURISDICTION_LABEL_TO_CODE));
    for (const s of statutes) if (s.jurisdictionLabel) set.add(s.jurisdictionLabel);
    return Array.from(set).sort();
  }, [statutes]);

  const projects = useProjects();
  const savedIds = useMemo(() => {
    const set = new Set<string>();
    for (const p of projects) {
      for (const id of p.statuteIds) set.add(id);
    }
    return set;
  }, [projects]);

  const useSemantic = debouncedQuery.length >= 2;

  // Only show detail when the user explicitly clicked something — no auto-select,
  // otherwise the Sheet would pop open as filters/search results change.
  // Resolution order: the visible result set, then anything the chat pulled
  // via tool calls (covers off-page jurisdictions / fresh searches).
  const selected = selectedId
    ? statutes.find((s) => s.id === selectedId) ??
      chatStatutes.get(selectedId) ??
      null
    : null;

  const toggle = (list: string[], v: string, set: (l: string[]) => void) =>
    set(list.includes(v) ? list.filter((x) => x !== v) : [...list, v]);

  return (
    <div className="h-screen flex flex-col bg-background">
      <AppHeader />

      <div className="flex-1 grid grid-cols-1 md:grid-cols-[420px_1fr] min-h-0">
        {/* LEFT: Search + filters + results */}
        <aside className="border-r border-border flex flex-col min-h-0 bg-secondary/30">
          {/* Search bar */}
          <div className="p-4 border-b border-border bg-card">
            <div className="relative">
              <Search className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search statutes, citations…"
                className="w-full h-11 pl-9 pr-3 rounded-lg border border-border bg-background text-sm focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary/50 font-medium"
              />
            </div>

            {(useSemantic || searching) && (
              <div className="mt-3 flex items-center gap-2">
                <span className="text-[10px] font-mono uppercase tracking-widest text-gold flex items-center gap-1">
                  <Sparkles className="h-2.5 w-2.5" />
                  {searching ? "searching…" : "live"}
                </span>
              </div>
            )}
          </div>

          {/* Filters */}
          <div className="px-4 py-3 border-b border-border space-y-3 bg-card/50">
            <div>
              <div className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground mb-1.5">
                Jurisdiction
              </div>
              <div className="flex flex-wrap gap-1">
                {jurisdictions.map((j) => {
                  const on = activeJurisdictions.includes(j);
                  return (
                    <button
                      key={j}
                      onClick={() => toggle(activeJurisdictions, j, setActiveJurisdictions)}
                      className={[
                        "text-[11px] px-2 py-1 rounded-full border transition-colors",
                        on
                          ? "bg-primary text-primary-foreground border-primary"
                          : "bg-card border-border hover:border-primary/40 text-muted-foreground",
                      ].join(" ")}
                    >
                      {j}
                    </button>
                  );
                })}
              </div>
            </div>
            <div>
              <div className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground mb-1.5">
                Contributing Factor
              </div>
              <div className="flex flex-wrap gap-1 max-h-24 overflow-y-auto scrollbar-thin">
                {/* "Other" is intentionally hidden — it's a tagging fallback
                    for definitional/administrative statutes, not something a
                    user would meaningfully filter on. The chat tool can still
                    pass it. */}
                {FACTOR_CATEGORIES.filter((f) => f !== "Other").map((f) => {
                  const on = activeFactors.includes(f);
                  return (
                    <button
                      key={f}
                      onClick={() => toggle(activeFactors, f, setActiveFactors)}
                      className={[
                        "text-[11px] px-2 py-1 rounded-full border transition-colors",
                        on
                          ? "bg-gold text-gold-foreground border-gold"
                          : "bg-card border-border hover:border-gold/50 text-muted-foreground",
                      ].join(" ")}
                    >
                      {f}
                    </button>
                  );
                })}
              </div>
            </div>
          </div>

          {/* Results count — `total` is the full DB match count for the
              current filter set; `statutes` is the server-capped page. */}
          <div className="px-4 py-2 border-b border-border flex items-center justify-between text-[11px] font-mono uppercase tracking-widest text-muted-foreground bg-secondary/50">
            <span>
              {statutes.length}
              {totalStatutes > statutes.length
                ? ` of ${totalStatutes.toLocaleString()}`
                : " results"}
            </span>
            <span>{savedIds.size} saved</span>
          </div>

          {/* Result list */}
          <div className="flex-1 overflow-y-auto scrollbar-thin divide-y divide-border/50">
            {statutes.map((s) => (
              <StatuteListItem
                key={s.id}
                statute={s}
                active={selected?.id === s.id}
                saved={savedIds.has(s.id)}
                onSelect={() => setSelectedId(s.id)}
              />
            ))}
            {statutes.length === 0 && !searching && (
              <div className="p-8 text-center text-sm text-muted-foreground">
                No statutes match your query.
              </div>
            )}
          </div>
        </aside>

        {/* RIGHT: Chat */}
        <main className="min-h-0 bg-background">
          <MainChat
            onSelectStatute={setSelectedId}
            onChatStatutes={mergeChatStatutes}
          />
        </main>
      </div>

      {/* Statute detail drawer — opens from chat card or sidebar list click */}
      <Sheet
        open={selected != null}
        onOpenChange={(o) => {
          if (!o) setSelectedId(null);
        }}
      >
        <SheetContent
          side="right"
          className="p-0 sm:max-w-3xl w-full overflow-y-auto"
        >
          <StatuteDetail statute={selected} />
        </SheetContent>
      </Sheet>
    </div>
  );
}
