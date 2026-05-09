import { createFileRoute } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { AppHeader } from "@/components/AppHeader";
import { StatuteListItem } from "@/components/StatuteListItem";
import { StatuteDetail } from "@/components/StatuteDetail";
import { MainChat } from "@/components/MainChat";
import { Sheet, SheetContent } from "@/components/ui/sheet";
import { FACTOR_CATEGORIES, STATUTES } from "@/lib/statutes";
import { api } from "@/lib/api";
import { useProjects } from "@/lib/projects";
import { Search, Sparkles } from "lucide-react";

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
  // Statutes come from the FastAPI backend (/api/statutes). Falls back to the
  // bundled mock list if the backend isn't running, so the UI never breaks.
  const { data: statutes = STATUTES } = useQuery({
    queryKey: ["statutes"],
    queryFn: api.statutes,
    staleTime: 60_000,
    placeholderData: STATUTES,
  });

  const jurisdictions = useMemo(
    () => Array.from(new Set(statutes.map((s) => s.jurisdictionLabel))),
    [statutes],
  );

  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [activeFactors, setActiveFactors] = useState<string[]>([]);
  const [activeJurisdictions, setActiveJurisdictions] = useState<string[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const projects = useProjects();
  const savedIds = useMemo(() => {
    const set = new Set<string>();
    for (const p of projects) {
      for (const id of p.statuteIds) set.add(id);
    }
    return set;
  }, [projects]);

  // Debounce typing → semantic search (avoid hitting /api/search every keystroke).
  useEffect(() => {
    const t = setTimeout(() => setDebouncedQuery(query.trim()), 350);
    return () => clearTimeout(t);
  }, [query]);

  const useSemantic = debouncedQuery.length >= 2;
  const { data: searchHits, isFetching: searching } = useQuery({
    queryKey: ["search", debouncedQuery],
    queryFn: () => api.search(debouncedQuery, 20),
    enabled: useSemantic,
    staleTime: 30_000,
  });

  const semanticOrdered = useMemo(() => {
    if (!useSemantic || !searchHits) return null;
    const bestByDoc = new Map<number, number>();
    for (const h of searchHits) {
      const prev = bestByDoc.get(h.doc_id);
      if (prev === undefined || h.score > prev) bestByDoc.set(h.doc_id, h.score);
    }
    const byId = new Map(statutes.map((s) => [s.id, s]));
    const ranked: typeof statutes = [];
    for (const [docId] of [...bestByDoc.entries()].sort((a, b) => b[1] - a[1])) {
      // Real DB statutes have id "ca-vc-<section-slug>"; SearchHit only carries doc_id.
      // Match by URL/section using the seed/list mapping built below.
      const stat = statutes.find((s) =>
        searchHits.find(
          (h) => h.doc_id === docId && h.source_url === s.source.url,
        ),
      );
      if (stat && !ranked.find((r) => r.id === stat.id)) ranked.push(stat);
    }
    return ranked;
  }, [searchHits, statutes, useSemantic]);

  const filtered = useMemo(() => {
    const base = semanticOrdered ?? statutes;
    return base.filter((s) => {
      const q = useSemantic ? "" : query.trim().toLowerCase();
      const matchesQuery =
        !q ||
        s.title.toLowerCase().includes(q) ||
        s.section.includes(q) ||
        s.summary.toLowerCase().includes(q) ||
        s.text.toLowerCase().includes(q) ||
        s.factors.some((f) => f.toLowerCase().includes(q));
      const matchesFactor =
        activeFactors.length === 0 || activeFactors.some((f) => s.factors.includes(f));
      const matchesJ =
        activeJurisdictions.length === 0 || activeJurisdictions.includes(s.jurisdictionLabel);
      return matchesQuery && matchesFactor && matchesJ;
    });
  }, [semanticOrdered, statutes, query, activeFactors, activeJurisdictions, useSemantic]);

  // Only show detail when the user explicitly clicked something — no auto-select,
  // otherwise the Sheet would pop open as filters/search results change.
  const selected = selectedId ? statutes.find((s) => s.id === selectedId) ?? null : null;

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
                placeholder="Search statutes, citations, factors…"
                className="w-full h-11 pl-9 pr-3 rounded-lg border border-border bg-background text-sm focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary/50 font-medium"
              />
            </div>

            {useSemantic && (
              <div className="mt-3 flex items-center gap-2">
                <span className="text-[10px] font-mono uppercase tracking-widest text-gold flex items-center gap-1">
                  <Sparkles className="h-2.5 w-2.5" />
                  {searching ? "searching…" : "semantic"}
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
                {FACTOR_CATEGORIES.map((f) => {
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

          {/* Results count */}
          <div className="px-4 py-2 border-b border-border flex items-center justify-between text-[11px] font-mono uppercase tracking-widest text-muted-foreground bg-secondary/50">
            <span>{filtered.length} results</span>
            <span>{savedIds.size} saved</span>
          </div>

          {/* Result list */}
          <div className="flex-1 overflow-y-auto scrollbar-thin divide-y divide-border/50">
            {filtered.map((s) => (
              <StatuteListItem
                key={s.id}
                statute={s}
                active={selected?.id === s.id}
                saved={savedIds.has(s.id)}
                onSelect={() => setSelectedId(s.id)}
              />
            ))}
            {filtered.length === 0 && (
              <div className="p-8 text-center text-sm text-muted-foreground">
                No statutes match your query.
              </div>
            )}
          </div>
        </aside>

        {/* RIGHT: Chat */}
        <main className="min-h-0 bg-background">
          <MainChat statutes={statutes} onSelectStatute={setSelectedId} />
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
