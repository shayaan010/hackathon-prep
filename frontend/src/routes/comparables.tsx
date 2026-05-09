import { createFileRoute } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import {
  ArrowDownUp,
  ChevronDown,
  ChevronUp,
  ExternalLink,
  Search,
} from "lucide-react";
import { AppHeader } from "@/components/AppHeader";
import { api, type Comparable } from "@/lib/api";

export const Route = createFileRoute("/comparables")({
  head: () => ({
    meta: [
      { title: "Damages Comparables — Lex Harvester" },
      {
        name: "description",
        content:
          "Sortable table of past PI verdicts and settlements: filter by jurisdiction, factor, kind; sort by award amount; click for the verbatim source quote.",
      },
    ],
  }),
  component: ComparablesPage,
});

type SortKey = "award_total_usd" | "year" | "case_name" | "jurisdictionLabel";
type SortDir = "asc" | "desc";

function fmtUsd(n: number): string {
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(n >= 10_000_000 ? 1 : 2)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(0)}k`;
  return `$${n.toLocaleString()}`;
}

function ComparablesPage() {
  const { data: comparables = [], isLoading } = useQuery({
    queryKey: ["comparables"],
    queryFn: api.comparables,
    staleTime: 60_000,
  });

  const [query, setQuery] = useState("");
  const [activeJurisdictions, setActiveJurisdictions] = useState<string[]>([]);
  const [activeFactors, setActiveFactors] = useState<string[]>([]);
  const [activeKinds, setActiveKinds] = useState<string[]>([]);
  const [sortKey, setSortKey] = useState<SortKey>("award_total_usd");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [expanded, setExpanded] = useState<string | null>(null);

  const allJurisdictions = useMemo(
    () => Array.from(new Set(comparables.map((c) => c.jurisdictionLabel))).sort(),
    [comparables],
  );
  const allFactors = useMemo(
    () => Array.from(new Set(comparables.flatMap((c) => c.factors))).sort(),
    [comparables],
  );

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return comparables.filter((c) => {
      if (q) {
        const hay =
          `${c.case_name} ${c.citation} ${c.fact_pattern} ${c.injuries.join(" ")} ${c.factors.join(" ")} ${c.plaintiff ?? ""} ${c.defendant ?? ""}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      if (activeJurisdictions.length && !activeJurisdictions.includes(c.jurisdictionLabel)) {
        return false;
      }
      if (activeFactors.length && !activeFactors.some((f) => c.factors.includes(f))) {
        return false;
      }
      if (activeKinds.length && !activeKinds.includes(c.kind)) return false;
      return true;
    });
  }, [comparables, query, activeJurisdictions, activeFactors, activeKinds]);

  const sorted = useMemo(() => {
    const sign = sortDir === "asc" ? 1 : -1;
    const arr = [...filtered];
    arr.sort((a, b) => {
      const va = a[sortKey];
      const vb = b[sortKey];
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === "number" && typeof vb === "number") return (va - vb) * sign;
      return String(va).localeCompare(String(vb)) * sign;
    });
    return arr;
  }, [filtered, sortKey, sortDir]);

  const stats = useMemo(() => {
    if (sorted.length === 0) return null;
    const awards = sorted.map((c) => c.award_total_usd).filter((n) => n > 0);
    const total = awards.reduce((a, b) => a + b, 0);
    const median = (() => {
      const s = [...awards].sort((a, b) => a - b);
      if (s.length === 0) return 0;
      const mid = Math.floor(s.length / 2);
      return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
    })();
    return {
      count: sorted.length,
      median,
      max: Math.max(...awards, 0),
      mean: total / Math.max(awards.length, 1),
    };
  }, [sorted]);

  const toggle = (list: string[], v: string, set: (l: string[]) => void) =>
    set(list.includes(v) ? list.filter((x) => x !== v) : [...list, v]);

  const setSort = (k: SortKey) => {
    if (k === sortKey) setSortDir(sortDir === "asc" ? "desc" : "asc");
    else {
      setSortKey(k);
      setSortDir(k === "award_total_usd" || k === "year" ? "desc" : "asc");
    }
  };

  const SortIcon = ({ k }: { k: SortKey }) =>
    sortKey === k ? (
      sortDir === "asc" ? (
        <ChevronUp className="h-3.5 w-3.5 inline -mt-0.5" />
      ) : (
        <ChevronDown className="h-3.5 w-3.5 inline -mt-0.5" />
      )
    ) : (
      <ArrowDownUp className="h-3 w-3 inline -mt-0.5 opacity-30" />
    );

  return (
    <div className="min-h-screen flex flex-col bg-background">
      <AppHeader />

      <main className="flex-1 px-6 py-6">
        <div className="max-w-7xl mx-auto space-y-5">
          <header className="space-y-1.5">
            <div className="flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-widest text-gold">
              Damages Workbench
            </div>
            <h1 className="font-serif text-3xl font-bold tracking-tight">
              Comparables
            </h1>
            <p className="text-sm text-muted-foreground">
              Past PI verdicts and settlements you can sort against. Filter by
              jurisdiction, contributing factor, or outcome type. Click a row
              for the verbatim source quote.
            </p>
          </header>

          {/* Search */}
          <div className="relative max-w-xl">
            <Search className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search by case, plaintiff, injury, fact pattern…"
              className="w-full h-10 pl-9 pr-3 rounded-lg border border-border bg-card text-sm focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary/50"
            />
          </div>

          {/* Filter chips */}
          <div className="flex flex-wrap gap-x-6 gap-y-3">
            <ChipGroup
              label="Jurisdiction"
              options={allJurisdictions}
              active={activeJurisdictions}
              onToggle={(v) => toggle(activeJurisdictions, v, setActiveJurisdictions)}
              tone="primary"
            />
            <ChipGroup
              label="Kind"
              options={["verdict", "settlement"]}
              active={activeKinds}
              onToggle={(v) => toggle(activeKinds, v, setActiveKinds)}
              tone="primary"
            />
            <ChipGroup
              label="Contributing Factor"
              options={allFactors}
              active={activeFactors}
              onToggle={(v) => toggle(activeFactors, v, setActiveFactors)}
              tone="gold"
            />
          </div>

          {/* Stats */}
          {stats && (
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <Stat label="Matching" value={`${stats.count} ${stats.count === 1 ? "case" : "cases"}`} />
              <Stat label="Median award" value={fmtUsd(stats.median)} accent />
              <Stat label="Mean award" value={fmtUsd(stats.mean)} />
              <Stat label="Top award" value={fmtUsd(stats.max)} />
            </div>
          )}

          {/* Table */}
          <div className="rounded-lg border border-border bg-card overflow-hidden">
            <div className="grid grid-cols-[2fr_1fr_0.6fr_1fr_2fr_36px] text-[10px] font-mono uppercase tracking-widest text-muted-foreground bg-secondary/50 border-b border-border px-4 py-2.5">
              <button onClick={() => setSort("case_name")} className="text-left hover:text-foreground">
                Case <SortIcon k="case_name" />
              </button>
              <button onClick={() => setSort("jurisdictionLabel")} className="text-left hover:text-foreground">
                Jurisdiction <SortIcon k="jurisdictionLabel" />
              </button>
              <button onClick={() => setSort("year")} className="text-left hover:text-foreground">
                Year <SortIcon k="year" />
              </button>
              <button onClick={() => setSort("award_total_usd")} className="text-left hover:text-foreground">
                Award <SortIcon k="award_total_usd" />
              </button>
              <span>Factors / Injuries</span>
              <span />
            </div>

            {isLoading && (
              <div className="px-6 py-12 text-center text-sm text-muted-foreground">
                Loading comparables…
              </div>
            )}

            {!isLoading && sorted.length === 0 && (
              <div className="px-6 py-12 text-center text-sm text-muted-foreground">
                No comparables match the current filters.
              </div>
            )}

            {sorted.map((c) => (
              <Row
                key={c.id}
                comparable={c}
                expanded={expanded === c.id}
                onToggle={() => setExpanded(expanded === c.id ? null : c.id)}
              />
            ))}
          </div>
        </div>
      </main>
    </div>
  );
}

function Row({
  comparable: c,
  expanded,
  onToggle,
}: {
  comparable: Comparable;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <div className="border-b border-border/60 last:border-b-0">
      <button
        onClick={onToggle}
        className="w-full grid grid-cols-[2fr_1fr_0.6fr_1fr_2fr_36px] items-start text-left px-4 py-3 hover:bg-secondary/40 transition-colors"
      >
        <div className="min-w-0 pr-3">
          <div className="font-serif font-bold text-sm leading-tight truncate">
            {c.case_name}
          </div>
          <div className="text-[11px] text-muted-foreground truncate font-mono">
            {c.citation}
          </div>
        </div>
        <div className="text-sm">
          <div className="font-medium">{c.jurisdictionLabel}</div>
          <span
            className={[
              "inline-block text-[10px] uppercase tracking-wide px-1.5 py-0.5 rounded font-mono mt-0.5",
              c.kind === "verdict"
                ? "bg-primary/15 text-primary"
                : "bg-gold/15 text-gold",
            ].join(" ")}
          >
            {c.kind}
          </span>
        </div>
        <div className="text-sm font-mono">{c.year ?? "—"}</div>
        <div className="text-sm">
          <div className="font-serif font-bold">{fmtUsd(c.award_total_usd)}</div>
          {c.punitive_usd > 0 && (
            <div className="text-[10px] text-muted-foreground">
              incl. {fmtUsd(c.punitive_usd)} punitive
            </div>
          )}
        </div>
        <div className="text-xs text-muted-foreground space-y-1 pr-2">
          <div className="flex flex-wrap gap-1">
            {c.factors.slice(0, 3).map((f) => (
              <span
                key={f}
                className="text-[10px] px-1.5 py-0.5 rounded-full border border-gold/40 text-gold bg-gold/5"
              >
                {f}
              </span>
            ))}
          </div>
          <div className="line-clamp-1 text-foreground/80">
            {c.injuries.join(" · ")}
          </div>
        </div>
        <div className="text-muted-foreground self-center">
          {expanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
        </div>
      </button>
      {expanded && (
        <div className="px-4 pb-4 pt-1 bg-secondary/30 border-t border-border/40">
          <div className="grid md:grid-cols-2 gap-4 text-sm">
            <div className="space-y-2">
              <Label>Parties</Label>
              <div className="text-foreground">
                <div><span className="text-muted-foreground">Plaintiff:</span> {c.plaintiff ?? "—"}</div>
                <div><span className="text-muted-foreground">Defendant:</span> {c.defendant ?? "—"}</div>
              </div>
              <Label>Fact pattern</Label>
              <p className="text-foreground/90 leading-relaxed">{c.fact_pattern || "—"}</p>
              <Label>Injuries</Label>
              <ul className="list-disc list-inside text-foreground/90 leading-relaxed">
                {c.injuries.length === 0 && <li className="list-none text-muted-foreground">—</li>}
                {c.injuries.map((inj) => (
                  <li key={inj}>{inj}</li>
                ))}
              </ul>
            </div>
            <div className="space-y-2">
              <Label>Award breakdown</Label>
              <div className="rounded-md border border-border bg-card overflow-hidden">
                <BreakdownRow label="Total" value={c.award_total_usd} bold />
                <BreakdownRow label="Economic" value={c.economic_usd} />
                <BreakdownRow label="Non-economic" value={c.non_economic_usd} />
                <BreakdownRow label="Punitive" value={c.punitive_usd} />
              </div>
              <Label>Source quote</Label>
              <blockquote className="border-l-2 border-gold pl-3 italic text-foreground/85">
                "{c.source_quote}"
              </blockquote>
              {c.source_url && (
                <a
                  href={c.source_url}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-1.5 text-[12px] text-primary hover:underline"
                >
                  View source <ExternalLink className="h-3 w-3" />
                </a>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function ChipGroup({
  label,
  options,
  active,
  onToggle,
  tone,
}: {
  label: string;
  options: readonly string[];
  active: string[];
  onToggle: (v: string) => void;
  tone: "primary" | "gold";
}) {
  if (options.length === 0) return null;
  return (
    <div>
      <div className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground mb-1.5">
        {label}
      </div>
      <div className="flex flex-wrap gap-1">
        {options.map((v) => {
          const on = active.includes(v);
          const cls = on
            ? tone === "primary"
              ? "bg-primary text-primary-foreground border-primary"
              : "bg-gold text-gold-foreground border-gold"
            : tone === "primary"
              ? "bg-card border-border hover:border-primary/40 text-muted-foreground"
              : "bg-card border-border hover:border-gold/50 text-muted-foreground";
          return (
            <button
              key={v}
              onClick={() => onToggle(v)}
              className={`text-[11px] px-2 py-1 rounded-full border transition-colors ${cls}`}
            >
              {v}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
  accent = false,
}: {
  label: string;
  value: string;
  accent?: boolean;
}) {
  return (
    <div className="rounded-lg border border-border bg-card px-4 py-3">
      <div className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">
        {label}
      </div>
      <div
        className={`font-serif font-bold text-xl mt-0.5 ${accent ? "text-gold" : ""}`}
      >
        {value}
      </div>
    </div>
  );
}

function Label({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">
      {children}
    </div>
  );
}

function BreakdownRow({ label, value, bold = false }: { label: string; value: number; bold?: boolean }) {
  return (
    <div
      className={`flex items-center justify-between px-3 py-2 text-sm border-b border-border/50 last:border-b-0 ${
        bold ? "bg-secondary/40 font-bold" : ""
      }`}
    >
      <span className={bold ? "" : "text-muted-foreground"}>{label}</span>
      <span className="font-mono">{value > 0 ? fmtUsd(value) : "—"}</span>
    </div>
  );
}
