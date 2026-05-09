import { createFileRoute } from "@tanstack/react-router";
import { AppHeader } from "@/components/AppHeader";
import { STATUTES, FACTOR_CATEGORIES } from "@/lib/statutes";
import { Network, AlertTriangle, CheckCircle2 } from "lucide-react";

export const Route = createFileRoute("/coverage")({
  head: () => ({
    meta: [
      { title: "Coverage — Lex Harvester" },
      { name: "description", content: "Coverage gaps across jurisdictions and contributing factors." },
    ],
  }),
  component: CoveragePage,
});

const ALL_JURISDICTIONS = ["California", "New York", "Texas", "Florida", "Illinois", "Washington", "Arizona", "Georgia"];

function CoveragePage() {
  const matrix = ALL_JURISDICTIONS.map((j) => ({
    j,
    cells: FACTOR_CATEGORIES.map((f) => ({
      f,
      count: STATUTES.filter((s) => s.jurisdictionLabel === j && s.factors.includes(f)).length,
    })),
  }));

  const total = ALL_JURISDICTIONS.length * FACTOR_CATEGORIES.length;
  const covered = matrix.flatMap((r) => r.cells).filter((c) => c.count > 0).length;
  const pct = Math.round((covered / total) * 100);

  return (
    <div className="h-screen flex flex-col bg-background">
      <AppHeader />

      <div className="border-b border-border bg-card/60 px-6 py-5">
        <div className="flex items-center gap-2 text-[11px] font-mono uppercase tracking-widest text-gold mb-1">
          <Network className="h-3 w-3" /> Harvester Coverage
        </div>
        <div className="flex items-end justify-between">
          <div>
            <h1 className="font-serif text-2xl font-bold">Jurisdiction × Contributing Factor</h1>
            <p className="text-sm text-muted-foreground mt-1">
              Where the database has statutes — and where to invest research next.
            </p>
          </div>
          <div className="flex gap-6">
            <Stat label="Total cells" value={total.toString()} />
            <Stat label="Covered" value={`${covered}`} accent="primary" />
            <Stat label="Coverage" value={`${pct}%`} accent="gold" />
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-auto scrollbar-thin p-6">
        <div className="inline-block min-w-full">
          <table className="border-separate border-spacing-1">
            <thead>
              <tr>
                <th className="sticky left-0 bg-background z-10 text-left text-[10px] font-mono uppercase tracking-widest text-muted-foreground px-3 py-2 min-w-[140px]">
                  Jurisdiction
                </th>
                {FACTOR_CATEGORIES.map((f) => (
                  <th
                    key={f}
                    className="text-[10px] font-mono uppercase tracking-wider text-muted-foreground px-2 py-2 align-bottom"
                  >
                    <div className="rotate-[-35deg] origin-bottom-left whitespace-nowrap h-20 w-6 flex items-end">
                      {f}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {matrix.map((row) => (
                <tr key={row.j}>
                  <td className="sticky left-0 bg-background z-10 font-serif text-sm font-bold pr-4 py-1">
                    {row.j}
                  </td>
                  {row.cells.map((c) => {
                    const intensity =
                      c.count === 0 ? 0 : Math.min(1, c.count / 2);
                    return (
                      <td key={c.f} className="p-0">
                        <div
                          className="h-9 w-9 rounded-md border grid place-items-center text-[11px] font-mono font-medium transition-all hover:scale-110 cursor-pointer"
                          style={
                            c.count === 0
                              ? {
                                  background:
                                    "color-mix(in oklab, var(--destructive) 6%, transparent)",
                                  borderColor:
                                    "color-mix(in oklab, var(--destructive) 25%, transparent)",
                                  color: "var(--destructive)",
                                }
                              : {
                                  background: `color-mix(in oklab, var(--primary) ${15 + intensity * 60}%, transparent)`,
                                  borderColor: "color-mix(in oklab, var(--primary) 40%, transparent)",
                                  color: intensity > 0.4 ? "var(--primary-foreground)" : "var(--primary)",
                                }
                          }
                          title={`${row.j} · ${c.f}: ${c.count}`}
                        >
                          {c.count > 0 ? c.count : "·"}
                        </div>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="mt-8 grid md:grid-cols-2 gap-4 max-w-4xl">
          <GapCard
            icon={<AlertTriangle className="h-4 w-4" />}
            title="Highest-leverage gaps"
            tone="destructive"
            items={[
              "Florida · DUI — 0 statutes",
              "Illinois · Pedestrian Right-of-Way — 0 statutes",
              "Washington · Improper Passing — 0 statutes",
            ]}
          />
          <GapCard
            icon={<CheckCircle2 className="h-4 w-4" />}
            title="Strongest coverage"
            tone="primary"
            items={[
              "California · Following Too Closely — 1 statute, 1 case",
              "California · DUI — 1 statute, 1 case",
              "Multi-state · Following Too Closely — 3 jurisdictions",
            ]}
          />
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value, accent }: { label: string; value: string; accent?: "primary" | "gold" }) {
  return (
    <div className="text-right">
      <div className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">{label}</div>
      <div
        className={[
          "font-serif text-2xl font-bold",
          accent === "primary" && "text-primary",
          accent === "gold" && "text-gold",
        ].filter(Boolean).join(" ")}
      >
        {value}
      </div>
    </div>
  );
}

function GapCard({
  icon, title, items, tone,
}: { icon: React.ReactNode; title: string; items: string[]; tone: "destructive" | "primary" }) {
  return (
    <div className="rounded-lg border border-border bg-card p-5">
      <div
        className={[
          "flex items-center gap-2 text-sm font-semibold mb-3",
          tone === "destructive" ? "text-destructive" : "text-primary",
        ].join(" ")}
      >
        {icon} {title}
      </div>
      <ul className="space-y-1.5 text-sm">
        {items.map((it) => (
          <li key={it} className="flex items-start gap-2">
            <span
              className={[
                "h-1.5 w-1.5 rounded-full mt-1.5 shrink-0",
                tone === "destructive" ? "bg-destructive" : "bg-primary",
              ].join(" ")}
            />
            <span className="text-muted-foreground">{it}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
