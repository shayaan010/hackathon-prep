import { createFileRoute } from "@tanstack/react-router";
import { AppHeader } from "@/components/AppHeader";
import { STATUTES, FACTOR_CATEGORIES } from "@/lib/statutes";
import { FolderKanban, Plus, GripVertical } from "lucide-react";

export const Route = createFileRoute("/organizer")({
  head: () => ({
    meta: [
      { title: "Organizer — Lex Harvester" },
      { name: "description", content: "Workspace for PI attorneys to organize statutes, case law, and damages comparables." },
    ],
  }),
  component: OrganizerPage,
});

const COLUMNS = [
  { id: "intake", label: "Intake", tint: "bg-secondary" },
  { id: "theory", label: "Theory of Liability", tint: "bg-primary/10" },
  { id: "discovery", label: "Discovery Pending", tint: "bg-gold/15" },
  { id: "ready", label: "Trial Ready", tint: "bg-primary/15" },
];

function OrganizerPage() {
  // assign demo statutes to columns
  const board: Record<string, typeof STATUTES> = {
    intake: STATUTES.slice(0, 3),
    theory: STATUTES.slice(3, 6),
    discovery: STATUTES.slice(6, 9),
    ready: STATUTES.slice(9, 12),
  };

  return (
    <div className="h-screen flex flex-col bg-background">
      <AppHeader />

      <div className="border-b border-border bg-card/60">
        <div className="px-6 py-5 flex items-end justify-between">
          <div>
            <div className="flex items-center gap-2 text-[11px] font-mono uppercase tracking-widest text-gold mb-1">
              <FolderKanban className="h-3 w-3" /> Matter Workspace
            </div>
            <h1 className="font-serif text-2xl font-bold">
              Reyes v. Western Logistics — Rear-end collision, I-880
            </h1>
            <p className="text-sm text-muted-foreground mt-1">
              17 authorities · 4 contributing factors · Coverage gap: <span className="text-destructive font-medium">commercial trucking regulations</span>
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button className="h-9 px-3 rounded-md border border-border bg-card text-sm font-medium hover:bg-secondary">
              Coverage report
            </button>
            <button className="h-9 px-3 rounded-md bg-primary text-primary-foreground text-sm font-medium flex items-center gap-1.5 hover:opacity-90">
              <Plus className="h-4 w-4" /> Add statute
            </button>
          </div>
        </div>

        {/* Factor coverage strip */}
        <div className="px-6 pb-4">
          <div className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground mb-2">
            Contributing factor coverage
          </div>
          <div className="flex flex-wrap gap-1.5">
            {FACTOR_CATEGORIES.map((f) => {
              const count = STATUTES.filter((s) => s.factors.includes(f)).length;
              const has = count > 0;
              return (
                <div
                  key={f}
                  className={[
                    "text-[11px] px-2 py-1 rounded-md border flex items-center gap-1.5",
                    has
                      ? "bg-card border-border text-foreground"
                      : "bg-destructive/5 border-destructive/30 text-destructive",
                  ].join(" ")}
                >
                  {f}
                  <span className={`font-mono text-[10px] ${has ? "text-muted-foreground" : ""}`}>
                    {count}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Kanban */}
      <div className="flex-1 overflow-x-auto overflow-y-hidden p-6">
        <div className="flex gap-4 h-full min-w-max">
          {COLUMNS.map((col) => (
            <div key={col.id} className="w-80 flex flex-col rounded-xl border border-border bg-card/50 overflow-hidden">
              <div className={`px-4 py-3 border-b border-border flex items-center justify-between ${col.tint}`}>
                <div className="flex items-center gap-2">
                  <span className="font-serif font-bold text-sm">{col.label}</span>
                  <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-card border border-border text-muted-foreground">
                    {board[col.id].length}
                  </span>
                </div>
                <button className="text-muted-foreground hover:text-foreground">
                  <Plus className="h-4 w-4" />
                </button>
              </div>
              <div className="flex-1 overflow-y-auto scrollbar-thin p-2 space-y-2">
                {board[col.id].map((s) => (
                  <div
                    key={s.id}
                    className="group rounded-lg border border-border bg-background p-3 hover:border-primary/40 hover:shadow-soft transition-all cursor-pointer"
                  >
                    <div className="flex items-start gap-2">
                      <GripVertical className="h-3.5 w-3.5 text-muted-foreground/40 mt-0.5 opacity-0 group-hover:opacity-100" />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-1.5 mb-1">
                          <span className="font-mono text-[10px] text-gold uppercase tracking-wider">
                            {s.jurisdiction}
                          </span>
                          <span className="font-mono text-[10px] text-muted-foreground">§{s.section}</span>
                        </div>
                        <div className="font-serif font-bold text-sm leading-snug mb-1">
                          {s.title}
                        </div>
                        <div className="flex flex-wrap gap-1 mt-2">
                          {s.factors.slice(0, 2).map((f) => (
                            <span
                              key={f}
                              className="text-[9px] px-1.5 py-0.5 rounded bg-secondary text-secondary-foreground"
                            >
                              {f}
                            </span>
                          ))}
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
