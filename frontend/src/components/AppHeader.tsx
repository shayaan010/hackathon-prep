import { Link, useRouterState } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
<<<<<<< Updated upstream
import { Scale, BookOpen, FolderOpen, Upload, DollarSign } from "lucide-react";
=======
import { Scale, BookOpen, FolderOpen, Upload } from "lucide-react";
>>>>>>> Stashed changes
import { api } from "@/lib/api";

const nav = [
  { to: "/", label: "Harvester", icon: BookOpen },
  { to: "/projects", label: "Projects", icon: FolderOpen },
<<<<<<< Updated upstream
  { to: "/comparables", label: "Comparables", icon: DollarSign },
=======
>>>>>>> Stashed changes
  { to: "/upload", label: "Upload", icon: Upload },
] as const;

export function AppHeader() {
  const path = useRouterState({ select: (s) => s.location.pathname });
  const { data: stats } = useQuery({
    queryKey: ["stats"],
    queryFn: api.stats,
    staleTime: 30_000,
    refetchOnWindowFocus: false,
  });
  const { data } = useQuery({
    queryKey: ["statutes"],
    queryFn: () => api.statutes(),
    staleTime: 60_000,
  });
  const statutes = data?.items;
  const jurisdictionCount = statutes
    ? new Set(statutes.map((s) => s.jurisdictionLabel)).size
    : 0;
  // Prefer the curated DB total — it's the real corpus size, not the
  // ≤50-row page the FE just received.
  const docCount = data?.total ?? stats?.documents ?? 0;
  const headerText = docCount
    ? `${docCount} statute${docCount === 1 ? "" : "s"} · ${jurisdictionCount} jurisdiction${jurisdictionCount === 1 ? "" : "s"}`
    : "loading…";
  return (
    <header className="border-b border-border/70 bg-card/80 backdrop-blur-md sticky top-0 z-30">
      <div className="flex items-center h-14 px-5 gap-8">
        <Link to="/" className="flex items-center gap-2.5 group">
          <div className="h-8 w-8 rounded-md gradient-primary grid place-items-center shadow-elegant">
            <Scale className="h-4 w-4 text-primary-foreground" />
          </div>
          <div className="flex items-baseline gap-2">
            <span className="font-serif text-lg font-bold tracking-tight">Lex</span>
            <span className="font-serif italic text-gold text-base -ml-1">Harvester</span>
          </div>
        </Link>
        <nav className="flex items-center gap-1">
          {nav.map((n) => {
            const active = path === n.to;
            const Icon = n.icon;
            return (
              <Link
                key={n.to}
                to={n.to}
                className={[
                  "flex items-center gap-2 px-3 h-9 rounded-md text-sm font-medium transition-colors",
                  active
                    ? "bg-primary text-primary-foreground shadow-soft"
                    : "text-muted-foreground hover:text-foreground hover:bg-secondary",
                ].join(" ")}
              >
                <Icon className="h-3.5 w-3.5" />
                {n.label}
              </Link>
            );
          })}
        </nav>
        <div className="ml-auto flex items-center gap-3">
          <div className="hidden md:flex items-center gap-2 text-xs text-muted-foreground">
            <span className="h-1.5 w-1.5 rounded-full bg-primary animate-pulse" />
            <span className="font-mono">{headerText}</span>
          </div>
          <div className="h-8 w-8 rounded-full bg-gradient-to-br from-primary to-gold grid place-items-center text-primary-foreground text-xs font-semibold">
            JM
          </div>
        </div>
      </div>
    </header>
  );
}
