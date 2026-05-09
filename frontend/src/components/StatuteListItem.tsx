import { Statute } from "@/lib/statutes";
import { Bookmark, MapPin } from "lucide-react";

interface Props {
  statute: Statute;
  active: boolean;
  saved: boolean;
  onSelect: () => void;
}

export function StatuteListItem({ statute, active, saved, onSelect }: Props) {
  return (
    <button
      onClick={onSelect}
      className={[
        "w-full text-left p-4 border-l-2 transition-all group relative",
        active
          ? "bg-card border-l-gold shadow-soft"
          : "border-l-transparent hover:bg-card/60 hover:border-l-primary/40",
      ].join(" ")}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1.5">
            <span className="font-mono text-[11px] font-medium text-gold uppercase tracking-wider">
              {statute.jurisdiction} · {statute.code}
            </span>
            <span className="font-mono text-[11px] text-muted-foreground">§ {statute.section}</span>
          </div>
          <h3 className="font-serif text-[15px] font-bold text-foreground leading-snug mb-1.5">
            {statute.title}
          </h3>
          <p className="text-xs text-muted-foreground line-clamp-2 leading-relaxed">
            {statute.summary}
          </p>
          <div className="flex items-center gap-1.5 mt-2.5 flex-wrap">
            {statute.factors.slice(0, 2).map((f) => (
              <span
                key={f}
                className="text-[10px] px-1.5 py-0.5 rounded bg-secondary text-secondary-foreground font-medium"
              >
                {f}
              </span>
            ))}
            <span className="text-[10px] text-muted-foreground flex items-center gap-1 ml-auto">
              <MapPin className="h-2.5 w-2.5" />
              {statute.jurisdictionLabel}
            </span>
          </div>
        </div>
        {saved && (
          <span
            className="shrink-0 p-1 text-gold"
            title="Saved in a project"
            aria-label="Saved"
          >
            <Bookmark className="h-4 w-4 fill-current" />
          </span>
        )}
      </div>
    </button>
  );
}
