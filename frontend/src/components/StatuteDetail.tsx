import { Statute } from "@/lib/statutes";
import { ExternalLink, Link2, BookMarked, Calendar, Copy, FileText } from "lucide-react";
import { useState } from "react";
import { SaveToProject } from "@/components/SaveToProject";

interface Props {
  statute: Statute | null;
}

export function StatuteDetail({ statute }: Props) {
  const [note, setNote] = useState("");

  if (!statute) {
    return (
      <div className="h-full grid place-items-center p-12">
        <div className="text-center max-w-sm">
          <div className="h-16 w-16 rounded-full gradient-parchment border border-border grid place-items-center mx-auto mb-4">
            <FileText className="h-7 w-7 text-muted-foreground" />
          </div>
          <h3 className="font-serif text-lg font-bold mb-2">No statute selected</h3>
          <p className="text-sm text-muted-foreground">
            Search or select a statute from the left to view its full text, related authorities, and case interpretations.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full overflow-y-auto scrollbar-thin">
      {/* Title block */}
      <div className="gradient-parchment border-b border-border px-8 pt-8 pb-6">
        <div className="flex items-center gap-2 mb-3">
          <span className="font-mono text-[11px] font-semibold uppercase tracking-widest text-gold">
            {statute.jurisdictionLabel} · {statute.code}
          </span>
          <span className="text-muted-foreground">·</span>
          <span className="font-mono text-[11px] text-muted-foreground flex items-center gap-1">
            <Calendar className="h-3 w-3" /> Verified {statute.lastVerified}
          </span>
        </div>
        <div className="flex items-start gap-4">
          <div>
            <div className="font-mono text-3xl font-light text-primary mb-1">§ {statute.section}</div>
            <h1 className="font-serif text-3xl font-bold leading-tight max-w-2xl">{statute.title}</h1>
          </div>
          <div className="ml-auto flex items-center gap-1.5">
            <SaveToProject statuteId={statute.id} />
            <button className="h-9 w-9 rounded-md border border-border bg-card hover:bg-secondary grid place-items-center">
              <Copy className="h-4 w-4" />
            </button>
            <a
              href={statute.source.url}
              target="_blank"
              rel="noreferrer"
              className="h-9 px-3 rounded-md bg-primary text-primary-foreground text-sm font-medium flex items-center gap-1.5 hover:opacity-90 transition-opacity"
            >
              Source <ExternalLink className="h-3.5 w-3.5" />
            </a>
          </div>
        </div>

        <div className="flex items-center gap-1.5 mt-5 flex-wrap">
          {statute.factors.map((f) => (
            <span
              key={f}
              className="text-xs px-2.5 py-1 rounded-full bg-primary/10 text-primary font-medium border border-primary/20"
            >
              {f}
            </span>
          ))}
        </div>
      </div>

      {/* Body */}
      <div className="px-8 py-8 space-y-8 max-w-4xl">
        <section>
          <h2 className="text-[11px] font-mono uppercase tracking-widest text-muted-foreground mb-3">
            Statute Text
          </h2>
          <blockquote className="border-l-4 border-gold pl-5 py-1 font-serif text-base leading-relaxed text-foreground">
            {statute.text}
          </blockquote>
        </section>

        <section>
          <h2 className="text-[11px] font-mono uppercase tracking-widest text-muted-foreground mb-3 flex items-center gap-2">
            <Link2 className="h-3 w-3" /> Related Sections
          </h2>
          <div className="flex flex-wrap gap-2">
            {statute.related.map((r) => (
              <button
                key={r}
                className="font-mono text-sm px-3 py-1.5 rounded-md border border-border bg-card hover:bg-secondary hover:border-primary/40 transition-colors"
              >
                § {r}
              </button>
            ))}
          </div>
        </section>

        {statute.cases.length > 0 && (
          <section>
            <h2 className="text-[11px] font-mono uppercase tracking-widest text-muted-foreground mb-3 flex items-center gap-2">
              <BookMarked className="h-3 w-3" /> Interpretive Case Law
            </h2>
            <div className="divide-y divide-border border border-border rounded-lg overflow-hidden bg-card">
              {statute.cases.map((c) => (
                <div key={c.citation} className="px-4 py-3 hover:bg-secondary/40 cursor-pointer">
                  <div className="font-serif italic font-semibold text-sm">{c.name}</div>
                  <div className="font-mono text-xs text-muted-foreground mt-0.5">{c.citation}</div>
                </div>
              ))}
            </div>
          </section>
        )}

        <section>
          <h2 className="text-[11px] font-mono uppercase tracking-widest text-muted-foreground mb-3">
            Source Authority
          </h2>
          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-sm font-medium">{statute.source.publisher}</div>
            <a
              href={statute.source.url}
              target="_blank"
              rel="noreferrer"
              className="text-xs font-mono text-primary hover:underline break-all mt-1 block"
            >
              {statute.source.url}
            </a>
          </div>
        </section>

        <section>
          <h2 className="text-[11px] font-mono uppercase tracking-widest text-muted-foreground mb-3">
            Case Notes
          </h2>
          <textarea
            value={note}
            onChange={(e) => setNote(e.target.value)}
            placeholder="Add notes for this statute — facts, theories, opposing arguments…"
            className="w-full min-h-[120px] rounded-lg border border-border bg-card p-4 text-sm font-sans focus:outline-none focus:ring-2 focus:ring-primary/40 resize-y"
          />
        </section>
      </div>
    </div>
  );
}
