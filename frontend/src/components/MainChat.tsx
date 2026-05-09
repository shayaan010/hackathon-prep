import { useEffect, useRef, useState } from "react";
import { Send, Sparkles, Scale, Loader2 } from "lucide-react";
import { api } from "@/lib/api";
import type { Statute } from "@/lib/statutes";

type Role = "user" | "assistant";
type Msg = {
  id: string;
  role: Role;
  text: string;
  statutes?: Statute[];
  pending?: boolean;
};

const uid = () => Math.random().toString(36).slice(2, 10);

const SUGGESTIONS = [
  "Fleeing a police officer",
  "Reckless driving statutes",
  "What governs hit and run?",
  "Cell phone use while driving",
];

const GREETING_TEXT =
  "Hi — I'm your statute search assistant. Ask me about a contributing factor, a vehicle code section, or describe a fact pattern, and I'll surface the relevant statutes.";

interface Props {
  statutes: Statute[];
  onSelectStatute: (id: string) => void;
}

function renderText(text: string) {
  const lines = text.split("\n");
  return lines.map((line, i) => {
    const parts = line.split(/(\*\*[^*]+\*\*)/g);
    return (
      <span key={i}>
        {parts.map((p, j) =>
          p.startsWith("**") && p.endsWith("**") ? (
            <strong key={j} className="font-semibold">
              {p.slice(2, -2)}
            </strong>
          ) : (
            <span key={j}>{p}</span>
          ),
        )}
        {i < lines.length - 1 && <br />}
      </span>
    );
  });
}

export function MainChat({ statutes, onSelectStatute }: Props) {
  const [messages, setMessages] = useState<Msg[]>(() => [
    { id: uid(), role: "assistant", text: GREETING_TEXT },
  ]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  const send = async (raw: string) => {
    const text = raw.trim();
    if (!text || busy) return;

    const userMsg: Msg = { id: uid(), role: "user", text };
    const placeholderId = uid();
    const placeholder: Msg = { id: placeholderId, role: "assistant", text: "", pending: true };
    const priorHistory = messages.map((m) => ({ role: m.role, text: m.text }));

    setMessages((m) => [...m, userMsg, placeholder]);
    setInput("");
    setBusy(true);

    try {
      const [chatRes, hitsRes] = await Promise.allSettled([
        api.chat({ message: text, history: priorHistory }),
        api.search(text, 6),
      ]);

      const answer =
        chatRes.status === "fulfilled"
          ? chatRes.value.text
          : `_(chat backend unavailable: ${
              chatRes.reason instanceof Error ? chatRes.reason.message : "unknown error"
            })_`;

      const matched: Statute[] = [];
      if (hitsRes.status === "fulfilled") {
        const seen = new Set<string>();
        for (const h of hitsRes.value) {
          const stat = statutes.find((s) => s.source.url === h.source_url);
          if (stat && !seen.has(stat.id)) {
            seen.add(stat.id);
            matched.push(stat);
            if (matched.length >= 4) break;
          }
        }
      }

      setMessages((m) =>
        m.map((msg) =>
          msg.id === placeholderId
            ? { ...msg, text: answer, statutes: matched, pending: false }
            : msg,
        ),
      );
    } finally {
      setBusy(false);
    }
  };

  const onKey = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send(input);
    }
  };

  const showSuggestions = messages.length <= 1 && !busy;

  return (
    <div className="h-full flex flex-col bg-background">
      <div className="border-b border-border bg-card px-6 py-3 flex items-center gap-3">
        <div className="h-9 w-9 rounded-md gradient-primary grid place-items-center shrink-0">
          <Scale className="h-4 w-4 text-primary-foreground" />
        </div>
        <div className="min-w-0">
          <div className="flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-widest text-gold">
            <Sparkles className="h-3 w-3" /> Statute Assistant
          </div>
          <div className="font-serif font-bold text-sm leading-tight">
            Ask a question, get the relevant statutes.
          </div>
        </div>
      </div>

      <div ref={scrollRef} className="flex-1 overflow-y-auto scrollbar-thin px-6 py-6 bg-secondary/30">
        <div className="max-w-3xl mx-auto space-y-4">
          {messages.map((m) => (
            <div key={m.id} className="flex">
              <div
                className={[
                  "rounded-lg px-4 py-3 text-sm leading-relaxed",
                  m.role === "user"
                    ? "ml-auto max-w-[80%] bg-primary text-primary-foreground"
                    : "mr-auto max-w-[90%] bg-card border border-border text-foreground",
                ].join(" ")}
              >
                {m.pending ? (
                  <span className="inline-flex items-center gap-2 text-muted-foreground">
                    <Loader2 className="h-3.5 w-3.5 animate-spin" /> Thinking…
                  </span>
                ) : (
                  <>
                    {renderText(m.text)}
                    {m.statutes && m.statutes.length > 0 && (
                      <div className="mt-4 pt-3 border-t border-border space-y-2">
                        <div className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">
                          Related statutes
                        </div>
                        {m.statutes.map((s) => (
                          <ChatStatuteCard
                            key={s.id}
                            statute={s}
                            onClick={() => onSelectStatute(s.id)}
                          />
                        ))}
                      </div>
                    )}
                  </>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {showSuggestions && (
        <div className="px-6 pt-2 pb-1 bg-secondary/30">
          <div className="max-w-3xl mx-auto flex flex-wrap gap-1.5">
            {SUGGESTIONS.map((s) => (
              <button
                key={s}
                onClick={() => send(s)}
                className="text-xs px-3 py-1.5 rounded-full border border-border bg-card hover:border-primary/40 text-muted-foreground hover:text-foreground transition-colors"
              >
                {s}
              </button>
            ))}
          </div>
        </div>
      )}

      <div className="border-t border-border bg-card p-3">
        <div className="max-w-3xl mx-auto flex items-end gap-2">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={onKey}
            rows={1}
            placeholder="Search statutes by topic, section, or fact pattern…"
            className="flex-1 resize-none max-h-32 min-h-10 rounded-md border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary/50"
          />
          <button
            onClick={() => send(input)}
            disabled={!input.trim() || busy}
            className="h-10 w-10 rounded-md bg-primary text-primary-foreground grid place-items-center disabled:opacity-40 hover:opacity-90"
            aria-label="Send"
          >
            <Send className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  );
}

function ChatStatuteCard({
  statute,
  onClick,
}: {
  statute: Statute;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className="w-full text-left rounded-md border border-border bg-card hover:border-primary/40 hover:bg-secondary/40 transition-colors px-3 py-2.5"
    >
      <div className="flex items-center gap-2 mb-1">
        <span className="font-mono text-[10px] font-medium text-gold uppercase tracking-wider">
          {statute.jurisdiction} · {statute.code}
        </span>
        <span className="font-mono text-[10px] text-muted-foreground">
          § {statute.section}
        </span>
      </div>
      <div className="font-serif text-sm font-bold leading-tight">{statute.title}</div>
      <div className="text-xs text-muted-foreground line-clamp-2 mt-1 leading-relaxed">
        {statute.summary}
      </div>
    </button>
  );
}
