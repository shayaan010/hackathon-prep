import { useEffect, useRef, useState } from "react";
import { MessageCircle, X, Send, Sparkles, Scale } from "lucide-react";
import { CURRENT_MATTER, type Matter } from "@/lib/matter";
import { api } from "@/lib/api";

type Role = "user" | "assistant";
type Message = { id: string; role: Role; text: string };

function uid() {
  return Math.random().toString(36).slice(2, 10);
}

function greeting(matter: Matter): Message {
  return {
    id: uid(),
    role: "assistant",
    text:
      `Hi — I'm your case assistant for **${matter.name}** (${matter.caption}). ` +
      `I've got ${matter.authoritiesCount} authorities and ${matter.factors.length} contributing factors loaded. ` +
      `Ask me about coverage gaps, statutes on file, discovery, or a summary of the matter.`,
  };
}

async function fetchReply(
  input: string,
  history: Message[],
  matter: Matter,
): Promise<string> {
  try {
    const res = await api.chat({
      message: input,
      history: history.map((m) => ({ role: m.role, text: m.text })),
      matter_name: matter.name,
      matter_caption: matter.caption,
    });
    return res.text;
  } catch (err) {
    return `_(chat backend unavailable: ${err instanceof Error ? err.message : "unknown error"})_`;
  }
}

function renderText(text: string) {
  // Minimal **bold** + newline rendering, no markdown lib needed.
  const lines = text.split("\n");
  return lines.map((line, i) => {
    const parts = line.split(/(\*\*[^*]+\*\*)/g);
    return (
      <span key={i}>
        {parts.map((p, j) =>
          p.startsWith("**") && p.endsWith("**") ? (
            <strong key={j} className="font-semibold">{p.slice(2, -2)}</strong>
          ) : (
            <span key={j}>{p}</span>
          ),
        )}
        {i < lines.length - 1 && <br />}
      </span>
    );
  });
}

export function ChatWidget() {
  const matter = CURRENT_MATTER;
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState<Message[]>(() => [greeting(matter)]);
  const [input, setInput] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, open]);

  const send = async () => {
    const text = input.trim();
    if (!text) return;
    const userMsg: Message = { id: uid(), role: "user", text };
    const priorHistory = messages;
    setMessages((m) => [...m, userMsg]);
    setInput("");
    const reply = await fetchReply(text, priorHistory, matter);
    setMessages((m) => [...m, { id: uid(), role: "assistant", text: reply }]);
  };

  const onKey = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  };

  const suggestions = [
    "What's our coverage gap?",
    "Summarize this matter",
    "List statutes on file",
    "What's pending in discovery?",
  ];

  return (
    <>
      {/* Floating launcher */}
      {!open && (
        <button
          onClick={() => setOpen(true)}
          className="fixed bottom-6 right-6 z-50 h-14 w-14 rounded-full gradient-primary text-primary-foreground shadow-elegant grid place-items-center hover:scale-105 transition-transform"
          aria-label="Open case assistant"
        >
          <MessageCircle className="h-6 w-6" />
        </button>
      )}

      {/* Chat panel */}
      {open && (
        <div className="fixed bottom-6 right-6 z-50 w-[380px] max-w-[calc(100vw-2rem)] h-[560px] max-h-[calc(100vh-3rem)] rounded-xl border border-border bg-card shadow-elegant flex flex-col overflow-hidden">
          {/* Header */}
          <div className="px-4 py-3 border-b border-border flex items-start gap-3 bg-card">
            <div className="h-8 w-8 rounded-md gradient-primary grid place-items-center shrink-0">
              <Scale className="h-4 w-4 text-primary-foreground" />
            </div>
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-widest text-gold">
                <Sparkles className="h-3 w-3" /> Case Assistant
              </div>
              <div className="font-serif font-bold text-sm leading-snug truncate">{matter.name}</div>
              <div className="text-[11px] text-muted-foreground truncate">{matter.caption}</div>
            </div>
            <button
              onClick={() => setOpen(false)}
              className="text-muted-foreground hover:text-foreground p-1 -mr-1"
              aria-label="Close"
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          {/* Messages */}
          <div ref={scrollRef} className="flex-1 overflow-y-auto scrollbar-thin px-4 py-3 space-y-3 bg-secondary/30">
            {messages.map((m) => (
              <div
                key={m.id}
                className={[
                  "max-w-[85%] rounded-lg px-3 py-2 text-sm leading-relaxed",
                  m.role === "user"
                    ? "ml-auto bg-primary text-primary-foreground"
                    : "mr-auto bg-card border border-border text-foreground",
                ].join(" ")}
              >
                {renderText(m.text)}
              </div>
            ))}
          </div>

          {/* Suggestions (only when convo is fresh) */}
          {messages.length <= 1 && (
            <div className="px-4 pb-2 pt-1 flex flex-wrap gap-1.5 bg-secondary/30">
              {suggestions.map((s) => (
                <button
                  key={s}
                  onClick={async () => {
                    const userMsg: Message = { id: uid(), role: "user", text: s };
                    const priorHistory = messages;
                    setMessages((prev) => [...prev, userMsg]);
                    setInput("");
                    const reply = await fetchReply(s, priorHistory, matter);
                    setMessages((prev) => [
                      ...prev,
                      { id: uid(), role: "assistant", text: reply },
                    ]);
                  }}
                  className="text-[11px] px-2 py-1 rounded-full border border-border bg-card hover:border-primary/40 text-muted-foreground hover:text-foreground transition-colors"
                >
                  {s}
                </button>
              ))}
            </div>
          )}

          {/* Composer */}
          <div className="border-t border-border bg-card p-2 flex items-end gap-2">
            <textarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={onKey}
              rows={1}
              placeholder={`Ask about ${matter.name}…`}
              className="flex-1 resize-none max-h-32 min-h-9 rounded-md border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary/50"
            />
            <button
              onClick={send}
              disabled={!input.trim()}
              className="h-9 w-9 rounded-md bg-primary text-primary-foreground grid place-items-center disabled:opacity-40 hover:opacity-90"
              aria-label="Send"
            >
              <Send className="h-4 w-4" />
            </button>
          </div>
        </div>
      )}
    </>
  );
}
