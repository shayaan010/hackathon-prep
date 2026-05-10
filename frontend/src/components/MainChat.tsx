import { useEffect, useRef, useState } from "react";
import { Send, Sparkles, Scale, Loader2, Paperclip, X, Eraser } from "lucide-react";
import { api, type AttachedFile } from "@/lib/api";
import type { Statute } from "@/lib/statutes";
import {
  chatStore,
  useChatHistory,
  type ChatMessage as Msg,
} from "@/lib/chat-store";

const uid = () => Math.random().toString(36).slice(2, 10);

// Used to bias the Related Statutes panel toward the jurisdictions actually
// being discussed. We scan both the user's message and Claude's reply because
// the question may be jurisdiction-implicit ("DUI") while the answer cites a
// specific code ("Cal. Veh. Code § 23152").
type JurisdictionCode = "CA" | "NY" | "TX";

const JURISDICTION_PATTERNS: Array<{ code: JurisdictionCode; re: RegExp }> = [
  {
    code: "CA",
    re: /\b(california|calif\.|cal\.?\s+veh|cal\.?\s+vehicle\s+code)\b/i,
  },
  {
    code: "NY",
    re: /\b(new\s+york|n\.?\s*y\.?\s+(?:veh|vat)|new\s+york\s+vehicle|vehicle\s+and\s+traffic\s+law)\b/i,
  },
  {
    code: "TX",
    re: /\b(texas|tex\.?\s+transp|texas\s+transportation|transportation\s+code)\b/i,
  },
];

function detectJurisdictions(...texts: string[]): Set<JurisdictionCode> {
  const blob = texts.filter(Boolean).join(" ");
  const out = new Set<JurisdictionCode>();
  for (const { code, re } of JURISDICTION_PATTERNS) {
    if (re.test(blob)) out.add(code);
  }
  return out;
}

// Pull every "§ <num>" / "§§ <num>, <num>" reference out of Claude's reply,
// inferring jurisdiction from a small window before each match. Used to surface
// the exact statute cards Claude cited rather than whatever ranks high on a
// parallel semantic search.
const CITATION_BLOCK_RE = /§§?\s*(\d+(?:\.\d+)?(?:\([a-z0-9]\))?(?:\s*,\s*\d+(?:\.\d+)?(?:\([a-z0-9]\))?)*)/gi;
const PREFIX_PATTERNS: Array<{ code: JurisdictionCode; re: RegExp }> = [
  { code: "CA", re: /\b(cal\.?\s+veh|california\s+vehicle|cal\.?\s+vehicle)\b/i },
  { code: "NY", re: /\b(n\.?\s*y\.?\s*(?:veh|vat)|vehicle\s+and\s+traffic|new\s+york\s+(?:veh|vehicle))\b/i },
  { code: "TX", re: /\b(tex\.?\s+transp|texas\s+transp|transportation\s+code)\b/i },
];

function extractCitedSections(text: string): Array<{ jurisdiction?: JurisdictionCode; section: string }> {
  if (!text) return [];
  const out: Array<{ jurisdiction?: JurisdictionCode; section: string }> = [];
  const seen = new Set<string>();
  let m: RegExpExecArray | null;
  CITATION_BLOCK_RE.lastIndex = 0;
  while ((m = CITATION_BLOCK_RE.exec(text))) {
    const start = Math.max(0, m.index - 120);
    const window = text.slice(start, m.index);
    let jur: JurisdictionCode | undefined;
    for (const { code, re } of PREFIX_PATTERNS) {
      if (re.test(window)) {
        jur = code;
        break;
      }
    }
    for (const raw of m[1].split(",")) {
      const sec = raw.trim();
      if (!sec) continue;
      const key = `${jur ?? "?"}|${sec}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ jurisdiction: jur, section: sec });
    }
  }
  return out;
}

function resolveCitedStatutes(
  reply: string,
  statutes: Statute[],
  fallbackJurisdictions: Set<JurisdictionCode>,
): Statute[] {
  const cites = extractCitedSections(reply);
  const out: Statute[] = [];
  const seen = new Set<string>();
  for (const c of cites) {
    const baseSection = c.section.split("(")[0];
    const candidates = statutes.filter((s) => {
      const sBase = (s.section || "").split("(")[0];
      if (sBase !== baseSection) return false;
      if (c.jurisdiction) return s.jurisdiction === c.jurisdiction;
      // No code prefix near the citation — fall back to the jurisdictions
      // mentioned anywhere in the conversation. If none, match any state.
      if (fallbackJurisdictions.size > 0) {
        return fallbackJurisdictions.has(s.jurisdiction as JurisdictionCode);
      }
      return true;
    });
    for (const stat of candidates) {
      if (seen.has(stat.id)) continue;
      seen.add(stat.id);
      out.push(stat);
    }
  }
  return out;
}

const SUGGESTIONS = [
  "Fleeing a police officer",
  "Reckless driving statutes",
  "What governs hit and run?",
  "Cell phone use while driving",
];

const GREETING_TEXT =
  "Hi — I'm your statute search assistant. Ask me about a contributing factor, a vehicle code section, or describe a fact pattern, and I'll surface the relevant statutes.";

interface Props {
  onSelectStatute: (id: string) => void;
  // Called when the chat reply includes statutes the model pulled via tools.
  // The parent merges these into a lookup map so clicking a chat-card opens
  // the StatuteDetail drawer even for statutes not currently in the sidebar.
  onChatStatutes?: (statutes: Statute[]) => void;
}

function linkify(text: string) {
  const URL_RE = /(https?:\/\/[^\s)\]"'>]+)/g;
  const parts: (string | React.ReactNode)[] = [];
  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = URL_RE.exec(text)) !== null) {
    if (m.index > last) parts.push(text.slice(last, m.index));
    parts.push(
      <a
        key={m.index}
        href={m[0]}
        target="_blank"
        rel="noreferrer"
        className="text-blue-400 underline hover:text-blue-300"
      >
        {m[0]}
      </a>,
    );
    last = m.index + m[0].length;
  }
  if (last < text.length) parts.push(text.slice(last));
  return parts.length > 0 ? parts : text;
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
            <span key={j}>{linkify(p)}</span>
          ),
        )}
        {i < lines.length - 1 && <br />}
      </span>
    );
  });
}

export function MainChat({ onSelectStatute, onChatStatutes }: Props) {
  const [messages, updateMessages] = useChatHistory();
  // Seed a one-shot greeting if this is the first visit.
  useEffect(() => {
    if (chatStore.get().length === 0) {
      chatStore.set([{ id: uid(), role: "assistant", text: GREETING_TEXT }]);
    }
  }, []);

  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [attachments, setAttachments] = useState<AttachedFile[]>([]);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  const onFilePicked = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = ""; // allow re-uploading the same name
    if (!file) return;
    setUploadError(null);
    setUploading(true);
    try {
      const res = await api.upload(file, false);
      setAttachments((prev) => [
        ...prev,
        { filename: res.filename, text: res.text },
      ]);
    } catch (err) {
      setUploadError(err instanceof Error ? err.message : "Upload failed");
    } finally {
      setUploading(false);
    }
  };

  const removeAttachment = (filename: string) => {
    setAttachments((prev) => prev.filter((a) => a.filename !== filename));
  };

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  const send = async (raw: string) => {
    const text = raw.trim();
    if ((!text && attachments.length === 0) || busy) return;

    const attachedNow = attachments;
    const filenames = attachedNow.map((a) => a.filename);
    const displayText =
      text ||
      `Reading attached file${attachedNow.length > 1 ? "s" : ""}: ${filenames.join(", ")}`;
    const userMsg: Msg = {
      id: uid(),
      role: "user",
      text: displayText,
      attachments: filenames.length ? filenames : undefined,
    };
    const placeholderId = uid();
    const placeholder: Msg = { id: placeholderId, role: "assistant", text: "", pending: true };
    const priorHistory = messages.map((m) => ({ role: m.role, text: m.text }));

    updateMessages((m) => [...m, userMsg, placeholder]);
    setInput("");
    setAttachments([]);
    setBusy(true);

<<<<<<< Updated upstream
    // Detect jurisdictions from the user's message ONLY — not Claude's reply.
    // A TX-focused question shouldn't drag in CA just because Claude mentioned
    // it tangentially. Pass the result through to /api/search so the database
    // does the filtering (otherwise off-state hits eat the top_k budget).
    const jurisdictions = Array.from(detectJurisdictions(text));
    try {
      const [chatRes, hitsRes] = await Promise.allSettled([
        api.chat({
          message: text || "Please analyze the attached file(s).",
          history: priorHistory,
          attached_files: attachedNow,
        }),
        api.search(text || filenames.join(" "), 6, jurisdictions),
      ]);
=======
    let answer: string;
    let pulled: Statute[] = [];
    try {
      const chatResult = await api.chat({
        message: text || "Please analyze the attached file(s).",
        history: priorHistory,
        attached_files: attachedNow,
      });
      answer = chatResult.text;
      pulled = chatResult.statutes ?? [];
    } catch (err) {
      answer = `_(chat backend unavailable: ${
        err instanceof Error ? err.message : "unknown error"
      })_`;
    }
>>>>>>> Stashed changes

    // Cap at 4 cards per message; same limit as before, just now sourced from
    // the model's actual tool calls instead of regex over the response text.
    const matched = pulled.slice(0, 4);

<<<<<<< Updated upstream
      // Prefer the EXACT statutes Claude cited in its reply (parsed from
      // citation patterns like "Cal. Veh. Code § 23152"). Only fall back to
      // the parallel /api/search hits when Claude didn't cite anything specific.
      const conversationJurisdictions = detectJurisdictions(text, answer);
      const cited = resolveCitedStatutes(answer, statutes, conversationJurisdictions).slice(0, 4);
      let matched: Statute[] = cited;
      if (matched.length === 0 && hitsRes.status === "fulfilled") {
        const seen = new Set<string>();
        const fallback: Statute[] = [];
        for (const h of hitsRes.value) {
          const stat = statutes.find((s) => s.source.url === h.source_url);
          if (!stat || seen.has(stat.id)) continue;
          seen.add(stat.id);
          fallback.push(stat);
          if (fallback.length >= 4) break;
        }
        matched = fallback;
      }
=======
    // Lift the full set up to the page so the StatuteDetail drawer can resolve
    // them by id when a card is clicked — even if they're not in the sidebar.
    if (pulled.length > 0) onChatStatutes?.(pulled);
>>>>>>> Stashed changes

    updateMessages((m) =>
      m.map((msg) =>
        msg.id === placeholderId
          ? { ...msg, text: answer, statutes: matched, pending: false }
          : msg,
      ),
    );
    setBusy(false);
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
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-widest text-gold">
            <Sparkles className="h-3 w-3" /> Statute Assistant
          </div>
          <div className="font-serif font-bold text-sm leading-tight">
            Ask a question, get the relevant statutes.
          </div>
        </div>
        {messages.length > 1 && (
          <button
            onClick={() => {
              chatStore.set([{ id: uid(), role: "assistant", text: GREETING_TEXT }]);
            }}
            className="text-xs text-muted-foreground hover:text-foreground inline-flex items-center gap-1.5 px-2 py-1 rounded hover:bg-secondary"
            aria-label="Clear conversation"
            title="Clear conversation"
          >
            <Eraser className="h-3.5 w-3.5" />
            Clear
          </button>
        )}
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
        <div className="max-w-3xl mx-auto space-y-2">
          {(attachments.length > 0 || uploading || uploadError) && (
            <div className="flex flex-wrap gap-1.5">
              {attachments.map((a) => (
                <span
                  key={a.filename}
                  className="inline-flex items-center gap-1.5 text-xs px-2 py-1 rounded-md border border-border bg-secondary/50"
                  title={`${a.text.length} chars`}
                >
                  <Paperclip className="h-3 w-3 text-muted-foreground" />
                  <span className="font-medium truncate max-w-[200px]">{a.filename}</span>
                  <span className="text-muted-foreground">
                    {Math.round(a.text.length / 1000)}k
                  </span>
                  <button
                    onClick={() => removeAttachment(a.filename)}
                    className="text-muted-foreground hover:text-foreground"
                    aria-label={`Remove ${a.filename}`}
                  >
                    <X className="h-3 w-3" />
                  </button>
                </span>
              ))}
              {uploading && (
                <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Loader2 className="h-3 w-3 animate-spin" /> Reading file…
                </span>
              )}
              {uploadError && (
                <span className="text-xs text-destructive">{uploadError}</span>
              )}
            </div>
          )}
          <div className="flex items-end gap-2">
            <input
              ref={fileInputRef}
              type="file"
              accept=".pdf,.docx,.txt,.md"
              onChange={onFilePicked}
              className="hidden"
            />
            <button
              onClick={() => fileInputRef.current?.click()}
              disabled={uploading || busy}
              className="h-10 w-10 rounded-md border border-border bg-background grid place-items-center text-muted-foreground hover:text-foreground hover:border-primary/40 disabled:opacity-40"
              aria-label="Attach file"
              title="Attach a PDF, .docx, or .txt"
            >
              <Paperclip className="h-4 w-4" />
            </button>
            <textarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={onKey}
              rows={1}
              placeholder="Search statutes, ask a question, or attach a file…"
              className="flex-1 resize-none max-h-32 min-h-10 rounded-md border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary/50"
            />
            <button
              onClick={() => send(input)}
              disabled={(!input.trim() && attachments.length === 0) || busy}
              className="h-10 w-10 rounded-md bg-primary text-primary-foreground grid place-items-center disabled:opacity-40 hover:opacity-90"
              aria-label="Send"
            >
              <Send className="h-4 w-4" />
            </button>
          </div>
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
