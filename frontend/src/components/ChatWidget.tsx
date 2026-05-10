import { useEffect, useRef, useState } from "react";
import { useLocation } from "@tanstack/react-router";
import {
  MessageCircle,
  X,
  Send,
  Sparkles,
  Scale,
  Loader2,
  Paperclip,
  Eraser,
} from "lucide-react";
import { api, type AttachedFile } from "@/lib/api";
import { type Statute } from "@/lib/statutes";
import {
  chatStore,
  useChatHistory,
  type ChatMessage as Message,
} from "@/lib/chat-store";

const uid = () => Math.random().toString(36).slice(2, 10);

const SUGGESTIONS = [
  "Statutes for hit and run",
  "What governs reckless driving?",
  "Cell phone use while driving",
  "Following too closely",
];

const GREETING_TEXT =
  "Hi — I'm your statute assistant. Ask me about a contributing factor, a vehicle code section, or describe a fact pattern, and I'll surface the relevant statutes.";

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
            <strong key={j} className="font-semibold">{p.slice(2, -2)}</strong>
          ) : (
            <span key={j}>{linkify(p)}</span>
          ),
        )}
        {i < lines.length - 1 && <br />}
      </span>
    );
  });
}

export function ChatWidget() {
  const { pathname } = useLocation();
  const [open, setOpen] = useState(false);
  const [messages, updateMessages] = useChatHistory();
  // Seed greeting on first ever load (shared with MainChat).
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
    const f = e.target.files?.[0];
    e.target.value = "";
    if (!f) return;
    setUploadError(null);
    setUploading(true);
    try {
      const res = await api.upload(f, false);
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

  const removeAttachment = (filename: string) =>
    setAttachments((prev) => prev.filter((a) => a.filename !== filename));

  useEffect(() => {
    if (open && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, open]);

  useEffect(() => {
    const handler = () => setOpen(true);
    window.addEventListener("open-chat", handler);
    return () => window.removeEventListener("open-chat", handler);
  }, []);

  // The index route already hosts a full-pane chat; suppress the floating
  // launcher there to avoid two competing chat surfaces. Guard goes after all
  // hooks so the hook count stays stable across route changes.
  if (pathname === "/") return null;

  const send = async (raw: string) => {
    const text = raw.trim();
    if ((!text && attachments.length === 0) || busy) return;

    const attachedNow = attachments;
    const filenames = attachedNow.map((a) => a.filename);
    const displayText =
      text ||
      `Reading attached file${attachedNow.length > 1 ? "s" : ""}: ${filenames.join(", ")}`;
    const userMsg: Message = {
      id: uid(),
      role: "user",
      text: displayText,
      attachments: filenames.length ? filenames : undefined,
    };
    const placeholderId = uid();
    const placeholder: Message = {
      id: placeholderId,
      role: "assistant",
      text: "",
      pending: true,
    };
    const priorHistory = messages.map((m) => ({ role: m.role, text: m.text }));

    updateMessages((m) => [...m, userMsg, placeholder]);
    setInput("");
    setAttachments([]);
    setBusy(true);

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

    // Cap at 3 cards in the widget surface — same as the prior search-based
    // matching, but now pulled directly from the model's tool calls.
    const matched = pulled.slice(0, 3);

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
    <>
      {/* Floating launcher */}
      {!open && (
        <button
          onClick={() => setOpen(true)}
          className="fixed bottom-6 right-6 z-50 h-14 w-14 rounded-full gradient-primary text-primary-foreground shadow-elegant grid place-items-center hover:scale-105 transition-transform"
          aria-label="Open statute assistant"
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
                <Sparkles className="h-3 w-3" /> Statute Assistant
              </div>
              <div className="font-serif font-bold text-sm leading-snug truncate">
                Ask a question, get the relevant statutes.
              </div>
            </div>
            {messages.length > 1 && (
              <button
                onClick={() => {
                  chatStore.set([{ id: uid(), role: "assistant", text: GREETING_TEXT }]);
                }}
                className="text-muted-foreground hover:text-foreground p-1"
                aria-label="Clear conversation"
                title="Clear conversation"
              >
                <Eraser className="h-4 w-4" />
              </button>
            )}
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
                  "max-w-[88%] rounded-lg px-3 py-2 text-sm leading-relaxed",
                  m.role === "user"
                    ? "ml-auto bg-primary text-primary-foreground"
                    : "mr-auto bg-card border border-border text-foreground",
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
                      <div className="mt-3 pt-2 border-t border-border space-y-1.5">
                        <div className="text-[9px] font-mono uppercase tracking-widest text-muted-foreground">
                          Related statutes
                        </div>
                        {m.statutes.map((s) => (
                          <a
                            key={s.id}
                            href={s.source.url}
                            target="_blank"
                            rel="noreferrer"
                            className="block rounded-md border border-border bg-card hover:border-primary/40 hover:bg-secondary/40 transition-colors px-2.5 py-2"
                          >
                            <div className="flex items-center gap-2 mb-0.5">
                              <span className="font-mono text-[9px] font-medium text-gold uppercase tracking-wider">
                                {s.jurisdiction} · {s.code}
                              </span>
                              <span className="font-mono text-[9px] text-muted-foreground">
                                § {s.section}
                              </span>
                            </div>
                            <div className="font-serif text-[12px] font-bold leading-tight">{s.title}</div>
                            <div className="text-[11px] text-muted-foreground line-clamp-2 mt-0.5 leading-snug">
                              {s.summary}
                            </div>
                          </a>
                        ))}
                      </div>
                    )}
                  </>
                )}
              </div>
            ))}
          </div>

          {/* Suggestions (only when convo is fresh) */}
          {showSuggestions && (
            <div className="px-4 pb-2 pt-1 flex flex-wrap gap-1.5 bg-secondary/30">
              {SUGGESTIONS.map((s) => (
                <button
                  key={s}
                  onClick={() => send(s)}
                  className="text-[11px] px-2 py-1 rounded-full border border-border bg-card hover:border-primary/40 text-muted-foreground hover:text-foreground transition-colors"
                >
                  {s}
                </button>
              ))}
            </div>
          )}

          {/* Composer */}
          <div className="border-t border-border bg-card p-2 space-y-1.5">
            {(attachments.length > 0 || uploading || uploadError) && (
              <div className="flex flex-wrap gap-1 px-1">
                {attachments.map((a) => (
                  <span
                    key={a.filename}
                    className="inline-flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded border border-border bg-secondary/50"
                    title={`${a.text.length} chars`}
                  >
                    <Paperclip className="h-2.5 w-2.5 text-muted-foreground" />
                    <span className="font-medium truncate max-w-[140px]">{a.filename}</span>
                    <button
                      onClick={() => removeAttachment(a.filename)}
                      className="text-muted-foreground hover:text-foreground"
                      aria-label={`Remove ${a.filename}`}
                    >
                      <X className="h-2.5 w-2.5" />
                    </button>
                  </span>
                ))}
                {uploading && (
                  <span className="inline-flex items-center gap-1 text-[10px] text-muted-foreground">
                    <Loader2 className="h-2.5 w-2.5 animate-spin" /> Reading…
                  </span>
                )}
                {uploadError && (
                  <span className="text-[10px] text-destructive truncate max-w-[200px]">
                    {uploadError}
                  </span>
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
                className="h-9 w-9 rounded-md border border-border bg-background grid place-items-center text-muted-foreground hover:text-foreground hover:border-primary/40 disabled:opacity-40"
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
                placeholder="Ask about a statute, factor, or fact pattern…"
                className="flex-1 resize-none max-h-32 min-h-9 rounded-md border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary/50"
              />
              <button
                onClick={() => send(input)}
                disabled={(!input.trim() && attachments.length === 0) || busy}
                className="h-9 w-9 rounded-md bg-primary text-primary-foreground grid place-items-center disabled:opacity-40 hover:opacity-90"
                aria-label="Send"
              >
                <Send className="h-4 w-4" />
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
