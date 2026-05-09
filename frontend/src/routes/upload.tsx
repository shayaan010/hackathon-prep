import { createFileRoute } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import { useRef, useState } from "react";
import {
  Upload as UploadIcon,
  FileText,
  CheckCircle2,
  AlertCircle,
  Loader2,
  Trash2,
} from "lucide-react";
import { AppHeader } from "@/components/AppHeader";
import { api, type UploadResponse } from "@/lib/api";

export const Route = createFileRoute("/upload")({
  head: () => ({
    meta: [
      { title: "Upload — Lex Harvester" },
      {
        name: "description",
        content:
          "Upload your own PDFs, Word documents, or text files. The assistant will read and reason about them alongside the statute corpus.",
      },
    ],
  }),
  component: UploadPage,
});

type Item =
  | { id: string; name: string; size: number; status: "uploading" }
  | { id: string; name: string; size: number; status: "done"; result: UploadResponse }
  | { id: string; name: string; size: number; status: "error"; error: string };

const uid = () => Math.random().toString(36).slice(2, 10);
const ACCEPTED = ".pdf,.docx,.txt,.md";

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

function UploadPage() {
  const qc = useQueryClient();
  const [items, setItems] = useState<Item[]>([]);
  const [ingest, setIngest] = useState(true);
  const [dragOver, setDragOver] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleFiles = async (files: FileList | File[]) => {
    const list = Array.from(files);
    if (list.length === 0) return;

    for (const file of list) {
      const id = uid();
      setItems((prev) => [
        ...prev,
        { id, name: file.name, size: file.size, status: "uploading" },
      ]);
      try {
        const result = await api.upload(file, ingest);
        setItems((prev) =>
          prev.map((it) =>
            it.id === id
              ? { id, name: file.name, size: file.size, status: "done", result }
              : it,
          ),
        );
      } catch (err) {
        setItems((prev) =>
          prev.map((it) =>
            it.id === id
              ? {
                  id,
                  name: file.name,
                  size: file.size,
                  status: "error",
                  error: err instanceof Error ? err.message : "Upload failed",
                }
              : it,
          ),
        );
      }
    }

    // Refresh stats / statutes so the corpus counters reflect new ingests.
    if (ingest) {
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["statutes"] });
    }
  };

  const onDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragOver(false);
    if (e.dataTransfer.files?.length) {
      handleFiles(e.dataTransfer.files);
    }
  };

  const removeItem = (id: string) =>
    setItems((prev) => prev.filter((it) => it.id !== id));

  return (
    <div className="min-h-screen flex flex-col bg-background">
      <AppHeader />

      <main className="flex-1 px-6 py-8">
        <div className="max-w-3xl mx-auto space-y-6">
          <header className="space-y-2">
            <h1 className="font-serif text-3xl font-bold tracking-tight">
              Upload your own files
            </h1>
            <p className="text-sm text-muted-foreground leading-relaxed">
              Drop in police reports, contracts, briefs, or any other documents
              you want the assistant to reason about. We support{" "}
              <span className="font-mono text-foreground">.pdf</span>,{" "}
              <span className="font-mono text-foreground">.docx</span>, and{" "}
              <span className="font-mono text-foreground">.txt / .md</span>.
            </p>
          </header>

          <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
            <input
              type="checkbox"
              checked={ingest}
              onChange={(e) => setIngest(e.target.checked)}
              className="h-4 w-4"
            />
            <span>
              <span className="font-medium">Add to corpus</span>{" "}
              <span className="text-muted-foreground">
                — index for semantic search and chat retrieval (recommended).
              </span>
            </span>
          </label>

          <div
            onDragOver={(e) => {
              e.preventDefault();
              setDragOver(true);
            }}
            onDragLeave={() => setDragOver(false)}
            onDrop={onDrop}
            onClick={() => inputRef.current?.click()}
            className={[
              "border-2 border-dashed rounded-xl p-10 text-center cursor-pointer transition-colors",
              dragOver
                ? "border-primary bg-primary/5"
                : "border-border bg-card hover:border-primary/40 hover:bg-secondary/30",
            ].join(" ")}
          >
            <input
              ref={inputRef}
              type="file"
              multiple
              accept={ACCEPTED}
              onChange={(e) => {
                if (e.target.files) handleFiles(e.target.files);
                e.target.value = "";
              }}
              className="hidden"
            />
            <div className="flex flex-col items-center gap-3">
              <div className="h-12 w-12 rounded-full gradient-primary grid place-items-center shadow-elegant">
                <UploadIcon className="h-5 w-5 text-primary-foreground" />
              </div>
              <div>
                <div className="font-serif font-bold">Drop files here</div>
                <div className="text-xs text-muted-foreground mt-1">
                  …or click to browse. Up to 25 MB per file.
                </div>
              </div>
            </div>
          </div>

          {items.length > 0 && (
            <div className="space-y-2">
              <div className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">
                Recent uploads
              </div>
              {items.map((it) => (
                <div
                  key={it.id}
                  className="rounded-lg border border-border bg-card px-4 py-3 flex items-start gap-3"
                >
                  <FileText className="h-4 w-4 mt-0.5 text-muted-foreground shrink-0" />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="font-medium text-sm truncate">{it.name}</span>
                      <span className="text-[11px] font-mono text-muted-foreground">
                        {fmtBytes(it.size)}
                      </span>
                    </div>

                    {it.status === "uploading" && (
                      <div className="mt-1 inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                        <Loader2 className="h-3 w-3 animate-spin" />
                        Reading and indexing…
                      </div>
                    )}

                    {it.status === "done" && (
                      <div className="mt-1 space-y-0.5 text-xs text-muted-foreground">
                        <div className="inline-flex items-center gap-1.5 text-green-600 dark:text-green-500">
                          <CheckCircle2 className="h-3 w-3" />
                          {it.result.char_count.toLocaleString()} characters extracted
                          {it.result.ingested
                            ? ` · added to corpus (${it.result.chunks} chunks)`
                            : ""}
                        </div>
                        <details className="cursor-pointer">
                          <summary className="text-muted-foreground hover:text-foreground">
                            Preview text
                          </summary>
                          <pre className="mt-2 max-h-48 overflow-auto whitespace-pre-wrap text-[11px] leading-relaxed bg-secondary/40 p-3 rounded font-mono">
                            {it.result.text.slice(0, 4000)}
                            {it.result.text.length > 4000 && "\n…"}
                          </pre>
                        </details>
                      </div>
                    )}

                    {it.status === "error" && (
                      <div className="mt-1 inline-flex items-start gap-1.5 text-xs text-destructive">
                        <AlertCircle className="h-3 w-3 mt-0.5 shrink-0" />
                        <span className="break-words">{it.error}</span>
                      </div>
                    )}
                  </div>
                  <button
                    onClick={() => removeItem(it.id)}
                    className="text-muted-foreground hover:text-foreground p-1 -m-1"
                    aria-label="Remove"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
