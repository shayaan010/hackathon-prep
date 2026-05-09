import { createFileRoute } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import {
  FolderOpen,
  FolderPlus,
  Trash2,
  X,
  ChevronDown,
  ChevronRight,
  Pencil,
  Check,
} from "lucide-react";
import { AppHeader } from "@/components/AppHeader";
import { StatuteDetail } from "@/components/StatuteDetail";
import { Sheet, SheetContent } from "@/components/ui/sheet";
import { STATUTES, type Statute } from "@/lib/statutes";
import { api } from "@/lib/api";
import {
  useProjects,
  createProject,
  deleteProject,
  removeStatuteFromProject,
  renameProject,
  type Project,
} from "@/lib/projects";

export const Route = createFileRoute("/projects")({
  head: () => ({
    meta: [
      { title: "Projects — Lex Harvester" },
      {
        name: "description",
        content:
          "Folders of saved statutes you've collected during research. Group authorities by case, motion, or theory.",
      },
    ],
  }),
  component: ProjectsPage,
});

function ProjectsPage() {
  const { data: statutes = STATUTES } = useQuery({
    queryKey: ["statutes"],
    queryFn: api.statutes,
    staleTime: 60_000,
    placeholderData: STATUTES,
  });

  const projects = useProjects();
  const statutesById = useMemo(
    () => new Map(statutes.map((s) => [s.id, s] as const)),
    [statutes],
  );

  const [newName, setNewName] = useState("");
  const [openIds, setOpenIds] = useState<Set<string>>(new Set());
  const [selectedStatuteId, setSelectedStatuteId] = useState<string | null>(null);

  const toggleOpen = (id: string) => {
    setOpenIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const submitNew = () => {
    const name = newName.trim();
    if (!name) return;
    const p = createProject(name);
    setNewName("");
    setOpenIds((prev) => new Set(prev).add(p.id));
  };

  const onNewKey = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      submitNew();
    }
  };

  const selectedStatute = selectedStatuteId
    ? statutesById.get(selectedStatuteId) ?? null
    : null;

  return (
    <div className="min-h-screen flex flex-col bg-background">
      <AppHeader />

      <main className="flex-1 max-w-5xl mx-auto w-full px-6 py-8">
        <div className="flex items-center justify-between mb-6">
          <div>
            <div className="text-[10px] font-mono uppercase tracking-widest text-gold flex items-center gap-1.5 mb-1">
              <FolderOpen className="h-3 w-3" /> Projects
            </div>
            <h1 className="font-serif text-3xl font-bold">Your saved statutes</h1>
            <p className="text-sm text-muted-foreground mt-1">
              Group authorities by case, motion, or theory. Saved statutes persist on
              this device.
            </p>
          </div>
        </div>

        <div className="rounded-lg border border-border bg-card p-3 flex items-center gap-2 mb-6">
          <FolderPlus className="h-4 w-4 text-muted-foreground ml-1 shrink-0" />
          <input
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            onKeyDown={onNewKey}
            placeholder="New project name… (e.g. Doe v. State, Hit-and-run motion)"
            className="flex-1 h-9 px-2 rounded border border-border bg-background text-sm focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary/50"
          />
          <button
            onClick={submitNew}
            disabled={!newName.trim()}
            className="h-9 px-3 rounded-md bg-primary text-primary-foreground text-sm font-medium disabled:opacity-40 hover:opacity-90"
          >
            Create
          </button>
        </div>

        {projects.length === 0 ? (
          <div className="rounded-lg border border-dashed border-border bg-card/40 p-12 text-center">
            <div className="h-12 w-12 rounded-full bg-secondary grid place-items-center mx-auto mb-3">
              <FolderOpen className="h-5 w-5 text-muted-foreground" />
            </div>
            <h3 className="font-serif font-bold text-lg mb-1">No projects yet</h3>
            <p className="text-sm text-muted-foreground max-w-sm mx-auto">
              Create a project above, or open any statute and click <strong>Save</strong> to
              start a folder.
            </p>
          </div>
        ) : (
          <div className="space-y-3">
            {projects
              .slice()
              .sort((a, b) => b.createdAt - a.createdAt)
              .map((p) => (
                <ProjectCard
                  key={p.id}
                  project={p}
                  statutesById={statutesById}
                  open={openIds.has(p.id)}
                  onToggleOpen={() => toggleOpen(p.id)}
                  onSelectStatute={setSelectedStatuteId}
                />
              ))}
          </div>
        )}
      </main>

      <Sheet
        open={selectedStatute != null}
        onOpenChange={(o) => {
          if (!o) setSelectedStatuteId(null);
        }}
      >
        <SheetContent
          side="right"
          className="p-0 sm:max-w-3xl w-full overflow-y-auto"
        >
          <StatuteDetail statute={selectedStatute} />
        </SheetContent>
      </Sheet>
    </div>
  );
}

interface ProjectCardProps {
  project: Project;
  statutesById: Map<string, Statute>;
  open: boolean;
  onToggleOpen: () => void;
  onSelectStatute: (id: string) => void;
}

function ProjectCard({
  project,
  statutesById,
  open,
  onToggleOpen,
  onSelectStatute,
}: ProjectCardProps) {
  const [editing, setEditing] = useState(false);
  const [draftName, setDraftName] = useState(project.name);

  const saveRename = () => {
    const name = draftName.trim();
    if (!name) {
      setDraftName(project.name);
    } else if (name !== project.name) {
      renameProject(project.id, name);
    }
    setEditing(false);
  };

  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 flex items-center gap-3">
        <button
          onClick={onToggleOpen}
          className="text-muted-foreground hover:text-foreground"
          aria-label={open ? "Collapse" : "Expand"}
        >
          {open ? (
            <ChevronDown className="h-4 w-4" />
          ) : (
            <ChevronRight className="h-4 w-4" />
          )}
        </button>
        <FolderOpen className="h-4 w-4 text-gold shrink-0" />

        {editing ? (
          <input
            value={draftName}
            autoFocus
            onChange={(e) => setDraftName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") saveRename();
              if (e.key === "Escape") {
                setDraftName(project.name);
                setEditing(false);
              }
            }}
            onBlur={saveRename}
            className="flex-1 min-w-0 h-8 px-2 rounded border border-border bg-background text-sm font-serif font-bold focus:outline-none focus:ring-2 focus:ring-primary/40"
          />
        ) : (
          <button
            onClick={onToggleOpen}
            className="font-serif font-bold text-base flex-1 min-w-0 truncate text-left"
          >
            {project.name}
          </button>
        )}

        <span className="text-[11px] font-mono text-muted-foreground">
          {project.statuteIds.length} saved
        </span>

        {editing ? (
          <button
            onClick={saveRename}
            className="h-7 w-7 rounded grid place-items-center text-muted-foreground hover:text-foreground hover:bg-secondary"
            aria-label="Save name"
          >
            <Check className="h-4 w-4" />
          </button>
        ) : (
          <button
            onClick={() => {
              setDraftName(project.name);
              setEditing(true);
            }}
            className="h-7 w-7 rounded grid place-items-center text-muted-foreground hover:text-foreground hover:bg-secondary opacity-60 hover:opacity-100"
            aria-label="Rename project"
          >
            <Pencil className="h-3.5 w-3.5" />
          </button>
        )}
        <button
          onClick={() => {
            if (
              window.confirm(
                `Delete project "${project.name}"? Saved statutes will be released from this folder.`,
              )
            ) {
              deleteProject(project.id);
            }
          }}
          className="h-7 w-7 rounded grid place-items-center text-muted-foreground hover:text-destructive hover:bg-secondary opacity-60 hover:opacity-100"
          aria-label="Delete project"
        >
          <Trash2 className="h-3.5 w-3.5" />
        </button>
      </div>

      {open && (
        <div className="border-t border-border divide-y divide-border/50 bg-secondary/20">
          {project.statuteIds.length === 0 ? (
            <div className="px-4 py-6 text-xs text-muted-foreground text-center">
              No statutes here yet. Open a statute and Save to add it.
            </div>
          ) : (
            project.statuteIds.map((id) => {
              const s = statutesById.get(id);
              if (!s) {
                return (
                  <div
                    key={id}
                    className="px-4 py-2 flex items-center justify-between gap-2 text-xs text-muted-foreground"
                  >
                    <span className="font-mono truncate">{id} (unavailable)</span>
                    <button
                      onClick={() => removeStatuteFromProject(project.id, id)}
                      className="shrink-0 h-6 w-6 rounded grid place-items-center hover:bg-secondary"
                      aria-label="Remove"
                    >
                      <X className="h-3.5 w-3.5" />
                    </button>
                  </div>
                );
              }
              return (
                <div
                  key={id}
                  className="px-4 py-2.5 flex items-start gap-3 hover:bg-card/60 group"
                >
                  <button
                    onClick={() => onSelectStatute(s.id)}
                    className="flex-1 min-w-0 text-left"
                  >
                    <div className="flex items-center gap-2 mb-0.5">
                      <span className="font-mono text-[10px] font-medium text-gold uppercase tracking-wider">
                        {s.jurisdiction} · {s.code}
                      </span>
                      <span className="font-mono text-[10px] text-muted-foreground">
                        § {s.section}
                      </span>
                    </div>
                    <div className="font-serif text-sm font-bold leading-tight">
                      {s.title}
                    </div>
                    <div className="text-xs text-muted-foreground line-clamp-1 mt-0.5">
                      {s.summary}
                    </div>
                  </button>
                  <button
                    onClick={() => removeStatuteFromProject(project.id, s.id)}
                    className="shrink-0 h-6 w-6 rounded grid place-items-center text-muted-foreground hover:text-destructive hover:bg-secondary opacity-0 group-hover:opacity-100 transition-opacity"
                    aria-label={`Remove ${s.section} from project`}
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                </div>
              );
            })
          )}
        </div>
      )}
    </div>
  );
}
