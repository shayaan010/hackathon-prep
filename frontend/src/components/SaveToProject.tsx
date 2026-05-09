import { useState } from "react";
import { Bookmark, FolderPlus, Check, Plus } from "lucide-react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  useProjects,
  createProject,
  addStatuteToProject,
  removeStatuteFromProject,
} from "@/lib/projects";

interface Props {
  statuteId: string;
}

export function SaveToProject({ statuteId }: Props) {
  const projects = useProjects();
  const [open, setOpen] = useState(false);
  const [newName, setNewName] = useState("");

  const savedCount = projects.filter((p) =>
    p.statuteIds.includes(statuteId),
  ).length;
  const hasAnySave = savedCount > 0;

  const toggle = (projectId: string) => {
    const project = projects.find((p) => p.id === projectId);
    if (!project) return;
    if (project.statuteIds.includes(statuteId)) {
      removeStatuteFromProject(projectId, statuteId);
    } else {
      addStatuteToProject(projectId, statuteId);
    }
  };

  const createAndSave = () => {
    const name = newName.trim();
    if (!name) return;
    const p = createProject(name);
    addStatuteToProject(p.id, statuteId);
    setNewName("");
  };

  const onNewKey = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      createAndSave();
    }
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          className={[
            "h-9 px-3 rounded-md border text-sm font-medium flex items-center gap-1.5 transition-colors",
            hasAnySave
              ? "bg-gold text-gold-foreground border-gold"
              : "border-border bg-card hover:bg-secondary",
          ].join(" ")}
        >
          <Bookmark className={`h-4 w-4 ${hasAnySave ? "fill-current" : ""}`} />
          {hasAnySave ? `Saved${savedCount > 1 ? ` · ${savedCount}` : ""}` : "Save"}
        </button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-72 p-0 overflow-hidden">
        <div className="px-3 py-2.5 border-b border-border bg-secondary/50">
          <div className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">
            Save to project
          </div>
        </div>

        {projects.length === 0 ? (
          <div className="px-3 py-4 text-xs text-muted-foreground">
            No projects yet — create one below.
          </div>
        ) : (
          <div className="max-h-64 overflow-y-auto scrollbar-thin py-1">
            {projects.map((p) => {
              const inProject = p.statuteIds.includes(statuteId);
              return (
                <button
                  key={p.id}
                  onClick={() => toggle(p.id)}
                  className="w-full text-left px-3 py-2 flex items-center gap-2 hover:bg-secondary/60 transition-colors"
                >
                  <span
                    className={[
                      "h-4 w-4 rounded border flex items-center justify-center shrink-0",
                      inProject
                        ? "bg-primary border-primary text-primary-foreground"
                        : "border-border bg-background",
                    ].join(" ")}
                  >
                    {inProject && <Check className="h-3 w-3" />}
                  </span>
                  <span className="flex-1 min-w-0 truncate text-sm font-medium">
                    {p.name}
                  </span>
                  <span className="text-[10px] font-mono text-muted-foreground">
                    {p.statuteIds.length}
                  </span>
                </button>
              );
            })}
          </div>
        )}

        <div className="border-t border-border p-2 flex items-center gap-2 bg-card">
          <FolderPlus className="h-4 w-4 text-muted-foreground shrink-0 ml-1" />
          <input
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            onKeyDown={onNewKey}
            placeholder="New project name…"
            className="flex-1 min-w-0 h-8 px-2 rounded border border-border bg-background text-xs focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary/50"
          />
          <button
            onClick={createAndSave}
            disabled={!newName.trim()}
            className="h-8 w-8 rounded bg-primary text-primary-foreground grid place-items-center disabled:opacity-40 hover:opacity-90 shrink-0"
            aria-label="Create project and save"
          >
            <Plus className="h-3.5 w-3.5" />
          </button>
        </div>
      </PopoverContent>
    </Popover>
  );
}
