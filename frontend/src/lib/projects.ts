import { useSyncExternalStore } from "react";

export type Project = {
  id: string;
  name: string;
  statuteIds: string[];
  createdAt: number;
};

const STORAGE_KEY = "lex.projects.v1";

let cache: Project[] | null = null;
const listeners = new Set<() => void>();

function isClient() {
  return typeof window !== "undefined";
}

function read(): Project[] {
  if (!isClient()) return [];
  if (cache !== null) return cache;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    cache = raw ? (JSON.parse(raw) as Project[]) : [];
  } catch {
    cache = [];
  }
  return cache;
}

function write(next: Project[]) {
  cache = next;
  if (isClient()) {
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    } catch {
      // quota exceeded, etc. — keep in-memory cache
    }
  }
  listeners.forEach((l) => l());
}

const EMPTY: Project[] = [];

export function useProjects(): Project[] {
  return useSyncExternalStore(
    (cb) => {
      listeners.add(cb);
      return () => {
        listeners.delete(cb);
      };
    },
    () => read(),
    () => EMPTY,
  );
}

function uid() {
  return Math.random().toString(36).slice(2, 12);
}

export function createProject(name: string): Project {
  const trimmed = name.trim() || "Untitled project";
  const project: Project = {
    id: uid(),
    name: trimmed,
    statuteIds: [],
    createdAt: Date.now(),
  };
  write([...read(), project]);
  return project;
}

export function addStatuteToProject(projectId: string, statuteId: string) {
  const next = read().map((p) =>
    p.id === projectId && !p.statuteIds.includes(statuteId)
      ? { ...p, statuteIds: [...p.statuteIds, statuteId] }
      : p,
  );
  write(next);
}

export function removeStatuteFromProject(projectId: string, statuteId: string) {
  const next = read().map((p) =>
    p.id === projectId
      ? { ...p, statuteIds: p.statuteIds.filter((id) => id !== statuteId) }
      : p,
  );
  write(next);
}

export function deleteProject(projectId: string) {
  write(read().filter((p) => p.id !== projectId));
}

export function renameProject(projectId: string, name: string) {
  const trimmed = name.trim();
  if (!trimmed) return;
  write(
    read().map((p) => (p.id === projectId ? { ...p, name: trimmed } : p)),
  );
}

export function projectsContaining(statuteId: string, projects: Project[]) {
  return projects.filter((p) => p.statuteIds.includes(statuteId));
}
