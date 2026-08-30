import { PlusIcon, SearchIcon, ChevronDownIcon } from "./Icons";
import type { Stats } from "../lib/api";
import type { View } from "./Sidebar";

interface Props {
  view: View;
  query: string;
  onQuery: (q: string) => void;
  onEnqueue: () => void;
  stats?: Stats;
}

// Top command bar: cluster selector on the left, contextual search in the
// middle, and the primary action (enqueue) on the right — Temporal's layout.
export default function TopBar({ view, query, onQuery, onEnqueue, stats }: Props) {
  return (
    <header className="flex h-14 shrink-0 items-center gap-3 border-b border-border bg-surface px-4">
      {/* Cluster selector */}
      <div className="flex items-center gap-2 rounded-lg border border-border bg-surface2 px-3 py-1.5">
        <span className="h-2 w-2 rounded-full bg-success animate-pulseDot" />
        <span className="text-sm font-medium text-text">local</span>
        <span className="text-xs text-subtle">· default</span>
        <ChevronDownIcon className="h-4 w-4 text-subtle" />
      </div>

      <div className="text-sm text-subtle">/</div>
      <div className="text-sm font-medium text-muted capitalize">{view}</div>

      {/* Search */}
      <div className="relative ml-auto w-full max-w-sm">
        <SearchIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-subtle" />
        <input
          value={query}
          onChange={(e) => onQuery(e.target.value)}
          placeholder={`Search ${view} by id, name, tag…`}
          className="cf-input pl-9"
        />
      </div>

      {/* Live count pill */}
      {stats && (
        <div className="hidden items-center gap-3 rounded-lg border border-border bg-surface2 px-3 py-1.5 text-xs text-muted lg:flex">
          <span>
            <b className="text-text">{stats.queue_length}</b> queued
          </span>
          <span className="h-3 w-px bg-border" />
          <span>
            <b className="text-text">{stats.tasks_processing}</b> running
          </span>
          <span className="h-3 w-px bg-border" />
          <span>
            <b className="text-text">{stats.total_tasks}</b> total
          </span>
        </div>
      )}

      {/* Primary action */}
      <button
        onClick={onEnqueue}
        className="inline-flex items-center gap-1.5 rounded-lg bg-primary px-3 py-1.5 text-sm font-medium text-white shadow-sm transition-transform duration-150 ease-out hover:bg-primary/90 active:scale-[0.98]"
      >
        <PlusIcon className="h-4 w-4" />
        New Task
      </button>
    </header>
  );
}
