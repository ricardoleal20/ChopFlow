import { useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { useCancel, useTasks } from "../hooks/useChopFlow";
import type { Task, TaskStatus } from "../lib/api";
import { StatusBadge } from "./StatusBadge";

const FILTERS: ("all" | TaskStatus)[] = [
  "all",
  "queued",
  "running",
  "completed",
  "failed",
  "dead-lettered",
  "cancelled",
];

export function TaskTable() {
  const [filter, setFilter] = useState<"all" | TaskStatus>("all");
  const { data, isLoading, error } = useTasks(filter === "all" ? undefined : filter);
  const cancel = useCancel();

  const tasks = data?.tasks ?? [];

  return (
    <section className="surface overflow-hidden">
      {/* Toolbar: title + filter pills */}
      <div className="flex flex-col gap-3 border-b border-border p-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 className="text-base font-semibold tracking-tight">Tasks</h2>
          <p className="text-xs text-muted">{data?.total ?? 0} total in cluster</p>
        </div>
        <div className="flex flex-wrap gap-1.5" role="tablist" aria-label="Filter tasks by status">
          {FILTERS.map((f) => (
            <button
              key={f}
              role="tab"
              aria-selected={filter === f}
              onClick={() => setFilter(f)}
              className={`rounded-lg px-2.5 py-1 text-xs font-medium capitalize transition-colors duration-150 ease-out active:scale-[0.98] ${
                filter === f
                  ? "bg-elevated text-text ring-1 ring-borderStrong"
                  : "text-muted hover:text-text hover:bg-surface2"
              }`}
            >
              {f.replace("-", " ")}
            </button>
          ))}
        </div>
      </div>

      {/* Body */}
      {isLoading ? (
        <SkeletonRows />
      ) : error ? (
        <div className="p-8 text-center text-sm text-danger">Failed to load tasks: {(error as Error).message}</div>
      ) : tasks.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs uppercase tracking-wide text-subtle">
                <th className="px-4 py-2.5 font-medium">Name</th>
                <th className="px-4 py-2.5 font-medium">Status</th>
                <th className="hidden px-4 py-2.5 font-medium md:table-cell">Tags</th>
                <th className="hidden px-4 py-2.5 font-medium sm:table-cell">Retries</th>
                <th className="hidden px-4 py-2.5 font-medium lg:table-cell">Result</th>
                <th className="px-4 py-2.5 text-right font-medium">Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              <AnimatePresence initial={false}>
                {tasks.map((t) => (
                  <TaskRow key={t.id} task={t} onCancel={() => cancel.mutate(t.id)} />
                ))}
              </AnimatePresence>
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function TaskRow({ task, onCancel }: { task: Task; onCancel: () => void }) {
  const cancellable = task.status === "queued" || task.status === "running";
  const result = task.result ? truncate(task.result, 60) : null;

  return (
    <motion.tr
      layout
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0, transition: { duration: 0.18, ease: "easeIn" } }}
      transition={{ duration: 0.2, ease: "easeOut" }}
      className="group hover:bg-surface2/60"
    >
      <td className="px-4 py-3">
        <div className="font-medium text-text">{task.name}</div>
        <div className="font-mono text-xs text-subtle">{shortId(task.id)}</div>
      </td>
      <td className="px-4 py-3">
        <StatusBadge status={task.status} />
      </td>
      <td className="hidden px-4 py-3 md:table-cell">
        <div className="flex flex-wrap gap-1">
          {task.tags.length === 0 ? (
            <span className="text-xs text-subtle">—</span>
          ) : (
            task.tags.map((tag) => (
              <span key={tag} className="rounded-md bg-canvas px-1.5 py-0.5 font-mono text-xs text-muted ring-1 ring-border">
                {tag}
              </span>
            ))
          )}
        </div>
      </td>
      <td className="hidden px-4 py-3 font-mono text-xs text-muted sm:table-cell">
        {task.retry_count}/{task.max_retries}
      </td>
      <td className="hidden px-4 py-3 lg:table-cell">
        {result ? (
          <code className="font-mono text-xs text-muted">{result}</code>
        ) : (
          <span className="text-xs text-subtle">—</span>
        )}
      </td>
      <td className="px-4 py-3 text-right">
        {cancellable ? (
          <button
            onClick={onCancel}
            className="rounded-lg px-2.5 py-1 text-xs font-medium text-danger ring-1 ring-danger/30 transition-colors duration-150 ease-out hover:bg-danger/10 active:scale-[0.98]"
          >
            Cancel
          </button>
        ) : (
          <span className="text-xs text-subtle">—</span>
        )}
      </td>
    </motion.tr>
  );
}

function SkeletonRows() {
  return (
    <div className="space-y-2 p-4" aria-busy="true" aria-label="Loading tasks">
      {Array.from({ length: 4 }).map((_, i) => (
        <div key={i} className="h-12 animate-pulse rounded-lg bg-surface2" />
      ))}
    </div>
  );
}

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center px-6 py-16 text-center">
      <div className="grid h-12 w-12 place-items-center rounded-2xl bg-surface2 ring-1 ring-border">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden>
          <path d="M4 7h16M4 12h16M4 17h10" stroke="#5b6478" strokeWidth="2" strokeLinecap="round" />
        </svg>
      </div>
      <h3 className="mt-4 text-sm font-medium">No tasks in this view</h3>
      <p className="mt-1 text-xs text-muted">Enqueue a task or switch filters to see activity.</p>
    </div>
  );
}

function shortId(id: string): string {
  return id.slice(0, 8);
}

function truncate(s: string, n: number): string {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}
