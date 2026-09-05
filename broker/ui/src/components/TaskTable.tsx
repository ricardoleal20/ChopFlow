import { motion, AnimatePresence } from "framer-motion";
import StatusBadge from "./StatusBadge";
import { STATUS_ORDER, statusStyle } from "../lib/status";
import { shortId, timeAgo, clockTime, truncate } from "../lib/format";
import type { Task, TaskStatus } from "../lib/api";

interface Props {
  tasks: Task[];
  isLoading: boolean;
  error?: Error | null;
  query: string;
  filter: TaskStatus | "all";
  onFilter: (f: TaskStatus | "all") => void;
  onOpen: (task: Task) => void;
}

type Filter = TaskStatus | "all";

// Dense workflows-style table: ID · Name · Status · Tags · Retries · Enqueued · Result.
// A row click opens the detail drawer. Status filter chips with live counts sit
// above the table; search filters by id / name / tag.
export default function TaskTable({ tasks, isLoading, error, query, filter, onFilter, onOpen }: Props) {
  const q = query.trim().toLowerCase();
  const seen = tasks.filter((t) => {
    if (filter !== "all" && t.status !== filter) return false;
    if (!q) return true;
    return (
      t.id.toLowerCase().includes(q) ||
      t.name.toLowerCase().includes(q) ||
      t.tags.some((tag) => tag.toLowerCase().includes(q))
    );
  });

  const counts: Record<string, number> = { all: tasks.length };
  for (const s of STATUS_ORDER) counts[s] = 0;
  for (const t of tasks) counts[t.status] = (counts[t.status] ?? 0) + 1;

  const chips: Filter[] = ["all", ...STATUS_ORDER];

  return (
    <div className="flex h-full min-h-0 flex-col">
      {/* Filter chips */}
      <div className="flex flex-wrap items-center gap-1.5 border-b border-border px-4 py-2.5">
        {chips.map((c) => {
          const active = filter === c;
          const label = c === "all" ? "All" : statusStyle(c as TaskStatus).label;
          const dot = c === "all" ? "bg-subtle" : statusStyle(c as TaskStatus).dot;
          return (
            <button
              key={c}
              onClick={() => onFilter(c)}
              className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium transition-colors duration-150 ease-out active:scale-[0.98] ${
                active
                  ? "bg-primary/15 text-primary ring-1 ring-primary/30"
                  : "text-muted hover:bg-surface2 hover:text-text"
              }`}
            >
              <span className={`h-1.5 w-1.5 rounded-full ${dot}`} />
              {label}
              <span className="text-subtle">{counts[c] ?? 0}</span>
            </button>
          );
        })}
      </div>

      {/* Body */}
      <div className="min-h-0 flex-1 overflow-auto">
        {isLoading ? (
          <div className="space-y-2 p-4">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="h-11 animate-pulse rounded-lg bg-surface2" />
            ))}
          </div>
        ) : error ? (
          <div className="p-8 text-center text-sm text-danger">
            Failed to load tasks: {error.message}
          </div>
        ) : (
          <table className="w-full border-collapse text-sm">
            <thead className="sticky top-0 z-10 bg-surface">
              <tr className="text-left text-[11px] font-semibold uppercase tracking-wider text-subtle">
                <th className="px-4 py-2.5">ID</th>
                <th className="px-4 py-2.5">Name</th>
                <th className="px-4 py-2.5">Status</th>
                <th className="px-4 py-2.5">Tags</th>
                <th className="px-4 py-2.5">Retries</th>
                <th className="px-4 py-2.5">Enqueued</th>
                <th className="px-4 py-2.5">Result</th>
              </tr>
            </thead>
            <tbody>
              {seen.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-16 text-center text-muted">
                    No tasks match. Enqueue one with{" "}
                    <span className="font-medium text-text">New Task</span>.
                  </td>
                </tr>
              )}
              <AnimatePresence initial={false}>
                {seen.map((t, i) => (
                  <motion.tr
                    key={t.id}
                    layout
                    initial={{ opacity: 0, y: 4 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, transition: { duration: 0.18, ease: "easeIn" } }}
                    transition={{ duration: 0.18, ease: "easeOut", delay: Math.min(i * 0.02, 0.15) }}
                    onClick={() => onOpen(t)}
                    className="group cursor-pointer border-t border-border transition-colors duration-150 ease-out hover:bg-surface2"
                  >
                    <td className="px-4 py-2.5 font-mono text-xs text-muted">{shortId(t.id)}</td>
                    <td className="px-4 py-2.5 font-medium text-text">{t.name}</td>
                    <td className="px-4 py-2.5">
                      <StatusBadge status={t.status} pulse={t.status === "running"} />
                    </td>
                    <td className="px-4 py-2.5">
                      <div className="flex flex-wrap gap-1">
                        {t.tags.length === 0 ? (
                          <span className="text-xs text-subtle">—</span>
                        ) : (
                          t.tags.map((tag) => (
                            <span
                              key={tag}
                              className="rounded bg-surface2 px-1.5 py-0.5 font-mono text-[10px] text-muted"
                            >
                              {tag}
                            </span>
                          ))
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-2.5 text-muted">
                      {t.retry_count}
                      {t.max_retries > 0 && <span className="text-subtle">/{t.max_retries}</span>}
                    </td>
                    <td className="px-4 py-2.5 text-muted" title={clockTime(t.enqueue_time)}>
                      {timeAgo(t.enqueue_time)}
                    </td>
                    <td className="max-w-[200px] truncate px-4 py-2.5 font-mono text-xs text-subtle">
                      {t.result ? truncate(t.result, 40) : "—"}
                    </td>
                  </motion.tr>
                ))}
              </AnimatePresence>
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
