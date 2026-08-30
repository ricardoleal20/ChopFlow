import { motion } from "framer-motion";
import { shortId, timeAgo } from "../lib/format";
import type { Worker } from "../lib/api";

interface Props {
  workers: Worker[];
  isLoading: boolean;
  error?: Error | null;
  query: string;
}

// Workers view: a grid of cards, each showing identity, liveness, declared
// tags, and live resource meters (available / total). Mirrors Temporal's
// worker-list density without being a bare table.
export default function WorkersView({ workers, isLoading, error, query }: Props) {
  const q = query.trim().toLowerCase();
  const seen = workers.filter((w) => {
    if (!q) return true;
    return (
      w.id.toLowerCase().includes(q) ||
      w.tags.some((t) => t.toLowerCase().includes(q)) ||
      Object.keys(w.resources_total).some((r) => r.toLowerCase().includes(q))
    );
  });

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 gap-4 p-4 md:grid-cols-2 xl:grid-cols-3">
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i} className="h-40 animate-pulse rounded-2xl bg-surface2" />
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-8 text-center text-sm text-danger">
        Failed to load workers: {error.message}
      </div>
    );
  }

  if (seen.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center px-6 py-20 text-center">
        <h3 className="text-sm font-medium text-text">No workers connected</h3>
        <p className="mt-1 text-xs text-muted">
          Start a worker with <code className="font-mono text-muted">chopflow_worker start -b http://localhost:8000</code>
        </p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 gap-4 p-4 md:grid-cols-2 xl:grid-cols-3">
      {seen.map((w, i) => (
        <motion.div
          key={w.id}
          initial={{ opacity: 0, y: 6 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.2, ease: "easeOut", delay: Math.min(i * 0.03, 0.2) }}
          className="surface p-4"
        >
          {/* Header */}
          <div className="flex items-start justify-between">
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <span
                  className={`h-2 w-2 shrink-0 rounded-full ${
                    w.alive ? "bg-success animate-pulseDot" : "bg-danger"
                  }`}
                />
                <span className="truncate font-mono text-sm font-medium text-text">
                  {shortId(w.id)}
                </span>
              </div>
              <div className="mt-0.5 truncate text-xs text-subtle">{w.address}</div>
            </div>
            <span
              className={`chip ${
                w.alive ? "bg-success/10 text-success" : "bg-danger/10 text-danger"
              }`}
            >
              {w.alive ? "Alive" : "Dead"}
            </span>
          </div>

          {/* Tags */}
          <div className="mt-3 flex flex-wrap gap-1">
            {w.tags.length === 0 ? (
              <span className="text-xs text-subtle">no tags</span>
            ) : (
              w.tags.map((tag) => (
                <span
                  key={tag}
                  className="rounded bg-surface2 px-1.5 py-0.5 font-mono text-[10px] text-muted"
                >
                  {tag}
                </span>
              ))
            )}
          </div>

          {/* Resource meters */}
          <div className="mt-3 space-y-2">
            {Object.entries(w.resources_total).map(([name, total]) => {
              const avail = w.resources_available[name] ?? 0;
              const used = Math.max(0, total - avail);
              const pct = total > 0 ? (used / total) * 100 : 0;
              return (
                <div key={name}>
                  <div className="flex items-center justify-between text-[11px] text-muted">
                    <span className="font-mono">{name}</span>
                    <span>
                      {used} / {total} used
                    </span>
                  </div>
                  <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-surface2">
                    <motion.div
                      className="h-full rounded-full bg-primary"
                      initial={{ width: 0 }}
                      animate={{ width: `${pct}%` }}
                      transition={{ duration: 0.3, ease: "easeOut" }}
                    />
                  </div>
                </div>
              );
            })}
          </div>

          {/* Footer */}
          <div className="mt-3 flex items-center justify-between border-t border-border pt-2 text-[11px] text-subtle">
            <span>{w.assigned_tasks} assigned</span>
            <span>heartbeat {timeAgo(w.last_heartbeat)}</span>
          </div>
        </motion.div>
      ))}
    </div>
  );
}
