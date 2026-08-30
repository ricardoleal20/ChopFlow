import { motion } from "framer-motion";
import { useWorkers } from "../hooks/useChopFlow";
import type { Worker } from "../lib/api";

export function WorkerRail() {
  const { data: workers, isLoading, error } = useWorkers();
  const list = workers ?? [];

  return (
    <section className="surface overflow-hidden">
      <div className="border-b border-border p-4">
        <h2 className="text-base font-semibold tracking-tight">Workers</h2>
        <p className="text-xs text-muted">
          {list.length} registered · {list.filter((w) => w.alive).length} alive
        </p>
      </div>

      {isLoading ? (
        <div className="space-y-2 p-4" aria-busy="true">
          {Array.from({ length: 2 }).map((_, i) => (
            <div key={i} className="h-16 animate-pulse rounded-lg bg-surface2" />
          ))}
        </div>
      ) : error ? (
        <div className="p-6 text-center text-sm text-danger">Failed to load workers</div>
      ) : list.length === 0 ? (
        <div className="flex flex-col items-center px-6 py-12 text-center">
          <div className="grid h-11 w-11 place-items-center rounded-2xl bg-surface2 ring-1 ring-border">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden>
              <rect x="3" y="4" width="18" height="12" rx="2" stroke="#5b6478" strokeWidth="2" />
              <path d="M8 20h8" stroke="#5b6478" strokeWidth="2" strokeLinecap="round" />
            </svg>
          </div>
          <h3 className="mt-3 text-sm font-medium">No workers connected</h3>
          <p className="mt-1 text-xs text-muted">Start a worker to claim queued tasks.</p>
        </div>
      ) : (
        <ul role="list" className="divide-y divide-border">
          {list.map((w, i) => (
            <WorkerCard key={w.id} worker={w} index={i} />
          ))}
        </ul>
      )}
    </section>
  );
}

function WorkerCard({ worker, index }: { worker: Worker; index: number }) {
  return (
    <motion.li
      initial={{ opacity: 0, x: 6 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.22, ease: "easeOut", delay: index * 0.03 }}
      className="p-4"
    >
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2.5">
          <span
            className={`h-2 w-2 rounded-full ${worker.alive ? "bg-accent animate-pulseDot" : "bg-subtle"}`}
            aria-hidden
          />
          <code className="font-mono text-xs text-text">{shortId(worker.id)}</code>
        </div>
        <span className={`text-xs ${worker.alive ? "text-accent" : "text-subtle"}`}>
          {worker.alive ? "online" : "offline"}
        </span>
      </div>

      <div className="mt-3 flex flex-wrap gap-1">
        {worker.tags.map((t) => (
          <span key={t} className="rounded-md bg-canvas px-1.5 py-0.5 font-mono text-xs text-muted ring-1 ring-border">
            {t}
          </span>
        ))}
      </div>

      <div className="mt-3 flex items-center justify-between text-xs text-muted">
        <span>{worker.assigned_tasks} active</span>
        <span className="font-mono text-subtle">{worker.address}</span>
      </div>

      <ResourceMeters worker={worker} />
    </motion.li>
  );
}

function ResourceMeters({ worker }: { worker: Worker }) {
  const keys = Object.keys(worker.resources_total);
  if (keys.length === 0) return null;

  return (
    <div className="mt-3 space-y-1.5">
      {keys.map((k) => {
        const total = worker.resources_total[k];
        const avail = worker.resources_available[k] ?? 0;
        const used = total - avail;
        const pct = total > 0 ? Math.max(0, Math.min(100, (used / total) * 100)) : 0;
        return (
          <div key={k}>
            <div className="flex items-center justify-between text-[11px] text-subtle">
              <span className="font-mono">{k}</span>
              <span className="font-mono tabular-nums">
                {used}/{total}
              </span>
            </div>
            <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-canvas ring-1 ring-border">
              <div
                className={`h-full rounded-full transition-all duration-300 ease-out ${
                  pct > 90 ? "bg-danger" : pct > 60 ? "bg-warn" : "bg-accent/70"
                }`}
                style={{ width: `${pct}%` }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}

function shortId(id: string): string {
  return id.slice(0, 8);
}
