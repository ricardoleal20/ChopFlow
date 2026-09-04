import { shortId, timeAgo } from "../lib/format";
import type { Worker } from "../lib/api";

interface Props {
  workers: Worker[];
  isLoading: boolean;
  error?: Error | null;
  query: string;
}

// Workers view: a header with liveness meta + a responsive grid of worker
// cards. Each card shows identity, declared tags, and live resource meters
// (used/total) with gradient fills — CPU in primary, GPU in warn, saturated in
// danger. Mirrors the OpenDesign reference density.
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

  const liveCount = workers.filter((w) => w.alive).length;

  return (
    <section className="view">
      <div className="view-head">
        <div>
          <h1>Workers</h1>
          <p>
            Polling processes registered to <b>local · default</b>
          </p>
        </div>
        <div className="vh-meta">
          <div>
            <b>●</b> {liveCount} live · {workers.length - liveCount} drained
          </div>
          <div>heartbeat 2s ago</div>
        </div>
      </div>

      <div className="workers-grid">
        {isLoading ? (
          Array.from({ length: 3 }).map((_, i) => (
            <div key={i} style={{ height: 180, borderRadius: "var(--radius-lg)", background: "var(--surface-3)", opacity: 0.5 }} />
          ))
        ) : error ? (
          <div className="empty-state" style={{ gridColumn: "1 / -1" }}>
            <p style={{ color: "var(--danger)" }}>Failed to load workers: {error.message}</p>
          </div>
        ) : seen.length === 0 ? (
          <div className="empty-state" style={{ gridColumn: "1 / -1" }}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.5}>
              <rect x="3" y="4" width="18" height="12" rx="2" />
              <path d="M6 20h12M9 16v4M15 16v4" />
            </svg>
            <p>
              No workers connected. Start one with{" "}
              <span className="mono" style={{ color: "var(--fg-2)" }}>
                chopflow_worker start -b http://localhost:8000
              </span>
            </p>
          </div>
        ) : (
          seen.map((w, i) => <WorkerCard key={w.id} worker={w} index={i} />)
        )}
      </div>
    </section>
  );
}

function WorkerCard({ worker: w, index }: { worker: Worker; index: number }) {
  const entries = Object.entries(w.resources_total);
  return (
    <div className="wcard" style={{ animationDelay: `${index * 60}ms` }}>
      <div className="wc-head">
        <span className={`wc-live${w.alive ? "" : " dead"}`} />
        <span className="wc-id mono">{shortId(w.id)}</span>
        <span className="wc-addr mono">{w.address || "—"}</span>
      </div>

      <div className="wc-tags">
        {w.tags.length === 0 ? (
          <span style={{ fontSize: 11, color: "var(--muted-2)" }}>no tags</span>
        ) : (
          w.tags.map((tag) => (
            <span key={tag} className={`tag ${tag}`}>
              {tag}
            </span>
          ))
        )}
      </div>

      <div className="wc-meters">
        {entries.length === 0 && (
          <div style={{ fontSize: 11.5, color: "var(--muted-2)" }}>no resources declared</div>
        )}
        {entries.map(([name, total]) => {
          const avail = w.resources_available[name] ?? 0;
          const used = Math.max(0, total - avail);
          const pct = total > 0 ? Math.round((used / total) * 100) : 0;
          const full = pct >= 100;
          const cls = name === "gpu" ? "gpu" : "";
          return (
            <div key={name} className={`meter ${cls}${full ? " full" : ""}`}>
              <span className="lbl">{name.toUpperCase()}</span>
              <div className="bar">
                <span className="fill" style={{ width: `${pct}%` }} />
              </div>
              <span className="val">
                {used}/{total}
              </span>
            </div>
          );
        })}
      </div>

      <div className="wc-foot">
        <span>
          <b>{w.assigned_tasks}</b> assigned
        </span>
        <span>
          heartbeat <b>{timeAgo(w.last_heartbeat)}</b> ago
        </span>
      </div>
    </div>
  );
}
