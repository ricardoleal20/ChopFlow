import StatusBadge from "./StatusBadge";
import { STATUS_LABEL, STATUS_ORDER, chipKey } from "../lib/status";
import { shortId, timeAgo, clockTime, truncate } from "../lib/format";
import { ScheduleIcon } from "./Icons";
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

const CHIPS: Filter[] = ["all", ...STATUS_ORDER];

// Tasks view: a header with live/polling meta, status filter chips with
// per-status counts, and the dense task ledger. A row click opens the drawer.
export default function TasksView({ tasks, isLoading, error, query, filter, onFilter, onOpen }: Props) {
  const q = query.trim().toLowerCase();
  const counts: Record<string, number> = { all: tasks.length };
  for (const s of STATUS_ORDER) counts[s] = 0;
  for (const t of tasks) counts[t.status] = (counts[t.status] ?? 0) + 1;

  const seen = tasks.filter((t) => {
    if (filter !== "all" && t.status !== filter) return false;
    if (!q) return true;
    return (
      t.id.toLowerCase().includes(q) ||
      t.name.toLowerCase().includes(q) ||
      t.tags.some((tag) => tag.toLowerCase().includes(q))
    );
  });

  return (
    <section className="view">
      <div className="view-head">
        <div>
          <h1>Tasks</h1>
          <p>
            Workflow executions across the <b>local · default</b> cluster
          </p>
        </div>
        <div className="vh-meta">
          <div>
            <b>●</b> live · polling 2s
          </div>
          <div>updated just now</div>
        </div>
      </div>

      <div className="chips">
        {CHIPS.map((c) => {
          const active = filter === c;
          const label = c === "all" ? "All" : STATUS_LABEL[c as TaskStatus];
          return (
            <button
              key={c}
              data-f={chipKey(c)}
              onClick={() => onFilter(c)}
              className={`chip${active ? " active" : ""}`}
            >
              <span className="d" />
              {label} <span className="n">{counts[c] ?? 0}</span>
            </button>
          );
        })}
      </div>

      <div className="table-wrap">
        <div className="table-scroll">
          <table className="tasks">
            <thead>
              <tr>
                <th style={{ width: 120 }}>ID</th>
                <th>Name</th>
                <th style={{ width: 130 }}>Status</th>
                <th style={{ width: 140 }}>Tags</th>
                <th className="num" style={{ width: 80 }}>
                  Retries
                </th>
                <th style={{ width: 110 }}>Enqueued</th>
                <th style={{ width: 160 }}>Result</th>
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <tr key={`sk-${i}`}>
                    {Array.from({ length: 7 }).map((__, j) => (
                      <td key={j}>
                        <div style={{ height: 14, borderRadius: 4, background: "var(--surface-3)", opacity: 0.5 }} />
                      </td>
                    ))}
                  </tr>
                ))
              ) : error ? (
                <tr>
                  <td colSpan={7}>
                    <div className="empty-state">
                      <p style={{ color: "var(--danger)" }}>Failed to load tasks: {error.message}</p>
                    </div>
                  </td>
                </tr>
              ) : seen.length === 0 ? (
                <tr>
                  <td colSpan={7}>
                    <div className="empty-state">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.5}>
                        <circle cx="11" cy="11" r="7" />
                        <path d="m21 21-4.3-4.3" />
                      </svg>
                      <p>No tasks match this filter.</p>
                    </div>
                  </td>
                </tr>
              ) : (
                seen.map((t, i) => {
                  const retryWarn = t.max_retries > 0 && t.retry_count >= t.max_retries * 0.6;
                  const resultOut = t.result ? (
                    <span className="result">{truncate(t.result, 24)}</span>
                  ) : (
                    <span className="result empty">{t.status === "running" ? "running…" : "—"}</span>
                  );
                  return (
                    <tr
                      key={t.id}
                      onClick={() => onOpen(t)}
                      style={{ animation: `cardIn 220ms var(--ease-out) ${Math.min(i * 40, 200)}ms backwards` }}
                    >
                      <td>
                        <span className="tid mono">
                          <b>{shortId(t.id)}</b>
                        </span>
                      </td>
                      <td>
                        <span className="tname">
                          {t.name}
                          {t.schedule_id && (
                            <span title="from schedule" className="sched-mark-wrap">
                              <ScheduleIcon className="sched-mark" />
                            </span>
                          )}
                        </span>
                      </td>
                      <td>
                        <StatusBadge status={t.status} />
                      </td>
                      <td>
                        <div className="tagpills">
                          {t.tags.length === 0 ? (
                            <span style={{ fontSize: 11, color: "var(--muted-2)" }}>—</span>
                          ) : (
                            t.tags.map((tag) => (
                              <span key={tag} className={`tag ${tag}`}>
                                {tag}
                              </span>
                            ))
                          )}
                        </div>
                      </td>
                      <td className={`retries${retryWarn ? " warn" : ""}`}>
                        {t.retry_count}/{t.max_retries}
                      </td>
                      <td>
                        <span className="enq" title={clockTime(t.enqueue_time)}>
                          {timeAgo(t.enqueue_time)}
                        </span>
                      </td>
                      <td>{resultOut}</td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
