import { shortId, timeAgo, clockTime } from "../lib/format";
import type { Schedule, OverlapPolicy } from "../lib/api";

interface Props {
  schedules: Schedule[];
  isLoading: boolean;
  error?: Error | null;
  query: string;
  filter: SchedFilter;
  onFilter: (f: SchedFilter) => void;
  onOpen: (s: Schedule) => void;
}

export type SchedFilter = "all" | "enabled" | "oneshot" | "cron" | "disabled";

const CHIPS: SchedFilter[] = ["all", "enabled", "oneshot", "cron", "disabled"];

const OVERLAP_LABEL: Record<OverlapPolicy, string> = {
  skip: "Skip",
  coalesce: "Coalesce",
  allow: "Allow",
};

// Schedules view: mirrors TasksView's structure (view-head + filter chips +
// dense table). Columns: Name / Kind / Trigger / Overlap / Last fired /
// Next fire / Enabled. Row click opens the ScheduleDrawer. Kind pills reuse
// the existing stbadge tints (cron=queued/warn, oneshot=running/info).
export default function SchedulesView({
  schedules,
  isLoading,
  error,
  query,
  filter,
  onFilter,
  onOpen,
}: Props) {
  const q = query.trim().toLowerCase();
  const counts: Record<string, number> = {
    all: schedules.length,
    enabled: 0,
    oneshot: 0,
    cron: 0,
    disabled: 0,
  };
  for (const s of schedules) {
    if (s.enabled) counts.enabled += 1;
    else counts.disabled += 1;
    if (s.kind.type === "cron") counts.cron += 1;
    else counts.oneshot += 1;
  }

  const seen = schedules.filter((s) => {
    if (filter === "enabled" && !s.enabled) return false;
    if (filter === "disabled" && s.enabled) return false;
    if (filter === "oneshot" && s.kind.type !== "oneshot") return false;
    if (filter === "cron" && s.kind.type !== "cron") return false;
    if (!q) return true;
    return (
      s.id.toLowerCase().includes(q) ||
      s.name.toLowerCase().includes(q) ||
      s.task_template.name.toLowerCase().includes(q)
    );
  });

  return (
    <section className="view">
      <div className="view-head">
        <div>
          <h1>Schedules</h1>
          <p>
            Recurring + one-shot task templates on <b>local · default</b>
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
          const label =
            c === "all"
              ? "All"
              : c === "oneshot"
                ? "One-shot"
                : c.charAt(0).toUpperCase() + c.slice(1);
          return (
            <button
              key={c}
              data-f={c}
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
                <th style={{ width: 110 }}>Kind</th>
                <th style={{ width: 200 }}>Trigger</th>
                <th style={{ width: 110 }}>Overlap</th>
                <th style={{ width: 150 }}>Last fired</th>
                <th style={{ width: 180 }}>Next fire</th>
                <th style={{ width: 100 }}>Enabled</th>
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <tr key={`sk-${i}`}>
                    {Array.from({ length: 8 }).map((__, j) => (
                      <td key={j}>
                        <div
                          style={{
                            height: 14,
                            borderRadius: 4,
                            background: "var(--surface-3)",
                            opacity: 0.5,
                          }}
                        />
                      </td>
                    ))}
                  </tr>
                ))
              ) : error ? (
                <tr>
                  <td colSpan={8}>
                    <div className="empty-state">
                      <p style={{ color: "var(--danger)" }}>
                        Failed to load schedules: {error.message}
                      </p>
                    </div>
                  </td>
                </tr>
              ) : seen.length === 0 ? (
                <tr>
                  <td colSpan={8}>
                    <div className="empty-state">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.5}>
                        <circle cx="11" cy="11" r="7" />
                        <path d="m21 21-4.3-4.3" />
                      </svg>
                      <p>No schedules match this filter.</p>
                    </div>
                  </td>
                </tr>
              ) : (
                seen.map((s, i) => {
                  const isCron = s.kind.type === "cron";
                  let trigger: string;
                  if (s.kind.type === "cron") {
                    trigger = s.kind.cron;
                  } else {
                    trigger = new Date(s.kind.eta).toLocaleString();
                  }
                  return (
                    <tr
                      key={s.id}
                      onClick={() => onOpen(s)}
                      style={{
                        animation: `cardIn 220ms var(--ease-out) ${Math.min(i * 40, 200)}ms backwards`,
                      }}
                    >
                      <td>
                        <span className="tid mono">
                          <b>{shortId(s.id)}</b>
                        </span>
                      </td>
                      <td>
                        <span className="tname">{s.name}</span>
                      </td>
                      <td>
                        <span
                          className={`stbadge badge ${isCron ? "queued" : "running"}`}
                          data-f={isCron ? "cron" : "oneshot"}
                        >
                          <span className="d" />
                          {isCron ? "Cron" : "One-shot"}
                        </span>
                      </td>
                      <td>
                        <span className="enq mono" title={trigger}>
                          {trigger}
                        </span>
                      </td>
                      <td>
                        <span className="enq">{OVERLAP_LABEL[s.overlap_policy]}</span>
                      </td>
                      <td>
                        {s.last_fired ? (
                          <span className="enq" title={clockTime(s.last_fired)}>
                            {timeAgo(s.last_fired)}
                          </span>
                        ) : (
                          <span style={{ fontSize: 11, color: "var(--muted-2)" }}>—</span>
                        )}
                      </td>
                      <td>
                        <span className="enq" title={clockTime(s.next_fire)}>
                          {new Date(s.next_fire).toLocaleString()}
                          <br />
                          <span
                            style={{
                              fontFamily: "var(--font-sans)",
                              fontSize: 11,
                              color: "var(--muted)",
                            }}
                          >
                            {timeAgo(s.next_fire)}
                          </span>
                        </span>
                      </td>
                      <td>
                        <span
                          className={`stbadge badge ${s.enabled ? "completed" : "failed"}`}
                          data-f={s.enabled ? "enabled" : "disabled"}
                        >
                          <span className="d" />
                          {s.enabled ? "On" : "Off"}
                        </span>
                      </td>
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
