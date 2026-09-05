import { TasksIcon, ScheduleIcon, WorkersIcon, MoonIcon, SunIcon } from "./Icons";
import brandIcon from "../assets/brand-icon.png";

export type View = "tasks" | "schedules" | "workers";

interface Props {
  view: View;
  onView: (v: View) => void;
  theme: "light" | "dark";
  onToggleTheme: () => void;
  taskCount: number;
  scheduleCount: number;
  workerCount: number;
  activeWorkers: number;
}

// Always-dark navy rail, like Temporal's left sidebar. Holds the gradient brand
// mark, the primary nav (Tasks / Schedules / Workers) with right-aligned mono
// count pills, a live cluster-pulse card, and the theme track toggle + version
// tag.
export default function Sidebar({
  view,
  onView,
  theme,
  onToggleTheme,
  taskCount,
  scheduleCount,
  workerCount,
  activeWorkers,
}: Props) {
  const nav: { key: View; label: string; icon: typeof TasksIcon; count: number }[] = [
    { key: "tasks", label: "Tasks", icon: TasksIcon, count: taskCount },
    { key: "schedules", label: "Schedules", icon: ScheduleIcon, count: scheduleCount },
    { key: "workers", label: "Workers", icon: WorkersIcon, count: workerCount },
  ];

  return (
    <aside className="sidebar">
      {/* Brand */}
      <div className="brand">
        <span className="brand-mark">
          <img src={brandIcon} alt="ChopFlow" />
        </span>
        <div className="brand-word">
          <b>Chop</b>
          <span>Flow</span>
        </div>
      </div>

      {/* Nav */}
      <nav className="nav">
        <div className="nav-label">Operate</div>
        {nav.map(({ key, label, icon: Icon, count }) => {
          const active = view === key;
          return (
            <button
              key={key}
              onClick={() => onView(key)}
              className={`nav-item${active ? " active" : ""}`}
            >
              <Icon />
              <span>{label}</span>
              <span className="count">{count}</span>
            </button>
          );
        })}
      </nav>

      {/* Cluster pulse */}
      <div className="cluster-pulse">
        <div className="cp-row">
          <span className="cp-dot" />
          <span className="cp-title">Cluster live</span>
        </div>
        <div className="cp-meta">
          <b>{activeWorkers}</b> workers · 1 region
        </div>
      </div>

      {/* Footer: theme toggle + version */}
      <div className="side-foot">
        <button className="theme-toggle" onClick={onToggleTheme} aria-label="Toggle appearance">
          {theme === "dark" ? <SunIcon /> : <MoonIcon />}
          <span>Appearance</span>
          <span className="tt-track">
            <span className="tt-knob" />
          </span>
        </button>
        <div className="side-tag">v0.1.0 · ops console</div>
      </div>
    </aside>
  );
}
