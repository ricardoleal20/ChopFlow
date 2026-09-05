import { motion } from "framer-motion";
import { FlowIcon, TasksIcon, WorkersIcon, SunIcon, MoonIcon, PulseIcon } from "./Icons";
import type { Stats } from "../lib/api";

export type View = "tasks" | "workers";

interface Props {
  view: View;
  onView: (v: View) => void;
  theme: "light" | "dark";
  onToggleTheme: () => void;
  stats?: Stats;
}

// Always-dark navy rail, like Temporal's left sidebar. Holds the brand, the
// primary nav (Tasks / Workers), a live cluster pulse, and the theme toggle.
export default function Sidebar({ view, onView, theme, onToggleTheme, stats }: Props) {
  const nav: { key: View; label: string; icon: typeof TasksIcon }[] = [
    { key: "tasks", label: "Tasks", icon: TasksIcon },
    { key: "workers", label: "Workers", icon: WorkersIcon },
  ];

  return (
    <aside className="flex h-full w-60 shrink-0 flex-col bg-sidebar text-sidebarText border-r border-sidebarBorder">
      {/* Brand */}
      <div className="flex items-center gap-2.5 px-4 h-14 border-b border-sidebarBorder">
        <span className="grid h-8 w-8 place-items-center rounded-lg bg-primary/20 text-primary">
          <FlowIcon className="h-5 w-5" />
        </span>
        <div className="leading-tight">
          <div className="text-sm font-semibold text-sidebarText">ChopFlow</div>
          <div className="text-[11px] text-sidebarMuted">Operations</div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-2.5 py-3 space-y-1">
        <div className="px-2 pb-1 text-[10px] font-semibold uppercase tracking-wider text-sidebarMuted">
          Workspace
        </div>
        {nav.map(({ key, label, icon: Icon }) => {
          const active = view === key;
          return (
            <button
              key={key}
              onClick={() => onView(key)}
              className={`nav-item relative w-full text-left ${
                active
                  ? "bg-sidebarSurface text-sidebarText"
                  : "text-sidebarMuted hover:bg-sidebarSurface/60 hover:text-sidebarText"
              }`}
            >
              {active && (
                <motion.span
                  layoutId="nav-active"
                  className="absolute left-0 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-full bg-primary"
                  transition={{ duration: 0.2, ease: "easeOut" }}
                />
              )}
              <Icon className="h-4 w-4" />
              {label}
            </button>
          );
        })}
      </nav>

      {/* Cluster pulse */}
      <div className="px-3 py-3 border-t border-sidebarBorder">
        <div className="flex items-center gap-2 text-[11px] text-sidebarMuted">
          <PulseIcon className="h-3.5 w-3.5 text-success" />
          <span>Cluster</span>
          <span className="ml-auto inline-flex items-center gap-1.5 text-success">
            <span className="h-1.5 w-1.5 rounded-full bg-success animate-pulseDot" />
            {stats?.active_workers ?? 0} live
          </span>
        </div>
      </div>

      {/* Theme toggle */}
      <div className="p-2.5 border-t border-sidebarBorder">
        <button
          onClick={onToggleTheme}
          className="nav-item w-full text-sidebarMuted hover:bg-sidebarSurface/60 hover:text-sidebarText"
          aria-label="Toggle theme"
        >
          {theme === "dark" ? <SunIcon className="h-4 w-4" /> : <MoonIcon className="h-4 w-4" />}
          {theme === "dark" ? "Light mode" : "Dark mode"}
        </button>
      </div>
    </aside>
  );
}
