import { useState } from "react";
import Sidebar, { type View } from "./components/Sidebar";
import TopBar from "./components/TopBar";
import TasksView from "./components/TasksView";
import SchedulesView, { type SchedFilter } from "./components/SchedulesView";
import WorkersView from "./components/WorkersView";
import TaskDrawer from "./components/TaskDrawer";
import ScheduleDrawer from "./components/ScheduleDrawer";
import { EnqueueDialog } from "./components/EnqueueDialog";
import Welcome from "./components/Welcome";
import { useTheme } from "./hooks/useTheme";
import { useStats, useTasks, useSchedules, useWorkers } from "./hooks/useChopFlow";
import { useAppTauri } from "./hooks/useAppTauri";
import type { Task, TaskStatus, Schedule } from "./lib/api";

// App shell. Two modes share one layout:
//   - Browser: the plain dashboard, exactly as the OpenDesign reference.
//   - Tauri (desktop): bootstraps the app's control plane; while the local
//     broker starts / connections are probed it shows the Welcome gate, and on
//     a first run the Welcome is the configuration surface.
export default function App() {
  const { theme, toggle } = useTheme();
  const shell = useAppTauri();
  const [view, setView] = useState<View>("tasks");
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<TaskStatus | "all">("all");
  const [schedFilter, setSchedFilter] = useState<SchedFilter>("all");
  const [enqueueOpen, setEnqueueOpen] = useState(false);
  const [selected, setSelected] = useState<Task | null>(null);
  const [schedSelected, setSchedSelected] = useState<Schedule | null>(null);

  const { data: stats } = useStats();
  const tasksQ = useTasks();
  const schedQ = useSchedules();
  const workersQ = useWorkers();

  const tasks = tasksQ.data?.tasks ?? [];
  const schedules = schedQ.data ?? [];
  const workers = workersQ.data ?? [];
  const envLabel = stats ? `${stats.env} · ${stats.region}` : "local · default";

  // In the desktop app, gate on the Welcome until boot completes and first
  // run is configured.
  if (shell.tauri && (!shell.ready || shell.firstRun)) {
    return (
      <Welcome
        state={shell.state}
        bootStep={shell.step}
        local={shell.local}
        error={shell.bootError}
        onStartLocal={() => void shell.startLocal()}
        onStopLocal={() => void shell.stopLocal()}
        onAddRemote={(name, url) => shell.addRemote(name, url)}
        onRemoveRemote={(name) => shell.removeRemote(name)}
        onProceed={() => void shell.completeFirstRun()}
      />
    );
  }

  return (
    <div className="app">
      <Sidebar
        view={view}
        onView={(v) => {
          setView(v);
          setQuery("");
        }}
        theme={theme}
        onToggleTheme={toggle}
        taskCount={tasks.length}
        scheduleCount={stats?.schedules ?? 0}
        workerCount={workers.filter((w) => w.alive).length}
        activeWorkers={stats?.active_workers ?? 0}
      />

      <div className="main">
        <TopBar
          view={view}
          query={query}
          onQuery={setQuery}
          onEnqueue={() => setEnqueueOpen(true)}
          stats={stats}
          shellActive={shell.tauri ? shell.active : undefined}
        />

        <main className="content">
          {view === "tasks" ? (
            <TasksView
              tasks={tasks}
              isLoading={tasksQ.isLoading}
              error={tasksQ.error}
              query={query}
              filter={filter}
              onFilter={setFilter}
              onOpen={setSelected}
            />
          ) : view === "schedules" ? (
            <SchedulesView
              schedules={schedules}
              isLoading={schedQ.isLoading}
              error={schedQ.error}
              query={query}
              filter={schedFilter}
              onFilter={setSchedFilter}
              onOpen={setSchedSelected}
            />
          ) : (
            <WorkersView
              workers={workers}
              isLoading={workersQ.isLoading}
              error={workersQ.error}
              query={query}
              envLabel={envLabel}
            />
          )}
        </main>
      </div>

      <TaskDrawer task={selected} onClose={() => setSelected(null)} />
      <ScheduleDrawer schedule={schedSelected} onClose={() => setSchedSelected(null)} />
      <EnqueueDialog open={enqueueOpen} onClose={() => setEnqueueOpen(false)} />
    </div>
  );
}
