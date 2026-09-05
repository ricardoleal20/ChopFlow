import { useState } from "react";
import Sidebar, { type View } from "./components/Sidebar";
import TopBar from "./components/TopBar";
import TasksView from "./components/TasksView";
import SchedulesView, { type SchedFilter } from "./components/SchedulesView";
import WorkersView from "./components/WorkersView";
import TaskDrawer from "./components/TaskDrawer";
import ScheduleDrawer from "./components/ScheduleDrawer";
import { EnqueueDialog } from "./components/EnqueueDialog";
import { useTheme } from "./hooks/useTheme";
import { useStats, useTasks, useSchedules, useWorkers } from "./hooks/useChopFlow";
import type { Task, TaskStatus, Schedule } from "./lib/api";

// App shell — a faithful port of the OpenDesign reference layout: a fixed navy
// sidebar (248px) + a main column with a 54px command bar and a scrollable
// content area. The selected view (Tasks / Schedules / Workers) swaps the
// content; a task or schedule detail drawer slides over from the right on row
// click.
export default function App() {
  const { theme, toggle } = useTheme();
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
