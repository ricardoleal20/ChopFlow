import { useState } from "react";
import Sidebar, { type View } from "./components/Sidebar";
import TopBar from "./components/TopBar";
import TasksView from "./components/TasksView";
import WorkersView from "./components/WorkersView";
import TaskDrawer from "./components/TaskDrawer";
import { EnqueueDialog } from "./components/EnqueueDialog";
import { useTheme } from "./hooks/useTheme";
import { useStats, useTasks, useWorkers } from "./hooks/useChopFlow";
import type { Task, TaskStatus } from "./lib/api";

// App shell — a faithful port of the OpenDesign reference layout: a fixed navy
// sidebar (248px) + a main column with a 54px command bar and a scrollable
// content area. The selected view (Tasks / Workers) swaps the content; a task
// detail drawer slides over from the right on row click.
export default function App() {
  const { theme, toggle } = useTheme();
  const [view, setView] = useState<View>("tasks");
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<TaskStatus | "all">("all");
  const [enqueueOpen, setEnqueueOpen] = useState(false);
  const [selected, setSelected] = useState<Task | null>(null);

  const { data: stats } = useStats();
  const tasksQ = useTasks();
  const workersQ = useWorkers();

  const tasks = tasksQ.data?.tasks ?? [];
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
      <EnqueueDialog open={enqueueOpen} onClose={() => setEnqueueOpen(false)} />
    </div>
  );
}
