import { useState } from "react";
import Sidebar, { type View } from "./components/Sidebar";
import TopBar from "./components/TopBar";
import TaskTable from "./components/TaskTable";
import WorkersView from "./components/WorkersView";
import TaskDrawer from "./components/TaskDrawer";
import { EnqueueDialog } from "./components/EnqueueDialog";
import { useTheme } from "./hooks/useTheme";
import { useStats, useTasks, useWorkers } from "./hooks/useChopFlow";
import type { Task, TaskStatus } from "./lib/api";

// App shell: a Temporal-style fixed sidebar + top command bar + scrollable
// content area. The selected view (Tasks / Workers) swaps the content; the
// task detail drawer slides over from the right on row click.
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

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-canvas">
      <Sidebar
        view={view}
        onView={(v) => {
          setView(v);
          setQuery("");
        }}
        theme={theme}
        onToggleTheme={toggle}
        stats={stats}
      />

      <div className="flex min-w-0 flex-1 flex-col">
        <TopBar
          view={view}
          query={query}
          onQuery={setQuery}
          onEnqueue={() => setEnqueueOpen(true)}
          stats={stats}
        />

        <main className="min-h-0 flex-1 overflow-hidden bg-canvas">
          {view === "tasks" ? (
            <div className="surface h-full overflow-hidden rounded-none border-0">
              <TaskTable
                tasks={tasks}
                isLoading={tasksQ.isLoading}
                error={tasksQ.error}
                query={query}
                filter={filter}
                onFilter={setFilter}
                onOpen={setSelected}
              />
            </div>
          ) : (
            <div className="h-full overflow-y-auto">
              <WorkersView
                workers={workersQ.data ?? []}
                isLoading={workersQ.isLoading}
                error={workersQ.error}
                query={query}
              />
            </div>
          )}
        </main>
      </div>

      <TaskDrawer task={selected} onClose={() => setSelected(null)} />
      <EnqueueDialog open={enqueueOpen} onClose={() => setEnqueueOpen(false)} />
    </div>
  );
}
