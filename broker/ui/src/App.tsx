import { useState } from "react";
import { EnqueueDialog } from "./components/EnqueueDialog";
import { Header } from "./components/Header";
import { StatCard } from "./components/StatCard";
import { TaskTable } from "./components/TaskTable";
import { WorkerRail } from "./components/WorkerRail";
import { useStats } from "./hooks/useChopFlow";

export default function App() {
  const [enqueueOpen, setEnqueueOpen] = useState(false);
  const { data: stats, isError } = useStats();

  return (
    <div className="min-h-full">
      <Header onEnqueue={() => setEnqueueOpen(true)} />

      <main className="mx-auto max-w-7xl px-4 pb-20 sm:px-6">
        {/* Cluster health stat row — 4-col gapless grid, collapses to 2 then 1. */}
        <section className="mt-6 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4 grid-flow-dense">
          <StatCard label="In queue" value={stats?.queue_length ?? 0} tone="info" hint="Waiting for a worker" index={0} />
          <StatCard label="Processing" value={stats?.tasks_processing ?? 0} tone="warn" hint="Claimed and running" index={1} />
          <StatCard label="Completed" value={stats?.tasks_completed ?? 0} tone="accent" hint="Finished successfully" index={2} />
          <StatCard label="Failed" value={stats?.tasks_failed ?? 0} tone="danger" hint="Dead-lettered included" index={3} />
        </section>

        {isError && (
          <div className="mt-4 rounded-xl border border-danger/30 bg-danger/10 px-4 py-3 text-sm text-danger">
            Cannot reach the broker. Is <code className="font-mono">chopflow_broker start</code> running?
          </div>
        )}

        {/* Primary surface: task ledger (wide) + worker rail (narrow). */}
        <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-[minmax(0,1fr)_320px]">
          <TaskTable />
          <WorkerRail />
        </div>

        <footer className="mt-12 border-t border-border pt-6 text-xs text-subtle">
          <p>
            ChopFlow · durable task queue. Workers pull work via gRPC; this dashboard reads the same live state over HTTP.
          </p>
        </footer>
      </main>

      <EnqueueDialog open={enqueueOpen} onClose={() => setEnqueueOpen(false)} />
    </div>
  );
}
