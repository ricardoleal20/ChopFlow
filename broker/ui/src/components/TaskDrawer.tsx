import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import StatusBadge from "./StatusBadge";
import { statusStyle } from "../lib/status";
import { clockTime, timeAgo, shortId, pretty } from "../lib/format";
import { CloseIcon, CancelIcon, CheckIcon, ClockIcon, RetryIcon } from "./Icons";
import { useCancel } from "../hooks/useChopFlow";
import type { Task, TaskStatus } from "../lib/api";

interface Props {
  task: Task | null;
  onClose: () => void;
}

// Slide-over detail drawer with two tabs: Summary (full task record) and
// Lifecycle (a Temporal-style timeline reconstructed from status + retries).
// Motion follows the 12 principles: ease-out entrance, ease-in exit, dimmed
// backdrop, active-press on buttons.
export default function TaskDrawer({ task, onClose }: Props) {
  const [tab, setTab] = useState<"summary" | "lifecycle">("summary");
  const cancel = useCancel();

  return (
    <AnimatePresence>
      {task && (
        <>
          {/* Backdrop */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0, transition: { duration: 0.18, ease: "easeIn" } }}
            transition={{ duration: 0.2, ease: "easeOut" }}
            onClick={onClose}
            className="fixed inset-0 z-40 bg-black/40 backdrop-blur-[1px]"
          />

          {/* Panel */}
          <motion.aside
            key={task.id}
            initial={{ x: "100%" }}
            animate={{ x: 0 }}
            exit={{ x: "100%", transition: { duration: 0.2, ease: "easeIn" } }}
            transition={{ type: "spring", stiffness: 380, damping: 38 }}
            className="fixed right-0 top-0 z-50 flex h-full w-full max-w-lg flex-col border-l border-border bg-surface shadow-drawer"
          >
            <DrawerHeader
              task={task}
              onClose={onClose}
              onCancel={() => cancel.mutate(task.id)}
              cancelling={cancel.isPending}
            />

            {/* Tabs */}
            <div className="flex shrink-0 border-b border-border px-4">
              {(["summary", "lifecycle"] as const).map((t) => (
                <button
                  key={t}
                  onClick={() => setTab(t)}
                  className={`relative px-3 py-3 text-sm font-medium capitalize transition-colors duration-150 ease-out ${
                    tab === t ? "text-text" : "text-subtle hover:text-muted"
                  }`}
                >
                  {t}
                  {tab === t && (
                    <motion.span
                      layoutId="drawer-tab"
                      className="absolute inset-x-0 -bottom-px h-0.5 rounded-full bg-primary"
                      transition={{ duration: 0.2, ease: "easeOut" }}
                    />
                  )}
                </button>
              ))}
            </div>

            {/* Body */}
            <div className="min-h-0 flex-1 overflow-y-auto p-4">
              {tab === "summary" ? (
                <Summary task={task} />
              ) : (
                <Lifecycle task={task} />
              )}
            </div>
          </motion.aside>
        </>
      )}
    </AnimatePresence>
  );
}

function DrawerHeader({
  task,
  onClose,
  onCancel,
  cancelling,
}: {
  task: Task;
  onClose: () => void;
  onCancel: () => void;
  cancelling: boolean;
}) {
  const cancellable = task.status === "queued" || task.status === "running";
  return (
    <div className="flex shrink-0 items-start gap-3 border-b border-border p-4">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <h2 className="truncate text-base font-semibold text-text">{task.name}</h2>
          <StatusBadge status={task.status} pulse={task.status === "running"} size="md" />
        </div>
        <div className="mt-1 flex items-center gap-2 font-mono text-xs text-subtle">
          <span>{shortId(task.id)}</span>
          <button
            onClick={() => navigator.clipboard?.writeText(task.id)}
            className="text-subtle transition-colors hover:text-primary"
            title="Copy full ID"
          >
            copy
          </button>
        </div>
      </div>
      {cancellable && (
        <button
          onClick={onCancel}
          disabled={cancelling}
          className="inline-flex items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-xs font-medium text-danger ring-1 ring-danger/30 transition-colors duration-150 ease-out hover:bg-danger/10 active:scale-[0.98] disabled:opacity-50"
        >
          <CancelIcon className="h-3.5 w-3.5" />
          Cancel
        </button>
      )}
      <button
        onClick={onClose}
        className="grid h-8 w-8 place-items-center rounded-lg text-subtle transition-colors duration-150 ease-out hover:bg-surface2 hover:text-text active:scale-[0.98]"
        aria-label="Close"
      >
        <CloseIcon className="h-4 w-4" />
      </button>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-3 gap-3 py-2">
      <dt className="text-xs font-medium uppercase tracking-wide text-subtle">{label}</dt>
      <dd className="col-span-2 text-sm text-text">{children}</dd>
    </div>
  );
}

function Summary({ task }: { task: Task }) {
  const result = task.result ? pretty(safeParse(task.result)) : null;
  return (
    <div className="divide-y divide-border">
      <dl>
        <Field label="Status">
          <StatusBadge status={task.status} pulse={task.status === "running"} />
        </Field>
        <Field label="Name">{task.name}</Field>
        <Field label="Tags">
          {task.tags.length === 0 ? (
            <span className="text-subtle">none</span>
          ) : (
            <div className="flex flex-wrap gap-1">
              {task.tags.map((t) => (
                <span key={t} className="rounded bg-surface2 px-1.5 py-0.5 font-mono text-[11px] text-muted">
                  {t}
                </span>
              ))}
            </div>
          )}
        </Field>
        <Field label="Retries">
          {task.retry_count}
          {task.max_retries > 0 && <span className="text-subtle"> / {task.max_retries} max</span>}
        </Field>
        <Field label="Enqueued">
          <span title={clockTime(task.enqueue_time)}>
            {clockTime(task.enqueue_time)} · {timeAgo(task.enqueue_time)}
          </span>
        </Field>
        {task.eta && (
          <Field label="ETA">
            <span title={clockTime(task.eta)}>{clockTime(task.eta)}</span>
          </Field>
        )}
        <Field label="Resources">
          {Object.keys(task.resources).length === 0 ? (
            <span className="text-subtle">none</span>
          ) : (
            <div className="flex flex-wrap gap-1.5">
              {Object.entries(task.resources).map(([k, v]) => (
                <span key={k} className="rounded bg-surface2 px-1.5 py-0.5 font-mono text-[11px] text-muted">
                  {k}: {v}
                </span>
              ))}
            </div>
          )}
        </Field>
      </dl>

      <div className="py-3">
        <div className="mb-2 text-xs font-medium uppercase tracking-wide text-subtle">Payload</div>
        <pre className="overflow-x-auto rounded-lg bg-surface2 p-3 font-mono text-xs text-text">
          {pretty(task.payload)}
        </pre>
      </div>

      <div className="py-3">
        <div className="mb-2 text-xs font-medium uppercase tracking-wide text-subtle">Result</div>
        {result ? (
          <pre className="overflow-x-auto rounded-lg bg-surface2 p-3 font-mono text-xs text-text">{result}</pre>
        ) : (
          <p className="text-sm text-subtle">No result yet.</p>
        )}
      </div>
    </div>
  );
}

// Reconstruct a timeline from the task's current state. We don't store per-event
// timestamps beyond enqueue_time, so we model the canonical lifecycle (Created →
// Queued → Running → Terminal) with a retry marker when retries occurred, and
// mark each node complete / current / pending based on the live status.
function Lifecycle({ task }: { task: Task }) {
  const steps = buildSteps(task.status);

  return (
    <div>
      <ol className="relative">
        {/* vertical rail */}
        <span className="absolute left-[11px] top-2 bottom-2 w-px bg-border" aria-hidden />
        {steps.map((s, i) => (
          <TimelineNode key={s.key} step={s} index={i} task={task} />
        ))}
      </ol>

      {task.retry_count > 0 && (
        <div className="mt-4 flex items-start gap-2 rounded-lg bg-warn/10 p-3 text-xs text-warn">
          <RetryIcon className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            Retried {task.retry_count} time{task.retry_count === 1 ? "" : "s"} of {task.max_retries} max.
            Each retry re-queues the task with an exponential backoff ETA.
          </span>
        </div>
      )}
    </div>
  );
}

type StepState = "done" | "current" | "pending";
interface Step {
  key: string;
  label: string;
  icon: typeof CheckIcon;
  state: StepState;
}

function buildSteps(status: TaskStatus): Step[] {
  // Ordering of lifecycle progression.
  const order: TaskStatus[] = ["created", "queued", "running", "completed"];
  const terminal: TaskStatus[] = ["completed", "failed", "dead-lettered", "cancelled"];

  // Map current status to an index in the happy-path order.
  let progressIdx: number;
  if (status === "failed" || status === "dead-lettered") progressIdx = 3; // reached a running→fail
  else if (status === "cancelled") progressIdx = 2;
  else progressIdx = Math.max(0, order.indexOf(status));

  const steps: Step[] = [
    { key: "created", label: "Created", icon: CheckIcon, state: stepState(0, progressIdx, status) },
    { key: "queued", label: "Queued", icon: ClockIcon, state: stepState(1, progressIdx, status) },
    { key: "running", label: "Running", icon: RetryIcon, state: stepState(2, progressIdx, status) },
  ];

  // Terminal node depends on outcome.
  if (status === "completed") {
    steps.push({ key: "completed", label: "Completed", icon: CheckIcon, state: "current" });
  } else if (status === "failed" || status === "dead-lettered") {
    steps.push({ key: "failed", label: status === "dead-lettered" ? "Dead-lettered" : "Failed", icon: CloseIcon, state: "current" });
  } else if (status === "cancelled") {
    steps.push({ key: "cancelled", label: "Cancelled", icon: CancelIcon, state: "current" });
  } else {
    steps.push({ key: "terminal", label: "Terminal", icon: CheckIcon, state: "pending" });
  }

  // Mark everything before progress as done.
  if (terminal.includes(status) === false && status !== "completed") {
    // already handled via stepState
  }
  return steps;
}

function stepState(idx: number, progress: number, _status: TaskStatus): StepState {
  if (idx < progress) return "done";
  if (idx === progress) return "current";
  return "pending";
}

function TimelineNode({ step, index, task }: { step: Step; index: number; task: Task }) {
  const Icon = step.icon;
  const tone =
    step.state === "done"
      ? "bg-success/15 text-success"
      : step.state === "current"
      ? `${statusStyle(currentStatusFor(step.key, task)).bg} ${statusStyle(currentStatusFor(step.key, task)).text}`
      : "bg-surface2 text-subtle";

  const time = timeForStep(step.key, task);

  return (
    <motion.li
      initial={{ opacity: 0, x: -6 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.2, ease: "easeOut", delay: Math.min(index * 0.05, 0.25) }}
      className="relative flex gap-3 pb-6 last:pb-0"
    >
      <span
        className={`relative z-10 grid h-6 w-6 shrink-0 place-items-center rounded-full ring-4 ring-surface ${tone} ${
          step.state === "current" ? "animate-pulseDot" : ""
        }`}
      >
        <Icon className="h-3.5 w-3.5" />
      </span>
      <div className="pt-0.5">
        <div
          className={`text-sm font-medium ${
            step.state === "pending" ? "text-subtle" : "text-text"
          }`}
        >
          {step.label}
        </div>
        {time && <div className="mt-0.5 text-xs text-subtle">{time}</div>}
        {step.state === "current" && step.key === "running" && (
          <div className="mt-1 text-xs text-info">In progress · awaiting acknowledgment</div>
        )}
      </div>
    </motion.li>
  );
}

function currentStatusFor(key: string, _task: Task): TaskStatus {
  switch (key) {
    case "created":
      return "created";
    case "queued":
      return "queued";
    case "running":
      return "running";
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "cancelled":
      return "cancelled";
    default:
      return "created";
  }
}

function timeForStep(key: string, task: Task): string | null {
  if (key === "created" || key === "queued") return clockTime(task.enqueue_time);
  return null;
}

function safeParse(s: string): unknown {
  try {
    return JSON.parse(s);
  } catch {
    return s;
  }
}
