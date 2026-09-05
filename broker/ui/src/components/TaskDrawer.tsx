import { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import StatusBadge from "./StatusBadge";
import { clockTime, timeAgo } from "../lib/format";
import { CloseIcon, CopyIcon, CheckIcon, AlertIcon } from "./Icons";
import { useCancel } from "../hooks/useChopFlow";
import type { Task, TaskStatus } from "../lib/api";

interface Props {
  task: Task | null;
  onClose: () => void;
}

const EASE_OUT = [0.22, 0.61, 0.36, 1] as const;
const EASE_IN = [0.4, 0, 1, 1] as const;

// Slide-over detail drawer with two tabs: Summary (full task record) and
// Lifecycle (a timeline reconstructed from status + retries). Motion follows
// the 12 principles: ease-out entrance, ease-in exit, dimmed backdrop.
export default function TaskDrawer({ task, onClose }: Props) {
  const [tab, setTab] = useState<"summary" | "lifecycle">("summary");

  // Esc closes; reset to Summary tab whenever a new task opens.
  useEffect(() => {
    if (!task) return;
    setTab("summary");
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [task, onClose]);

  return (
    <AnimatePresence>
      {task && (
        <>
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0, transition: { duration: 0.18, ease: EASE_IN } }}
            transition={{ duration: 0.2, ease: EASE_OUT }}
            onClick={onClose}
            className="backdrop show"
          />
          <motion.aside
            key={task.id}
            initial={{ x: "100%" }}
            animate={{ x: 0 }}
            exit={{ x: "100%", transition: { duration: 0.2, ease: EASE_IN } }}
            transition={{ duration: 0.22, ease: EASE_OUT }}
            className="drawer open"
          >
            <DrawerHeader task={task} onClose={onClose} />
            <div className="dr-tabs">
              <button
                className={`dr-tab${tab === "summary" ? " active" : ""}`}
                onClick={() => setTab("summary")}
              >
                Summary
              </button>
              <button
                className={`dr-tab${tab === "lifecycle" ? " active" : ""}`}
                onClick={() => setTab("lifecycle")}
              >
                Lifecycle
              </button>
            </div>
            <div className="dr-body">
              {tab === "summary" ? <Summary task={task} /> : <Lifecycle task={task} />}
            </div>
          </motion.aside>
        </>
      )}
    </AnimatePresence>
  );
}

function DrawerHeader({ task, onClose }: { task: Task; onClose: () => void }) {
  const cancel = useCancel();
  const [copied, setCopied] = useState(false);
  const cancellable = task.status === "queued" || task.status === "running";

  const copy = () => {
    navigator.clipboard?.writeText(task.id).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1400);
    });
  };

  return (
    <div className="dr-head">
      <div className="dr-head-top">
        <div className="dr-title">
          <div className="dr-name">
            {task.name} <StatusBadge status={task.status} />
          </div>
          <div className="dr-idrow">
            <span className="dr-id mono">{task.id}</span>
            <span className={`copylink${copied ? " done" : ""}`} onClick={copy}>
              {copied ? <CheckIcon /> : <CopyIcon />}
              {copied ? "copied" : "copy"}
            </span>
          </div>
        </div>
        <div className="dr-actions">
          {cancellable && (
            <button
              className="btn btn-danger btn-sm"
              disabled={cancel.isPending}
              onClick={() => cancel.mutate(task.id)}
            >
              <CloseIcon />
              Cancel
            </button>
          )}
        </div>
        <button className="dr-close" onClick={onClose} aria-label="Close">
          <CloseIcon />
        </button>
      </div>
    </div>
  );
}

function retryCallout(retries: number, max: number, body: string) {
  return (
    <div className="callout">
      <AlertIcon />
      <div>
        <div className="t">
          {retries} retr{retries > 1 ? "ies" : "y"} {body}
        </div>
        <div className="d">
          {max > 0
            ? `Backoff schedule: 1s → 4s → 9s. Retry budget of ${max} ${retries >= max ? "exhausted." : "remaining."}`
            : "This task was re-queued after a transient failure."}
        </div>
      </div>
    </div>
  );
}

function Summary({ task }: { task: Task }) {
  const resultTag =
    task.status === "completed" ? "output" : task.status === "failed" ? "error" : task.status === "cancelled" ? "—" : "pending";

  return (
    <div className="dr-pane">
      {task.retry_count > 0 && retryCallout(task.retry_count, task.max_retries, "recorded")}
      {task.schedule_id && (
        <div className="callout sched-lineage">
          <div>
            <div className="t">Spawned by schedule</div>
            <div className="d mono">{task.schedule_id}</div>
          </div>
        </div>
      )}
      <div className="field-grid">
        <div className="field">
          <div className="k">Status</div>
          <div className="v">
            <StatusBadge status={task.status} />
          </div>
        </div>
        <div className="field">
          <div className="k">Name</div>
          <div className="v mono">{task.name}</div>
        </div>
        <div className="field">
          <div className="k">Tags</div>
          <div className="v">
            <div className="tagpills">
              {task.tags.length === 0 ? (
                <span style={{ fontSize: 11, color: "var(--muted-2)" }}>—</span>
              ) : (
                task.tags.map((t) => (
                  <span key={t} className={`tag ${t}`}>
                    {t}
                  </span>
                ))
              )}
            </div>
          </div>
        </div>
        <div className="field">
          <div className="k">Retries</div>
          <div className="v mono">
            {task.retry_count}/{task.max_retries}
          </div>
        </div>
        <div className="field">
          <div className="k">Enqueued</div>
          <div className="v mono">
            {clockTime(task.enqueue_time)}
            <br />
            <span style={{ fontFamily: "var(--font-sans)", fontSize: 11.5, color: "var(--muted)" }}>
              {timeAgo(task.enqueue_time)}
            </span>
          </div>
        </div>
        <div className="field">
          <div className="k">Resources</div>
          <div className="v">
            <div className="res-pills">
              {Object.keys(task.resources).length === 0 ? (
                <span style={{ fontSize: 11, color: "var(--muted-2)" }}>—</span>
              ) : (
                Object.entries(task.resources).map(([k, v]) => (
                  <span key={k} className="res-pill">
                    {k} {v}
                  </span>
                ))
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="json-block">
        <div className="json-head">
          <span className="lbl">Payload</span>
          <span className="tag">input</span>
        </div>
        <pre className="json-pre" dangerouslySetInnerHTML={{ __html: highlight(task.payload) }} />
      </div>

      <div className="json-block">
        <div className="json-head">
          <span className="lbl">Result</span>
          <span className="tag">{resultTag}</span>
        </div>
        <pre className="json-pre" dangerouslySetInnerHTML={{ __html: highlightResult(task.result) }} />
      </div>
    </div>
  );
}

// ---- Lifecycle timeline -----------------------------------------------------

interface Step {
  cls: "done" | "cur" | "fail" | "canc" | "pend" | "queued-done";
  title: string;
  badge?: TaskStatus;
  time: string;
  note: string;
}

function buildSteps(task: Task): Step[] {
  const t = clockTime(task.enqueue_time);
  const steps: Step[] = [
    { cls: "done", title: "Created", time: t, note: "Task admitted by the scheduler." },
  ];

  if (task.status === "cancelled") {
    steps.push({ cls: "queued-done", title: "Queued", badge: "queued", time: "+", note: "Awaiting worker pickup." });
    steps.push({ cls: "canc", title: "Cancelled", badge: "cancelled", time: t, note: "Cancellation requested by operator before execution." });
  } else if (task.status === "queued") {
    steps.push({ cls: "cur", title: "Queued", badge: "queued", time: t, note: "Waiting for an available worker matching the task tags." });
    steps.push({ cls: "pend", title: "Running", time: "—", note: "" });
    steps.push({ cls: "pend", title: task.max_retries > 0 ? "Completed / Failed" : "Completed", time: "—", note: "" });
  } else if (task.status === "created") {
    steps.push({ cls: "pend", title: "Queued", time: "—", note: "" });
    steps.push({ cls: "pend", title: "Running", time: "—", note: "" });
    steps.push({ cls: "pend", title: "Completed", time: "—", note: "" });
  } else {
    // running / completed / failed / dead-lettered all went through queued
    steps.push({ cls: "queued-done", title: "Queued", badge: "queued", time: "+", note: "Picked up by a worker." });
    if (task.status === "running") {
      steps.push({ cls: "cur", title: "Running", badge: "running", time: t, note: "Executing on a worker. Heartbeat healthy." });
      steps.push({ cls: "pend", title: "Completed", time: "—", note: "" });
    } else if (task.status === "completed") {
      steps.push({ cls: "done", title: "Running", time: "+", note: "Execution finished without error." });
      steps.push({ cls: "done", title: "Completed", badge: "completed", time: t, note: "Result persisted to storage." });
    } else if (task.status === "failed" || task.status === "dead-lettered") {
      const label = task.status === "dead-lettered" ? "Dead-lettered" : "Failed";
      steps.push({ cls: "fail", title: "Running", time: "+", note: `Execution failed after ${task.retry_count} retr${task.retry_count === 1 ? "y" : "ies"}.` });
      steps.push({ cls: "fail", title: label, badge: task.status, time: t, note: "Retry budget exhausted — moved to dead-letter." });
    }
  }
  return steps;
}

function Lifecycle({ task }: { task: Task }) {
  const steps = buildSteps(task);
  return (
    <div className="dr-pane">
      {task.retry_count > 0 && retryCallout(task.retry_count, task.max_retries, "before terminal state")}
      <div className="timeline">
        {steps.map((s, i) => (
          <div key={i} className={`tl-node ${s.cls}`}>
            <div className="tl-dot" />
            <div className="tl-rail" />
            <div className="tl-content">
              <div className="tl-title">
                {s.title}
                {s.badge && <StatusBadge status={s.badge} />}
              </div>
              <div className="tl-time">{s.time}</div>
              {s.note && <div className="tl-note">{s.note}</div>}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---- JSON syntax highlighting ----------------------------------------------

function esc(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;");
}

function highlight(value: unknown): string {
  const s = JSON.stringify(value, null, 2);
  return esc(s)
    .replace(/("(?:\\.|[^"\\])*")(\s*:)/g, '<span class="k">$1</span>$2')
    .replace(/:\s*("(?:\\.|[^"\\])*")/g, ': <span class="s">$1</span>')
    .replace(/:\s*(true|false)/g, ": <span class='b'>$1</span>")
    .replace(/:\s*(-?\d+\.?\d*)/g, ": <span class='n'>$1</span>");
}

function highlightResult(result: string | null): string {
  if (result === null || result === "") return '<span class="empty">— no result yet —</span>';
  let parsed: unknown = result;
  try {
    parsed = JSON.parse(result);
  } catch {
    /* keep as plain string */
  }
  if (typeof parsed === "string") {
    return `<span class="s">${esc(JSON.stringify(parsed))}</span>`;
  }
  return highlight(parsed);
}
