import { useEffect, useRef, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { useEnqueue, useCreateSchedule } from "../hooks/useChopFlow";
import type { OverlapPolicy } from "../lib/api";
import { CloseIcon } from "./Icons";

interface EnqueueDialogProps {
  open: boolean;
  onClose: () => void;
}

const EASE_OUT = [0.22, 0.61, 0.36, 1] as const;
const EASE_IN = [0.4, 0, 1, 1] as const;

type Mode = "now" | "schedule";
type SchedKind = "oneshot" | "cron";

// Enqueue dialog — the only solid-primary action surface besides the TopBar
// CTA. Ease-out entrance / ease-in exit per the motion principles. A segmented
// control at the top branches between Run immediately (POST /api/tasks via
// useEnqueue) and Schedule (POST /api/schedules via useCreateSchedule).
export function EnqueueDialog({ open, onClose }: EnqueueDialogProps) {
  const [mode, setMode] = useState<Mode>("now");
  const [schedKind, setSchedKind] = useState<SchedKind>("oneshot");
  const [etaLocal, setEtaLocal] = useState("");
  const [cronExpr, setCronExpr] = useState("*/5 * * * *");
  const [overlap, setOverlap] = useState<OverlapPolicy>("skip");

  const [name, setName] = useState("echo");
  const [tags, setTags] = useState("default");
  const [payload, setPayload] = useState('{\n  "message": "hello"\n}');
  const [maxRetries, setMaxRetries] = useState("3");
  const [priority, setPriority] = useState("0");
  const [resources, setResources] = useState("");
  const [formError, setFormError] = useState<string | null>(null);

  const enqueue = useEnqueue();
  const createSchedule = useCreateSchedule();
  const nameRef = useRef<HTMLInputElement>(null);

  // Focus the name field on open + clear errors when (re)opening.
  useEffect(() => {
    if (open) {
      setFormError(null);
      const t = setTimeout(() => nameRef.current?.focus(), 60);
      return () => clearTimeout(t);
    }
  }, [open]);

  // Close on Escape.
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  const pending = enqueue.isPending || createSchedule.isPending;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setFormError(null);

    let parsedPayload: unknown;
    try {
      parsedPayload = JSON.parse(payload);
    } catch {
      setFormError("Payload must be valid JSON.");
      return;
    }

    let parsedResources: Record<string, number> = {};
    if (resources.trim()) {
      try {
        const obj = JSON.parse(resources);
        if (typeof obj !== "object" || obj === null || Array.isArray(obj)) {
          setFormError("Resources must be a JSON object, e.g. {\"cpu\": 1}.");
          return;
        }
        parsedResources = obj as Record<string, number>;
      } catch {
        setFormError("Resources must be a JSON object, e.g. {\"cpu\": 1}.");
        return;
      }
    }

    const retries = Number(maxRetries);
    if (Number.isNaN(retries) || retries < 0) {
      setFormError("Max retries must be a non-negative number.");
      return;
    }

    const prio = Number(priority);
    if (Number.isNaN(prio)) {
      setFormError("Priority must be an integer.");
      return;
    }

    const taskName = name.trim() || "task";
    const tagList = tags.split(",").map((t) => t.trim()).filter(Boolean);

    if (mode === "now") {
      enqueue.mutate(
        {
          name: taskName,
          payload: parsedPayload,
          tags: tagList,
          max_retries: retries,
          resources: parsedResources,
          priority: prio,
        },
        {
          onSuccess: () => onClose(),
          onError: (err) => setFormError((err as Error).message),
        }
      );
      return;
    }

    // Schedule mode — build a CreateScheduleBody.
    const template = {
      name: taskName,
      payload: parsedPayload,
      tags: tagList,
      resources: parsedResources,
      max_retries: retries,
      priority: prio,
    };

    if (schedKind === "oneshot") {
      if (!etaLocal) {
        setFormError("Pick a date/time for the one-shot schedule.");
        return;
      }
      // datetime-local is wall-clock in the user's locale; convert to UTC RFC3339.
      const dt = new Date(etaLocal);
      if (Number.isNaN(dt.getTime())) {
        setFormError("Invalid date/time.");
        return;
      }
      createSchedule.mutate(
        {
          name: taskName,
          task_template: template,
          kind: { type: "oneshot", eta: dt.toISOString() },
          overlap_policy: overlap,
        },
        {
          onSuccess: () => onClose(),
          onError: (err) => setFormError((err as Error).message),
        }
      );
    } else {
      const expr = cronExpr.trim();
      if (!expr) {
        setFormError("Enter a cron expression.");
        return;
      }
      createSchedule.mutate(
        {
          name: taskName,
          task_template: template,
          kind: { type: "cron", cron: expr },
          overlap_policy: overlap,
        },
        {
          onSuccess: () => onClose(),
          onError: (err) => setFormError((err as Error).message),
        }
      );
    }
  };

  const submitLabel = mode === "now"
    ? (enqueue.isPending ? "Enqueuing…" : "Enqueue")
    : (createSchedule.isPending ? "Scheduling…" : "Schedule");

  return (
    <AnimatePresence>
      {open && (
        <motion.div
          className="modal-backdrop"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0, transition: { duration: 0.15, ease: EASE_IN } }}
          transition={{ duration: 0.18, ease: EASE_OUT }}
          onClick={onClose}
        >
          <motion.div
            role="dialog"
            aria-modal="true"
            aria-labelledby="enqueue-title"
            className="modal"
            initial={{ opacity: 0, y: 10, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 8, scale: 0.98, transition: { duration: 0.18, ease: EASE_IN } }}
            transition={{ duration: 0.22, ease: EASE_OUT }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-start justify-between">
              <div>
                <h2 id="enqueue-title">New task</h2>
                <p className="sub">Run a task now, or schedule it to repeat.</p>
              </div>
              <button className="btn-icon" onClick={onClose} aria-label="Close dialog">
                <CloseIcon />
              </button>
            </div>

            <form onSubmit={handleSubmit} className="mt-5 space-y-4">
              {/* Mode segmented control */}
              <div className="seg" role="tablist" aria-label="Execution mode">
                <button
                  type="button"
                  role="tab"
                  aria-selected={mode === "now"}
                  className={`seg-btn${mode === "now" ? " active" : ""}`}
                  onClick={() => setMode("now")}
                >
                  Run immediately
                </button>
                <button
                  type="button"
                  role="tab"
                  aria-selected={mode === "schedule"}
                  className={`seg-btn${mode === "schedule" ? " active" : ""}`}
                  onClick={() => setMode("schedule")}
                >
                  Schedule
                </button>
              </div>

              <Field label="Task name">
                <input
                  ref={nameRef}
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  className="cf-input"
                  placeholder="echo"
                />
              </Field>

              <Field label="Tags" hint="Comma-separated">
                <input
                  value={tags}
                  onChange={(e) => setTags(e.target.value)}
                  className="cf-input"
                  placeholder="default, gpu"
                />
              </Field>

              <Field label="Payload" hint="JSON">
                <textarea
                  value={payload}
                  onChange={(e) => setPayload(e.target.value)}
                  rows={4}
                  className="cf-input mono"
                  spellCheck={false}
                />
              </Field>

              <div className="grid grid-cols-3 gap-4">
                <Field label="Max retries">
                  <input
                    value={maxRetries}
                    onChange={(e) => setMaxRetries(e.target.value)}
                    className="cf-input"
                    inputMode="numeric"
                  />
                </Field>
                <Field label="Priority" hint="higher = first">
                  <input
                    value={priority}
                    onChange={(e) => setPriority(e.target.value)}
                    className="cf-input"
                    inputMode="numeric"
                  />
                </Field>
                <Field label="Resources" hint="JSON, optional">
                  <input
                    value={resources}
                    onChange={(e) => setResources(e.target.value)}
                    className="cf-input mono"
                    placeholder='{"cpu": 1}'
                  />
                </Field>
              </div>

              {mode === "schedule" && (
                <div className="sched-fields">
                  <Field label="Trigger">
                    <div className="radio-row">
                      <label className="radio">
                        <input
                          type="radio"
                          name="sched-kind"
                          checked={schedKind === "oneshot"}
                          onChange={() => setSchedKind("oneshot")}
                        />
                        One-shot
                      </label>
                      <label className="radio">
                        <input
                          type="radio"
                          name="sched-kind"
                          checked={schedKind === "cron"}
                          onChange={() => setSchedKind("cron")}
                        />
                        Cron
                      </label>
                    </div>
                  </Field>

                  {schedKind === "oneshot" ? (
                    <Field label="Run at" hint="local time → UTC">
                      <input
                        type="datetime-local"
                        value={etaLocal}
                        onChange={(e) => setEtaLocal(e.target.value)}
                        className="cf-input"
                      />
                    </Field>
                  ) : (
                    <Field label="Cron expression" hint="5-field, e.g. */5 * * * *">
                      <input
                        value={cronExpr}
                        onChange={(e) => setCronExpr(e.target.value)}
                        className="cf-input mono"
                        placeholder="0 9 * * *"
                      />
                    </Field>
                  )}

                  <Field label="Overlap policy" hint="when a prior run is still active">
                    <select
                      value={overlap}
                      onChange={(e) => setOverlap(e.target.value as OverlapPolicy)}
                      className="cf-input"
                    >
                      <option value="skip">Skip</option>
                      <option value="coalesce">Coalesce</option>
                      <option value="allow">Allow</option>
                    </select>
                  </Field>
                </div>
              )}

              {formError && <p className="form-error">{formError}</p>}

              <div className="flex items-center justify-end gap-2 pt-1">
                <button type="button" className="btn btn-ghost btn-sm" onClick={onClose}>
                  Cancel
                </button>
                <button type="submit" className="btn btn-primary btn-sm" disabled={pending}>
                  {submitLabel}
                </button>
              </div>
            </form>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="block">
      <div className="flex items-center justify-between">
        <span className="fld-label" style={{ marginBottom: 0 }}>
          {label}
        </span>
        {hint && (
          <span style={{ fontSize: 11, color: "var(--muted-2)" }}>{hint}</span>
        )}
      </div>
      <div className="mt-1.5">{children}</div>
    </label>
  );
}
