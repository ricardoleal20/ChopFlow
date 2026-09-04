import { useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { CloseIcon } from "./Icons";
import { clockTime, timeAgo, shortId } from "../lib/format";
import { useEnqueue, usePatchSchedule, useDeleteSchedule } from "../hooks/useChopFlow";
import type { Schedule, OverlapPolicy } from "../lib/api";

interface Props {
  schedule: Schedule | null;
  onClose: () => void;
}

const EASE_OUT = [0.22, 0.61, 0.36, 1] as const;
const EASE_IN = [0.4, 0, 1, 1] as const;

const OVERLAP_LABEL: Record<OverlapPolicy, string> = {
  skip: "Skip",
  coalesce: "Coalesce",
  allow: "Allow",
};

// Schedule slide-over drawer — mirrors TaskDrawer's motion pattern (backdrop +
// aside, ease-out in / ease-in out, Esc to close). Shows the frozen template +
// fire schedule, and exposes three actions: Run now (materializes a one-off
// task via POST /api/tasks with schedule_id unset), Enable/Disable toggle, and
// Delete.
export default function ScheduleDrawer({ schedule, onClose }: Props) {
  useEffect(() => {
    if (!schedule) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [schedule, onClose]);

  return (
    <AnimatePresence>
      {schedule && (
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
            key={schedule.id}
            initial={{ x: "100%" }}
            animate={{ x: 0 }}
            exit={{ x: "100%", transition: { duration: 0.2, ease: EASE_IN } }}
            transition={{ duration: 0.22, ease: EASE_OUT }}
            className="drawer open"
          >
            <DrawerBody schedule={schedule} onClose={onClose} />
          </motion.aside>
        </>
      )}
    </AnimatePresence>
  );
}

function DrawerBody({ schedule, onClose }: { schedule: Schedule; onClose: () => void }) {
  const enqueue = useEnqueue();
  const patch = usePatchSchedule();
  const del = useDeleteSchedule();

  const tmpl = schedule.task_template;
  const isCron = schedule.kind.type === "cron";
  let trigger: string;
  if (schedule.kind.type === "cron") {
    trigger = schedule.kind.cron;
  } else {
    trigger = new Date(schedule.kind.eta).toLocaleString();
  }

  const runNow = () => {
    enqueue.mutate(
      {
        name: tmpl.name,
        payload: tmpl.payload,
        tags: tmpl.tags,
        resources: tmpl.resources,
        max_retries: tmpl.max_retries,
      },
      { onError: () => { /* surfaced via isError; keep drawer open */ } }
    );
  };

  const toggleEnabled = () => {
    patch.mutate({ id: schedule.id, body: { enabled: !schedule.enabled } });
  };

  const remove = () => {
    del.mutate(schedule.id, { onSuccess: () => onClose() });
  };

  return (
    <>
      <div className="dr-head">
        <div className="dr-head-top">
          <div className="dr-title">
            <div className="dr-name">
              {schedule.name}{" "}
              <span
                className={`stbadge badge ${schedule.enabled ? "completed" : "failed"}`}
              >
                <span className="d" />
                {schedule.enabled ? "Enabled" : "Disabled"}
              </span>
            </div>
            <div className="dr-idrow">
              <span className="dr-id mono">{schedule.id}</span>
            </div>
          </div>
          <div className="dr-actions">
            <button
              className="btn btn-ghost btn-sm"
              disabled={enqueue.isPending}
              onClick={runNow}
            >
              Run now
            </button>
            <button
              className="btn btn-ghost btn-sm"
              disabled={patch.isPending}
              onClick={toggleEnabled}
            >
              {schedule.enabled ? "Disable" : "Enable"}
            </button>
            <button
              className="btn btn-danger btn-sm"
              disabled={del.isPending}
              onClick={remove}
            >
              <CloseIcon />
              Delete
            </button>
          </div>
          <button className="dr-close" onClick={onClose} aria-label="Close">
            <CloseIcon />
          </button>
        </div>
      </div>

      <div className="dr-body">
        <div className="dr-pane">
          <div className="field-grid">
            <div className="field">
              <div className="k">Kind</div>
              <div className="v">
                <span
                  className={`stbadge badge ${isCron ? "queued" : "running"}`}
                >
                  <span className="d" />
                  {isCron ? "Cron" : "One-shot"}
                </span>
              </div>
            </div>
            <div className="field">
              <div className="k">Overlap</div>
              <div className="v mono">{OVERLAP_LABEL[schedule.overlap_policy]}</div>
            </div>
            <div className="field">
              <div className="k">Trigger</div>
              <div className="v mono">{trigger}</div>
            </div>
            <div className="field">
              <div className="k">Template</div>
              <div className="v mono">{tmpl.name}</div>
            </div>
            <div className="field">
              <div className="k">Next fire</div>
              <div className="v mono">
                {clockTime(schedule.next_fire)}
                <br />
                <span style={{ fontFamily: "var(--font-sans)", fontSize: 11.5, color: "var(--muted)" }}>
                  {timeAgo(schedule.next_fire)}
                </span>
              </div>
            </div>
            <div className="field">
              <div className="k">Last fired</div>
              <div className="v mono">
                {schedule.last_fired ? (
                  <>
                    {clockTime(schedule.last_fired)}
                    <br />
                    <span style={{ fontFamily: "var(--font-sans)", fontSize: 11.5, color: "var(--muted)" }}>
                      {timeAgo(schedule.last_fired)}
                    </span>
                  </>
                ) : (
                  <span style={{ color: "var(--muted-2)" }}>never</span>
                )}
              </div>
            </div>
            <div className="field">
              <div className="k">Tags</div>
              <div className="v">
                <div className="tagpills">
                  {tmpl.tags.length === 0 ? (
                    <span style={{ fontSize: 11, color: "var(--muted-2)" }}>—</span>
                  ) : (
                    tmpl.tags.map((t) => (
                      <span key={t} className={`tag ${t}`}>
                        {t}
                      </span>
                    ))
                  )}
                </div>
              </div>
            </div>
            <div className="field">
              <div className="k">Resources</div>
              <div className="v">
                <div className="res-pills">
                  {Object.keys(tmpl.resources).length === 0 ? (
                    <span style={{ fontSize: 11, color: "var(--muted-2)" }}>—</span>
                  ) : (
                    Object.entries(tmpl.resources).map(([k, v]) => (
                      <span key={k} className="res-pill">
                        {k} {v}
                      </span>
                    ))
                  )}
                </div>
              </div>
            </div>
            <div className="field">
              <div className="k">Max retries</div>
              <div className="v mono">{tmpl.max_retries}</div>
            </div>
            <div className="field">
              <div className="k">Created</div>
              <div className="v mono">
                {clockTime(schedule.created_at)}
                <br />
                <span style={{ fontFamily: "var(--font-sans)", fontSize: 11.5, color: "var(--muted)" }}>
                  {timeAgo(schedule.created_at)}
                </span>
              </div>
            </div>
          </div>

          <div className="json-block">
            <div className="json-head">
              <span className="lbl">Payload</span>
              <span className="tag">template</span>
            </div>
            <pre className="json-pre">{JSON.stringify(tmpl.payload, null, 2)}</pre>
          </div>

          <div className="dr-note" style={{ fontSize: 11.5, color: "var(--muted-2)", marginTop: 4 }}>
            Schedule id <span className="mono">{shortId(schedule.id)}</span> ·
            Run now materializes a one-off task without advancing this schedule.
          </div>
        </div>
      </div>
    </>
  );
}
