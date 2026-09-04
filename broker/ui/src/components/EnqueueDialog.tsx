import { useEffect, useRef, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { useEnqueue } from "../hooks/useChopFlow";
import { CloseIcon } from "./Icons";

interface EnqueueDialogProps {
  open: boolean;
  onClose: () => void;
}

const EASE_OUT = [0.22, 0.61, 0.36, 1] as const;
const EASE_IN = [0.4, 0, 1, 1] as const;

// Enqueue dialog — the only solid-primary action surface besides the TopBar
// CTA. Ease-out entrance / ease-in exit per the motion principles. Submits a
// task to the broker; a worker claims it on the next poll.
export function EnqueueDialog({ open, onClose }: EnqueueDialogProps) {
  const [name, setName] = useState("echo");
  const [tags, setTags] = useState("default");
  const [payload, setPayload] = useState('{\n  "message": "hello"\n}');
  const [maxRetries, setMaxRetries] = useState("3");
  const [resources, setResources] = useState("");
  const [formError, setFormError] = useState<string | null>(null);

  const enqueue = useEnqueue();
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

    enqueue.mutate(
      {
        name: name.trim() || "task",
        payload: parsedPayload,
        tags: tags.split(",").map((t) => t.trim()).filter(Boolean),
        max_retries: retries,
        resources: parsedResources,
      },
      {
        onSuccess: () => onClose(),
        onError: (err) => setFormError((err as Error).message),
      }
    );
  };

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
                <h2 id="enqueue-title">Enqueue task</h2>
                <p className="sub">Submit a task to the cluster. A worker claims it on the next poll.</p>
              </div>
              <button className="btn-icon" onClick={onClose} aria-label="Close dialog">
                <CloseIcon />
              </button>
            </div>

            <form onSubmit={handleSubmit} className="mt-5 space-y-4">
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

              <div className="grid grid-cols-2 gap-4">
                <Field label="Max retries">
                  <input
                    value={maxRetries}
                    onChange={(e) => setMaxRetries(e.target.value)}
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

              {formError && <p className="form-error">{formError}</p>}

              <div className="flex items-center justify-end gap-2 pt-1">
                <button type="button" className="btn btn-ghost btn-sm" onClick={onClose}>
                  Cancel
                </button>
                <button type="submit" className="btn btn-primary btn-sm" disabled={enqueue.isPending}>
                  {enqueue.isPending ? "Enqueuing…" : "Enqueue"}
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
