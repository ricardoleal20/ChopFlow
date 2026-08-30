import { useEffect, useRef, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { useEnqueue } from "../hooks/useChopFlow";

interface EnqueueDialogProps {
  open: boolean;
  onClose: () => void;
}

export function EnqueueDialog({ open, onClose }: EnqueueDialogProps) {
  const [name, setName] = useState("echo");
  const [tags, setTags] = useState("default");
  const [payload, setPayload] = useState('{\n  "message": "hello"\n}');
  const [maxRetries, setMaxRetries] = useState("3");
  const [resources, setResources] = useState("");
  const [formError, setFormError] = useState<string | null>(null);

  const enqueue = useEnqueue();
  const closeRef = useRef<HTMLButtonElement>(null);

  // Focus the close button on open + clear errors when (re)opening.
  useEffect(() => {
    if (open) {
      setFormError(null);
      closeRef.current?.focus();
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
        parsedResources = JSON.parse(resources);
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
        <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto p-4 sm:items-center">
          {/* Dimmed backdrop — staging-dim-background */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0, transition: { duration: 0.15, ease: "easeIn" } }}
            transition={{ duration: 0.18, ease: "easeOut" }}
            className="fixed inset-0 bg-black/60 backdrop-blur-sm"
            onClick={onClose}
            aria-hidden
          />

          {/* Dialog — ease-out entrance / ease-in exit per animation principles */}
          <motion.div
            role="dialog"
            aria-modal="true"
            aria-labelledby="enqueue-title"
            initial={{ opacity: 0, y: 12, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 8, scale: 0.98, transition: { duration: 0.18, ease: "easeIn" } }}
            transition={{ duration: 0.22, ease: [0.22, 1, 0.36, 1] }}
            className="relative z-10 w-full max-w-lg surface p-6"
          >
            <div className="flex items-start justify-between">
              <div>
                <h2 id="enqueue-title" className="text-base font-semibold tracking-tight">
                  Enqueue task
                </h2>
                <p className="mt-0.5 text-xs text-muted">Submit a task to the cluster. A worker claims it on the next poll.</p>
              </div>
              <button
                ref={closeRef}
                onClick={onClose}
                aria-label="Close dialog"
                className="rounded-lg p-1.5 text-muted transition-colors duration-150 ease-out hover:bg-surface2 hover:text-text active:scale-[0.98]"
              >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden>
                  <path d="M6 6l12 12M18 6L6 18" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                </svg>
              </button>
            </div>

            <form onSubmit={handleSubmit} className="mt-5 space-y-4">
              <Field label="Task name">
                <input
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
                  className="cf-input font-mono text-xs resize-y"
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
                    className="cf-input font-mono text-xs"
                    placeholder='{"cpu": 1}'
                  />
                </Field>
              </div>

              {formError && (
                <p role="alert" className="rounded-lg bg-danger/10 px-3 py-2 text-xs text-danger ring-1 ring-danger/25">
                  {formError}
                </p>
              )}

              <div className="flex items-center justify-end gap-2 pt-1">
                <button
                  type="button"
                  onClick={onClose}
                  className="rounded-xl px-3.5 py-2 text-sm font-medium text-muted transition-colors duration-150 ease-out hover:bg-surface2 hover:text-text active:scale-[0.98]"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={enqueue.isPending}
                  className="rounded-xl bg-accent px-3.5 py-2 text-sm font-semibold text-canvas shadow-glow transition-transform duration-150 ease-out hover:bg-accentDim active:scale-[0.98] disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {enqueue.isPending ? "Enqueuing…" : "Enqueue"}
                </button>
              </div>
            </form>
          </motion.div>
        </div>
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
      <div className="mb-1.5 flex items-center justify-between">
        <span className="text-xs font-medium text-muted">{label}</span>
        {hint && <span className="text-[11px] text-subtle">{hint}</span>}
      </div>
      {children}
    </label>
  );
}
