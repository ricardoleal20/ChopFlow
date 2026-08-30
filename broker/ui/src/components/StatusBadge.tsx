import type { TaskStatus } from "../lib/api";

// Status → {label, classes, dot}. Never rely on color alone: every badge has
// a text label + a dot, so it survives colorblindness and grayscale.
const MAP: Record<
  TaskStatus,
  { label: string; classes: string; dot: string }
> = {
  queued: { label: "Queued", classes: "text-info bg-info/10 ring-1 ring-info/20", dot: "bg-info" },
  running: { label: "Running", classes: "text-warn bg-warn/10 ring-1 ring-warn/25", dot: "bg-warn animate-pulseDot" },
  completed: { label: "Completed", classes: "text-accent bg-accent/10 ring-1 ring-accent/20", dot: "bg-accent" },
  failed: { label: "Failed", classes: "text-danger bg-danger/10 ring-1 ring-danger/25", dot: "bg-danger" },
  "dead-lettered": { label: "Dead-lettered", classes: "text-danger bg-danger/15 ring-1 ring-danger/30", dot: "bg-danger" },
  cancelled: { label: "Cancelled", classes: "text-subtle bg-muted/10 ring-1 ring-border", dot: "bg-subtle" },
  created: { label: "Created", classes: "text-muted bg-muted/10 ring-1 ring-border", dot: "bg-muted" },
};

export function StatusBadge({ status }: { status: TaskStatus }) {
  const s = MAP[status];
  return (
    <span className={`chip ${s.classes}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${s.dot}`} aria-hidden />
      {s.label}
    </span>
  );
}
