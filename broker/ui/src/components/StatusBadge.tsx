import type { TaskStatus } from "../lib/api";

interface Props {
  status: TaskStatus;
  pulse?: boolean; // reserved — the running dot pulses via CSS automatically
  className?: string;
}

// The sole sanctioned status representation: a dot + label pill on a soft
// tint. The running dot pulses to signal liveness; everything else is static.
// Color is never the only signal — the label always travels with it.
const LABEL: Record<TaskStatus, string> = {
  created: "Created",
  queued: "Queued",
  running: "Running",
  completed: "Completed",
  failed: "Failed",
  "dead-lettered": "Dead-lettered",
  cancelled: "Cancelled",
};

export default function StatusBadge({ status, className = "" }: Props) {
  return (
    <span className={`stbadge badge ${status} ${className}`.trim()}>
      <span className="d" />
      {LABEL[status]}
    </span>
  );
}
