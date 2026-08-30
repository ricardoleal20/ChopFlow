import { statusStyle } from "../lib/status";
import type { TaskStatus } from "../lib/api";

interface Props {
  status: TaskStatus;
  pulse?: boolean; // animate the dot for in-flight tasks
  size?: "sm" | "md";
}

// Status badge mirroring Temporal: a colored dot + label on a soft chip. The
// running dot pulses to signal liveness; everything else is static.
export default function StatusBadge({ status, pulse, size = "sm" }: Props) {
  const s = statusStyle(status);
  const pad = size === "md" ? "px-2.5 py-1 text-xs" : "px-2 py-0.5 text-[11px]";
  const dot = size === "md" ? "h-2 w-2" : "h-1.5 w-1.5";

  return (
    <span className={`chip ${s.bg} ${s.text} ${pad}`}>
      <span
        className={`${dot} rounded-full ${s.dot} ${pulse ? "animate-pulseDot" : ""}`}
        aria-hidden
      />
      {s.label}
    </span>
  );
}
