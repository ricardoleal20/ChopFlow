import type { TaskStatus } from "./api";

// Status → visual semantics. Temporal-style: each status maps to a hue (text
// + dot + soft background) and a label. Color is never the only signal — the
// label travels with it, so the palette stays colorblind-safe.
export interface StatusStyle {
  label: string;
  dot: string; // tailwind classes for the dot
  text: string; // tailwind classes for text
  bg: string; // soft background
}

const MAP: Record<TaskStatus, StatusStyle> = {
  created: {
    label: "Created",
    dot: "bg-slate2",
    text: "text-slate2",
    bg: "bg-slate2/10",
  },
  queued: {
    label: "Queued",
    dot: "bg-warn",
    text: "text-warn",
    bg: "bg-warn/10",
  },
  running: {
    label: "Running",
    dot: "bg-info",
    text: "text-info",
    bg: "bg-info/10",
  },
  completed: {
    label: "Completed",
    dot: "bg-success",
    text: "text-success",
    bg: "bg-success/10",
  },
  failed: {
    label: "Failed",
    dot: "bg-danger",
    text: "text-danger",
    bg: "bg-danger/10",
  },
  "dead-lettered": {
    label: "Dead-lettered",
    dot: "bg-danger",
    text: "text-danger",
    bg: "bg-danger/10",
  },
  cancelled: {
    label: "Cancelled",
    dot: "bg-purple",
    text: "text-purple",
    bg: "bg-purple/10",
  },
};

export function statusStyle(s: TaskStatus): StatusStyle {
  return MAP[s] ?? MAP.created;
}

export const STATUS_ORDER: TaskStatus[] = [
  "running",
  "queued",
  "completed",
  "failed",
  "dead-lettered",
  "cancelled",
  "created",
];
