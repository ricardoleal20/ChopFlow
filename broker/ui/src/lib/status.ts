import type { TaskStatus } from "./api";

// Status ordering for filter chips and timelines. Color is never the only
// signal — every status renders through StatusBadge (dot + label).

export const STATUS_LABEL: Record<TaskStatus, string> = {
  created: "Created",
  queued: "Queued",
  running: "Running",
  completed: "Completed",
  failed: "Failed",
  "dead-lettered": "Dead-lettered",
  cancelled: "Cancelled",
};

// Chip/filter order (matches the OpenDesign reference).
export const STATUS_ORDER: TaskStatus[] = [
  "running",
  "queued",
  "completed",
  "failed",
  "dead-lettered",
  "cancelled",
  "created",
];

// The chip data-f attribute the CSS keys dot colors off of.
export function chipKey(s: TaskStatus | "all"): string {
  return s === "dead-lettered" ? "dead" : s;
}
