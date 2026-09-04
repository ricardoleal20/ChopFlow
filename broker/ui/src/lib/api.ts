// Types + fetch client for the ChopFlow HTTP API. Mirrors the DTOs in
// broker/src/http.rs. Status is a lowercase string the backend owns.

export type TaskStatus =
  | "created"
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "dead-lettered"
  | "cancelled";

export interface Task {
  id: string;
  name: string;
  payload: unknown;
  tags: string[];
  status: TaskStatus;
  retry_count: number;
  max_retries: number;
  enqueue_time: number; // epoch ms
  eta: number | null;
  resources: Record<string, number>;
  result: string | null;
  schedule_id: string | null;
}

export interface TaskListResponse {
  tasks: Task[];
  total: number;
}

export interface Stats {
  queue_length: number;
  tasks_processing: number;
  tasks_completed: number;
  tasks_failed: number;
  active_workers: number;
  total_tasks: number;
  schedules: number;
}

export interface Worker {
  id: string;
  address: string;
  tags: string[];
  alive: boolean;
  assigned_tasks: number;
  resources_total: Record<string, number>;
  resources_available: Record<string, number>;
  last_heartbeat: number; // epoch ms
}

export interface EnqueueBody {
  name: string;
  payload: unknown;
  tags?: string[];
  max_retries?: number;
  resources?: Record<string, number>;
}

// ---- Schedules --------------------------------------------------------------
// Mirrors the DTOs in broker/src/http.rs. The server owns the lowercase enum
// tag names (cron / oneshot) and the lowercase overlap strings.

export type OverlapPolicy = "skip" | "coalesce" | "allow";

export type ScheduleKind =
  | { type: "cron"; cron: string }
  | { type: "oneshot"; eta: string }; // RFC3339 (UTC)

export interface TaskTemplate {
  name: string;
  payload: unknown;
  tags: string[];
  resources: Record<string, number>;
  max_retries: number;
}

export interface Schedule {
  id: string;
  name: string;
  task_template: TaskTemplate;
  kind: ScheduleKind;
  overlap_policy: OverlapPolicy;
  enabled: boolean;
  last_fired: number | null; // epoch ms
  next_fire: number; // epoch ms
  created_at: number; // epoch ms
}

export interface CreateScheduleBody {
  name: string;
  task_template: TaskTemplate;
  kind: ScheduleKind;
  overlap_policy: OverlapPolicy;
}

// API base: relative "/api" when served by the broker (web) or via the Vite
// dev proxy; an absolute URL (e.g. http://127.0.0.1:8080/api) when bundled as
// a Tauri desktop app talking to a locally-running broker. Override via
// VITE_API_BASE at build time (see broker/ui/.env.tauri).
const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) || "/api";

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = body.error ?? detail;
    } catch {
      /* non-json error body */
    }
    throw new Error(detail);
  }
  return res.json() as Promise<T>;
}

export const api = {
  stats: () => json<Stats>("/stats"),
  listTasks: (status?: TaskStatus) =>
    json<TaskListResponse>("/tasks" + (status ? `?status=${status}` : "")),
  getTask: (id: string) => json<Task>(`/tasks/${id}`),
  enqueue: (body: EnqueueBody) =>
    json<{ task_id: string }>("/tasks", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  cancel: (id: string) =>
    json<{ success: boolean }>(`/tasks/${id}/cancel`, { method: "POST" }),
  workers: () => json<Worker[]>("/workers"),
  listSchedules: () => json<Schedule[]>("/schedules"),
  createSchedule: (body: CreateScheduleBody) =>
    json<{ schedule_id: string }>("/schedules", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  patchSchedule: (id: string, body: Record<string, unknown>) =>
    json<Schedule>(`/schedules/${id}`, {
      method: "PATCH",
      body: JSON.stringify(body),
    }),
  deleteSchedule: (id: string) =>
    json<{ success: boolean }>(`/schedules/${id}`, { method: "DELETE" }),
};
