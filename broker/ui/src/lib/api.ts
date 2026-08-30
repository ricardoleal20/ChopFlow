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

const API = "/api";

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API}${path}`, {
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
};
