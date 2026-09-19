import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  api,
  setApiBase,
  type CreateScheduleBody,
  type EnqueueBody,
  type Environment,
  type TaskStatus,
} from "../lib/api";

// Live polling cadence: stats + workers update every 2s, the task ledger
// every 2s as well. Fast enough to feel live, gentle on the broker.
const REFETCH_MS = 2000;

export const qk = {
  stats: ["stats"] as const,
  tasks: (status?: TaskStatus) => ["tasks", status ?? "all"] as const,
  task: (id: string) => ["task", id] as const,
  workers: ["workers"] as const,
  schedules: ["schedules"] as const,
  environments: ["environments"] as const,
};

export function useStats() {
  return useQuery({
    queryKey: qk.stats,
    queryFn: api.stats,
    refetchInterval: REFETCH_MS,
  });
}

export function useTasks(status?: TaskStatus) {
  return useQuery({
    queryKey: qk.tasks(status),
    queryFn: () => api.listTasks(status),
    refetchInterval: REFETCH_MS,
  });
}

// Single-task detail: unlike the list endpoint, GET /tasks/:id carries the
// task's recorded checkpoints (and stages / idempotency key), so the task
// drawer polls this while open to stay live at the same 2s cadence.
export function useTask(id?: string) {
  return useQuery({
    queryKey: qk.task(id ?? ""),
    queryFn: () => api.getTask(id ?? ""),
    enabled: id != null && id !== "",
    refetchInterval: REFETCH_MS,
  });
}

export function useWorkers() {
  return useQuery({
    queryKey: qk.workers,
    queryFn: api.workers,
    refetchInterval: REFETCH_MS,
  });
}

// ---- Environments -----------------------------------------------------------
// The fleet catalog is read-only and rarely changes, so it polls gently (30s)
// rather than every 2s. After a switch, every active query is invalidated so
// stats / tasks / workers / schedules all refetch against the new broker.

export function useEnvironments() {
  return useQuery({
    queryKey: qk.environments,
    queryFn: api.environments,
    refetchInterval: 30_000,
    staleTime: 30_000,
    retry: false,
  });
}

/**
 * Retarget the dashboard at a different broker. Swaps the API base to the
 * environment's `http_url` (or back to same-origin `/api` for the current
 * entry) and invalidates every query so all views refetch live.
 */
export function useSwitchEnvironment() {
  const qc = useQueryClient();
  return (env: Environment) => {
    setApiBase(env.http_url);
    qc.invalidateQueries();
  };
}

export function useEnqueue() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: EnqueueBody) => api.enqueue(body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["tasks"] });
    },
  });
}

export function useCancel() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.cancel(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["tasks"] });
    },
  });
}

// ---- Schedules --------------------------------------------------------------
// Same invalidation contract as useEnqueue/useCancel: on success, refresh both
// the schedules list and the cluster stats (the schedules count in the sidebar).

export function useSchedules() {
  return useQuery({
    queryKey: qk.schedules,
    queryFn: api.listSchedules,
    refetchInterval: REFETCH_MS,
  });
}

export function useCreateSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: CreateScheduleBody) => api.createSchedule(body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["schedules"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export function usePatchSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, body }: { id: string; body: Record<string, unknown> }) =>
      api.patchSchedule(id, body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["schedules"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export function useDeleteSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.deleteSchedule(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["schedules"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}
