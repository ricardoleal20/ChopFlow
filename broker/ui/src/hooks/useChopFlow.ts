import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type EnqueueBody, type TaskStatus } from "../lib/api";

// Live polling cadence: stats + workers update every 2s, the task ledger
// every 2s as well. Fast enough to feel live, gentle on the broker.
const REFETCH_MS = 2000;

export const qk = {
  stats: ["stats"] as const,
  tasks: (status?: TaskStatus) => ["tasks", status ?? "all"] as const,
  workers: ["workers"] as const,
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

export function useWorkers() {
  return useQuery({
    queryKey: qk.workers,
    queryFn: api.workers,
    refetchInterval: REFETCH_MS,
  });
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
