#!/usr/bin/env python3
"""
Ray head-to-head benchmark against ChopFlow.

CATEGORY CAVEAT — Ray is not a task queue. It's a distributed compute framework
(clusters of head + workers with a shared object store), closer to Dask/Spark
than to Celery/BullMQ/ChopFlow. We include it because people evaluating a task
queue sometimes weigh it against Ray for "run N units of work," and showing it
with an explicit caveat is more honest than omitting it. Ray's throughput here
includes the cost of its object store + cross-actor scheduler, which is *more*
machinery than a fire-and-forget queue carries — so its numbers are context,
not a verdict, same shape as the Temporal caveat.

Fairness notes (see bench/compare/README.md):
  - Local cluster (ray.init()), one head + workers. The echo/resize task runs
    as a @ray.remote function with num_cpus=1 so Ray schedules ~4 concurrent
    on a default local cluster (matches the 4-slot worker shape).
  - Drain is Ray's native completion: ray.wait() returns ready futures. Ray
    signals completion through its own runtime — this is Ray's lightest
    completion signal, the same way ChopFlow uses /api/stats and Celery uses
    Redis result keys.
  - Latency is sampled (uniform stride): for sampled tasks we wrap the remote
    call so it records submit→completion via a timekeeper actor.

Prerequisites:
  - ray installed: uv pip install ray  (into bench/compare/.venv)
  - numpy (for the resize workload)

Usage:
  python3 bench/compare/echo_ray.py --tasks 10000 --concurrency 32 --sample-size 500

Emits the shared machine-readable line:
  RESULT system=ray tasks=N conc=C workload=echo|resize throughput=T submit_s=.. drain_s=.. p50_ms=.. p95_ms=.. p99_ms=.. failures=..
"""
from __future__ import annotations

import argparse
import sys
import time

try:
    import ray
except ImportError:
    sys.exit("ray is required: uv pip install ray  (into bench/compare/.venv)")


@ray.remote
class Timekeeper:
    """Records completion timestamps (epoch ms) for sampled task indices."""

    def __init__(self):
        self.completed = {}

    def record(self, i: int):
        self.completed[i] = time.time() * 1000.0

    def get_completed(self):
        return dict(self.completed)


@ray.remote(num_cpus=1)
def echo_task(i: int, tk, work: str, w: int, h: int, sample: bool):
    if work == "resize":
        _do_resize(w, h)
    if sample:
        ray.get(tk.record.remote(i))
    return i


def _do_resize(w: int, h: int):
    import numpy as np
    w, h = max(1, w), max(1, h)
    xs = (np.arange(w) / w * 255).astype(np.uint8)
    ys = (np.arange(h) / h * 255).astype(np.uint8)
    img = np.empty((h, w, 3), dtype=np.uint8)
    img[:, :, 0] = xs
    img[:, :, 1] = ys[:, None]
    img[:, :, 2] = 128
    img[::2, ::2]  # nearest-neighbor half-size via slicing


def pick_sample(n: int, size: int) -> set[int]:
    if size <= 0 or size >= n:
        return set(range(n))
    stride = n / size
    return {int(round(i * stride)) for i in range(size)}


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
    return s[k]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tasks", type=int, default=1000)
    p.add_argument("--concurrency", type=int, default=16, help="submit-side concurrency (producer)")
    p.add_argument("--sample-size", type=int, default=500)
    p.add_argument("--workload", choices=["echo", "resize"], default="echo")
    p.add_argument("--width", type=int, default=256)
    p.add_argument("--height", type=int, default=256)
    p.add_argument("--time-budget", type=float, default=0.0)
    args = p.parse_args()

    n = args.tasks
    sample_idx = pick_sample(n, args.sample_size)
    print(f"ray driver: {n} tasks (submit conc {args.concurrency}, workload {args.workload}, "
          f"latency sample {len(sample_idx)})")

    ray.init(ignore_reinit_error=True, log_to_driver=False, include_dashboard=False,
             num_cpus=4)  # 4 CPUs → num_cpus=1 per task ⇒ 4 concurrent (worker shape)
    tk = Timekeeper.remote()

    # Submit all tasks. Ray's scheduler backpressures via num_cpus=1, so all N
    # are enqueued and ~4 execute at once — same 4-slot shape as the others.
    print(f"submitting {n} tasks…")
    wall_start = time.perf_counter()
    submit_start = time.perf_counter()
    submitted_at: dict[int, float] = {}
    futures = []
    for i in range(n):
        is_sample = i in sample_idx
        if is_sample:
            submitted_at[i] = time.time() * 1000.0
        futures.append(echo_task.remote(i, tk, args.workload, args.width, args.height, is_sample))
    submit_elapsed = time.perf_counter() - submit_start
    print(f"  submitted {n} tasks in {submit_elapsed:.2f}s ({n/submit_elapsed:,.0f} submit/s)")

    # Drain: ray.wait returns ready futures. Poll in batches with progress.
    print("draining via ray.wait…")
    sys.stdout.flush()
    drain_start = time.perf_counter()
    done = 0
    failures = 0
    pending = list(futures)
    while pending:
        # Wait for at least one to be ready, fetch up to 256 at a time.
        ready, pending = ray.wait(pending, num_returns=min(len(pending), 256), timeout=5.0)
        if not ready:
            # timed out with nothing ready — check budget, keep waiting
            if args.time_budget > 0 and (time.perf_counter() - wall_start) >= args.time_budget:
                print(f"\n  ⚠ time budget ({args.time_budget}s) exceeded with {len(pending)} pending")
                break
            continue
        for ref in ready:
            try:
                ray.get(ref)
            except Exception:
                failures += 1
            done += 1
        elapsed = time.perf_counter() - drain_start
        print(f"  completed={done:>7}/{n}  pending={len(pending):>7}  elapsed={elapsed:6.1f}s")
        sys.stdout.flush()
        if args.time_budget > 0 and (time.perf_counter() - wall_start) >= args.time_budget:
            print(f"\n  ⚠ time budget ({args.time_budget}s) exceeded with {len(pending)} pending")
            break
    drain_elapsed = time.perf_counter() - drain_start
    print()

    completed_map = ray.get(tk.get_completed.remote())
    latencies_ms = [t1 - submitted_at[i] for i, t1 in completed_map.items() if i in submitted_at]

    wall_elapsed = submit_elapsed + drain_elapsed
    e2e = done / wall_elapsed if wall_elapsed > 0 else float("inf")

    print("─" * 52)
    print(f"system:             ray")
    print(f"tasks:              {n}")
    print(f"concurrency:        {args.concurrency}")
    print(f"submit time:        {submit_elapsed:.2f}s")
    print(f"drain time:         {drain_elapsed:.2f}s")
    print(f"e2e throughput:     {e2e:,.0f} tasks/s")
    if latencies_ms:
        print(f"latency p50:        {pct(latencies_ms, 0.50):.1f} ms")
        print(f"latency p95:        {pct(latencies_ms, 0.95):.1f} ms")
        print(f"latency p99:        {pct(latencies_ms, 0.99):.1f} ms")
        print(f"(end-to-end; from {len(latencies_ms)} sampled tasks)")
    else:
        print("latency p50/p95/p99: (no timing samples captured)")
    print(f"failures:           {failures}")
    print("─" * 52)
    print(f"RESULT system=ray tasks={n} conc={args.concurrency} workload={args.workload} throughput={e2e:.0f} "
          f"submit_s={submit_elapsed:.2f} drain_s={drain_elapsed:.2f} "
          f"p50_ms={pct(latencies_ms,0.50):.2f} "
          f"p95_ms={pct(latencies_ms,0.95):.2f} "
          f"p99_ms={pct(latencies_ms,0.99):.2f} "
          f"failures={failures}")

    ray.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
