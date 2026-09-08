#!/usr/bin/env python3
"""
Temporal head-to-head benchmark against ChopFlow.

Same workload: a no-op echo activity that returns its payload immediately. One
workflow per task, one activity per workflow. One worker with 4 activity slots
(matches ChopFlow cpu:4 and Celery --concurrency=4).

Category caveat (stated on the benchmarks page): Temporal is a durable workflow
engine that persists every workflow step + activity to its database, by design.
That durability is a feature ChopFlow and Celery don't provide at this layer, so
Temporal's numbers are context, not a verdict. This is apples-to-pears.

Prerequisites:
  - Temporal dev server running on localhost:7233:
        temporal server start-dev --log-level error
    (the `temporal` CLI from temporalio/cli, NOT the raw server binary)
  - Python temporalio SDK installed (see requirements.txt)

Usage:
  python3 bench/compare/echo_temporal.py --tasks 10000 --concurrency 32 --sample-size 500

Emits the shared machine-readable line:
  RESULT system=temporal tasks=N conc=C throughput=T submit_s=.. drain_s=.. p50_ms=.. p95_ms=.. p99_ms=.. failures=..
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import timedelta

try:
    from temporalio import activity, workflow
    from temporalio.client import Client
    from temporalio.worker import Worker
except ImportError:
    sys.exit("temporalio is required: uv pip install -r bench/compare/requirements.txt")


@activity.defn
async def echo_activity(payload: dict) -> dict:
    """No-op echo activity. Returns the payload immediately."""
    return payload


@workflow.defn
class EchoWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        return await workflow.execute_activity(
            echo_activity, payload,
            start_to_close_timeout=timedelta(seconds=30),
        )


def pick_sample(indices: list[int], sample_size: int) -> set[int]:
    n = len(indices)
    if sample_size <= 0 or sample_size >= n:
        return set(indices)
    stride = n / sample_size
    return {indices[int(round(i * stride))] for i in range(sample_size)}


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
    return s[k]


async def main_async(args) -> int:
    n = args.tasks
    conc = args.concurrency
    sample_idx = pick_sample(list(range(n)), args.sample_size)
    print(f"temporal driver: {n} tasks (submit conc {conc}, latency sample {len(sample_idx)})")

    target = os.environ.get("TEMPORAL_TARGET", "localhost:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    task_queue = "echo-bench"

    try:
        client = await Client.connect(target, namespace=namespace)
    except Exception as e:
        sys.exit(f"cannot reach Temporal dev server at {target}: {e}\n"
                 f"(start it with: temporal server start-dev --log-level error)")

    # One worker, 4 activity slots — same execution shape as ChopFlow cpu:4.
    worker = Worker(
        client, task_queue=task_queue,
        workflows=[EchoWorkflow], activities=[echo_activity],
        max_concurrent_activities=4,
    )
    worker_task = asyncio.create_task(worker.run())
    print("worker started (4 activity slots)")

    wall_start = time.perf_counter()
    submitted_at: dict[int, float] = {}
    completed_at: dict[int, float] = {}
    failures = 0

    async def submit_and_track(i: int, sem: asyncio.Semaphore):
        async with sem:
            handle = await client.start_workflow(
                EchoWorkflow.run, {"i": i}, id=f"echo-{i}-{wall_start:.0f}",
                task_queue=task_queue,
            )
            if i in sample_idx:
                submitted_at[i] = time.perf_counter()
            try:
                await handle.result()
                if i in sample_idx and i not in completed_at:
                    completed_at[i] = time.perf_counter()
            except Exception:
                nonlocal_failures[0] += 1

    nonlocal_failures = [0]

    # Submit all, bounded by the producer semaphore; results resolve as they
    # complete (submit + drain overlap, same as ChopFlow/Celery).
    print(f"submitting {n} workflows (producer concurrency {conc})…")
    submit_start = time.perf_counter()
    sem = asyncio.Semaphore(conc)
    coros = [submit_and_track(i, sem) for i in range(n)]

    # Drain with a budget. as_completed lets us report progress and honor the
    # time budget without waiting forever on a straggler.
    done_count = 0
    last_report = time.perf_counter()
    try:
        for coro in asyncio.as_completed(coros, timeout=None):
            try:
                await coro
            except Exception:
                pass
            done_count += 1
            now = time.perf_counter()
            if now - last_report >= args.poll_interval:
                total_elapsed = now - wall_start
                print(f"  done={done_count:>7}/{n}  elapsed={total_elapsed:6.1f}s", end="\r")
                last_report = now
            if args.time_budget > 0 and (now - wall_start) >= args.time_budget:
                print(f"\n  ⚠ time budget ({args.time_budget}s) exceeded at {done_count}/{n}")
                break
    except asyncio.TimeoutError:
        pass

    wall_elapsed = time.perf_counter() - wall_start
    # split: submit phase ends when all start_workflow calls have been issued.
    # With the semaphore, issuance is interleaved with completion; we approximate
    # submit_s as the time until the last start_workflow returned. For Temporal
    # the honest headline is e2e throughput = N / wall_elapsed.
    submit_elapsed = wall_elapsed  # Temporal overlaps submit+drain; report e2e as primary
    drain_elapsed = 0.0
    failures = nonlocal_failures[0]

    # Sample poller already captured completion times inline (submitted_at→completed_at).
    latencies_ms = []
    for i, t0 in submitted_at.items():
        if i in completed_at:
            latencies_ms.append((completed_at[i] - t0) * 1000.0)

    completed = done_count
    drained = completed
    e2e = drained / wall_elapsed if wall_elapsed > 0 else float("inf")

    # Stop the worker cleanly.
    worker_task.cancel()
    try:
        await asyncio.wait_for(worker_task, timeout=10)
    except (asyncio.CancelledError, Exception):
        pass

    print()
    print("─" * 52)
    print(f"system:             temporal")
    print(f"tasks:              {n}")
    print(f"concurrency:        {conc}")
    print(f"wall time:          {wall_elapsed:.2f}s (submit+drain overlap)")
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
    print(f"RESULT system=temporal tasks={n} conc={conc} throughput={e2e:.0f} "
          f"submit_s={submit_elapsed:.2f} drain_s={drain_elapsed:.2f} "
          f"p50_ms={pct(latencies_ms,0.50):.2f} "
          f"p95_ms={pct(latencies_ms,0.95):.2f} "
          f"p99_ms={pct(latencies_ms,0.99):.2f} "
          f"failures={failures}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tasks", type=int, default=int(os.environ.get("TEMPORAL_BENCH_TASKS", "1000")))
    p.add_argument("--concurrency", type=int, default=int(os.environ.get("TEMPORAL_BENCH_CONC", "16")))
    p.add_argument("--sample-size", type=int, default=int(os.environ.get("TEMPORAL_BENCH_SAMPLE", "500")))
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--poll-interval", type=float, default=0.5)
    p.add_argument("--time-budget", type=float, default=float(os.environ.get("TEMPORAL_BENCH_BUDGET", "0")),
                   help="overall timeout in seconds (0 = unlimited)")
    args = p.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
