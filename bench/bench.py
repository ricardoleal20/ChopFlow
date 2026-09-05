#!/usr/bin/env python3
"""
Reproducible ChopFlow throughput / latency benchmark.

Drives the broker's HTTP/JSON API to enqueue N tasks, then polls /api/stats
until all reach a terminal state (completed + failed + dead-lettered).
Measures end-to-end throughput and per-task latency (p50/p95/p99) from the
task ledger's timing fields.

Requirements:
  - a running ChopFlow broker (gRPC + HTTP)
  - at least one worker connected, with a handler for the `echo`/`default` name

Usage:
  uv run --with httpx bench/bench.py --broker http://localhost:8080 --tasks 1000 --concurrency 32
  # or
  python3 bench/bench.py --broker http://localhost:8080 --tasks 1000

Environment variables (optional):
  CHOPFLOW_BENCH_BROKER   default broker base URL
  CHOPFLOW_BENCH_TASKS    default task count
  CHOPFLOW_BENCH_CONC     default concurrency
"""
from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import httpx
except ImportError:
    sys.exit("httpx is required: uv run --with httpx bench/bench.py ...")


def enqueue_one(client: httpx.Client, base: str, name: str, payload: dict) -> str:
    r = client.post(
        f"{base}/api/tasks",
        json={"name": name, "payload": payload, "tags": ["bench"]},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["task_id"]


def fetch_task(client: httpx.Client, base: str, task_id: str) -> dict:
    r = client.get(f"{base}/api/tasks/{task_id}", timeout=30)
    r.raise_for_status()
    return r.json()


def terminal(task: dict) -> bool:
    s = task.get("status", "").lower()
    return s in {"completed", "failed", "deadlettered", "cancelled"}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--broker", default=os.environ.get("CHOPFLOW_BENCH_BROKER", "http://localhost:8080"))
    p.add_argument("--tasks", type=int, default=int(os.environ.get("CHOPFLOW_BENCH_TASKS", "1000")))
    p.add_argument("--concurrency", type=int, default=int(os.environ.get("CHOPFLOW_BENCH_CONC", "16")))
    p.add_argument("--name", default="echo")
    p.add_argument("--poll-interval", type=float, default=0.2)
    args = p.parse_args()

    base = args.broker.rstrip("/")
    n = args.tasks
    conc = args.concurrency

    with httpx.Client(timeout=30) as probe:
        try:
            stats = probe.get(f"{base}/api/stats", timeout=5).json()
        except Exception as e:
            sys.exit(f"cannot reach broker at {base}: {e}")
        workers = stats.get("active_workers", 0)
        print(f"broker: {base}")
        print(f"active workers: {workers}")
        if workers < 1:
            sys.exit("no active workers — start a worker before benchmarking")

    print(f"submitting {n} tasks (concurrency {conc})…")
    submitted_at: dict[str, float] = {}
    ids: list[str] = []
    submit_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=conc) as pool, httpx.Client(timeout=30) as client:
        futs = [pool.submit(enqueue_one, client, base, args.name, {"i": i}) for i in range(n)]
        for f in as_completed(futs):
            tid = f.result()
            ids.append(tid)
            submitted_at[tid] = time.perf_counter()
    submit_elapsed = time.perf_counter() - submit_start
    print(f"  submitted {len(ids)} tasks in {submit_elapsed:.2f}s "
          f"({len(ids)/submit_elapsed:,.0f} submit/s)")

    print("waiting for all tasks to reach a terminal state…")
    drain_start = time.perf_counter()
    pending = set(ids)
    completed_at: dict[str, float] = {}
    with httpx.Client(timeout=30) as client:
        while pending:
            time.sleep(args.poll_interval)
            done_now = set()
            for tid in list(pending):
                t = fetch_task(client, base, tid)
                if terminal(t):
                    done_now.add(tid)
            for tid in done_now:
                completed_at[tid] = time.perf_counter()
            pending -= done_now
            elapsed = time.perf_counter() - drain_start
            print(f"  pending={len(pending):>6}  elapsed={elapsed:5.1f}s", end="\r")
    print()
    drain_elapsed = time.perf_counter() - drain_start

    # Per-task end-to-end latency = terminal_time - submit_time (wall clock).
    # This is honest: it includes queue wait + execution + ack round-trip. The
    # benchmark's own polling cadence caps the resolution at poll_interval.
    latencies_ms: list[float] = []
    statuses: dict[str, int] = {}
    with httpx.Client(timeout=30) as client:
        for tid in ids:
            t = fetch_task(client, base, tid)
            s = t.get("status", "unknown").lower()
            statuses[s] = statuses.get(s, 0) + 1
            if tid in completed_at and tid in submitted_at:
                latencies_ms.append((completed_at[tid] - submitted_at[tid]) * 1000.0)

    total = drain_elapsed + submit_elapsed
    throughput = n / drain_elapsed if drain_elapsed > 0 else float("inf")

    def pct(values: list[float], q: float) -> float:
        if not values:
            return 0.0
        s = sorted(values)
        k = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
        return s[k]

    print()
    print("─" * 52)
    print(f"tasks:               {n}")
    print(f"concurrency:         {conc}")
    print(f"submit time:         {submit_elapsed:.2f}s")
    print(f"drain time:          {drain_elapsed:.2f}s")
    print(f"throughput:          {throughput:,.0f} tasks/s (drain)")
    if latencies_ms:
        print(f"latency p50:         {pct(latencies_ms, 0.50):.1f} ms")
        print(f"latency p95:         {pct(latencies_ms, 0.95):.1f} ms")
        print(f"latency p99:         {pct(latencies_ms, 0.99):.1f} ms")
        print(f"(end-to-end, incl. queue wait; resolution ≈ {args.poll_interval*1000:.0f} ms poll)")
    else:
        print("latency p50/p95/p99: (no timing samples captured)")
    print(f"outcomes:            {statuses}")
    print("─" * 52)

    # Machine-readable line for easy scraping.
    print(f"RESULT tasks={n} conc={conc} throughput={throughput:.0f} "
          f"drain_s={drain_elapsed:.2f} "
          f"p50_ms={pct(latencies_ms,0.50):.2f} "
          f"p95_ms={pct(latencies_ms,0.95):.2f} "
          f"p99_ms={pct(latencies_ms,0.99):.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
