#!/usr/bin/env python3
"""
Reproducible ChopFlow throughput / latency benchmark.

Drives the broker's HTTP/JSON API to enqueue N tasks, then polls /api/stats
until all reach a terminal state (completed + failed == total_tasks). Measures
end-to-end throughput and per-task latency (p50/p95/p99) from a bounded sample
of task ids — the sample path scales to 1M tasks without per-task polling.

Requirements:
  - a running ChopFlow broker (gRPC + HTTP)
  - at least one worker connected, with a handler for the `echo`/`default` name

Usage:
  uv run --with httpx bench/bench.py --broker http://localhost:8080 --tasks 1000 --concurrency 32
  # large scale: sample latency instead of measuring every task
  python3 bench/bench.py --broker http://localhost:8080 --tasks 1000000 --sample-size 1000

Environment variables (optional):
  CHOPFLOW_BENCH_BROKER   default broker base URL
  CHOPFLOW_BENCH_TASKS    default task count
  CHOPFLOW_BENCH_CONC     default concurrency
  CHOPFLOW_BENCH_SAMPLE   default sample size (0 = all)
"""
from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import httpx
except ImportError:
    sys.exit("httpx is required: uv run --with httpx bench/bench.py ...")


def enqueue_one(client: httpx.Client, base: str, name: str, payload: dict) -> str:
    last_err = None
    for _attempt in range(5):
        try:
            r = client.post(
                f"{base}/api/tasks",
                json={"name": name, "payload": payload, "tags": ["bench"]},
                timeout=30,
            )
            r.raise_for_status()
            return r.json()["task_id"]
        except (httpx.ConnectError, httpx.RemoteProtocolError, httpx.ReadError) as e:
            last_err = e
            time.sleep(0.2)
    raise last_err  # type: ignore[misc]


def enqueue_batch(client: httpx.Client, base: str, name: str, payloads: list[dict]) -> list[str]:
    """Enqueue a batch of tasks in a single POST /api/tasks/batch request.

    Amortizes the HTTP/JSON/handler overhead over `len(payloads)` tasks — the
    HTTP analog of a Redis pipeline (BullMQ). Without this, per-task POST
    overhead dominates submit time at 100k+ tasks even though storage insert is
    O(log N)."""
    body = {"tasks": [{"name": name, "payload": p, "tags": ["bench"]} for p in payloads]}
    last_err = None
    for _attempt in range(5):
        try:
            r = client.post(f"{base}/api/tasks/batch", json=body, timeout=60)
            r.raise_for_status()
            return r.json()["task_ids"]
        except (httpx.ConnectError, httpx.RemoteProtocolError, httpx.ReadError) as e:
            last_err = e
            time.sleep(0.2)
    raise last_err  # type: ignore[misc]


def fetch_task(client: httpx.Client, base: str, task_id: str) -> dict:
    r = client.get(f"{base}/api/tasks/{task_id}", timeout=30)
    r.raise_for_status()
    return r.json()


def terminal(task: dict) -> bool:
    s = task.get("status", "").lower()
    return s in {"completed", "failed", "deadlettered", "cancelled"}


def pick_sample(ids: list[str], sample_size: int) -> set[str]:
    """Choose a bounded, representative sample of task indices.

    Uniform stride across the submission order (every n/S-th task), so the
    sample spans the whole burst — front, middle, and back of the queue —
    giving a latency distribution that reflects the real queue-wait spread.
    First+last would only sample the two extremes, both of which have low wait.
    ``sample_size <= 0`` or ``>= len(ids)`` returns every index (exact
    measurement, used for small runs).
    """
    n = len(ids)
    if sample_size <= 0 or sample_size >= n:
        return set(ids)
    stride = n / sample_size
    return {ids[int(round(i * stride))] for i in range(sample_size)}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--broker", default=os.environ.get("CHOPFLOW_BENCH_BROKER", "http://localhost:8080"))
    p.add_argument("--tasks", type=int, default=int(os.environ.get("CHOPFLOW_BENCH_TASKS", "1000")))
    p.add_argument("--concurrency", type=int, default=int(os.environ.get("CHOPFLOW_BENCH_CONC", "16")))
    p.add_argument("--batch-size", type=int, default=int(os.environ.get("CHOPFLOW_BENCH_BATCH", "200")),
                   help="tasks per POST /api/tasks/batch request (1 = one POST per task, legacy)")
    p.add_argument("--name", default="echo")
    p.add_argument("--workload", choices=["echo", "resize"], default="echo",
                   help="echo = no-op (dispatch overhead); resize = real image-resize work")
    p.add_argument("--width", type=int, default=256)
    p.add_argument("--height", type=int, default=256)
    p.add_argument("--poll-interval", type=float, default=0.1)
    p.add_argument("--sample-interval", type=float, default=0.5,
                   help="cadence (s) at which sampled tasks are polled for completion (decoupled from stats)")
    p.add_argument("--sample-size", type=int, default=int(os.environ.get("CHOPFLOW_BENCH_SAMPLE", "200")),
                   help="latency sample size (0 = measure every task; only viable for small N)")
    p.add_argument("--time-budget", type=float, default=float(os.environ.get("CHOPFLOW_BENCH_BUDGET", "0")),
                   help="drain timeout in seconds (0 = unlimited)")
    args = p.parse_args()

    base = args.broker.rstrip("/")
    n = args.tasks
    conc = args.concurrency
    # Keep every in-flight connection alive for the whole run. httpx's default
    # pool (max_keepalive_connections=20) is smaller than a typical submit
    # concurrency, so it churns connections → TIME_WAIT → ephemeral port
    # exhaustion at 100k+ tasks. Size the keepalive pool to the concurrency.
    limits = httpx.Limits(max_connections=conc * 2 + 8,
                          max_keepalive_connections=conc + 4,
                          keepalive_expiry=120)

    with httpx.Client(timeout=30, limits=limits) as probe:
        try:
            stats = probe.get(f"{base}/api/stats", timeout=5).json()
        except Exception as e:
            sys.exit(f"cannot reach broker at {base}: {e}")
        workers = stats.get("active_workers", 0)
        print(f"broker: {base}")
        print(f"active workers: {workers}")
        if workers < 1:
            sys.exit("no active workers — start a worker before benchmarking")

    sample_ids = pick_sample(list(range(n)), args.sample_size)
    print(f"submitting {n} tasks (concurrency {conc}, latency sample {len(sample_ids)})…")

    # Shared state between the submit pool and the background sample-poller.
    # `submitted_at` is filled as sampled task ids are returned by the broker;
    # the poller reads it concurrently to capture true completion wall-times,
    # including tasks that finish *during* the submit phase (otherwise their
    # latency would be falsely recorded as "submit duration").
    submitted_at: dict[str, float] = {}
    completed_at: dict[str, float] = {}
    stop_poll = threading.Event()

    def sample_poller(client: httpx.Client) -> None:
        next_poll = 0.0
        while not stop_poll.is_set():
            now = time.perf_counter()
            if now >= next_poll:
                for tid in list(submitted_at.keys()):
                    if tid in completed_at:
                        continue
                    try:
                        t = fetch_task(client, base, tid)
                    except Exception:
                        continue
                    if terminal(t):
                        completed_at[tid] = time.perf_counter()
                next_poll = now + args.sample_interval
            stop_poll.wait(0.05)

    ids: list[str] = []
    wall_start = time.perf_counter()
    poller_client = httpx.Client(timeout=30, limits=limits)
    poller = threading.Thread(target=sample_poller, args=(poller_client,), daemon=True)
    poller.start()
    submit_start = time.perf_counter()
    task_name = "resize_image" if args.workload == "resize" else args.name
    def payload_for(i: int) -> dict:
        if args.workload == "resize":
            return {"width": args.width, "height": args.height, "i": i}
        return {"i": i}

    batch_size = max(1, args.batch_size)
    # Chunk the task indices into batches. Each batch is one POST /api/tasks/batch
    # request, submitted with `conc`-way concurrency — the HTTP analog of a
    # Redis pipeline. `batch_size=1` reproduces the legacy one-POST-per-task path.
    batches: list[list[int]] = [list(range(start, min(start + batch_size, n)))
                                for start in range(0, n, batch_size)]

    def submit_batch(idx_batch: list[int]) -> tuple[list[str], list[int], float]:
        payloads = [payload_for(i) for i in idx_batch]
        tids = enqueue_batch(client, base, task_name, payloads)
        return tids, idx_batch, time.perf_counter()

    with ThreadPoolExecutor(max_workers=conc) as pool, httpx.Client(timeout=30, limits=limits) as client:
        futs = {pool.submit(submit_batch, b): b for b in batches}
        for f in as_completed(futs):
            tids, idx_batch, ts = f.result()
            for i, tid in zip(idx_batch, tids):
                ids.append(tid)
                if i in sample_ids:
                    submitted_at[tid] = ts
    submit_elapsed = time.perf_counter() - submit_start
    print(f"  submitted {len(ids)} tasks in {submit_elapsed:.2f}s "
          f"({len(ids)/submit_elapsed:,.0f} submit/s)")

    # --- Drain via /api/stats: one call per poll, regardless of N.
    # The sample poller keeps running through the drain phase on its own
    # cadence, so per-task GETs don't contend the storage lock with the
    # worker's fetch path and don't gate the drain completion check.
    print("waiting for all tasks to reach a terminal state (via /api/stats)…")
    drain_start = time.perf_counter()
    with httpx.Client(timeout=30, limits=limits) as client:
        # First snapshot fixes the universe of tasks we expect to drain.
        stats = client.get(f"{base}/api/stats", timeout=30).json()
        total_tasks = stats.get("total_tasks", len(ids))
        while True:
            now = time.perf_counter()
            done_now = client.get(f"{base}/api/stats", timeout=30).json()
            completed = done_now.get("tasks_completed", 0)
            failed = done_now.get("tasks_failed", 0)
            terminal_total = completed + failed
            elapsed = now - drain_start
            pending = max(0, total_tasks - terminal_total)
            print(f"  terminal={terminal_total:>7}/{total_tasks}  pending={pending:>7}  elapsed={elapsed:6.1f}s", end="\r")
            if terminal_total >= total_tasks and total_tasks > 0:
                break
            if args.time_budget > 0 and (now - wall_start) >= args.time_budget:
                print(f"\n  ⚠ time budget ({args.time_budget}s) exceeded with {pending} pending")
                break
            time.sleep(args.poll_interval)
    drain_elapsed = time.perf_counter() - drain_start
    # Give the poller a last chance to catch stragglers, then stop it.
    time.sleep(args.sample_interval)
    stop_poll.set()
    poller.join(timeout=5)
    poller_client.close()

    # --- Latency from the bounded sample (captured across submit + drain).
    latencies_ms: list[float] = []
    for tid, t0 in submitted_at.items():
        if tid in completed_at:
            latencies_ms.append((completed_at[tid] - t0) * 1000.0)

    # Outcomes from the final stats snapshot (no per-task fetch).
    with httpx.Client(timeout=30, limits=limits) as client:
        final = client.get(f"{base}/api/stats", timeout=30).json()
    completed = final.get("tasks_completed", 0)
    failed = final.get("tasks_failed", 0)
    statuses = {"completed": completed, "failed": failed}
    failures = failed

    drained = completed + failed
    # End-to-end throughput: total tasks / wall-clock from first submit to last
    # completion. The worker processes tasks *during* submit, so "drain-only"
    # throughput is artificially high at scale (nearly everything is done by the
    # time submit ends). E2E is the honest headline; it's also directly
    # comparable to Celery/Temporal which overlap submit + process the same way.
    wall_elapsed = submit_elapsed + drain_elapsed
    e2e_throughput = drained / wall_elapsed if wall_elapsed > 0 else float("inf")
    drain_throughput = drained / drain_elapsed if drain_elapsed > 0 else float("inf")

    def pct(values: list[float], q: float) -> float:
        if not values:
            return 0.0
        s = sorted(values)
        k = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
        return s[k]

    print()
    print("─" * 52)
    print(f"system:             chopflow")
    print(f"tasks:              {n}")
    print(f"concurrency:        {conc}")
    print(f"submit time:        {submit_elapsed:.2f}s")
    print(f"drain time:         {drain_elapsed:.2f}s")
    print(f"e2e throughput:     {e2e_throughput:,.0f} tasks/s (submit→last completion)")
    print(f"drain throughput:   {drain_throughput:,.0f} tasks/s (post-submit only)")
    if latencies_ms:
        print(f"latency p50:        {pct(latencies_ms, 0.50):.1f} ms")
        print(f"latency p95:        {pct(latencies_ms, 0.95):.1f} ms")
        print(f"latency p99:        {pct(latencies_ms, 0.99):.1f} ms")
        print(f"(end-to-end, incl. queue wait; from {len(latencies_ms)} sampled tasks)")
    else:
        print("latency p50/p95/p99: (no timing samples captured)")
    print(f"outcomes:           {statuses}")
    print(f"failures:           {failures}")
    print("─" * 52)

    # Machine-readable line for easy scraping (shared format across systems).
    # `throughput` is the e2e headline; `drain_s` is the post-submit wait only.
    print(f"RESULT system=chopflow tasks={n} conc={conc} workload={args.workload} throughput={e2e_throughput:.0f} "
          f"submit_s={submit_elapsed:.2f} drain_s={drain_elapsed:.2f} "
          f"p50_ms={pct(latencies_ms,0.50):.2f} "
          f"p95_ms={pct(latencies_ms,0.95):.2f} "
          f"p99_ms={pct(latencies_ms,0.99):.2f} "
          f"failures={failures}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
