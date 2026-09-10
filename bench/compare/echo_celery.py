#!/usr/bin/env python3
"""
Celery head-to-head benchmark against ChopFlow.

Same workload as bench/bench.py: a no-op `echo` task that returns its payload
immediately, isolating queue/dispatch overhead rather than app logic. Same
machine, same worker shape (one prefork worker, --concurrency=4), same sweep.

Fairness notes (see bench/compare/README.md):
  - Redis broker + Redis result backend.
  - Drain uses the backend's native bulk join (mget over the group), NOT a
    per-task polling loop — mirrors ChopFlow's single /api/stats call so Celery
    isn't unfairly penalized by its own polling cadence.
  - Latency is sampled (uniform stride) and polled on a separate cadence, the
    same technique as ChopFlow's harness, so the comparison is apples-to-apples.

Prerequisites:
  - redis-server running on localhost:6379 (brew install redis && redis-server)
  - a Celery worker started with:
      celery -A echo_celery worker --concurrency=4 --loglevel=warning --pool=prefork
  - this script is the DRIVER only; it does not start the worker.

Usage:
  python3 bench/compare/echo_celery.py --tasks 10000 --concurrency 32 --sample-size 500

Emits the shared machine-readable line:
  RESULT system=celery tasks=N conc=C throughput=T submit_s=.. drain_s=.. p50_ms=.. p95_ms=.. p99_ms=.. failures=..
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from celery import Celery
    from celery.result import AsyncResult
except ImportError:
    sys.exit("celery is required: uv pip install -r bench/compare/requirements.txt")

# --- Celery app ----------------------------------------------------------
# Broker + result backend both on local Redis. The result backend is what makes
# the bulk join (and thus a fair drain) possible.
REDIS_URL = os.environ.get("CELERY_REDIS_URL", "redis://localhost:6379/0")
app = Celery("echo_bench", broker=REDIS_URL, backend=REDIS_URL)
app.conf.update(
    result_expires=300,          # don't let 1M results pile up in Redis forever
    task_acks_late=False,        # echo never fails; early ack = less broker churn
    worker_prefetch_multiplier=4,
    broker_connection_retry_on_startup=True,
)


@app.task(name="echo")
def echo(payload):
    """No-op echo: return the payload immediately."""
    return payload


@app.task(name="resize")
def resize(payload):
    """Real work: generate a gradient image and resize it (Pillow).

    Mirrors ChopFlow's demos `resize_image` handler so the workload is
    comparable across systems — isolates how each queue's dispatch overhead
    behaves when the task itself takes ~milliseconds of real CPU.
    """
    from PIL import Image
    w = int(payload.get("width", 256))
    h = int(payload.get("height", 256))
    w, h = max(1, w), max(1, h)
    img = Image.new("RGB", (w, h))
    px = img.load()
    for x in range(w):
        for y in range(h):
            px[x, y] = (int(x / w * 255), int(y / h * 255), 128)
    img.resize((max(1, w // 2), max(1, h // 2)), Image.NEAREST)
    return {"status": "ok", "dims": [w, h]}


# --- helpers (mirror bench/bench.py) -------------------------------------
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


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tasks", type=int, default=int(os.environ.get("CELERY_BENCH_TASKS", "1000")))
    p.add_argument("--concurrency", type=int, default=int(os.environ.get("CELERY_BENCH_CONC", "16")),
                   help="submit-side concurrency (producer threads), NOT worker concurrency")
    p.add_argument("--sample-size", type=int, default=int(os.environ.get("CELERY_BENCH_SAMPLE", "500")))
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--workload", choices=["echo", "resize"], default="echo")
    p.add_argument("--width", type=int, default=256)
    p.add_argument("--height", type=int, default=256)
    p.add_argument("--poll-interval", type=float, default=0.5)
    p.add_argument("--time-budget", type=float, default=float(os.environ.get("CELERY_BENCH_BUDGET", "0")),
                   help="overall timeout in seconds (0 = unlimited)")
    args = p.parse_args()

    n = args.tasks
    conc = args.concurrency
    sample_idx = pick_sample(list(range(n)), args.sample_size)
    print(f"celery driver: {n} tasks (submit conc {conc}, latency sample {len(sample_idx)})")

    # Probe the broker before doing anything else.
    try:
        app.connection_for_read().ensure_connection(max_retries=3, timeout=5)
    except Exception as e:
        sys.exit(f"cannot reach Redis at {REDIS_URL}: {e}\n(start redis-server, and a celery worker: "
                 f"celery -A echo_celery worker --concurrency=4 --loglevel=warning)")

    # Submit via a thread pool: build each task signature and apply_async.
    # Group join would enqueue all at once; we want a producer-side concurrency
    # cap comparable to ChopFlow's submit pool.
    sample_results: dict[int, AsyncResult] = {}
    all_results: list[AsyncResult] = [None] * n  # type: ignore[list-item]
    submitted_at: dict[int, float] = {}

    def submit_one(i: int) -> int:
        if args.workload == "resize":
            sig = resize.s({"width": args.width, "height": args.height, "i": i})
        else:
            sig = echo.s({"i": i})
        r = sig.apply_async()
        return i, r

    print(f"submitting {n} tasks…")
    wall_start = time.perf_counter()
    submit_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=conc) as pool:
        futs = {pool.submit(submit_one, i): i for i in range(n)}
        for f in as_completed(futs):
            i, r = f.result()
            all_results[i] = r
            if i in sample_idx:
                submitted_at[i] = time.perf_counter()
                sample_results[i] = r
    submit_elapsed = time.perf_counter() - submit_start
    print(f"  submitted {n} tasks in {submit_elapsed:.2f}s ({n/submit_elapsed:,.0f} submit/s)")

    # Drain via a direct Redis mget over the result-backend keys, NOT per-task
    # `.ready()` polling (one GET per task — unfairly slow) and NOT Celery's
    # `get_many` (a *blocking* wait, not a non-blocking readiness check). A
    # pipelined mget in batches is one round-trip per batch regardless of N,
    # mirroring ChopFlow's single /api/stats call so Celery's poll cadence
    # doesn't unfairly penalize it. Celery's Redis backend stores each result
    # under `celery-task-meta-<id>`; key existence == terminal.
    import threading
    import redis as redis_lib

    rconn = redis_lib.Redis.from_url(REDIS_URL, decode_responses=False)
    META_PREFIX = "celery-task-meta-"

    def is_terminal(task_id: str) -> bool:
        return rconn.exists(META_PREFIX + task_id) == 1

    def batch_terminal(ids: list[str]) -> set[str]:
        """Return the subset of task ids whose result key exists (terminal)."""
        if not ids:
            return set()
        pipe = rconn.pipeline()
        for tid in ids:
            pipe.exists(META_PREFIX + tid)
        results = pipe.execute()
        return {tid for tid, present in zip(ids, results) if present}

    print("draining via bulk Redis mget over result keys…")
    drain_start = time.perf_counter()
    completed_at: dict[int, float] = {}
    failures = 0
    pending_set = set(range(n))

    # Background sample poller for latency (same shape as ChopFlow harness):
    # polls only sampled, still-pending results on its own cadence.
    stop_poll = threading.Event()

    def sample_poller():
        next_poll = 0.0
        while not stop_poll.is_set():
            now = time.perf_counter()
            if now >= next_poll:
                for i in list(sample_results.keys()):
                    if i in completed_at:
                        continue
                    try:
                        if is_terminal(sample_results[i].id):
                            completed_at[i] = time.perf_counter()
                    except Exception:
                        continue
                next_poll = now + args.sample_interval
            stop_poll.wait(0.05)

    poller = threading.Thread(target=sample_poller, daemon=True)
    poller.start()

    # Bulk ready-count drain in batches.
    BATCH = 2000
    while True:
        now = time.perf_counter()
        pending_idx = sorted(pending_set)
        for start in range(0, len(pending_idx), BATCH):
            chunk_idx = pending_idx[start:start + BATCH]
            chunk_ids = [all_results[i].id for i in chunk_idx]
            done_ids = batch_terminal(chunk_ids)
            if not done_ids:
                continue
            for i in chunk_idx:
                if all_results[i].id in done_ids:
                    pending_set.discard(i)
            # time-slice so we report progress and honor the budget mid-sweep
            if time.perf_counter() - now > args.poll_interval:
                break
        elapsed = now - drain_start
        total_elapsed = (time.perf_counter() - wall_start)
        print(f"  ready={n - len(pending_set):>7}/{n}  pending={len(pending_set):>7}  "
              f"elapsed={elapsed:6.1f}s", end="\r")
        if not pending_set:
            break
        if args.time_budget > 0 and total_elapsed >= args.time_budget:
            print(f"\n  ⚠ time budget ({args.time_budget}s) exceeded with {len(pending_set)} pending")
            break
        time.sleep(args.poll_interval)
    drain_elapsed = time.perf_counter() - drain_start

    # Count failures across all terminal results (echo shouldn't fail, but the
    # page commits to an honest failure total, not a sampled estimate).
    failures = 0
    for i in range(n):
        if i in pending_set:
            continue
        try:
            if all_results[i].failed():
                failures += 1
        except Exception:
            pass
    time.sleep(args.sample_interval)
    stop_poll.set()
    poller.join(timeout=5)

    # Outcomes
    completed = n - len(pending_set)
    drained = completed

    latencies_ms = []
    for i, t0 in submitted_at.items():
        if i in completed_at:
            latencies_ms.append((completed_at[i] - t0) * 1000.0)

    wall_elapsed = submit_elapsed + drain_elapsed
    e2e = drained / wall_elapsed if wall_elapsed > 0 else float("inf")

    print()
    print("─" * 52)
    print(f"system:             celery")
    print(f"tasks:              {n}")
    print(f"concurrency:        {conc}")
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
    print(f"RESULT system=celery tasks={n} conc={conc} workload={args.workload} throughput={e2e:.0f} "
          f"submit_s={submit_elapsed:.2f} drain_s={drain_elapsed:.2f} "
          f"p50_ms={pct(latencies_ms,0.50):.2f} "
          f"p95_ms={pct(latencies_ms,0.95):.2f} "
          f"p99_ms={pct(latencies_ms,0.99):.2f} "
          f"failures={failures}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
