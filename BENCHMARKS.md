# Benchmarks

Reproducible throughput and latency benchmarks for ChopFlow.

These are **end-to-end** measurements through the broker's HTTP/JSON API and a
single real worker — not microbenchmarks of an isolated storage layer. The
goal is an honest, reproducible number you can check on your own machine, not
a marketing claim.

## How to reproduce

```bash
bash bench/run.sh
# or, against an already-running broker + worker:
uv run --with httpx python3 bench/bench.py \
    --broker http://localhost:8080 --tasks 10000 --concurrency 32
```

`bench/run.sh` builds the release binaries, starts a fresh in-memory broker
and one worker (`cpu:4`), runs the harness, and tears down. The harness
(`bench/bench.py`) submits N `echo` tasks via the HTTP API at a fixed
concurrency, then polls until every task reaches a terminal state.

## Reference machine

| | |
|---|---|
| CPU    | Apple M3 Pro (11 cores) |
| Memory | 18 GB |
| OS     | macOS 26.6 (arm64) |
| Build  | `cargo build --release` (Rust 1.97) |
| Storage backend | in-memory |
| Workers | 1 (`--resources cpu:4`) |

## Results

| Tasks  | Concurrency | Throughput (drain) | p50 latency | p95 latency | p99 latency |
|--------|-------------|--------------------|-------------|-------------|-------------|
| 1,000  | 16          | 540 tasks/s        | 2,067 ms    | 2,323 ms    | 2,344 ms    |
| 10,000 | 32          | 3,462 tasks/s      | 7,481 ms    | 10,560 ms   | 10,786 ms   |

All tasks completed; 0 failed, 0 dead-lettered.

### What these numbers mean

- **Throughput** is measured as `N / drain_time`, where `drain_time` is the
  wall-clock time from "all tasks submitted" to "all tasks terminal". This is
  the broker + worker's real processing rate and is the least
  harness-dependent number here.
- **Latency** is end-to-end wall time per task, from enqueue to terminal
  state, observed by the harness. It **includes queue wait time** — at 10k
  tasks with a single worker, most of the latency is time spent queued behind
  other tasks, not execution time (the `echo` handler is near-instant).

### Caveats / honest limitations

1. **The harness polls over HTTP.** Per-task terminal detection happens via
   `GET /api/tasks/:id` polling, so the latency resolution is bounded by the
   poll interval (200 ms at 1k, 500 ms at 10k) and the latency figures are
   upper bounds, not precise per-task durations. A future broker change to
   expose `completed_time` / `duration_ms` on the task object would let the
   harness report true execution latency.
2. **Single worker, in-memory storage.** These numbers reflect a single
   worker with no persistence I/O. SQLite-backed durability and multi-worker
   fan-out will have different characteristics — both are worth benchmarking
   (see roadmap).
3. **`echo` handler.** The handler does no real work, so throughput is
   dominated by broker dispatch + ack round-trips, not task execution. A
   CPU-bound handler would show very different numbers.
4. **No comparison yet.** A `ChopFlow vs BullMQ vs Celery` comparison on the
   same machine is the highest-value next benchmark and is tracked as a
   follow-up.

## Scaling the benchmark

```bash
# more tasks
TASKS="1000 10000 100000" bash bench/run.sh

# more concurrency
CONC=64 bash bench/run.sh

# more workers: start them manually, then point the harness at the broker
./target/release/chopflow_worker start --broker http://localhost:8000 --tags bench --resources cpu:8 &
./target/release/chopflow_worker start --broker http://localhost:8000 --tags bench --resources cpu:8 &
uv run --with httpx python3 bench/bench.py --broker http://localhost:8080 --tasks 100000 --concurrency 64
```

## What we'd like to measure next

- SQLite-backed throughput vs in-memory (cost of durability).
- Multi-worker scaling: throughput vs worker count (1, 4, 16, 64).
- Failure/retry overhead: throughput under a flaky handler.
- A head-to-head against BullMQ and Celery on this machine.
- Sustained throughput over 1M tasks.
