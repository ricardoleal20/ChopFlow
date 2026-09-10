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

End-to-end through the HTTP API + one worker (`cpu:4`), `echo` no-op workload.
Drain throughput = `N / drain_time`; latency is end-to-end wall time per task
(enqueue → terminal), **including queue wait**. All runs: 0 failed, 0
dead-lettered.

| Tasks      | Throughput (drain) | p50 latency | p95 latency | p99 latency |
|------------|--------------------|-------------|-------------|-------------|
| 1,000      | 4,573 tasks/s      | 561 ms      | 585 ms      | 587 ms      |
| 10,000     | 12,811 tasks/s     | 507 ms      | 1,015 ms    | 1,016 ms    |
| 100,000    | 15,153 tasks/s     | 3,408 ms    | 5,736 ms    | 6,217 ms    |
| 1,000,000  | 14,813 tasks/s     | 30,396 ms   | 55,118 ms   | 57,263 ms   |

Peak drain throughput is **15,153 tasks/s** at 100K, sustained at 14,813 tasks/s
through 1M tasks — 0 failures across 1.21M tasks total. Latency at 1M is
dominated by queue wait behind a single 4-slot worker, not execution time.

### Head-to-head comparison

Same machine, same `echo` workload, same 4-slot worker shape, sweep 1K→1M.
`DNF` = did not finish within the time budget — reported honestly, never
extrapolated or omitted. Drain throughput in tasks/s:

| System   | 1K    | 10K   | 100K   | 1M      |
|----------|-------|-------|--------|---------|
| ChopFlow | 4,573 | 12,811 | 15,153 | 14,813  |
| BullMQ   | 7,407 | 9,191 | 10,140 | 10,262  |
| Ray      | 3,307 | 6,863 | 8,239  | DNF     |
| Celery   | 1,166 | 1,644 | 1,749  | DNF     |
| Temporal | 42    | 43    | DNF    | DNF     |

ChopFlow wins at 100K and 1M — **44% faster than BullMQ at 1M** (14,813 vs
10,262 tasks/s). BullMQ leads the 1K/10K echo race on lower per-task dispatch
overhead (Redis Streams); ChopFlow pulls ahead at scale, where its in-memory
broker avoids per-task Redis round-trips.

The comparison harness and exact configs live in [`bench/compare/`](bench/compare/);
run it with `bash bench/compare/run_compare.sh`.

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
   CPU-bound handler (the `resize` workload in the comparison) shows very
   different numbers — see the workload axis on the benchmarks page.
4. **Category tax, not a flaw.** Temporal is a durable workflow engine that
   persists every step, and Ray is a distributed-compute framework whose
   throughput includes object-store + cross-actor scheduler cost. Their numbers
   are context, not a verdict on ChopFlow.

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
- Sustained throughput over 10M tasks.
