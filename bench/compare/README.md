# ChopFlow comparison benchmarks

A fair, reproducible head-to-head of **ChopFlow** against **Celery**,
**Temporal**, **BullMQ**, and **Ray** on the same machine, same workload, same
sweep. The numbers feed the Comparison section of
`docs/design/chopflow-benchmarks.html`.

The benchmarks page's ethos is *"honest numbers you can reproduce, not marketing
claims."* This harness exists so that anyone can run it on their own machine and
check the comparison themselves.

## What's compared

| System | Category | Broker / runtime | Worker shape |
|---|---|---|---|
| **ChopFlow** | distributed task queue (Rust) | in-memory broker (HTTP/gRPC) | 1 worker, 4 slots (`cpu:4`) |
| **Celery** | distributed task queue (Python) | Redis (broker + result backend) | 1 prefork worker, `--concurrency=4` |
| **BullMQ** | distributed task queue (Node.js) | Redis Streams | 1 worker process, `concurrency: 4` |
| **Temporal** | durable workflow engine (Go) | dev server + SQLite | 1 worker, 4 activity slots |
| **Ray** | distributed compute runtime (Python) | local cluster (head + workers, shared object store) | `num_cpus=4` ⇒ 4 concurrent |

**Celery** and **BullMQ** are the direct apples-to-apples peers: fire-and-forget
task queues, just like ChopFlow. Celery is Python+Redis; BullMQ is Node.js+Redis
Streams; ChopFlow is Rust+in-memory. All three dispatch and forget, with a
separate worker process consuming from a shared broker.

**Temporal** is **apples-to-pears**: it's a durable workflow engine that
*persists every workflow step and activity invocation* to its database, by
design. That durability is a feature neither ChopFlow nor Celery nor BullMQ
provide at this layer. Its numbers here are **context, not a verdict** — we
include it because it's the system people most often weigh against a task queue,
and showing it with an explicit caveat is more honest than omitting it.

**Ray** is a second pears category: a *distributed compute framework* (clusters
with a shared object store and cross-actor scheduler), closer to Dask/Spark than
to a task queue. Its throughput includes the cost of its object store + scheduler
— more machinery than a fire-and-forget queue carries. We include it because
people evaluating "run N units of work" sometimes weigh it against a queue, and
showing it with a caveat is more honest than omitting it.

## Workloads

Two workloads, on the same sweep:

- **`echo`** (default) — a no-op task that returns its payload immediately
  (`{"i": n}`). Isolates queue/dispatch overhead, not application logic, so the
  comparison is about the infrastructure, not the workload.
- **`resize`** — real CPU work: generate a 256×256 gradient image and resize it
  to half-size (nearest-neighbor). Mirrors ChopFlow's `demos` `resize_image`
  handler, implemented per-system with the native image library (Rust `image`
  crate for ChopFlow, Pillow for Celery, sharp for BullMQ, numpy for Ray). Once
  each task does real work, the ranking flips toward runtime speed rather than
  dispatch overhead — see the Workload axis card on the benchmarks page.

Temporal is `echo`-only: its per-step durability model isn't the right tool for a
tight resize loop.

## Sweep

`1K · 10K · 100K · 1M` tasks. The 1M run is opt-in and time-budgeted (default
20 min via `BUDGET=1200`). If a system doesn't finish within the budget, the
page says so honestly ("did not complete within budget") rather than omitting or
faking a number.

- **Temporal's 1M** is not attempted — at ~40–60 tasks/s it would take hours; the
  durability-per-step design makes this expected, not a regression.
- **Ray's 1M** is not attempted — its local-cluster startup + per-task
  object-store overhead make a million-task sweep impractical on one laptop; this
  is a category property of a distributed-compute runtime, not a queue-engine
  limit.

## Metrics (shared format)

Every driver prints one machine-readable line:

```
RESULT system=<chopflow|celery|bullmq|temporal|ray> tasks=N conc=C workload=<echo|resize> throughput=T submit_s=.. drain_s=.. p50_ms=.. p95_ms=.. p99_ms=.. failures=..
```

- **throughput** — end-to-end: `tasks / (submit + drain)` wall-clock. The worker
  processes tasks *during* submit, so drain-only throughput is artificially high
  at scale. E2E is the honest, directly-comparable headline.
- **p50 / p95 / p99** — end-to-end latency (queue wait + execution) from a
  uniformly-strided bounded sample (default 500). Sampling — not per-task
  measurement — is what makes 1M viable.
- **failures** — expected to be 0 for echo.

## Prerequisites

```sh
# Redis (Celery + BullMQ broker)
brew install redis

# Temporal CLI — provides `temporal server start-dev`
brew install temporal

# Python drivers (Celery + Temporal + Ray)
uv venv bench/compare/.venv
uv pip install -r bench/compare/requirements.txt   # includes ray + numpy

# BullMQ driver (Node.js)
(cd bench/compare/echo_bullmq && npm install)
```

ChopFlow's own numbers come from `bench/run.sh` (needs only `cargo` + `uv`).

## Reproduce

```sh
# 1. ChopFlow sweep (1k/10k/100k; add 1000000 for the 1M run)
TASKS="1000 10000 100000" bash bench/run.sh

# 2. Comparison sweep (starts Redis + Temporal dev server itself)
bash bench/compare/run_compare.sh
TASKS="1000 10000 100000 1000000" BUDGET=1200 bash bench/compare/run_compare.sh  # +1M

# 3. Real-work axis (resize workload; ranking flips toward runtime speed)
WORKLOAD=resize bash bench/compare/run_compare.sh
```

Each `RESULT` line can be scraped and dropped straight into the benchmarks page's
data tables.

## Fairness notes

- **Same machine, same sweep, same worker shape** (1 process, 4 slots) for all
  five systems. Config is scripted in `run_compare.sh` so there's no manual
  knob to forget.
- **Each system drains through its own lightest completion signal** — the same
  shape of "how do I know a task is done?" that a real user would use:
  - **ChopFlow** — single `GET /api/stats` poll (`completed + failed == total`).
  - **Celery** — Redis backend's native bulk ready-check (`get_many` / mget in
    batches), **not** a per-task polling loop.
  - **BullMQ** — the worker `INCR`s a Redis counter on each completion; the
    driver polls `GET` on that counter. Network-mediated, same shape as Celery's
    result keys — **not** an in-process atomic, so BullMQ isn't given a free
    drain the others don't get.
  - **Temporal** — `workflow.result()` / await on the workflow handles.
  - **Ray** — `ray.wait()` returns ready futures; Ray signals completion through
    its own runtime.
- **Latency** is sampled identically (uniform stride, same cadence) across all
  drivers, so the percentile distributions are comparable. Each driver timestamps
  submit and completion on the **same clock** (wall-clock epoch ms) so
  cross-process latency is correct.
- **Temporal** runs against its local dev server with SQLite. Its per-step
  durability persistence is an intentional category tax, stated on the page.
- **Ray** runs a local cluster (`ray.init(num_cpus=4)`); `num_cpus=1` per task
  schedules ~4 concurrent, matching the 4-slot worker shape. Its object-store +
  scheduler overhead is part of the measurement — that's the category, not a
  tuning gap.
- No system is given a warm cache the others aren't; each run starts from a clean
  broker/runtime state.

## Why these five, and not more

Each system earns its slot by sitting in a distinct category. We stopped at five
rather than building an ever-larger matrix, because beyond this the marginal
systems would add noise without signal:

- **RQ, Dramatiq** — same Python/Redis bucket as Celery. Celery is the canonical
  representative; the others would cluster with it.
- **Sidekiq, Asynq, Hatchet** — would require a Ruby / Go / extra runtime and a
  separate broker topology, broadening the comparison without sharpening it.
- **AWS SQS / RabbitMQ** — managed broker services; their numbers reflect network
  round-trips and cloud quotas, not queue-engine overhead, and aren't
  reproducible on one laptop.

If you want to add a system, the contract is just the `RESULT` line above. Drop a
new `echo_<system>.{py,rs,go,mjs}` next to the others and wire it into
`run_compare.sh`.

## Versions

Pin the exact versions you run with and record them on the benchmarks page.
This harness was developed against:

- Celery 5.4+, Redis 5.0+ (client), redis-server 7.x
- Temporal CLI (`temporalio/cli`) — `temporal server start-dev`
- `temporalio` Python SDK 1.7+
- BullMQ 5.81, ioredis 5.x, sharp 0.33, Node.js 22
- Ray 2.58, numpy 2.5
- ChopFlow `main` (release build)
