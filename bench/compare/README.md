# ChopFlow comparison benchmarks

A fair, reproducible head-to-head of **ChopFlow** against **Celery** and
**Temporal** on the same machine, same workload, same sweep. The numbers feed
the Comparison section of `docs/design/chopflow-benchmarks.html`.

The benchmarks page's ethos is *"honest numbers you can reproduce, not marketing
claims."* This harness exists so that anyone can run it on their own machine and
check the comparison themselves.

## What's compared

| System | Category | Broker / runtime | Worker shape |
|---|---|---|---|
| **ChopFlow** | distributed task queue (Rust) | in-memory broker (HTTP/gRPC) | 1 worker, 4 slots (`cpu:4`) |
| **Celery** | distributed task queue (Python) | Redis (broker + result backend) | 1 prefork worker, `--concurrency=4` |
| **Temporal** | durable workflow engine (Go) | dev server + SQLite | 1 worker, 4 activity slots |

**Celery** is the direct apples-to-apples peer: a task queue that dispatches and
forgets, just like ChopFlow.

**Temporal** is **apples-to-pears**: it's a durable workflow engine that
*persists every workflow step and activity invocation* to its database, by
design. That durability is a feature neither ChopFlow nor Celery provide at this
layer. Its numbers here are **context, not a verdict** — we include it because
it's the system people most often weigh against a task queue, and showing it
with an explicit caveat is more honest than omitting it.

## Workload

A no-op `echo` task that returns its payload immediately (`{"i": n}`). This
isolates queue/dispatch overhead, not application logic — so the comparison is
about the infrastructure, not the workload.

## Sweep

`1K · 10K · 100K · 1M` tasks. The 1M run is opt-in and time-budgeted (default
20 min via `BUDGET=1200`). If a system doesn't finish within the budget, the
page says so honestly ("did not complete within budget") rather than omitting or
faking a number.

## Metrics (shared format)

Every driver prints one machine-readable line:

```
RESULT system=<chopflow|celery|temporal> tasks=N conc=C throughput=T submit_s=.. drain_s=.. p50_ms=.. p95_ms=.. p99_ms=.. failures=..
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
# Redis (Celery's broker + result backend)
brew install redis

# Temporal CLI — provides `temporal server start-dev`
brew install temporal

# Python drivers
uv venv bench/compare/.venv
uv pip install -r bench/compare/requirements.txt
```

ChopFlow's own numbers come from `bench/run.sh` (needs only `cargo` + `uv`).

## Reproduce

```sh
# 1. ChopFlow sweep (1k/10k/100k; add 1000000 for the 1M run)
TASKS="1000 10000 100000" bash bench/run.sh

# 2. Comparison sweep (starts Redis + Temporal dev server itself)
bash bench/compare/run_compare.sh
TASKS="1000 10000 100000 1000000" BUDGET=1200 bash bench/compare/run_compare.sh  # +1M
```

Each `RESULT` line can be scraped and dropped straight into the benchmarks page's
data tables.

## Fairness notes

- **Same machine, same sweep, same worker shape** (1 process, 4 slots) for all
  three systems. Config is scripted in `run_compare.sh` so there's no manual
  knob to forget.
- **Celery drain** uses the Redis backend's native bulk ready-check (`get_many`,
  mget in batches), **not** a per-task polling loop — mirroring ChopFlow's single
  `/api/stats` call so Celery isn't unfairly penalized by its own poll cadence.
- **Temporal** runs against its local dev server with SQLite. Its per-step
  durability persistence is an intentional category tax, stated on the page.
- **Latency** is sampled identically (uniform stride, same cadence) across all
  three drivers, so the percentile distributions are comparable.
- No system is given a warm cache the others aren't; each run starts from a clean
  broker/runtime state.

## Why not X

We deliberately did **not** build a six-way matrix. Adding more systems would
add noise without adding signal:

- **RQ, Dramatiq** — same Python/Redis bucket as Celery. Celery is the canonical
  representative; the others would cluster with it.
- **Sidekiq, Asynq, Hatchet** — would require a Ruby / Go / extra runtime and a
  separate broker topology, broadening the comparison without sharpening it.
- **AWS SQS / RabbitMQ** — managed broker services; their numbers reflect network
  round-trips and cloud quotas, not queue-engine overhead, and aren't
  reproducible on one laptop.

If you want to add a system, the contract is just the `RESULT` line above. Drop a
new `echo_<system>.py` next to the others and wire it into `run_compare.sh`.

## Versions

Pin the exact versions you run with and record them on the benchmarks page.
This harness was developed against:

- Celery 5.4+, Redis 5.0+ (client), redis-server 7.x
- Temporal CLI (`temporalio/cli`) — `temporal server start-dev`
- `temporalio` Python SDK 1.7+
- ChopFlow `main` (release build)
