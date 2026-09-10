# ChopFlow comparison benchmarks

A fair, reproducible head-to-head of **ChopFlow** against **Celery**,
**Temporal**, **apalis**, and **River** on the same machine, same workload, same
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
| **Temporal** | durable workflow engine (Go) | dev server + SQLite | 1 worker, 4 activity slots |
| **apalis** | distributed task queue (Rust) | Redis | 1 worker, 4 slots (`ConcurrencyLimit(4)`) |
| **River** | durable lightweight queue (Go) | PostgreSQL | 1 client, `MaxWorkers: 4` |

**Celery** and **apalis** are the direct apples-to-apples peers: fire-and-forget
task queues, just like ChopFlow. Celery is Python+Redis; apalis is Rust+Redis, so
it isolates language/runtime against the *same* broker as Celery.

**Temporal** is **apples-to-pears**: it's a durable workflow engine that
*persists every workflow step and activity invocation* to its database, by
design. That durability is a feature neither ChopFlow nor Celery provide at this
layer. Its numbers here are **context, not a verdict** — we include it because
it's the system people most often weigh against a task queue, and showing it
with an explicit caveat is more honest than omitting it.

**River** sits between Celery and Temporal: a durable, transactional
Postgres-backed queue that persists per-task state without Temporal's
workflow/replay machinery. It's the "cost of lightweight durability" datapoint.
Its driver is built and compiles but **pending a local PostgreSQL instance**
(none of the other systems require one); its row on the page stays empty until
Postgres is installed rather than being guessed or marked did-not-finish.

## Workloads

Two workloads, on the same sweep:

- **`echo`** (default) — a no-op task that returns its payload immediately
  (`{"i": n}`). Isolates queue/dispatch overhead, not application logic, so the
  comparison is about the infrastructure, not the workload.
- **`resize`** — real CPU work: generate a 256×256 gradient image and resize it
  to half-size (nearest-neighbor). Mirrors ChopFlow's `demos` `resize_image`
  handler, implemented per-system with the native image library (Rust `image`
  crate for ChopFlow + apalis, Pillow for Celery). Once each task does real work,
  the ranking flips toward runtime speed rather than dispatch overhead — see the
  Workload axis card on the benchmarks page.

Temporal is `echo`-only: its per-step durability model isn't the right tool for a
tight resize loop.

## Sweep

`1K · 10K · 100K · 1M` tasks. The 1M run is opt-in and time-budgeted (default
20 min via `BUDGET=1200`). If a system doesn't finish within the budget, the
page says so honestly ("did not complete within budget") rather than omitting or
faking a number.

## Metrics (shared format)

Every driver prints one machine-readable line:

```
RESULT system=<chopflow|celery|temporal|apalis|river> tasks=N conc=C workload=<echo|resize> throughput=T submit_s=.. drain_s=.. p50_ms=.. p95_ms=.. p99_ms=.. failures=..
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
# Redis (Celery + apalis broker)
brew install redis

# Temporal CLI — provides `temporal server start-dev`
brew install temporal

# PostgreSQL (River only — none of the other systems need it)
brew install postgresql@16 && pg_ctl start
createdb riverbench
export DATABASE_URL=postgres://localhost:5432/riverbench?sslmode=disable

# Python drivers (Celery + Temporal)
uv venv bench/compare/.venv
uv pip install -r bench/compare/requirements.txt

# apalis + River drivers build themselves on first run (cargo / go)
```

ChopFlow's own numbers come from `bench/run.sh` (needs only `cargo` + `uv`).
River is skipped automatically when PostgreSQL / `DATABASE_URL` is absent — the
runner prints a clear note rather than failing.

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
- **Celery drain** uses the Redis backend's native bulk ready-check (`get_many`,
  mget in batches), **not** a per-task polling loop — mirroring ChopFlow's single
  `/api/stats` call so Celery isn't unfairly penalized by its own poll cadence.
- **apalis** defaults (`buffer_size=10`, `poll_interval=100ms`) cap throughput at
  ~100 jobs/s — an artificial throttle. We tune the storage `Config` to a 1ms
  poll and 1000-job fetch batch so Redis feeds the 4 worker slots without
  starving them. This is the apalis equivalent of Celery's
  `worker_prefetch_multiplier` tuning; without it apalis is unfairly slow.
  apalis's default dispatch (`CallAllUnordered`) is unbounded, so we cap it to 4
  slots with a tower `ConcurrencyLimitLayer` to match the other systems.
- **River** runs its worker in-process with an in-process atomic completion
  counter, the same drain shape as the Temporal and apalis drivers.
- **Temporal** runs against its local dev server with SQLite. Its per-step
  durability persistence is an intentional category tax, stated on the page.
- **Latency** is sampled identically (uniform stride, same cadence) across all
  drivers, so the percentile distributions are comparable.
- No system is given a warm cache the others aren't; each run starts from a clean
  broker/runtime state.

## Why these five, and not more

Each system earns its slot by sitting in a distinct category (in-memory vs.
Redis vs. durable). We stopped at five rather than building an ever-larger
matrix, because beyond this the marginal systems would add noise without signal:

- **RQ, Dramatiq** — same Python/Redis bucket as Celery. Celery is the canonical
  representative; the others would cluster with it.
- **Sidekiq, Asynq, Hatchet** — would require a Ruby / Go / extra runtime and a
  separate broker topology, broadening the comparison without sharpening it.
- **AWS SQS / RabbitMQ** — managed broker services; their numbers reflect network
  round-trips and cloud quotas, not queue-engine overhead, and aren't
  reproducible on one laptop.

If you want to add a system, the contract is just the `RESULT` line above. Drop a
new `echo_<system>.{py,rs,go}` next to the others and wire it into
`run_compare.sh`.

## Versions

Pin the exact versions you run with and record them on the benchmarks page.
This harness was developed against:

- Celery 5.4+, Redis 5.0+ (client), redis-server 7.x
- Temporal CLI (`temporalio/cli`) — `temporal server start-dev`
- `temporalio` Python SDK 1.7+
- apalis 0.7 (`apalis-redis` 0.7.4), Rust `image` 0.25, tower 0.5
- River 0.47 (`riverqueue/river`), `pgx/v5` 5.11, Go 1.23+
- ChopFlow `main` (release build)
