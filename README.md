# ChopFlow

ChopFlow is a distributed task queue written in Rust with a Python interface, designed for both production applications and research in distributed systems.

## Features

- **Task Queue Management**: Enqueue and dequeue tasks with metadata (tags, ETA)
- **Worker Dispatch**: Assign tasks to worker instances based on tags and resource availability
- **Acknowledgments**: Workers confirm success or failure, with retry policies
- **Resource Tracking**: CPU, GPU, and memory resource allocation and tracking
- **Python Interface**: Familiar Celery-like interface with `@task` decorator and `AsyncResult`
- **Metrics**: Queue length, latency, throughput, and resource utilization metrics
- **Research Focus**: Designed for reproducible experiments and performance comparison

## Components

- **Core**: Rust library with queue, dispatcher, and worker implementations
- **Broker**: Rust executable for the broker service
- **Worker**: Rust executable for task execution
- **CLI**: Command-line interface for interacting with ChopFlow
- **Python Interface**: Python package for easy integration with Python applications
- **Experiment**: Configuration and analysis tools for research experiments

## Getting Started

### Prerequisites

- Rust (2021 edition)
- Python 3.8+
- Cargo

### Building from Source

```bash
# Clone the repository
git clone https://github.com/ricardoleal20/ChopFlow.git
cd ChopFlow

# Build Rust components
cargo build --release

# Install Python interface
cd python_interface
pip install -e .
```

### Basic Usage

Start the broker:

```bash
./target/release/chopflow_broker start --host 127.0.0.1 --port 8000
```

Start a worker:

```bash
./target/release/chopflow_worker start --broker http://localhost:8000 --tags gpu,ml --resources gpu:1,cpu:4
```

Use the CLI to enqueue a task:

```bash
./target/release/chopflow_cli enqueue --task task.json --name training --tags gpu,ml
```

### Operations Dashboard UI

The broker ships with an embedded web dashboard — no separate frontend deploy
needed. It serves HTTP/JSON (for the UI and any external tooling) alongside
gRPC, both reading the same live broker state. The same dashboard is also
available as a native **macOS app** via Tauri.

```bash
# gRPC on :8000 (workers/CLI), dashboard + HTTP API on :8080
./target/release/chopflow_broker start --host 127.0.0.1 --port 8000 --http-port 8080
```

Open `http://localhost:8080` in a browser to:

- Watch live cluster stats (queue length, processing, completed, failed, workers)
- Browse and filter the task ledger, with status badges and result previews
- See connected workers with live resource meters
- Enqueue tasks and cancel queued/running ones from the UI

The HTTP API is also available directly:

| Method | Endpoint                  | Purpose                  |
|--------|---------------------------|--------------------------|
| GET    | `/api/stats`              | Cluster counts           |
| GET    | `/api/tasks?status=`      | List/filter tasks        |
| GET    | `/api/tasks/:id`          | Single task              |
| POST   | `/api/tasks`              | Enqueue a task           |
| POST   | `/api/tasks/:id/cancel`   | Cancel a non-terminal task |
| GET    | `/api/workers`            | Registered workers       |

**The UI stack:** the dashboard is a React + Vite + Tailwind app in
`broker/ui/` (TypeScript, TanStack Query, framer-motion), built to the visual
contract in `docs/design/DESIGN.md`. It polls the HTTP API above every 2s and
renders live stats, the task ledger, workers, a task detail drawer, and an
enqueue dialog. The built bundle in `broker/ui/dist/` is what the broker
embeds (via `rust-embed`).

**Iterative web dev** against a live broker (the Vite dev server proxies
`/api` to `http://localhost:8080`):

```bash
pnpm --dir broker/ui install     # first time only
pnpm --dir broker/ui dev         # http://localhost:5173, hot reload
```

**Rebuild the embedded web bundle** (committed so the broker compiles from a
fresh clone without a Node toolchain):

```bash
pnpm --dir broker/ui build
touch broker/src/http.rs         # force rust-embed to re-read ui/dist
cargo build
```

Or run the broker with `--open` to launch the embedded dashboard in a browser.

**Native macOS app (Tauri):** the same React frontend is wrapped as a desktop
app via Tauri 2, living in `app/src-tauri/`. It points at the broker's HTTP
API at `http://127.0.0.1:8080` (set via `VITE_API_BASE` in
`broker/ui/.env.tauri`), so it is a first-class client of the same broker a
browser would use — nothing is forked or re-implemented.

```bash
pnpm --dir broker/ui install          # first time only
# Start the broker first (gRPC :8000, HTTP :8080):
./target/release/chopflow_broker start --port 8000 --http-port 8080

cd app && ../broker/ui/node_modules/.bin/tauri dev    # launch the desktop app
# or build a signed .app bundle:
cd app && ../broker/ui/node_modules/.bin/tauri build
```

## Scheduled tasks

ChopFlow ships a first-class `Schedule` entity that the broker materializes into
`Task`s on a background ticker (one tick per second). A schedule is either:

- **Cron** — a standard 5-field cron expression (e.g. `*/2 * * * *`, `0 9 * * *`).
  The broker normalizes it to the 6-field form the `cron` crate expects. The
  server evaluates cron and ETAs in **UTC**; the dashboard converts from the
  local picker before sending.
- **One-shot** — fires once at an RFC3339 ETA, then self-disables.

Each schedule carries an **overlap policy** that governs what happens when a
fire is due but a previous run for the same schedule is still in flight
(Queued or Running):

| Policy    | Behavior                                                          |
|-----------|-------------------------------------------------------------------|
| `skip`    | Skip this fire and advance `next_fire` (cron). Default.           |
| `coalesce`| Skip this fire (treated like `skip` for now; reserved for merging).|
| `allow`   | Materialize the task anyway — concurrent runs permitted.          |

Missed cron runs are **not** backfilled: on broker startup, enabled cron
schedules have their `next_fire` recomputed from `now`, so the ticker only
fires future matches. A disabled one-shot with a past ETA stays disabled until
re-enabled.

### Managing schedules

**CLI** (`schedule` subcommand):

```bash
# A recurring cron schedule
./target/release/chopflow_cli schedule create \
    --name nightly-build --task build --cron "0 9 * * *" \
    --tags ci --resources cpu:4 --overlap skip

# A one-shot 5 minutes out
./target/release/chopflow_cli schedule create \
    --name one-off-report --task report --eta 2026-09-03T14:30:00Z --overlap allow

./target/release/chopflow_cli schedule list
./target/release/chopflow_cli schedule delete <schedule-id>
```

**HTTP API**:

| Method | Endpoint                  | Purpose                                  |
|--------|---------------------------|------------------------------------------|
| GET    | `/api/schedules`          | List all schedules                       |
| POST   | `/api/schedules`          | Create a schedule (cron or oneshot)      |
| GET    | `/api/schedules/:id`      | Get a single schedule                    |
| PATCH  | `/api/schedules/:id`      | Toggle `enabled`, change overlap or cron |
| DELETE | `/api/schedules/:id`      | Delete a schedule                        |

`GET /api/stats` also reports `schedules` (count of enabled schedules). The
dashboard's Schedules view lists every schedule, lets you enable/disable, run
now (which enqueues a one-off task from the template without disturbing the
schedule's overlap accounting), and delete.

## Demo handlers

The `demos` workspace crate ships a drop-in demo worker plus a seeding tool so
`cargo run` produces a live dashboard end-to-end. The demo worker registers
four showcase handlers (plus `echo` and a `default` fallback):

| Handler           | What it does                                                        |
|-------------------|---------------------------------------------------------------------|
| `resize_image`    | Generates a synthetic gradient PNG and resizes it (image-rs).        |
| `batch_compute`   | CPU-bound `n x n` f64 matrix multiply (nalgebra), reports timings.  |
| `simulate_pipeline`| Multi-stage pipeline (download/process/upload) with staged sleeps. |
| `flaky_handler`   | Fails ~30% of the time (seeded) to exercise the retry policy.        |

### One-command demo run

```bash
bash demos/run.sh
```

This builds the workspace, starts an in-memory broker with `--open` (launches
the dashboard at `http://localhost:8080`), starts a demo worker wired to the
four handlers above (`--tags demo,ml --resources cpu:4`), then seeds:

- One task of each handler type (the `flaky_handler` one with `max_retries: 5`
  so you can watch it retry).
- A `*/2 * * * *` cron schedule running `batch_compute` with overlap `skip`.
- A one-shot `simulate_pipeline` schedule 5 minutes out with overlap `allow`.

You'll see four tasks flow through distinct lifecycles in the dashboard, the
`flaky_handler` visibly retrying, and two schedules ticking in the Schedules
view. Ctrl+C stops the broker and worker.

You can also seed a running broker manually:

```bash
cargo run -p chopflow_demos --bin chopflow_demo_seed -- [broker_base_url]
# broker_base_url defaults to http://localhost:8080
```

### Python Interface

```python
from chopflow import task, Client

@task(tags=["ml"], resources={"gpu": 1})
def train_model(dataset, hyperparams):
    # Your training code here
    return {"accuracy": 0.95}

# Enqueue for asynchronous execution
result = train_model.delay("imagenet", {"lr": 0.001})

# Get result when ready
output = result.get(timeout=3600)
```

## Research Experiments

ChopFlow is designed to facilitate research in distributed task queues. The `experiment` directory contains tools for running reproducible experiments and analyzing results.

To run an experiment:

```bash
# Configure experiment
nano experiment/configs/exp01_ml_training.yml

# Run experiment (implementation details TBD)
python experiment/run_experiment.py --config experiment/configs/exp01_ml_training.yml

# Analyze results
python experiment/analysis_scripts/analyze_results.py --db experiment/results/exp01.db
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- Inspired by systems like Celery, Ray, and Dask