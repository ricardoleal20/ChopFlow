# Changelog

All notable changes to ChopFlow are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Durable task storage with two backends: SQLite (default) and in-memory (tests/demos).
- Worker registration with tag subscriptions and resource declaration (CPU, GPU, memory).
- Pull-based task dispatch: workers fetch matching work by tag + available resources,
  so backpressure is implicit.
- Task lifecycle: `Created → Queued → Running → Completed | Failed → DeadLettered | Cancelled`,
  with an explicit state machine in `core/src/task.rs`.
- Acknowledgments & retries: workers ack success or structured failure; failures retry
  with exponential backoff up to `max_retries`, then dead-letter.
- Resource-aware scheduling: tasks declare resource requirements; a worker only claims
  a task if its available resources satisfy the requirement.
- Scheduling: cron (5-field) and one-shot (RFC3339 ETA) schedules with overlap policies
  (`skip` / `coalesce` / `allow`). Missed cron runs are not backfilled.
- Broker reconciliation on restart: in-flight `Running` tasks are reset to `Queued`
  (their worker is gone); schedule `next_fire` times are recomputed from `now`.
- gRPC broker service (`broker/proto/chopflow.proto`) plus an HTTP/JSON API serving
  the same live `BrokerState`.
- Embedded operations dashboard (React + Vite + Tailwind) served from the broker
  binary via `rust-embed` — no separate frontend deploy.
- Native macOS dashboard app via Tauri 2 (`app/src-tauri`), a first-class client of
  the broker HTTP API.
- CLI (`chopflow_cli`) for enqueue, status, and schedule management.
- Demo workspace crate (`chopflow_demos`) with four showcase handlers
  (`resize_image`, `batch_compute`, `simulate_pipeline`, `flaky_handler`) and a
  one-command demo run (`demos/run.sh`).
- Timeout monitor: long-running tasks past their deadline are failed and retried.

### Notes
- Python and Java client libraries are on the roadmap. The gRPC contract they target
  is already defined in `broker/proto/chopflow.proto`.
- This is an **experimental** preview. APIs may change between minor versions.
