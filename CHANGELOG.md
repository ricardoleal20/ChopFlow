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

## [0.1.3] - 2026-09-12

### Added
- **Unified `chopflow` command.** The umbrella crate now ships a single binary
  with subcommands instead of three separate binaries:
  `chopflow broker start`, `chopflow mcp`, `chopflow enqueue`, `chopflow status`,
  `chopflow schedule …`. One `cargo install chopflow` (or `brew install chopflow`)
  gives you the broker, CLI, and MCP server behind one command. The standalone
  `chopflow-broker` / `chopflow-cli` / `chopflow-mcp` binaries still build from
  their own crates, but the umbrella install is the unified entry point.

### Fixed
- **`cargo install chopflow` no longer needs system `protoc`.** `chopflow-proto`'s
  build script now vendors `protoc` via `protoc-bin-vendored` and points
  `tonic-build` at it, so the build succeeds on machines without `protobuf`
  installed. (CI preinstalled `protoc`, so this failed only for end users — the
  v0.1.2 install failure.)

### Changed
- `broker`, `cli`, and `mcp` library `run()` entry points now accept the
  already-parsed `Cli` struct (was: parsed `std::env::args` internally), so the
  unified binary can route subcommands into them. The standalone binaries pass
  `Cli::parse()` through. Public enum variant fields are now accessible.
- Bumped workspace and all inter-crate dependency versions to `0.1.3`.
- Homebrew formula test block exercises `chopflow --help` / `chopflow broker
  --help` / `chopflow mcp --help` (was: the three separate binaries).

## [0.1.2] - 2026-09-12

### Added
- **`chopflow` umbrella crate** — one `cargo install chopflow` (or `brew install
  chopflow`) now yields all three binaries: `chopflow-broker`, `chopflow-cli`,
  `chopflow-mcp`. Each is a thin wrapper over the matching library crate's new
  `pub async fn run()` entry point. Workers and the LLM worker stay separate.
- **Homebrew tap** (`ricardoleal20/homebrew-chopflow`) with a source-build
  formula, auto-bumped by the release workflow (`bump-homebrew` job computes the
  real `sha256` from the published crates.io tarball and pushes `Formula/chopflow.rb`).
- **`mcp` crate lib/main split** — `pub async fn run()` exposed; `main.rs` is now
  a thin wrapper (was main-only).
- **`broker` / `cli` `run()` entry points** — orchestration moved from `main.rs`
  into the library so the umbrella can call it; `main.rs` reduced to a one-liner.

### Changed
- Bumped workspace and all inter-crate dependency versions to `0.1.2` (Rust +
  Python client).
- `release.yml` publishes the `chopflow` umbrella last in the crates.io order;
  the GitHub Release install snippet now features the umbrella package + Homebrew.

### Notes
- `cargo install chopflow`, `brew install chopflow`, and `pip install chopflow`
  are all live as of this release.
