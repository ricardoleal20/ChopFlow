# Changelog

All notable changes to ChopFlow are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- 

## [0.1.6] - 2026-09-19

### Added
- **Checkpointed pipelines (durable stages).** Tasks may declare `stages`
  (CLI `--stages`, HTTP `stages`, MCP `enqueue_task`, proto `EnqueueTaskRequest`);
  context-aware worker handlers (`registry.register_ctx`) persist a checkpoint
  per stage via `TaskCtx::checkpoint` (upserted per `(task_id, stage)`, new
  `SaveCheckpoint` / `GetCheckpoints` gRPCs, `checkpoints` table in SQLite),
  and a retried task re-fetches them and resumes from the last completed
  stage. `GET /api/tasks/:id`, `chopflow status get`, and the MCP `get_task` /
  `wait_for_task` outputs carry `stages`, `idempotency_key`, and `checkpoints`.
- **Rate-limit-aware replenishing resources.** Worker `--resources` entries
  may declare a token bucket: `llm.rpm:10@10/60` = capacity 10 refilled by 10
  per 60s (plain `cpu:4` stays a static slot). Consuming a replenishing
  resource never restores on release — tokens return only via lazy time-based
  refill (capped at capacity) — so a task requiring `{"llm.rpm": 1}` waits
  for quota instead of failing.
- **Idempotent submits.** `POST /api/tasks` accepts `idempotency_key`; a task
  already stored with that key (any status) is returned as-is with
  `"deduplicated": true` and 200 OK instead of creating a duplicate. The
  check-and-insert runs under a broker-wide submit lock, so concurrent
  same-key submits create exactly one task.
- **RAG ingestion demo** (`demos/rag.sh`): a checkpointed `rag.ingest`
  pipeline (`chunk → embed → index` over a sample document, deterministic
  16-dim pseudo-embeddings, per-chunk `embed` checkpoints for mid-flight
  resume) requiring `{"llm.rpm": 1}` against a worker declaring a replenishing
  `llm.rpm` bucket, seeded twice with the same idempotency key.
- **Dashboard pipeline stages.** The task drawer polls `GET /api/tasks/:id`
  while open and renders a "Pipeline stages" timeline (completed stages with
  timestamps and progress, the in-flight stage pulsing, pending stages muted)
  plus the task's idempotency key.
- **macOS desktop app** (`app/src-tauri`, Tauri 2). Starts/stops the local
  broker (spawn + `--parent-pid` watchdog, adopt-or-rebind), runs the optional
  persistent MCP-over-HTTP gateway, owns the connection store (local +
  remotes), and lives in the macOS menu bar (Open · Local broker · MCP ·
  Remote brokers with live ●/○ status · Quit). Ships a first-run wizard
  (animated hero → Environment · MCP · Security · Learn → Finish setup with a
  loading rail). Distributed as a self-contained `.dmg` from GitHub Releases;
  the landing page's "Download the macOS App — Free!" button (Material icon)
  links to the latest release.
- **Broker security:** `--api-token` is repeatable (one credential per
  occurrence, all equally valid); a bare flag auto-generates one;
  `CHOPFLOW_API_TOKEN` env is the fallback; no flag = open. Non-2xx statuses
  and `auth_required` are surfaced to clients.
- **MCP gateway access token:** `--access-token` gates the gateway itself
  (`Authorization: Bearer`, `?access_token=` / `?code=`), independent from the
  broker's tokens — set/generate/regenerate/remove in the app.
- **Non-destructive security switches** in Settings (Security: broker tokens
  enforced on/off; MCP: gateway access token enforced on/off). Switches never
  create or delete tokens; the first-run wizard is where the decision creates
  the first one.
- **Settings full screen** (sidebar or ⌘,): data folder (editable, native
  folder picker), remote servers, MCP + access token, Security, Logs (copy /
  download `.log` via the native save dialog), Danger zone (delete-all and a
  non-destructive "Preview first-run welcome").
- **Menu bar icons:** Google Material Symbols (play_arrow / stop /
  power_settings_new), white-on-transparent; squircle app icon (black
  gradient tile + white mark); real Rust/Python/Java icons on the Learn
  screen (homarr-labs/dashboard-icons, MIT).

### Changed
- Workspace + Python client bumped to `0.1.5`.
- Dev executable is named `ChopFlow`; packaged app display name comes from
  `productName` in `tauri.conf.json`.
- The `.dmg` bundles the `chopflow` CLI as a sidecar (`externalBin`), so the
  app's broker/MCP spawns work offline from the packaged app.

### Fixed
- Settings destructive confirmations use inline two-step confirms
  (`window.confirm` is unsupported in the Tauri webview — it silently returns
  false, so "Revoke"/"Delete"/"Move data" never fired).
- Toggling the MCP gateway no longer reloads the whole app (in-place state
  refresh).
- The active-connection "pill" (and the oversized 16px menu glyphs, since
  corrected to 62%-padded 16px) cleaned up.

## [0.1.5] - 2026-09-18

- **Checkpointed pipelines (durable stages).** Tasks may declare `stages`
  (CLI `--stages`, HTTP `stages`, MCP `enqueue_task`, proto `EnqueueTaskRequest`);
  context-aware worker handlers (`registry.register_ctx`) persist a checkpoint
  per stage via `TaskCtx::checkpoint` (upserted per `(task_id, stage)`, new
  `SaveCheckpoint` / `GetCheckpoints` gRPCs, `checkpoints` table in SQLite),
  and a retried task re-fetches them and resumes from the last completed
  stage. `GET /api/tasks/:id`, `chopflow status get`, and the MCP `get_task` /
  `wait_for_task` outputs carry `stages`, `idempotency_key`, and `checkpoints`.
- **Rate-limit-aware replenishing resources.** Worker `--resources` entries
  may declare a token bucket: `llm.rpm:10@10/60` = capacity 10 refilled by 10
  per 60s (plain `cpu:4` stays a static slot). Consuming a replenishing
  resource never restores on release — tokens return only via lazy time-based
  refill (capped at capacity) — so a task requiring `{"llm.rpm": 1}` waits
  for quota instead of failing.
- **Idempotent submits.** `POST /api/tasks` accepts `idempotency_key`; a task
  already stored with that key (any status) is returned as-is with
  `"deduplicated": true` and 200 OK instead of creating a duplicate. The
  check-and-insert runs under a broker-wide submit lock, so concurrent
  same-key submits create exactly one task.
- **RAG ingestion demo** (`demos/rag.sh`): a checkpointed `rag.ingest`
  pipeline (`chunk → embed → index` over a sample document, deterministic
  16-dim pseudo-embeddings, per-chunk `embed` checkpoints for mid-flight
  resume) requiring `{"llm.rpm": 1}` against a worker declaring a replenishing
  `llm.rpm` bucket, seeded twice with the same idempotency key.
- **Dashboard pipeline stages.** The task drawer polls `GET /api/tasks/:id`
  while open and renders a "Pipeline stages" timeline (completed stages with
  timestamps and progress, the in-flight stage pulsing, pending stages muted)
  plus the task's idempotency key.
### Fixed
- The Java and Python clients now build `RegisterWorkerRequest` with the new
  `ResourceSpec` map (the proto change left them sending `map<string, uint32>`,
  breaking the Java compile and failing Python worker registration forever);
  Python stubs regenerated, `replenishing(...)` builders added to both clients,
  and pytest gained a `--timeout=180` guard so a registration regression fails
  in minutes instead of hanging CI for hours.

## [0.1.4] - 2026-09-16

## [0.1.4] - 2026-09-16

### Added
- **Environments.** Brokers gain `--env`/`--region` identity and an optional fleet
  catalog (`config/environments.yml`) served read-only at `GET /api/environments`.
  The dashboard cluster selector becomes a real switcher that retargets the API base
  between brokers at runtime (permissive CORS for cross-broker fetches), and the CLI
  resolves `--env <name>` to a broker gRPC URL from the catalog. Workers stay
  single-homed by design; no auth or federation.
- **Dashboard share meta + icons.** The broker-served dashboard now ships a proper
  title, description, Open Graph + Twitter card meta, and the ChopFlow brand mark as
  favicon / apple-touch-icon.
- **Docs: Environments.** New Operations → Environments concept page and HTTP API →
  Environments reference page on the docs site; `/api/stats` documents `env`/`region`.
- **Local CI hook.** Version-controlled `pre-push` git hook (`scripts/setup-hooks.sh`)
  mirrors the CI workflow locally (`scripts/ci-local.sh`: fmt/clippy/test,
  lint/format/build, ruff, pytest, mvn verify) so formatting and lint slips are caught
  before they turn a PR red.

### Notes
- Bumped workspace and all inter-crate dependency versions to `0.1.4` (Python client
  to `0.1.4` on PyPI).

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
