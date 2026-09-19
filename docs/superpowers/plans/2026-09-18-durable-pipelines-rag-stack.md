# Durable pipelines + rate-limit resources + idempotency + RAG demo — Implementation Plan

> **For agentic workers:** This plan is executed by dispatched subagents in
> dependency order (stages below). Steps use checkbox (`- [ ]`) syntax for
> tracking. Each stage's agent gets a self-contained handoff prompt derived
> from the corresponding section.

**Goal:** Make ChopFlow robust and AI-workload-ready with four features:
(1) checkpointed pipelines (Temporal-style "save-states" without the workflow
engine), (2) replenishing rate-limit-aware resources, (3) idempotency keys
with result reuse, (4) a real RAG ingestion demo that showcases all three.

**Architecture:** All four features are additive to the existing crates:
`core` (task model, storage trait, resource accounting), `proto` (wire
surface), `broker` (gRPC + HTTP), `worker` (handler context), `cli`/`mcp`
(plumbing), `demos` (RAG handlers), `broker/ui` (drawer stages). Storage keeps
its single `Storage` trait with new methods implemented for both InMemory and
SQLite. Refill is a token-bucket computed lazily (no background timers).

**Tech stack:** Rust workspace (tonic/prost gRPC, axum HTTP, rusqlite,
tokio), React+TS dashboard, static docs pages.

**Spec:** This document. Related prior specs:
`docs/superpowers/specs/2026-09-17-macos-app-design.md` (security model that
the new HTTP endpoints must respect).

## Global Constraints

- Handler signature compatibility: existing `register(name, fn(Value) -> Result<Value>)`
  handlers (demos, llm-worker) must keep compiling unchanged. New
  context-aware handlers go through a NEW registration API.
- Wire compatibility is NOT required across versions (all crates ship
  together), but `Task` JSON must stay backward-compatible with stored
  SQLite rows (new fields serde-default to `None`).
- `main` branch is protected; all work stays on
  `ricardo/durable-pipelines-rag-stack` until PR.
- Commit format: `<gitmoji> <Action>: <summary>` imperative, no
  Co-Authored-By, message via `git commit -F <file>`. Subagents do NOT
  commit — the orchestrator reviews and commits each stage.
- Never `git add -A` (bench/, ROADMAP.md hazards). No secrets.
- CI must pass: `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`,
  `cargo test --workspace`, frontend eslint/prettier/build (dist must be
  rebuilt when `broker/ui/src` changes), Python ruff/pytest, Java mvn.

---

## Feature A — Checkpointed pipelines (durable stages)

**Semantics:** A task may declare `stages: ["download","process","upload"]`.
A context-aware handler reports progress with `ctx.checkpoint(stage, payload)`
(persisted, upsert per stage). On retry, the worker re-fetches the task's
checkpoints and passes them to the handler, which resumes from the last
completed stage instead of restarting. Checkpoints survive broker restarts
(SQLite) and are visible in the task drawer.

### A1. Core model + storage

- `core/src/task.rs`:
  - `Task` gains `pub stages: Option<Vec<String>>` (serde default None,
    `#[serde(skip_serializing_if = "Option::is_none")]`) and
    `pub idempotency_key: Option<String>` (same treatment).
  - `TaskBuilder::with_stages(Vec<String>)`, `with_idempotency_key(String)`.
- `core/src/storage.rs` trait additions:
  - `async fn save_checkpoint(&self, task_id: &Uuid, stage: &str, payload: &str) -> Result<()>;` (upsert)
  - `async fn checkpoints(&self, task_id: &Uuid) -> Result<Vec<Checkpoint>>;` (recorded_at order)
- New `core/src/checkpoint.rs` (or in task.rs): `pub struct Checkpoint { pub task_id: Uuid, pub stage: String, pub payload: String, pub recorded_at: DateTime<Utc> }` (Serialize/Deserialize/Clone/Debug).
- InMemory: `Mutex<HashMap<Uuid, Vec<Checkpoint>>>`; SQLite: `checkpoints`
  table `(task_id TEXT, stage TEXT, payload TEXT, recorded_at TEXT, PRIMARY
  KEY(task_id, stage))` created via the existing migration path (IF NOT
  EXISTS).
- SQLite `tasks` table: `ALTER TABLE ... ADD COLUMN stages TEXT` (JSON) and
  `idempotency_key TEXT` following the existing pre-existing-DB migration
  pattern (`sqlite_migrates_pre_existing_db_without_priority_column` test is
  the model).
- Tests: checkpoint roundtrip + upsert-overwrite + ordering (both backends);
  task stages/idempotency_key serde round-trip; sqlite migration of pre-existing DB.

### A2. Replenishing (rate-limit-aware) resources

**Semantics:** Worker resources may be static (`cpu:4`) or replenishing
(`llm.rpm:10@10/60` = capacity 10, refills 10 per 60s). Static: allocate
decrements, release restores. Replenishing: allocate consumes; release does
NOT restore; tokens return only via time-based refill (lazy token bucket,
no timers). Task requirements stay `map<string, u32>` — a task requiring
`llm.rpm: 1` waits until a worker's bucket has ≥1.

- `core/src/resources.rs`:
  - `pub struct RefillSpec { pub amount: u32, pub period_secs: u32 }` (Serialize/Deserialize/Clone/Debug/PartialEq).
  - `ResourceAvailability` gains `pub refill: HashMap<String, RefillSpec>` and
    internal per-resource refill bookkeeping (`last_refill: HashMap<String, f64>`
    storing accumulated fractional tokens — or an equivalent lazy-bucket
    design with an injectable clock for tests).
  - `add_replenishing_resource(name, capacity, refill_amount, period_secs)`.
  - New `fn refill_now(&mut self, now: Instant)` (lazy bucket top-up, capped
    at capacity) called at the top of `can_be_satisfied_by`/`allocate`/
    `from_availability`. Clock injectable: an `Option<Instant>` override or
    taking `now` as a parameter on the internal paths with public wrappers
    using `Instant::now()`.
  - `release`: skip resources that have a refill spec.
  - `parse` helper (used by worker CLI): `parse_resources_ext("llm.rpm:10@10/60,cpu:4")`
    -> `(HashMap<String,u32> capacity, HashMap<String,RefillSpec>)` — keep
    legacy `parse_resources` behavior for plain `name:amount`.
- Tests: capacity cap, refill over time with fake clock, consumed tokens not
  restored on release, static resources unchanged, mixed worker.

### A3. Dispatcher integration

- `core/src/dispatcher.rs`: worker `ResourceAvailability` already lives here —
  wire registration to accept refill specs; call `refill_now` before
  `can_handle`/`assign`. ListWorkers/stats surface availability as today.
- Tests: rate-limited task waits when bucket empty, proceeds after refill
  (fake clock), static path regression.

## Feature B — Proto + broker surface

- `proto/proto/chopflow.proto`:
  - `message Checkpoint { string task_id = 1; string stage = 2; string payload = 3; string recorded_at = 4; }`
  - `message ResourceSpec { uint32 capacity = 1; uint32 refill_amount = 2; uint32 refill_period_secs = 3; }`
  - `RegisterWorkerRequest.resources` -> `map<string, ResourceSpec>` (refill_amount=0 => static).
  - `EnqueueTaskRequest` gains `repeated string stages = <next>` and
    `string idempotency_key = <next>`.
  - `TaskInfo` gains `repeated string stages` + `string idempotency_key`.
  - New RPCs: `rpc SaveCheckpoint(SaveCheckpointRequest) returns (SaveCheckpointResponse);`
    `{ string task_id, string stage, string payload }`; `rpc GetCheckpoints(GetCheckpointsRequest) returns (GetCheckpointsResponse);`
    `{ string task_id }` -> `{ repeated Checkpoint checkpoints }`.
  - Regenerate prost code the way the proto crate builds it today.
- `broker` gRPC: implement SaveCheckpoint + GetCheckpoints (honor `api_tokens`
  auth exactly like the other RPCs); thread stages/idempotency_key through
  EnqueueTask -> Task; expose them in TaskInfo.
- `broker` HTTP (`http.rs`):
  - `POST /api/tasks` accepts `"stages": [...]`, `"idempotency_key": "..."`.
  - **Idempotency semantics:** if `idempotency_key` present and a task with
    that key exists (any status), return the existing task with
    `"deduplicated": true` added to the response JSON (HTTP 200/201 either
    way, document which). The check+insert must happen under the same lock /
    transaction as insert (TaskStore lock for both backends) so concurrent
    duplicates cannot both create.
  - `GET /api/tasks/{id}` includes `"checkpoints": [...]` (stage, payload,
    recorded_at).
  - All new routes respect the existing `api_auth` middleware (no token
    needed on open brokers; bearer when protected).
- `core/src/storage.rs` trait addition for B: `async fn task_by_idempotency_key(&self, key: &str) -> Result<Option<Task>>;`
  (InMemory scan-or-index; SQLite `WHERE idempotency_key = ?` + index).
- Tests (broker/tests): checkpoint save+fetch over gRPC; HTTP task with
  stages round-trips; idempotent submit returns existing task twice and
  creates exactly one; token-protected broker still enforces auth on the new
  endpoints.

## Feature C — Worker handler context

- `worker/src/lib.rs`:
  - New `pub struct TaskCtx { pub task_id: String, pub task_name: String, pub stages: Option<Vec<String>>, pub checkpoints: Vec<Checkpoint>, client: <shared gRPC client>, }`
    with `pub async fn checkpoint(&self, stage: &str, payload: serde_json::Value) -> Result<(), String>`
    (serializes payload, calls SaveCheckpoint; errors are returned, not panics).
    Add `pub fn completed_stages(&self) -> Vec<&str>`.
  - New handler type `pub type CtxHandler = Arc<dyn Fn(TaskCtx, serde_json::Value) -> BoxFuture<Result<serde_json::Value, String>> + Send + Sync>;`
    (exact shape may follow the crate's existing async idiom).
  - `TaskRegistry::register_ctx(name, handler)`; existing `register` keeps
    working unchanged (old handlers get an empty ctx built with a no-op
    checkpoint sink so the loop is uniform).
  - Main fetch loop: after FetchTasks, for tasks with a registered ctx
    handler, `GetCheckpoints(task_id)` then invoke `handler(ctx, payload)`.
    Old-style handlers skip the extra RPC.
- `llm-worker`: must compile unchanged (no changes needed).
- Tests (worker/tests): ctx handler receives prior checkpoints on a retried
  task and `ctx.checkpoint()` persists through the broker (integration-style
  against an in-process broker, like existing worker integration tests).

## Feature D — CLI + MCP plumbing

- `cli`: `enqueue` gains `--stages <STAGES>` (comma-separated) and
  `--idempotency-key <KEY>`; task JSON files may also carry `"stages"` /
  `"idempotency_key"`. `status get` prints checkpoints (stage + time) when
  present. Unit tests for the new parse paths (mirror
  `cli/tests/unit.rs` style).
- `mcp`: `submit_task` tool gains optional `stages` / `idempotency_key`
  params; `get_task` output includes checkpoints automatically (it proxies
  HTTP). Update the tool descriptions + the server instructions text.

## Feature E — RAG demo (demos crate)

- `demos/src/handlers.rs` (or a new `rag.rs` module):
  - `rag.ingest` registered via `register_ctx` — the showcase pipeline with
    declared stages `["chunk", "embed", "index"]`:
    1. **chunk** (skipped if a `"chunk"` checkpoint exists): split
       `payload.document` into chunks of `payload.chunk_size` (default 64
       words); `ctx.checkpoint("chunk", {"chunks": [...]})`.
    2. **embed** (resumable): start from the last `"embed"` checkpoint's
       `next_index` (0 if none); for each remaining chunk produce a
       deterministic pseudo-embedding (hash-based vector, dim 16 — no API
       calls), checkpointing progress every chunk batch
       (`{"next_index": n}`) so a mid-flight retry demonstrates resumption.
    3. **index**: `ctx.checkpoint("index", {"stored": chunks.len()})`; return
       `{"chunks": n, "dim": 16, "indexed": true, "resumed_from": <stage or null>}`.
  - The seed posts `rag.ingest` with a sample multi-paragraph document AND
    exercises the idempotency key (same key twice -> same task id returned).
  - Demo worker declares a replenishing resource (`--resources
    "cpu:4,llm.rpm:60@60/60"`) and the ingest task requires
    `{"llm.rpm": 1}` so the rate-limit path is visible.
- `demos/run.sh` / new `demos/rag.sh`: broker + demo worker + RAG seed; print
  where to look in the dashboard.
- README section for the demo (one code block + what to observe).

## Feature F — Dashboard + docs

- `broker/ui/src/components/TaskDrawer.tsx`: when the task has `stages`,
  render a "Pipeline stages" list — completed stages (checkpoint exists,
  with time) vs pending, reusing existing timeline/badge styles. Types in
  the API layer get `stages?`, `idempotency_key?`, `checkpoints?`.
- Rebuild `broker/ui/dist` (`pnpm build` in broker/ui) and commit dist
  together with src.
- `README.md`: sections "Checkpointed pipelines", "Rate-limit-aware
  resources", "Idempotent submits" (concise, example JSON/flags each) + RAG
  demo section; update the features table if one exists.
- `CHANGELOG.md`: new `[Unreleased]` entries for all four features.
- `docs/design/chopflow-docs.html`: new sections under the existing docs
  structure — pipelines/checkpoints (with API + ctx example), replenishing
  resources (declaration syntax table), idempotency (HTTP example), RAG demo
  walkthrough. Keep the page's existing tone/format.

## Stage / agent dispatch order

- [ ] **Stage 1 — Core** (features A1, A2, A3, plus `task_by_idempotency_key`
      storage method): one agent, no commit. Verify: `cargo test -p chopflow-core`.
- [ ] **Stage 2 — Proto + broker** (feature B): one agent after Stage 1.
      Verify: `cargo test -p chopflow-proto -p chopflow-broker`.
- [ ] **Stage 3a — Worker ctx + demos RAG** (features C + E): one agent
      after Stage 2. Verify: `cargo test -p chopflow-worker -p chopflow-demos`.
- [ ] **Stage 3b — CLI + MCP** (feature D): one agent after Stage 2 (parallel
      with 3a — disjoint files). Verify: `cargo test -p chopflow-cli -p chopflow-mcp`.
- [ ] **Stage 4 — Dashboard UI + docs** (feature F): one agent after 3a+3b.
      Verify: `pnpm build` + eslint + prettier in broker/ui.
- [ ] **Stage 5 — Orchestrator**: full `scripts/ci-local.sh`, fix fallout,
      staged commits per feature, push branch, open PR (four-section body,
      self-assigned, labeled; no merge).

## Verification (end-to-end, Stage 5)

1. `cargo test --workspace` green; clippy `-D warnings` clean; fmt clean.
2. Manual smoke: start broker + demo worker; enqueue `rag.ingest` with
   idempotency key; verify (a) second submit returns same task id, (b) task
   JSON carries stages, (c) checkpoints grow chunk→embed→index, (d) a forced
   mid-embed retry resumes from the embed checkpoint, (e) the drawer renders
   the stage list.
3. Frontend: eslint/prettier/build green; dist rebuilt.
4. Docs pages updated and consistent with actual flags/API.
