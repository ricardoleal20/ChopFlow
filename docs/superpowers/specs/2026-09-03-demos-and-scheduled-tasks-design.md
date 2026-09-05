# ChopFlow: Scheduled Tasks + Demo Handlers — Design

**Date:** 2026-09-03
**Status:** Approved (brainstorming complete, pre-implementation)
**Branch target:** `ricardo/scheduled-tasks-and-demos`
**Supersedes:** none. Extends the unified-`Storage` architecture from `ab180f0`.

## 1. Purpose & scope

Turn ChopFlow from "a queue that echoes" into a functional, demonstrable
operations platform in two reinforcing moves:

1. **Scheduled tasks** — run a task once at a future time (one-shot ETA) or
   on a recurring cron schedule, with a per-schedule overlap policy. The
   broker owns a ticker that materializes tasks from schedule templates.
2. **Demo handlers** — a `demos` crate with real (simulated, no external I/O)
   handlers and a seeding tool, so `cargo run` produces a live, interesting
   dashboard out of the box.

The two are designed together because demos give scheduled tasks something
real to run, and scheduling makes the demos interesting (e.g. "every 2
minutes, run a compute batch").

### Out of scope (deferred)

- Worker concurrency pool (separate workstream; today the worker runs one
  task at a time — demos will still showcase the system sequentially).
- Wiring the existing unused `RetryPolicy` enum into the failure path
  (retries continue to use the ad-hoc `next_retry_time` mechanism; the
  `flaky_handler` demo exercises that path as-is).
- Timezone-aware cron (server evaluates in UTC; the UI converts from a
  user-chosen local zone before sending).

## 2. Approach: first-class `Schedule` entity

A `Schedule` is a distinct entity from `Task`. The broker runs a background
ticker that, each second, queries schedules whose `next_fire` has passed and
materializes a `Task` from the schedule's frozen `task_template`. One-shot
schedules self-disable after firing; cron schedules advance `next_fire` to
the next match.

This unifies one-shot and recurring under one model, gives the UI a clean
"Schedules" view, and makes the overlap policy a natural per-schedule field.
Alternatives considered (ETA-only with no `Schedule` entity; `Schedule`
cron-only with one-shot staying on `Task`) were rejected for creating either
lifecycle mess or two unrelated "run later" mechanisms.

## 3. Data model (`core/src/schedule.rs`, new)

```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::HashMap;

/// Frozen task spec expanded into a `Task` at fire time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTemplate {
    pub name: String,
    pub payload: serde_json::Value,
    pub tags: Vec<String>,
    pub resources: HashMap<String, u32>,
    pub max_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScheduleKind {
    /// Fires once at `eta`, then self-disables.
    OneShot { eta: DateTime<Utc> },
    /// Fires on every cron match.
    Cron { cron: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OverlapPolicy {
    /// Skip the fire if any task from this schedule is Queued or Running.
    Skip,
    /// Keep at most one pending; skip if one is already Queued/Running.
    /// (Operationally identical to Skip at the storage layer; the
    /// distinction is surfaced in the UI for operator intent.)
    Coalesce,
    /// Always enqueue a new task regardless of in-flight runs.
    Allow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: Uuid,
    pub name: String,
    pub task_template: TaskTemplate,
    pub kind: ScheduleKind,
    pub overlap_policy: OverlapPolicy,
    pub enabled: bool,
    pub last_fired: Option<DateTime<Utc>>,
    /// When the ticker should next consider firing. For OneShot this is the
    /// `eta`; for Cron it is the next cron match. The ticker recomputes it
    /// after each fire and on broker startup reconciliation.
    pub next_fire: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}
```

### `Task` change (`core/src/task.rs`)

Add one optional field:

```rust
/// If this task was spawned by a schedule, the schedule's id. `None` for
/// ad-hoc enqueues. The ticker sets it; the overlap check queries by it.
pub schedule_id: Option<Uuid>,
```

`Task::new` sets `schedule_id: None`. The ticker's materialization sets it.
`TaskDto` (HTTP) and the proto `Task` message gain the field so the UI can
show lineage. Serialized into `task_json` for SQLite (no schema migration —
the column stores the whole JSON blob, as today).

## 4. Storage (`core/src/storage.rs`)

The `Storage` trait gains six methods. `InMemoryStorage` and `SqliteStorage`
each implement them, mirroring the existing `tasks` table pattern.

```rust
async fn insert_schedule(&self, schedule: Schedule) -> Result<()>;
async fn get_schedule(&self, id: &Uuid) -> Result<Option<Schedule>>;
async fn list_schedules(&self) -> Result<Vec<Schedule>>;
async fn delete_schedule(&self, id: &Uuid) -> Result<()>;
async fn update_schedule(&self, schedule: Schedule) -> Result<()>;
/// Schedules with `enabled = true` and `next_fire <= now`, for the ticker.
async fn due_schedules(&self, now: DateTime<Utc>) -> Result<Vec<Schedule>>;
/// Tasks spawned by `schedule_id` that are still in flight (Queued/Running).
/// Used by the overlap check. Reuses the existing tasks table + a filter.
async fn in_flight_for_schedule(&self, schedule_id: &Uuid) -> Result<Vec<Task>>;
```

### SQLite schema

A second table alongside `tasks`:

```sql
CREATE TABLE IF NOT EXISTS schedules (
    id          TEXT PRIMARY KEY,
    next_fire_ms INTEGER NOT NULL,
    enabled     INTEGER NOT NULL,
    schedule_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS schedules_next_fire ON schedules(next_fire_ms);
```

`next_fire_ms` is the indexed millis-since-epoch used by `due_schedules`
(`WHERE enabled = 1 AND next_fire_ms <= ?now`). The full `Schedule` (including
`next_fire` as ISO, `kind`, `overlap_policy`) is serialized to
`schedule_json`, exactly as `tasks` serializes to `task_json`. `update_schedule`
writes both columns atomically.

`in_flight_for_schedule` queries `tasks` by scanning `task_json` for the
`schedule_id` (the tasks table has no dedicated `schedule_id` column; a scan
is acceptable at ChopFlow's scale and avoids a migration). If performance ever
matters, a `schedule_id` column + index can be added later without changing
the trait.

## 5. Broker ticker (`broker/src/lib.rs`)

A new background task spawned in `main.rs` alongside `spawn_timeout_monitor`:

```rust
impl ChopFlowBrokerService {
    pub fn spawn_schedule_ticker(self) { /* tokio::spawn loop */ }
}
```

Loop (every 1s):
1. `let now = Utc::now();`
2. `let due = storage.due_schedules(now).await?;`
3. For each `schedule` in `due`:
   a. **Overlap check:** `let in_flight = storage.in_flight_for_schedule(&schedule.id).await?;`
      - If `overlap_policy != Allow` and `!in_flight.is_empty()`: **skip this
        fire** but still advance `next_fire` (cron) so the next match is
        computed; do not update `last_fired`. (OneShot: leave `next_fire`
        alone — it already passed; the schedule stays enabled and will retry
        next tick until it can fire, then self-disable.)
   b. **Materialize:** build a `Task` from `schedule.task_template`:
      fresh `id`, `enqueue_time = now`, `status = Queued`, `eta = None`,
      `schedule_id = Some(schedule.id)`.
   c. **Insert task:** `storage.insert(task).await`.
   d. **Advance schedule:**
      - Cron: `next_fire = next cron match after now`; `last_fired = Some(now)`.
      - OneShot: `enabled = false`; `last_fired = Some(now)`.
   e. **Update schedule:** `storage.update_schedule(schedule).await`.
4. A single bad schedule (e.g. unparseable cron that slipped through) is
   logged and skipped; the loop continues. Mirrors `handle_task_timeouts`
   resilience.

### Startup reconciliation

When the broker starts (before serving), for each enabled schedule:
- Cron: recompute `next_fire` to the **next future match from `now`**. Do
  **not** backfill missed runs (backfilling is a footgun for overloaded
  systems). This is the same "skip missed" semantics as Temporal.
- OneShot: if `eta < now` and `enabled`, it will fire on the first ticker
  tick (the ticker's `due_schedules` returns it). If `enabled = false`, leave
  it (a past one-shot that already fired).

## 6. API (HTTP + gRPC + CLI)

### HTTP (`broker/src/http.rs`)

New routes, mirroring the tasks CRUD:

| Method | Path | Body / Query | Returns |
|---|---|---|---|
| `GET` | `/api/schedules` | `?enabled=true` | `[{schedule}]` |
| `GET` | `/api/schedules/:id` | — | `{schedule}` |
| `POST` | `/api/schedules` | create body (below) | `{schedule_id}` |
| `PATCH` | `/api/schedules/:id` | `{enabled?, overlap_policy?, cron?}` | `{schedule}` |
| `DELETE` | `/api/schedules/:id` | — | `{success}` |

`POST /api/schedules` body:

```json
{
  "name": "nightly-digest",
  "task_template": {
    "name": "email_digest",
    "payload": { "segment": "weekly-active" },
    "tags": ["notif"],
    "resources": {},
    "max_retries": 3
  },
  "kind": { "type": "cron", "cron": "0 9 * * *" },
  "overlap_policy": "skip"
}
```

One-shot variant:

```json
{
  "name": "one-off-pipeline",
  "task_template": { "name": "simulate_pipeline", "payload": {}, "tags": ["demo"], "resources": {}, "max_retries": 1 },
  "kind": { "type": "oneshot", "eta": "2026-09-10T14:30:00Z" },
  "overlap_policy": "allow"
}
```

The broker validates the cron expression (via the `cron` crate) on `POST`
and `PATCH`; invalid → `400 Bad Request` with `{"error":"invalid cron: ..."}`.
`next_fire` is computed server-side on insert and is not accepted from the
client.

`GET /api/stats` gains `schedules: usize` (count of enabled schedules), so
the dashboard can surface it.

### gRPC (`broker/proto/chopflow.proto`)

Add a `Schedule` message (mirroring the struct) and three RPCs on the broker
service so the CLI and programmatic clients can manage schedules:

```proto
message Schedule { /* id, name, task_template, kind, overlap_policy, enabled, last_fired, next_fire, created_at */ }
message CreateScheduleRequest { Schedule schedule = 1; }
message ListSchedulesRequest {}
message ListSchedulesResponse { repeated Schedule schedules = 1; }
message DeleteScheduleRequest { string id = 1; }

rpc CreateSchedule(CreateScheduleRequest) returns (CreateScheduleResponse);
rpc ListSchedules(ListSchedulesRequest) returns (ListSchedulesResponse);
rpc DeleteSchedule(DeleteScheduleRequest) returns (DeleteScheduleResponse);
```

The ticker runs server-side regardless of who created the schedule.

### CLI (`cli/src/main.rs`)

A `schedule` subcommand:

```
chopflow_cli schedule create --name X --task email_digest --cron "0 9 * * *" --tags notif --overlap skip
chopflow_cli schedule create --name X --task echo --eta 2026-09-10T14:30:00Z   # one-shot
chopflow_cli schedule list
chopflow_cli schedule delete <id>
```

`--cron` and `--eta` are mutually exclusive (exactly one required). Payload
defaults to `{}`; `--payload '{...}'` overrides. Tags `--tags a,b`. Resources
`--resources cpu:2,gpu:1`. `--max-retries N`.

`chopflow_cli enqueue` already passes `--eta` through gRPC (the proto field
exists); no change needed there. The HTTP `POST /api/tasks` body **does not**
gain `eta` in this spec — ad-hoc one-shot tasks are created via the
`/api/schedules` one-shot kind. (Keeps "run now" vs "run later" as distinct
intents.)

### Timezone

Server evaluates `eta` and cron in **UTC**. The UI offers a local-timezone
datetime picker but converts to UTC before sending. Documented in the README.

## 7. Dashboard UI (`broker/ui/dashboard.html`)

Three additions, all consuming `/api/schedules` with the existing 2s polling
+ in-place diff pattern. No new design tokens.

### 7.1 Schedules view

New sidebar nav item "Schedules" (between Tasks and Workers) with a live
count badge. Third view `#view-schedules`: view-head, filter chips
(All / Enabled / One-shot / Cron / Disabled), and a table:

| Name | Kind | Trigger | Overlap | Last fired | Next fire | Enabled |
|---|---|---|---|---|---|---|

- **Kind:** `stbadge` chip — `Cron` (warn tint) or `One-shot` (info tint).
- **Trigger:** cron expression (`0 9 * * *`) or one-shot ETA
  (`Sep 10, 14:30 UTC`).
- **Next fire:** live "time-until" (`in 4h 12m`) patched by the 1s ticker,
  same in-place technique as the tasks enqueued cell.
- Row click → schedule drawer: Summary (template name/payload/tags/resources,
  next/last fire, overlap policy), "Run now" button (materializes a task
  immediately regardless of `next_fire` — implemented as a client-side
  `POST /api/tasks` using the schedule's `task_template`, with `schedule_id`
  left unset so it doesn't disturb the schedule's own overlap accounting),
  Disable/Enable toggle (`PATCH`), Delete (`DELETE`). No new server endpoint
  is added for "Run now" — it reuses the existing enqueue path.

### 7.2 New-Task modal: "Schedule" toggle

The modal gains a segmented control: **Run immediately** (default, today's
behavior → `POST /api/tasks`) vs **Schedule** (→ `POST /api/schedules`).
Choosing Schedule reveals:
- One-shot vs Cron radio.
- One-shot: a `datetime-local` picker → `kind: {type:"oneshot", eta}`
  (converted to UTC).
- Cron: a cron expression text field with a live "next fire" preview
  computed client-side.
- Overlap-policy dropdown (Skip / Coalesce / Allow), default Skip.

### 7.3 Scheduled-task lineage

- Tasks table: a small schedule-icon indicator on rows with `schedule_id`
  set (hover tooltip: "from schedule *name*").
- Task drawer Lifecycle tab: a "Spawned by schedule" note at the Created
  node when `schedule_id` is present.

Theme, motion (≤300ms ease-out entrances, in-place live updates), and the
existing token system apply unchanged.

## 8. Demo handlers (`demos/` crate, new workspace member)

Two artifacts: a demo worker binary and a seeding tool.

### 8.1 `demos/src/main.rs` — `chopflow_demo_worker`

A worker binary that reuses the worker's registration + `FetchTasks` pull
loop (factored or copied) but registers a richer `TaskRegistry`. Connects to
the broker exactly like `chopflow_worker` (`--broker`, `--tags`,
`--resources`) — a drop-in worker with demo handlers instead of `echo`.

Four handlers, all pure-Rust, no external I/O, run out-of-the-box:

| Handler | What it does | Demonstrates |
|---|---|---|
| `resize_image` | Generates a synthetic gradient PNG (via the `image` crate), resizes to payload target dimensions, returns output size + dims. | CPU + memory bound; realistic payload/result; resource requirements. |
| `batch_compute` | CPU-bound matrix-multiply over random f64 matrices (via `nalgebra`) sized by payload; returns timing + checksum. | Pure CPU; resource model; (later) concurrency. |
| `simulate_pipeline` | Multi-stage simulated pipeline (download→process→upload) with staged sleeps + progress; returns per-stage timings. | Long-running; great for watching Queued→Running→Completed live. |
| `flaky_handler` | Fails ~30% of the time (seeded RNG from payload), forcing retries; succeeds eventually. | Retry/backoff/ETA + dead-letter path — the most interesting lifecycle. |

Each handler: `fn(payload: serde_json::Value) -> Result<serde_json::Value>`,
registered by name. The worker's existing handler-lookup
(`task_registry.get(name).or(get("default"))`) is reused unchanged.

### 8.2 `demos/src/bin/seed.rs` — `chopflow_demo_seed`

Hits the broker HTTP API to enqueue a varied set of demo tasks and create
demo schedules, so opening the dashboard immediately shows life:
- Enqueues one of each handler type (all four lifecycles visible).
- Creates a cron schedule `"*/2 * * * *"` running `batch_compute`
  (overlap: skip) — a new task appears every 2 minutes.
- Creates a one-shot schedule 5 minutes out running `simulate_pipeline`.

### 8.3 Run story

`demos/run.sh` + README section:

```bash
cargo build
./target/debug/chopflow_broker start --http-port 8080 --storage memory --open &
./target/debug/chopflow_demo_worker --broker http://localhost:8000 --tags demo,ml --resources cpu:4 &
./target/debug/chopflow_demo_seed --broker http://localhost:8080
# → browser shows 4 tasks flowing + 2 schedules ticking
```

### 8.4 Dependencies

- `image = "0.25"` — image gen/resize (pure Rust, no system libs).
- `nalgebra = "0.33"` — matrix compute (pure Rust).
- `rand` — already transitively available; add explicitly to `demos`.
- `cron = "0.12"` — cron parsing/next-fire (in `core`, for the ticker +
  validation).
- `reqwest` (blocking or async) — in `demos` only, for the seed tool's HTTP
  calls. (Or reuse `tonic` if we seed via gRPC; HTTP is simpler for a
  throwaway seeder.)

## 9. Error handling & edge cases

- **Invalid cron:** `POST`/`PATCH` validate via the `cron` crate; invalid →
  `400` with `{"error":"invalid cron: ..."}`. CLI parses client-side too.
- **Missed fires (broker down):** on startup, cron schedules recompute
  `next_fire` to the next future match from `now` — no backfill. One-shot
  schedules with `eta < now` and `enabled` fire on the first tick; disabled
  past one-shots are left alone.
- **Overlap:** `Skip`/`Coalesce` skip the fire if `in_flight_for_schedule`
  returns any task (Queued/Running); `Allow` always enqueues. Skipped cron
  fires still advance `next_fire`.
- **Ticker failure:** one bad schedule is logged + skipped; the loop
  continues (mirrors `handle_task_timeouts`).
- **Schedule deletion:** does NOT cancel already-spawned tasks (they run to
  completion). Documented in the UI drawer.
- **`flaky_handler` retries:** exercised through the existing retry path
  (`mark_failed` → `next_retry_time` → ETA-gated `claim_ready`). Implicit
  integration test for retries.

## 10. Testing

- **`core` unit tests (`schedule.rs`):** `next_fire` computation for cron +
  one-shot; `OverlapPolicy` resolution; one-shot self-disable after fire;
  cron advance after fire.
- **`core` storage tests (`storage.rs`):** `insert/get/list/delete/update`
  for schedules (both backends); `due_schedules(now)` returns only due;
  `in_flight_for_schedule` correctness. Mirror the existing
  `in_memory_*` + `sqlite_*` test pairs.
- **`broker/tests/grpc_flow.rs` (extend):** `CreateSchedule` (cron,
  every-second) → wait one tick → assert a task was materialized with the
  template name + `schedule_id` set; `CreateSchedule` (one-shot, past) →
  fires immediately; overlap `Skip` with a pre-existing running task → no
  new task.
- **`broker/tests/` (new):** ticker-reconciliation test — schedule with
  `next_fire` in the past, start broker, assert it fires once and advances.
- **Demos:** not unit-tested (showcase code); the `seed` binary is
  smoke-tested by the manual run script. `flaky_handler` is the implicit
  retry integration test.

## 11. Files touched

**New:**
- `core/src/schedule.rs` — `Schedule`, `ScheduleKind`, `OverlapPolicy`,
  `TaskTemplate`, `next_fire` helpers.
- `demos/` — crate (`Cargo.toml`, `src/main.rs`, `src/bin/seed.rs`,
  `src/handlers/`, `run.sh`).
- `broker/tests/schedule_ticker.rs` (or extend `grpc_flow.rs`).

**Modified:**
- `core/src/lib.rs` — re-export `schedule`.
- `core/src/task.rs` — `+schedule_id: Option<Uuid>`.
- `core/src/storage.rs` — 6 trait methods + InMemory + Sqlite impls + SQLite
  `schedules` table.
- `core/Cargo.toml` — `+cron`.
- `broker/src/lib.rs` — `spawn_schedule_ticker` + reconciliation.
- `broker/src/main.rs` — spawn the ticker.
- `broker/src/http.rs` — `/api/schedules*` routes + DTOs + `stats.schedules`.
- `broker/proto/chopflow.proto` — `Schedule` message + 3 RPCs.
- `cli/src/main.rs` — `schedule` subcommand.
- `broker/ui/dashboard.html` — Schedules view + modal toggle + lineage.
- `broker/ui/dist/index.html` — re-embedded after edits.
- `Cargo.toml` (workspace) — `+demos` member.
- `README.md` — scheduling + demos sections.

## 12. Verification

1. `cargo build --workspace` — compiles incl. `demos` + `cron`.
2. `cargo test --workspace` — all existing tests still pass + new schedule
   tests pass.
3. Manual demo run (`demos/run.sh`): broker + demo worker + seed → dashboard
   shows 4 tasks flowing through distinct lifecycles + 2 schedules ticking,
   `flaky_handler` visibly retrying.
4. Schedule lifecycle: create cron schedule via UI → watch it materialize a
   task each interval; disable it → stops; one-shot fires once and
   self-disables.
5. Overlap: set a `batch_compute` schedule to `*/1 * * * *` with overlap
   `Skip` while the worker is paused → only one task in flight at a time.
6. Restart: with SQLite, schedules persist across broker restart and
   reconcile `next_fire` without backfilling.
