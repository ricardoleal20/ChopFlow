# Scheduled Tasks + Demo Handlers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn ChopFlow from "a queue that echoes" into a demonstrable operations platform by adding a first-class `Schedule` entity (one-shot + cron, per-schedule overlap policy, in-broker ticker) and a `demos` crate with real simulated handlers + a seeding tool.

**Architecture:** A `Schedule` is a distinct entity from `Task`. The broker runs a 1s background ticker that queries due schedules, applies an overlap check, materializes a `Task` from the schedule's frozen `TaskTemplate`, and advances/ disables the schedule. One-shot and recurring unify under one model. Storage gains a `schedules` table + 6 trait methods (InMemory + SQLite). HTTP/gRPC/CLI all expose schedule CRUD. The React dashboard gains a Schedules view, a Schedule toggle in the enqueue modal, and scheduled-task lineage. A new `demos` workspace crate ships a drop-in demo worker (4 handlers) + a seed binary so `cargo run` produces a live dashboard.

**Tech Stack:** Rust 2021 workspace (core, broker, worker, cli + new demos); `cron` crate 0.12 (cron parsing/next-fire, in core); `image` 0.25 + `nalgebra` 0.33 (demos only); tonic 0.10 gRPC; axum 0.7 HTTP; rust-embed 8 SPA; React + Vite + TS + Tailwind + TanStack Query + framer-motion (dashboard).

**Spec:** `docs/superpowers/specs/2026-09-03-demos-and-scheduled-tasks-design.md` (approved, commit 59ced8c). The plan argues from the spec; executors read both.

## Global Constraints

- **Commit format:** gitmoji + Action imperative (`:sparkles: Add: …`). NEVER add `Co-Authored-By: Claude` or any AI-attribution trailer (project-level prohibition).
- **Branch:** `ricardo/scheduled-tasks-and-demos`. Commit incrementally per task; do not push/PR unless asked.
- **TDD:** every Rust task writes the failing in-module `#[cfg(test)] mod tests` (or `broker/tests/`) first, runs it red, implements, runs green, then commits. Demos (Task 11/12) are showcase code — not unit-tested; verified by the run script.
- **Rust edition 2021; workspace deps in root `Cargo.toml`.** New shared deps go in `[workspace.dependencies]` and are referenced `.workspace = true` in crate manifests.
- **Cron format gotcha:** the `cron` crate (0.12) expects 6–7 fields (with a leading seconds field). User-facing cron in this project is standard 5-field (e.g. `"0 9 * * *"`, `"*/2 * * * *"`). A `normalize_cron` helper in core prepends `"0 "` when exactly 5 fields are present, so users write familiar 5-field cron. All cron parsing goes through this helper.
- **Server evaluates ETA + cron in UTC.** The UI converts from a local picker before sending.
- **No SQLite schema migration:** `schedule_id` rides inside the existing `task_json` blob (the `tasks` table stores whole JSON). The `schedules` table is new and created `IF NOT EXISTS`.
- **"Run now" reuses `POST /api/tasks`** (no new endpoint) — it materializes a task from the template with `schedule_id` unset so it doesn't disturb the schedule's overlap accounting.
- **Dashboard UI is React** (`broker/ui/src/`), NOT the deleted `dashboard.html`. Target the real component files listed in each UI task. After UI edits, rebuild `broker/ui/dist` (`pnpm --dir broker/ui build`) and commit the rebuilt bundle so the broker embeds it.
- **Workspace test gate:** after every Rust task, `cargo build -p <crate>` and `cargo test -p <crate>` must pass before committing.

---

## File Structure

**New files:**
- `core/src/schedule.rs` — `TaskTemplate`, `ScheduleKind`, `OverlapPolicy`, `Schedule`, cron `next_fire` helpers.
- `demos/Cargo.toml` — workspace member crate `chopflow_demos`.
- `demos/build.rs` — compiles `../broker/proto/chopflow.proto` (same as worker/cli).
- `demos/src/main.rs` — `chopflow_demo_worker` binary (registration + pull loop + 4 handlers).
- `demos/src/handlers.rs` — the 4 demo handler functions.
- `demos/src/bin/seed.rs` — `chopflow_demo_seed` binary (HTTP seeding).
- `demos/run.sh` — one-shot demo run story.
- `broker/ui/src/components/SchedulesView.tsx` — Schedules table view + filter chips.
- `broker/ui/src/components/ScheduleDrawer.tsx` — schedule detail drawer (Run now / Enable / Delete).
- `broker/tests/schedule_ticker.rs` — ticker + overlap integration tests.

**Modified files:**
- `core/src/lib.rs` — `pub mod schedule;` + re-exports.
- `core/src/task.rs` — `+schedule_id: Option<Uuid>`.
- `core/src/storage.rs` — 6 trait methods + InMemory + SQLite impls + `schedules` table.
- `core/Cargo.toml` — `+cron`, dev `+tokio` (already present).
- `broker/src/lib.rs` — `spawn_schedule_ticker` + reconciliation helper + Schedule RPC impls.
- `broker/src/main.rs` — spawn ticker; call startup reconciliation.
- `broker/src/http.rs` — `/api/schedules*` routes + DTOs + `stats.schedules`.
- `broker/proto/chopflow.proto` — `Schedule` message + 3 RPCs + `Task.schedule_id`.
- `cli/src/main.rs` — `schedule` subcommand.
- `Cargo.toml` (workspace) — `+demos` member, `+cron`/`image`/`nalgebra`/`reqwest` workspace deps.
- `broker/ui/src/lib/api.ts` — `Schedule` types + `schedules` API methods.
- `broker/ui/src/hooks/useChopFlow.ts` — `useSchedules` + schedule mutations.
- `broker/ui/src/components/Sidebar.tsx` — `View` type gains `"schedules"` + nav item.
- `broker/ui/src/App.tsx` — render `SchedulesView` for the new view.
- `broker/ui/src/components/EnqueueDialog.tsx` — Schedule toggle (run-now vs schedule).
- `broker/ui/src/components/TasksView.tsx` — lineage icon on `schedule_id` rows.
- `broker/ui/src/components/TaskDrawer.tsx` — "Spawned by schedule" note.
- `broker/ui/dist/` — rebuilt bundle (committed).
- `README.md` — scheduling + demos sections.

---

### Task 1: Core `schedule` module

**Files:**
- Create: `core/src/schedule.rs`
- Modify: `core/Cargo.toml` (add `cron`), `core/src/lib.rs` (re-export)

**Interfaces:**
- Consumes: `chrono`, `serde`, `uuid`, `serde_json`, `cron` crate.
- Produces: `TaskTemplate`, `ScheduleKind` (`OneShot { eta }`, `Cron { cron }`), `OverlapPolicy` (`Skip`/`Coalesce`/`Allow`), `Schedule`, and free functions `pub fn normalize_cron(expr: &str) -> String`, `pub fn next_fire(kind: &ScheduleKind, after: DateTime<Utc>) -> Result<DateTime<Utc>>`, `pub fn initial_next_fire(kind: &ScheduleKind, now: DateTime<Utc>) -> Result<DateTime<Utc>>`. Later tasks (Storage ticker, HTTP validation) call these.

- [ ] **Step 1: Add the `cron` dependency**

Edit `core/Cargo.toml` dependencies block, add after `rusqlite`:

```toml
cron = "0.12"
```

- [ ] **Step 2: Write the failing tests**

Create `core/src/schedule.rs` with only the test module first:

```rust
/*!
# Schedule module

A `Schedule` is a recurring or one-shot task template that the broker ticker
materializes into `Task`s. See the design spec section 3.
*/

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;
use std::collections::HashMap;

use crate::error::{ChopFlowError, Result};

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
    OneShot { eta: DateTime<Utc> },
    Cron { cron: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OverlapPolicy {
    Skip,
    Coalesce,
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
    pub next_fire: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl Schedule {
    pub fn new(name: String, task_template: TaskTemplate, kind: ScheduleKind, overlap_policy: OverlapPolicy) -> Result<Self> {
        let now = Utc::now();
        let next_fire = initial_next_fire(&kind, now)?;
        Ok(Self {
            id: Uuid::new_v4(),
            name,
            task_template,
            kind,
            overlap_policy,
            enabled: true,
            last_fired: None,
            next_fire,
            created_at: now,
        })
    }
}

/// The `cron` crate expects 6–7 fields (leading seconds). Users write familiar
/// 5-field cron; this prepends "0 " (seconds) when exactly 5 fields are given.
pub fn normalize_cron(expr: &str) -> String {
    let fields = expr.split_whitespace().count();
    if fields == 5 {
        format!("0 {}", expr.trim())
    } else {
        expr.trim().to_string()
    }
}

/// Next fire time strictly after `after` for the given kind.
pub fn next_fire(kind: &ScheduleKind, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
    match kind {
        ScheduleKind::OneShot { eta } => Ok(*eta),
        ScheduleKind::Cron { cron } => {
            let sched = cron::Schedule::from_str(&normalize_cron(cron))
                .map_err(|e| ChopFlowError::Other(anyhow::anyhow!("invalid cron '{}': {}", cron, e)))?;
            sched
                .after(&after)
                .next()
                .ok_or_else(|| ChopFlowError::Other(anyhow::anyhow!("cron '{}' has no future fire", cron)))
        }
    }
}

/// Initial `next_fire` for a freshly created schedule. OneShot → its eta;
/// Cron → next match from `now`.
pub fn initial_next_fire(kind: &ScheduleKind, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    match kind {
        ScheduleKind::OneShot { eta } => Ok(*eta),
        ScheduleKind::Cron { cron } => next_fire(kind, now),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cron_kind(expr: &str) -> ScheduleKind {
        ScheduleKind::Cron { cron: expr.into() }
    }

    #[test]
    fn normalize_cron_prepends_seconds_for_5_fields() {
        assert_eq!(normalize_cron("0 9 * * *"), "0 0 9 * * *");
        assert_eq!(normalize_cron("*/2 * * * *"), "0 */2 * * * *");
        // 6-field left alone
        assert_eq!(normalize_cron("0 0 9 * * *"), "0 0 9 * * *");
    }

    #[test]
    fn cron_next_fire_is_in_the_future_and_advances() {
        let now = Utc::now();
        let kind = cron_kind("*/2 * * * *");
        let first = next_fire(&kind, now).unwrap();
        assert!(first > now);
        let second = next_fire(&kind, first).unwrap();
        assert!(second > first);
        // roughly 2 minutes apart
        let delta = second - first;
        assert!(delta.num_seconds() >= 119 && delta.num_seconds() <= 121);
    }

    #[test]
    fn oneslot_next_fire_is_the_eta() {
        let eta = Utc::now() + chrono::Duration::hours(1);
        let kind = ScheduleKind::OneShot { eta };
        assert_eq!(next_fire(&kind, Utc::now()).unwrap(), eta);
    }

    #[test]
    fn invalid_cron_errors() {
        let kind = cron_kind("not a cron");
        assert!(next_fire(&kind, Utc::now()).is_err());
    }

    #[test]
    fn schedule_new_computes_next_fire() {
        let tmpl = TaskTemplate {
            name: "x".into(),
            payload: serde_json::json!({}),
            tags: vec![],
            resources: HashMap::new(),
            max_retries: 3,
        };
        let s = Schedule::new("n".into(), tmpl, cron_kind("*/5 * * * *"), OverlapPolicy::Skip).unwrap();
        assert!(s.enabled);
        assert!(s.next_fire > Utc::now());
        assert!(s.last_fired.is_none());
    }
}
```

- [ ] **Step 3: Run the tests — verify they fail to compile, then pass**

The module won't be wired into `lib.rs` yet. First add the re-export (Step 4) so the crate compiles, then run:

```bash
cargo test -p chopflow_core schedule
```
Expected: the 5 schedule tests PASS (the types exist in the same file). If `anyhow` is not a direct dep of core it already is (`anyhow.workspace = true`).

- [ ] **Step 4: Re-export the module from `core/src/lib.rs`**

In `core/src/lib.rs`, add `pub mod schedule;` (alphabetical, after `retry`) and re-exports:

```rust
pub mod schedule;
pub use schedule::{Schedule, ScheduleKind, OverlapPolicy, TaskTemplate};
```

- [ ] **Step 5: Build + test the whole core crate**

Run: `cargo build -p chopflow_core && cargo test -p chopflow_core`
Expected: PASS, no warnings about the new module.

- [ ] **Step 6: Commit**

```bash
git add core/src/schedule.rs core/src/lib.rs core/Cargo.toml
git commit -m ":sparkles: Add: Schedule entity + cron next-fire helpers in core

Refs: SCHEDULE-1"
```

---

### Task 2: `Task.schedule_id` field

**Files:**
- Modify: `core/src/task.rs`

**Interfaces:**
- Consumes: `uuid::Uuid` (already imported).
- Produces: `Task.schedule_id: Option<Uuid>` (defaults `None` in `Task::new`). The HTTP `TaskDto`, proto `Task`, and storage JSON all carry it automatically once the field exists (serde derive). Later UI/CLI tasks read it.

- [ ] **Step 1: Write the failing test**

In `core/src/task.rs` `#[cfg(test)] mod tests`, add:

```rust
#[test]
fn new_task_has_no_schedule_id() {
    let task = Task::new("train".into(), serde_json::json!({}));
    assert!(task.schedule_id.is_none());
}

#[test]
fn schedule_id_round_trips_through_serde() {
    let mut task = Task::new("train".into(), serde_json::json!({}));
    let sid = Uuid::new_v4();
    task.schedule_id = Some(sid);
    let json = serde_json::to_string(&task).unwrap();
    let back: Task = serde_json::from_str(&json).unwrap();
    assert_eq!(back.schedule_id, Some(sid));
}
```

- [ ] **Step 2: Run — verify failure**

Run: `cargo test -p chopflow_core task::tests`
Expected: FAIL — `no field schedule_id`.

- [ ] **Step 3: Add the field**

In the `Task` struct in `core/src/task.rs`, add after the `result` field:

```rust
    /// If this task was spawned by a schedule, the schedule's id. `None` for
    /// ad-hoc enqueues. The ticker sets it; the overlap check queries by it.
    pub schedule_id: Option<Uuid>,
```

In `Task::new`, add to the struct literal:

```rust
            schedule_id: None,
```

- [ ] **Step 4: Run — verify pass**

Run: `cargo test -p chopflow_core`
Expected: PASS (all core tests, including the storage round-trip tests which serialize tasks).

- [ ] **Step 5: Commit**

```bash
git add core/src/task.rs
git commit -m ":sparkles: Add: schedule_id field on Task for schedule lineage

Refs: SCHEDULE-1"
```

---

### Task 3: Storage trait schedule methods — InMemory

**Files:**
- Modify: `core/src/storage.rs`

**Interfaces:**
- Consumes: `crate::schedule::{Schedule, ScheduleKind, OverlapPolicy}` (from Task 1), `crate::task::Task` (now with `schedule_id`, Task 2), `chrono::Utc`.
- Produces: 6 new `Storage` trait methods. The broker ticker (Task 5) and HTTP layer (Task 6) call them.

- [ ] **Step 1: Write the failing tests**

In `core/src/storage.rs` `#[cfg(test)] mod tests`, add a helper and tests:

```rust
    use crate::schedule::{OverlapPolicy, Schedule, ScheduleKind, TaskTemplate};
    use std::collections::HashMap;

    fn tmpl(name: &str) -> TaskTemplate {
        TaskTemplate {
            name: name.into(),
            payload: serde_json::json!({}),
            tags: vec![],
            resources: HashMap::new(),
            max_retries: 3,
        }
    }

    fn cron_schedule(name: &str, cron: &str) -> Schedule {
        Schedule::new(name.into(), tmpl(name), ScheduleKind::Cron { cron: cron.into() }, OverlapPolicy::Skip).unwrap()
    }

    #[tokio::test]
    async fn in_memory_schedule_crud() {
        let s = InMemoryStorage::new();
        let sch = cron_schedule("nightly", "0 9 * * *");
        s.insert_schedule(sch.clone()).await.unwrap();
        assert_eq!(s.get_schedule(&sch.id).await.unwrap().unwrap().name, "nightly");

        let listed = s.list_schedules().await.unwrap();
        assert_eq!(listed.len(), 1);

        let mut updated = sch.clone();
        updated.enabled = false;
        s.update_schedule(updated.clone()).await.unwrap();
        assert_eq!(s.get_schedule(&sch.id).await.unwrap().unwrap().enabled, false);

        s.delete_schedule(&sch.id).await.unwrap();
        assert!(s.get_schedule(&sch.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn in_memory_due_schedules_respects_next_fire_and_enabled() {
        let s = InMemoryStorage::new();
        let now = Utc::now();

        // due: enabled, next_fire in the past
        let mut due = cron_schedule("due", "*/2 * * * *");
        due.next_fire = now - chrono::Duration::minutes(1);
        s.insert_schedule(due.clone()).await.unwrap();

        // not due: next_fire in the future
        let mut future = cron_schedule("future", "*/2 * * * *");
        future.next_fire = now + chrono::Duration::hours(1);
        s.insert_schedule(future).await.unwrap();

        // not due: disabled even though next_fire is past
        let mut disabled = cron_schedule("disabled", "*/2 * * * *");
        disabled.next_fire = now - chrono::Duration::minutes(1);
        disabled.enabled = false;
        s.insert_schedule(disabled).await.unwrap();

        let got = s.due_schedules(now).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "due");
    }

    #[tokio::test]
    async fn in_flight_for_schedule_returns_queued_and_running_only() {
        let s = InMemoryStorage::new();
        let sch = cron_schedule("s", "*/2 * * * *");
        s.insert_schedule(sch.clone()).await.unwrap();

        let mut queued = Task::new("a".into(), serde_json::json!({}));
        queued.status = TaskStatus::Queued;
        queued.schedule_id = Some(sch.id);
        s.insert(queued).await.unwrap();

        let mut running = Task::new("b".into(), serde_json::json!({}));
        running.status = TaskStatus::Running;
        running.schedule_id = Some(sch.id);
        s.insert(running).await.unwrap();

        let mut done = Task::new("c".into(), serde_json::json!({}));
        done.status = TaskStatus::Completed;
        done.schedule_id = Some(sch.id);
        s.insert(done).await.unwrap();

        let in_flight = s.in_flight_for_schedule(&sch.id).await.unwrap();
        assert_eq!(in_flight.len(), 2); // queued + running, not completed
    }
```

- [ ] **Step 2: Run — verify failure**

Run: `cargo test -p chopflow_core in_memory_schedule`
Expected: FAIL — methods not on trait / `InMemoryStorage`.

- [ ] **Step 3: Extend the `Storage` trait**

In `core/src/storage.rs`, add imports at the top:

```rust
use crate::schedule::Schedule;
```

Add these 6 methods to the `Storage` trait (after `reconcile`):

```rust
    /// Insert or replace a schedule (upsert keyed by id).
    async fn insert_schedule(&self, schedule: Schedule) -> Result<()>;

    /// Get a schedule by id.
    async fn get_schedule(&self, id: &Uuid) -> Result<Option<Schedule>>;

    /// List all schedules, ordered by created_at.
    async fn list_schedules(&self) -> Result<Vec<Schedule>>;

    /// Delete a schedule by id.
    async fn delete_schedule(&self, id: &Uuid) -> Result<()>;

    /// Update a schedule (upsert).
    async fn update_schedule(&self, schedule: Schedule) -> Result<()>;

    /// Enabled schedules whose `next_fire <= now`, for the ticker.
    async fn due_schedules(&self, now: chrono::DateTime<chrono::Utc>) -> Result<Vec<Schedule>>;

    /// Tasks spawned by `schedule_id` still in flight (Queued or Running).
    async fn in_flight_for_schedule(&self, schedule_id: &Uuid) -> Result<Vec<Task>>;
```

- [ ] **Step 4: Implement on `InMemoryStorage`**

Add a schedules map to the struct:

```rust
pub struct InMemoryStorage {
    tasks: Arc<Mutex<HashMap<Uuid, Task>>>,
    schedules: Arc<Mutex<HashMap<Uuid, Schedule>>>,
}
```

Update `InMemoryStorage::new` and `default` to also init `schedules: Arc::new(Mutex::new(HashMap::new()))`.

Add the impl block methods:

```rust
    async fn insert_schedule(&self, schedule: Schedule) -> Result<()> {
        let mut scheds = self.schedules.lock().await;
        scheds.insert(schedule.id, schedule);
        Ok(())
    }

    async fn get_schedule(&self, id: &Uuid) -> Result<Option<Schedule>> {
        let scheds = self.schedules.lock().await;
        Ok(scheds.get(id).cloned())
    }

    async fn list_schedules(&self) -> Result<Vec<Schedule>> {
        let scheds = self.schedules.lock().await;
        let mut all: Vec<Schedule> = scheds.values().cloned().collect();
        all.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        Ok(all)
    }

    async fn delete_schedule(&self, id: &Uuid) -> Result<()> {
        let mut scheds = self.schedules.lock().await;
        scheds.remove(id);
        Ok(())
    }

    async fn update_schedule(&self, schedule: Schedule) -> Result<()> {
        let mut scheds = self.schedules.lock().await;
        scheds.insert(schedule.id, schedule);
        Ok(())
    }

    async fn due_schedules(&self, now: chrono::DateTime<chrono::Utc>) -> Result<Vec<Schedule>> {
        let scheds = self.schedules.lock().await;
        let mut due: Vec<Schedule> = scheds
            .values()
            .filter(|s| s.enabled && s.next_fire <= now)
            .cloned()
            .collect();
        due.sort_by_key(|s| s.next_fire);
        Ok(due)
    }

    async fn in_flight_for_schedule(&self, schedule_id: &Uuid) -> Result<Vec<Task>> {
        let tasks = self.tasks.lock().await;
        Ok(tasks
            .values()
            .filter(|t| t.schedule_id == Some(*schedule_id)
                && matches!(t.status, TaskStatus::Queued | TaskStatus::Running))
            .cloned()
            .collect())
    }
```

- [ ] **Step 5: Run — verify pass**

Run: `cargo test -p chopflow_core in_memory_schedule && cargo test -p chopflow_core in_flight_for_schedule`
Expected: PASS.

- [ ] **Step 6: Build the whole core crate (SqliteStorage will not yet impl the new methods — expect a compile error there)**

This is expected: the trait now has 6 methods `SqliteStorage` doesn't implement. That is Task 4. Do NOT commit yet. Proceed directly to Task 4; commit together at the end of Task 4 so the workspace compiles.

> **Note:** Tasks 3 and 4 are committed together (Task 4 Step 6) because the `Storage` trait change must be implemented by both backends for the crate to compile.

---

### Task 4: Storage trait schedule methods — SQLite

**Files:**
- Modify: `core/src/storage.rs`

**Interfaces:**
- Consumes: same trait additions from Task 3.
- Produces: `SqliteStorage` impl of the 6 methods + the `schedules` table.

- [ ] **Step 1: Write the failing tests**

In `core/src/storage.rs` test module, add SQLite mirrors of the in-memory schedule tests:

```rust
    #[tokio::test]
    async fn sqlite_schedule_crud() {
        let s = SqliteStorage::open_in_memory().unwrap();
        let sch = cron_schedule("nightly", "0 9 * * *");
        s.insert_schedule(sch.clone()).await.unwrap();
        assert_eq!(s.get_schedule(&sch.id).await.unwrap().unwrap().name, "nightly");
        assert_eq!(s.list_schedules().await.unwrap().len(), 1);

        let mut updated = sch.clone();
        updated.enabled = false;
        s.update_schedule(updated).await.unwrap();
        assert_eq!(s.get_schedule(&sch.id).await.unwrap().unwrap().enabled, false);

        s.delete_schedule(&sch.id).await.unwrap();
        assert!(s.get_schedule(&sch.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn sqlite_due_schedules_and_in_flight() {
        let s = SqliteStorage::open_in_memory().unwrap();
        let now = Utc::now();
        let mut due = cron_schedule("due", "*/2 * * * *");
        due.next_fire = now - chrono::Duration::minutes(1);
        s.insert_schedule(due.clone()).await.unwrap();

        let mut future = cron_schedule("future", "*/2 * * * *");
        future.next_fire = now + chrono::Duration::hours(1);
        s.insert_schedule(future).await.unwrap();

        let got = s.due_schedules(now).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "due");

        // in-flight tasks for the schedule
        let mut t = Task::new("a".into(), serde_json::json!({}));
        t.status = TaskStatus::Queued;
        t.schedule_id = Some(due.id);
        s.insert(t).await.unwrap();
        assert_eq!(s.in_flight_for_schedule(&due.id).await.unwrap().len(), 1);
    }
```

- [ ] **Step 2: Run — verify failure**

Run: `cargo build -p chopflow_core`
Expected: FAIL — `SqliteStorage` does not implement the 6 new trait methods.

- [ ] **Step 3: Add the `schedules` table**

In `SqliteStorage::open` and `SqliteStorage::open_in_memory`, append to the `execute_batch` SQL string (after the `tasks` indexes):

```sql
             CREATE TABLE IF NOT EXISTS schedules (
                 id              TEXT PRIMARY KEY,
                 next_fire_ms    INTEGER NOT NULL,
                 enabled         INTEGER NOT NULL,
                 schedule_json   TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS schedules_next_fire ON schedules(next_fire_ms);
```

- [ ] **Step 4: Add marshal helpers + implement the 6 methods on `SqliteStorage`**

Add inside the `impl SqliteStorage` block:

```rust
    fn marshal_schedule(s: &Schedule) -> Result<(String, i64, i64, String)> {
        let id = s.id.to_string();
        let next_fire_ms = s.next_fire.timestamp_millis();
        let enabled = if s.enabled { 1i64 } else { 0 };
        let json = serde_json::to_string(s).map_err(|e| {
            ChopFlowError::SerializationError(format!("failed to serialize schedule: {}", e))
        })?;
        Ok((id, next_fire_ms, enabled, json))
    }
```

Add the trait impl methods to `impl Storage for SqliteStorage`:

```rust
    async fn insert_schedule(&self, schedule: Schedule) -> Result<()> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let (id, next_fire_ms, enabled, json) = Self::marshal_schedule(&schedule)?;
            let conn = lock_conn(&conn)?;
            conn.execute(
                "INSERT OR REPLACE INTO schedules (id, next_fire_ms, enabled, schedule_json) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![id, next_fire_ms, enabled, json],
            ).map_err(rusqlite_err)?;
            Ok(())
        }).await.map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(())
    }

    async fn get_schedule(&self, id: &Uuid) -> Result<Option<Schedule>> {
        let conn = self.conn.clone();
        let id_str = id.to_string();
        let sch = tokio::task::spawn_blocking(move || -> Result<Option<Schedule>> {
            let conn = lock_conn(&conn)?;
            let mut stmt = conn.prepare("SELECT schedule_json FROM schedules WHERE id = ?1").map_err(rusqlite_err)?;
            let mut rows = stmt.query(rusqlite::params![id_str]).map_err(rusqlite_err)?;
            match rows.next().map_err(rusqlite_err)? {
                Some(row) => {
                    let json: String = row.get(0)?;
                    let s: Schedule = serde_json::from_str(&json).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?;
                    Ok(Some(s))
                }
                None => Ok(None),
            }
        }).await.map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(sch)
    }

    async fn list_schedules(&self) -> Result<Vec<Schedule>> {
        let conn = self.conn.clone();
        let schs = tokio::task::spawn_blocking(move || -> Result<Vec<Schedule>> {
            let conn = lock_conn(&conn)?;
            let mut stmt = conn.prepare("SELECT schedule_json FROM schedules ORDER BY rowid").map_err(rusqlite_err)?;
            let rows: rusqlite::Result<Vec<Schedule>> = stmt.query_map([], |row| {
                let json: String = row.get(0)?;
                serde_json::from_str(&json).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))
            }).map_err(rusqlite_err)?.collect();
            Ok(rows.map_err(rusqlite_err)?)
        }).await.map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(schs)
    }

    async fn delete_schedule(&self, id: &Uuid) -> Result<()> {
        let conn = self.conn.clone();
        let id_str = id.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = lock_conn(&conn)?;
            conn.execute("DELETE FROM schedules WHERE id = ?1", rusqlite::params![id_str]).map_err(rusqlite_err)?;
            Ok(())
        }).await.map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(())
    }

    async fn update_schedule(&self, schedule: Schedule) -> Result<()> {
        // Same as insert (upsert).
        self.insert_schedule(schedule).await
    }

    async fn due_schedules(&self, now: chrono::DateTime<chrono::Utc>) -> Result<Vec<Schedule>> {
        let conn = self.conn.clone();
        let now_ms = now.timestamp_millis();
        let schs = tokio::task::spawn_blocking(move || -> Result<Vec<Schedule>> {
            let conn = lock_conn(&conn)?;
            let mut stmt = conn.prepare(
                "SELECT schedule_json FROM schedules WHERE enabled = 1 AND next_fire_ms <= ?1 ORDER BY next_fire_ms"
            ).map_err(rusqlite_err)?;
            let rows: rusqlite::Result<Vec<Schedule>> = stmt.query_map(rusqlite::params![now_ms], |row| {
                let json: String = row.get(0)?;
                serde_json::from_str(&json).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))
            }).map_err(rusqlite_err)?.collect();
            Ok(rows.map_err(rusqlite_err)?)
        }).await.map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(schs)
    }

    async fn in_flight_for_schedule(&self, schedule_id: &Uuid) -> Result<Vec<Task>> {
        // No dedicated schedule_id column; scan task_json (acceptable at
        // ChopFlow's scale — spec section 4). Filter to Queued/Running in Rust.
        let conn = self.conn.clone();
        let target = schedule_id.to_string();
        let tasks = tokio::task::spawn_blocking(move || -> Result<Vec<Task>> {
            let conn = lock_conn(&conn)?;
            let mut stmt = conn.prepare(
                "SELECT task_json FROM tasks WHERE status = ?1 OR status = ?2"
            ).map_err(rusqlite_err)?;
            let rows: rusqlite::Result<Vec<Task>> = stmt.query_map(
                rusqlite::params![TaskStatus::Queued as i64, TaskStatus::Running as i64],
                |row| {
                    let json: String = row.get(0)?;
                    serde_json::from_str(&json).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))
                },
            ).map_err(rusqlite_err)?.collect();
            let all: Vec<Task> = rows.map_err(rusqlite_err)?;
            Ok(all.into_iter().filter(|t| t.schedule_id.as_ref().map(|s| s.to_string()) == Some(target.clone())).collect())
        }).await.map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(tasks)
    }
```

- [ ] **Step 5: Run — verify pass**

Run: `cargo test -p chopflow_core`
Expected: all core tests PASS (including both backends' schedule tests + existing task tests).

- [ ] **Step 6: Commit (Tasks 3 + 4 together)**

```bash
git add core/src/storage.rs
git commit -m ":sparkles: Add: schedule storage methods for InMemory + SQLite

Adds the schedules table + 6 Storage trait methods (insert/get/list/delete/
update/due_schedules/in_flight_for_schedule) on both backends.

Refs: SCHEDULE-1"
```

---

### Task 5: Broker schedule ticker + startup reconciliation

**Files:**
- Modify: `broker/src/lib.rs`, `broker/src/main.rs`

**Interfaces:**
- Consumes: `chopflow_core::schedule::{Schedule, ScheduleKind, next_fire}` (Task 1), the 6 storage methods (Tasks 3–4), `Task::new` + `schedule_id` (Tasks 1–2).
- Produces: `ChopFlowBrokerService::spawn_schedule_ticker(&self)` and `pub async fn reconcile_schedules(storage: &dyn Storage) -> Result<()>`. `main.rs` calls both.

- [ ] **Step 1: Write the failing integration test**

Create `broker/tests/schedule_ticker.rs`:

```rust
use chopflow_broker::{ChopFlowBrokerService, BrokerState};
use chopflow_core::storage::Storage;
use chopflow_core::schedule::{Schedule, ScheduleKind, OverlapPolicy, TaskTemplate};
use chopflow_core::task::TaskStatus;
use std::collections::HashMap;

fn tmpl(name: &str) -> TaskTemplate {
    TaskTemplate { name: name.into(), payload: serde_json::json!({}), tags: vec![], resources: HashMap::new(), max_retries: 3 }
}

#[tokio::test]
async fn ticker_materializes_task_from_cron_schedule() {
    let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::new(storage.clone());
    let service = ChopFlowBrokerService::from_state(state);

    // A cron schedule due now (every second).
    let mut sch = Schedule::new("every-sec".into(), tmpl("ping"), ScheduleKind::Cron { cron: "* * * * * *".into() }, OverlapPolicy::Allow).unwrap();
    sch.next_fire = chrono::Utc::now() - chrono::Duration::seconds(1);
    storage.insert_schedule(sch.clone()).await.unwrap();

    // Run one tick.
    service.tick_once().await.unwrap();

    // A task should have been materialized with schedule_id set + advanced next_fire.
    let in_flight = storage.in_flight_for_schedule(&sch.id).await.unwrap();
    assert_eq!(in_flight.len(), 1);
    assert_eq!(in_flight[0].name, "ping");
    assert_eq!(in_flight[0].schedule_id, Some(sch.id));
    let updated = storage.get_schedule(&sch.id).await.unwrap().unwrap();
    assert!(updated.next_fire > chrono::Utc::now());
    assert!(updated.last_fired.is_some());
}

#[tokio::test]
async fn ticker_oneshot_self_disables_after_fire() {
    let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::new(storage.clone());
    let service = ChopFlowBrokerService::from_state(state);

    let eta = chrono::Utc::now() - chrono::Duration::minutes(1);
    let sch = Schedule::new("once".into(), tmpl("ping"), ScheduleKind::OneShot { eta }, OverlapPolicy::Allow).unwrap();
    let id = sch.id;
    storage.insert_schedule(sch).await.unwrap();

    service.tick_once().await.unwrap();

    let updated = storage.get_schedule(&id).await.unwrap().unwrap();
    assert!(!updated.enabled, "one-shot should self-disable");
    assert_eq!(storage.in_flight_for_schedule(&id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn ticker_overlap_skip_skips_when_in_flight() {
    let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::new(storage.clone());
    let service = ChopFlowBrokerService::from_state(state);

    let mut sch = Schedule::new("skipper".into(), tmpl("ping"), ScheduleKind::Cron { cron: "* * * * * *".into() }, OverlapPolicy::Skip).unwrap();
    sch.next_fire = chrono::Utc::now() - chrono::Duration::seconds(1);
    let id = sch.id;
    storage.insert_schedule(sch).await.unwrap();

    // Pre-place a running task for this schedule.
    let mut t = chopflow_core::task::Task::new("ping".into(), serde_json::json!({}));
    t.status = TaskStatus::Running;
    t.schedule_id = Some(id);
    storage.insert(t).await.unwrap();

    service.tick_once().await.unwrap();

    // Only the pre-existing task — no new one materialized.
    assert_eq!(storage.in_flight_for_schedule(&id).await.unwrap().len(), 1);
}
```

- [ ] **Step 2: Run — verify failure**

Run: `cargo test -p chopflow_broker --test schedule_ticker`
Expected: FAIL — no `tick_once` method.

- [ ] **Step 3: Implement the ticker in `broker/src/lib.rs`**

Add imports near the top of `broker/src/lib.rs`:

```rust
use chopflow_core::schedule::{next_fire, Schedule, ScheduleKind, OverlapPolicy};
```

Add to `impl ChopFlowBrokerService` (alongside `spawn_timeout_monitor`):

```rust
    /// Spawn the background schedule ticker. Every second it materializes a
    /// Task for each due schedule (subject to the overlap policy) and advances
    /// the schedule. A single bad schedule is logged and skipped.
    pub fn spawn_schedule_ticker(self) {
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = service.tick_once().await {
                    warn!("schedule ticker iteration failed: {}", e);
                }
                sleep(Duration::from_secs(1)).await;
            }
        });
    }

    /// One ticker iteration. Exposed for tests so they don't need to sleep.
    pub async fn tick_once(&self) -> chopflow_core::Result<()> {
        let now = chrono::Utc::now();
        let due = self.state.storage.due_schedules(now).await?;

        for mut schedule in due {
            // Overlap check (skip/coalesce skip when any in-flight task exists).
            if schedule.overlap_policy != OverlapPolicy::Allow {
                let in_flight = self.state.storage.in_flight_for_schedule(&schedule.id).await?;
                if !in_flight.is_empty() {
                    // Skip the fire but still advance next_fire for cron so the
                    // next match is computed. OneShot is left to retry next tick.
                    if let ScheduleKind::Cron { .. } = &schedule.kind {
                        if let Ok(nf) = next_fire(&schedule.kind, now) {
                            schedule.next_fire = nf;
                        }
                    }
                    if let Err(e) = self.state.storage.update_schedule(schedule).await {
                        warn!("failed to update skipped schedule {}: {}", schedule.id, e);
                    }
                    continue;
                }
            }

            // Materialize a Task from the template.
            let t = &schedule.task_template;
            let mut task = Task::new(t.name.clone(), t.payload.clone())
                .with_tags(t.tags.clone());
            for (k, v) in &t.resources {
                task = task.with_resource(k.clone(), *v);
            }
            if t.max_retries > 0 {
                task = task.with_max_retries(t.max_retries);
            }
            task.schedule_id = Some(schedule.id);
            task.status = TaskStatus::Queued;

            if let Err(e) = self.state.storage.insert(task).await {
                warn!("failed to insert materialized task for schedule {}: {}", schedule.id, e);
                continue;
            }

            // Advance / disable the schedule.
            schedule.last_fired = Some(now);
            match &schedule.kind {
                ScheduleKind::OneShot { .. } => {
                    schedule.enabled = false;
                }
                ScheduleKind::Cron { .. } => {
                    if let Ok(nf) = next_fire(&schedule.kind, now) {
                        schedule.next_fire = nf;
                    }
                }
            }
            if let Err(e) = self.state.storage.update_schedule(schedule).await {
                warn!("failed to advance schedule {}: {}", schedule.id, e);
            }
        }
        Ok(())
    }
```

Add the startup reconciliation free function (after `serve_with_listener` or near the top-level fns):

```rust
/// Recompute `next_fire` for enabled cron schedules from `now` (no backfill of
/// missed runs). One-shot schedules with a past eta are left enabled so the
/// ticker fires them on the first tick. Run once at broker startup.
pub async fn reconcile_schedules(storage: &dyn Storage) -> chopflow_core::Result<()> {
    let now = chrono::Utc::now();
    let schedules = storage.list_schedules().await?;
    for mut s in schedules {
        if !s.enabled {
            continue;
        }
        if let ScheduleKind::Cron { .. } = &s.kind {
            if let Ok(nf) = next_fire(&s.kind, now) {
                s.next_fire = nf;
                if let Err(e) = storage.update_schedule(s).await {
                    warn!("failed to reconcile schedule: {}", e);
                }
            }
        }
        // OneShot: leave next_fire as the eta; if past, ticker fires it.
    }
    Ok(())
}
```

- [ ] **Step 4: Wire the ticker + reconciliation into `broker/src/main.rs`**

In `broker/src/main.rs`, after `service.spawn_timeout_monitor();` add:

```rust
    // Recompute schedule next_fire times (skip missed cron runs), then spawn
    // the 1s schedule ticker.
    if let Err(e) = chopflow_broker::reconcile_schedules(storage.as_ref()).await {
        tracing::warn!("schedule reconcile failed: {}", e);
    }
    service.spawn_schedule_ticker();
```

- [ ] **Step 5: Run — verify pass**

Run: `cargo test -p chopflow_broker --test schedule_ticker`
Expected: the 3 ticker tests PASS.

- [ ] **Step 6: Build the whole broker crate**

Run: `cargo build -p chopflow_broker`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add broker/src/lib.rs broker/src/main.rs broker/tests/schedule_ticker.rs
git commit -m ":sparkles: Add: broker schedule ticker + startup reconciliation

Ticker materializes tasks from due schedules with overlap-skip; one-shots
self-disable; cron advances next_fire. Reconciliation skips missed runs.

Refs: SCHEDULE-1"
```

---

### Task 6: HTTP `/api/schedules*` routes + `stats.schedules`

**Files:**
- Modify: `broker/src/http.rs`

**Interfaces:**
- Consumes: `chopflow_core::schedule::{Schedule, ScheduleKind, OverlapPolicy, TaskTemplate, next_fire, normalize_cron}` (Task 1), the storage schedule methods (Tasks 3–4), `BrokerState`.
- Produces: `GET/POST /api/schedules`, `GET/PATCH/DELETE /api/schedules/:id`, and `schedules` added to `GET /api/stats`'s `StatsDto`.

- [ ] **Step 1: Write the failing integration test**

Append to a new `broker/tests/http_schedules.rs`:

```rust
use chopflow_broker::http::router;
use chopflow_broker::BrokerState;
use chopflow_core::storage::Storage;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

async fn app() -> (axum::Router, std::sync::Arc<dyn Storage>) {
    let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::new(storage.clone());
    (router(state), storage)
}

#[tokio::test]
async fn create_list_get_delete_schedule_over_http() {
    let (app, _storage) = app().await;
    let body = r#"{"name":"nightly","task_template":{"name":"email","payload":{},"tags":["notif"],"resources":{},"max_retries":3},"kind":{"type":"cron","cron":"0 9 * * *"},"overlap_policy":"skip"}"#;
    let resp = app
        .clone()
        .oneshot(Request::builder().method("POST").uri("/api/schedules").header("content-type", "application/json").body(Body::from(body)).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // List
    let list = app.clone().oneshot(Request::builder().uri("/api/schedules").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(list.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(v.as_array().unwrap().len() >= 1);

    // Stats now include schedules
    let stats = app.clone().oneshot(Request::builder().uri("/api/stats").body(Body::empty()).unwrap()).await.unwrap();
    let sb = axum::body::to_bytes(stats.into_body(), usize::MAX).await.unwrap();
    let sv: serde_json::Value = serde_json::from_slice(&sb).unwrap();
    assert!(sv.get("schedules").is_some());
}

#[tokio::test]
async fn invalid_cron_returns_400() {
    let (app, _storage) = app().await;
    let body = r#"{"name":"bad","task_template":{"name":"x","payload":{},"tags":[],"resources":{},"max_retries":1},"kind":{"type":"cron","cron":"not cron"},"overlap_policy":"allow"}"#;
    let resp = app.oneshot(Request::builder().method("POST").uri("/api/schedules").header("content-type", "application/json").body(Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
```

- [ ] **Step 2: Run — verify failure**

Run: `cargo test -p chopflow_broker --test http_schedules`
Expected: FAIL — no `/api/schedules` route (404 / no `schedules` in stats).

- [ ] **Step 3: Add DTOs + request bodies to `broker/src/http.rs`**

Add imports:

```rust
use chopflow_core::schedule::{Schedule, ScheduleKind, OverlapPolicy, TaskTemplate, next_fire};
```

Add DTOs (mirror `TaskDto`'s lowercase-string approach for enums):

```rust
#[derive(Debug, Serialize)]
struct ScheduleDto {
    id: String,
    name: String,
    task_template: TaskTemplate,
    kind: ScheduleKindDto,
    overlap_policy: String,
    enabled: bool,
    last_fired: Option<i64>,
    next_fire: i64,
    created_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ScheduleKindDto {
    Cron { cron: String },
    Oneshot { eta: String }, // RFC3339
}

impl TryFrom<ScheduleKindDto> for ScheduleKind {
    type Error = String;
    fn try_from(d: ScheduleKindDto) -> std::result::Result<Self, Self::Error> {
        match d {
            ScheduleKindDto::Cron { cron } => {
                // validate by parsing
                let _ = next_fire(&ScheduleKind::Cron { cron: cron.clone() }, chrono::Utc::now())
                    .map_err(|e| format!("invalid cron: {}", e))?;
                Ok(ScheduleKind::Cron { cron })
            }
            ScheduleKindDto::Oneshot { eta } => {
                let dt = chrono::DateTime::parse_from_rfc3339(&eta)
                    .map_err(|e| format!("invalid eta: {}", e))?
                    .with_timezone(&chrono::Utc);
                Ok(ScheduleKind::OneShot { eta: dt })
            }
        }
    }
}

impl From<Schedule> for ScheduleDto {
    fn from(s: Schedule) -> Self {
        let kind = match s.kind {
            ScheduleKind::Cron { cron } => ScheduleKindDto::Cron { cron },
            ScheduleKind::OneShot { eta } => ScheduleKindDto::Oneshot { eta: eta.to_rfc3339() },
        };
        Self {
            id: s.id.to_string(),
            name: s.name,
            task_template: s.task_template,
            overlap_policy: match s.overlap_policy { OverlapPolicy::Skip => "skip", OverlapPolicy::Coalesce => "coalesce", OverlapPolicy::Allow => "allow" }.to_string(),
            enabled: s.enabled,
            last_fired: s.last_fired.map(|t| t.timestamp_millis()),
            next_fire: s.next_fire.timestamp_millis(),
            created_at: s.created_at.timestamp_millis(),
            kind,
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateScheduleBody {
    name: String,
    task_template: TaskTemplate,
    kind: ScheduleKindDto,
    #[serde(default = "default_overlap")]
    overlap_policy: String,
}
fn default_overlap() -> String { "skip".into() }

#[derive(Debug, Deserialize)]
struct PatchScheduleBody {
    enabled: Option<bool>,
    overlap_policy: Option<String>,
    cron: Option<String>,
}

#[derive(Debug, Serialize)]
struct ScheduleIdResponse { schedule_id: String }
#[derive(Debug, Serialize)]
struct SuccessResponse { success: bool }
```

- [ ] **Step 4: Add `schedules` to `StatsDto` + the `stats` handler**

In the `StatsDto` struct add `schedules: usize,`. In the `stats` handler, before constructing the DTO:

```rust
    let schedules = state.storage.list_schedules().await.map_err(|e| internal(e))?.iter().filter(|s| s.enabled).count();
```

and add `schedules,` to the `StatsDto { .. }` literal.

- [ ] **Step 5: Add routes + handlers**

In `router`, extend the `api` router:

```rust
        .route("/schedules", get(list_schedules).post(create_schedule))
        .route("/schedules/:id", get(get_schedule).patch(patch_schedule).delete(delete_schedule))
```

Add the handlers:

```rust
async fn list_schedules(State(state): SharedState) -> Result<Json<Vec<ScheduleDto>>, Json<ErrorResponse>> {
    let schs = state.storage.list_schedules().await.map_err(|e| internal(e))?;
    Ok(Json(schs.into_iter().map(ScheduleDto::from).collect()))
}

async fn get_schedule(State(state): SharedState, Path(id): Path<String>) -> Result<Json<ScheduleDto>, Json<ErrorResponse>> {
    let uuid = Uuid::parse_str(&id).map_err(|_| bad("Invalid schedule ID format"))?;
    let s = state.storage.get_schedule(&uuid).await.map_err(|e| internal(e))?
        .ok_or_else(|| not_found(&format!("Schedule not found: {id}")))?;
    Ok(Json(ScheduleDto::from(s)))
}

fn parse_overlap(s: &str) -> Result<OverlapPolicy, Json<ErrorResponse>> {
    match s {
        "skip" => Ok(OverlapPolicy::Skip),
        "coalesce" => Ok(OverlapPolicy::Coalesce),
        "allow" => Ok(OverlapPolicy::Allow),
        other => Err(bad(&format!("invalid overlap_policy: {} (skip|coalesce|allow)", other))),
    }
}

async fn create_schedule(
    State(state): SharedState,
    Json(body): Json<CreateScheduleBody>,
) -> Result<(StatusCode, Json<ScheduleIdResponse>), Json<ErrorResponse>> {
    let kind: ScheduleKind = body.kind.try_into().map_err(bad)?;
    let overlap = parse_overlap(&body.overlap_policy)?;
    let schedule = Schedule::new(body.name, body.task_template, kind, overlap).map_err(|e| bad(&e.to_string()))?;
    let id = schedule.id;
    state.storage.insert_schedule(schedule).await.map_err(|e| internal(e))?;
    Ok((StatusCode::CREATED, Json(ScheduleIdResponse { schedule_id: id.to_string() })))
}

async fn patch_schedule(
    State(state): SharedState,
    Path(id): Path<String>,
    Json(body): Json<PatchScheduleBody>,
) -> Result<Json<ScheduleDto>, Json<ErrorResponse>> {
    let uuid = Uuid::parse_str(&id).map_err(|_| bad("Invalid schedule ID format"))?;
    let mut s = state.storage.get_schedule(&uuid).await.map_err(|e| internal(e))?
        .ok_or_else(|| not_found(&format!("Schedule not found: {id}")))?;
    if let Some(enabled) = body.enabled { s.enabled = enabled; }
    if let Some(op) = body.overlap_policy { s.overlap_policy = parse_overlap(&op)?; }
    if let Some(cron) = body.cron {
        let kind = ScheduleKind::Cron { cron: cron.clone() };
        let _ = next_fire(&kind, chrono::Utc::now()).map_err(|e| bad(&format!("invalid cron: {}", e)))?;
        s.kind = kind;
        s.next_fire = chopflow_core::schedule::next_fire(&s.kind, chrono::Utc::now()).map_err(|e| bad(&e.to_string()))?;
    }
    state.storage.update_schedule(s.clone()).await.map_err(|e| internal(e))?;
    Ok(Json(ScheduleDto::from(s)))
}

async fn delete_schedule(State(state): SharedState, Path(id): Path<String>) -> Result<Json<SuccessResponse>, Json<ErrorResponse>> {
    let uuid = Uuid::parse_str(&id).map_err(|_| bad("Invalid schedule ID format"))?;
    state.storage.delete_schedule(&uuid).await.map_err(|e| internal(e))?;
    Ok(Json(SuccessResponse { success: true }))
}
```

- [ ] **Step 6: Run — verify pass**

Run: `cargo test -p chopflow_broker --test http_schedules`
Expected: PASS.

- [ ] **Step 7: Build + run all broker tests**

Run: `cargo test -p chopflow_broker`
Expected: PASS (existing grpc_flow + new schedule_ticker + http_schedules).

- [ ] **Step 8: Commit**

```bash
git add broker/src/http.rs broker/tests/http_schedules.rs
git commit -m ":sparkles: Add: HTTP /api/schedules CRUD + schedules count in stats

Refs: SCHEDULE-1"
```

---

### Task 7: gRPC Schedule RPCs

**Files:**
- Modify: `broker/proto/chopflow.proto`, `broker/src/lib.rs`

**Interfaces:**
- Consumes: core schedule types (Task 1).
- Produces: `CreateSchedule` / `ListSchedules` / `DeleteSchedule` RPCs on `ChopFlowBroker`; `Task.schedule_id` field on the proto `Task`.

- [ ] **Step 1: Edit the proto**

In `broker/proto/chopflow.proto`, add to the `ChopFlowBroker` service:

```proto
  rpc CreateSchedule (CreateScheduleRequest) returns (CreateScheduleResponse);
  rpc ListSchedules (ListSchedulesRequest) returns (ListSchedulesResponse);
  rpc DeleteSchedule (DeleteScheduleRequest) returns (DeleteScheduleResponse);
```

Add `string schedule_id = 12;` to the `Task` message (after `result = 11;`).

Add the schedule messages + enums (mirror the core struct; `kind` as a oneof-like message):

```proto
message TaskTemplate {
  string name = 1;
  string payload = 2;
  repeated string tags = 3;
  map<string, uint32> resources = 4;
  uint32 max_retries = 5;
}

enum OverlapPolicy { OVERLAP_SKIP = 0; OVERLAP_COALESCE = 1; OVERLAP_ALLOW = 2; }

message ScheduleKind {
  oneof kind {
    string cron = 1;
    google.protobuf.Timestamp eta = 2;
  }
}

message Schedule {
  string id = 1;
  string name = 2;
  TaskTemplate task_template = 3;
  ScheduleKind kind = 4;
  OverlapPolicy overlap_policy = 5;
  bool enabled = 6;
  google.protobuf.Timestamp last_fired = 7;
  google.protobuf.Timestamp next_fire = 8;
  google.protobuf.Timestamp created_at = 9;
}

message CreateScheduleRequest { Schedule schedule = 1; }
message CreateScheduleResponse { string schedule_id = 1; }
message ListSchedulesRequest {}
message ListSchedulesResponse { repeated Schedule schedules = 1; }
message DeleteScheduleRequest { string id = 1; }
message DeleteScheduleResponse { bool success = 1; }
```

- [ ] **Step 2: Rebuild proto + fix compile errors in `broker/src/lib.rs`**

Run: `cargo build -p chopflow_broker`
This regenerates proto code via `build.rs`. Expect errors: `From<Task> for ProtoTask` missing `schedule_id`; trait missing 3 RPCs. Fix them.

In the `From<Task> for ProtoTask` impl, add:

```rust
            schedule_id: task.schedule_id.map(|u| u.to_string()).unwrap_or_default(),
```

- [ ] **Step 3: Add the 3 RPC impls to `impl ChopFlowBroker for ChopFlowBrokerService`**

Add imports at the top of `broker/src/lib.rs`:

```rust
use chopflow_core::schedule::{Schedule, ScheduleKind, OverlapPolicy, TaskTemplate};
```

And in the use block for proto types, add `CreateScheduleRequest, CreateScheduleResponse, ListSchedulesRequest, ListSchedulesResponse, DeleteScheduleRequest, DeleteScheduleResponse, Schedule as ProtoSchedule, TaskTemplate as ProtoTaskTemplate, ScheduleKind as ProtoScheduleKind, OverlapPolicy as ProtoOverlapPolicy`.

Add conversions (core ↔ proto) and the RPC methods:

```rust
fn proto_to_template(p: ProtoTaskTemplate) -> Result<TaskTemplate, Status> {
    let payload: serde_json::Value = serde_json::from_str(&p.payload)
        .map_err(|e| Status::invalid_argument(format!("invalid template payload: {}", e)))?;
    Ok(TaskTemplate { name: p.name, payload, tags: p.tags, resources: p.resources, max_retries: p.max_retries })
}

fn proto_to_schedule(p: ProtoSchedule) -> Result<Schedule, Status> {
    let kind = match p.kind {
        Some(ProtoScheduleKind::Kind::Cron(c)) => ScheduleKind::Cron { cron: c },
        Some(ProtoScheduleKind::Kind::Eta(ts)) => {
            let eta = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                .ok_or_else(|| Status::invalid_argument("invalid eta"))?;
            ScheduleKind::OneShot { eta }
        }
        None => return Err(Status::invalid_argument("schedule kind required")),
    };
    let overlap = match p.overlap_policy {
        ProtoOverlapPolicy::OverlapSkip => OverlapPolicy::Skip,
        ProtoOverlapPolicy::OverlapCoalesce => OverlapPolicy::Coalesce,
        ProtoOverlapPolicy::OverlapAllow => OverlapPolicy::Allow,
    };
    let tmpl = proto_to_template(p.task_template.ok_or_else(|| Status::invalid_argument("task_template required"))?)?;
    let s = Schedule::new(p.name, tmpl, kind, overlap).map_err(|e| Status::invalid_argument(e.to_string()))?;
    Ok(s)
}

fn schedule_to_proto(s: Schedule) -> ProtoSchedule {
    let kind = Some(match s.kind {
        ScheduleKind::Cron { cron } => ProtoScheduleKind::Kind::Cron(cron),
        ScheduleKind::OneShot { eta } => ProtoScheduleKind::Kind::Eta(prost_types::Timestamp {
            seconds: eta.timestamp(), nanos: eta.timestamp_subsec_nanos() as i32,
        }),
    });
    let overlap = match s.overlap_policy {
        OverlapPolicy::Skip => ProtoOverlapPolicy::OverlapSkip,
        OverlapPolicy::Coalesce => ProtoOverlapPolicy::OverlapCoalesce,
        OverlapPolicy::Allow => ProtoOverlapPolicy::OverlapAllow,
    };
    let ts = |t: chrono::DateTime<chrono::Utc>| prost_types::Timestamp { seconds: t.timestamp(), nanos: t.timestamp_subsec_nanos() as i32 };
    ProtoSchedule {
        id: s.id.to_string(), name: s.name,
        task_template: Some(ProtoTaskTemplate {
            name: s.task_template.name,
            payload: serde_json::to_string(&s.task_template.payload).unwrap_or_default(),
            tags: s.task_template.tags, resources: s.task_template.resources, max_retries: s.task_template.max_retries,
        }),
        kind, overlap_policy: overlap as i32, enabled: s.enabled,
        last_fired: s.last_fired.map(ts), next_fire: Some(ts(s.next_fire)), created_at: Some(ts(s.created_at)),
    }
}
```

Then the RPC trait methods:

```rust
    async fn create_schedule(&self, request: Request<CreateScheduleRequest>) -> std::result::Result<Response<CreateScheduleResponse>, Status> {
        let schedule = proto_to_schedule(request.into_inner().schedule.ok_or_else(|| Status::invalid_argument("schedule required"))?)?;
        let id = schedule.id;
        self.state.storage.insert_schedule(schedule).await.map_err(|e| Status::internal(format!("insert schedule: {}", e)))?;
        Ok(Response::new(CreateScheduleResponse { schedule_id: id.to_string() }))
    }

    async fn list_schedules(&self, _: Request<ListSchedulesRequest>) -> std::result::Result<Response<ListSchedulesResponse>, Status> {
        let schs = self.state.storage.list_schedules().await.map_err(|e| Status::internal(format!("list schedules: {}", e)))?;
        let schedules = schs.into_iter().map(schedule_to_proto).collect();
        Ok(Response::new(ListSchedulesResponse { schedules }))
    }

    async fn delete_schedule(&self, request: Request<DeleteScheduleRequest>) -> std::result::Result<Response<DeleteScheduleResponse>, Status> {
        let id = Uuid::parse_str(&request.into_inner().id).map_err(|_| Status::invalid_argument("invalid schedule id"))?;
        self.state.storage.delete_schedule(&id).await.map_err(|e| Status::internal(format!("delete schedule: {}", e)))?;
        Ok(Response::new(DeleteScheduleResponse { success: true }))
    }
```

- [ ] **Step 4: Build + run all broker tests**

Run: `cargo build -p chopflow_broker && cargo test -p chopflow_broker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add broker/proto/chopflow.proto broker/src/lib.rs
git commit -m ":sparkles: Add: gRPC Schedule RPCs + schedule_id on proto Task

Refs: SCHEDULE-1"
```

---

### Task 8: CLI `schedule` subcommand

**Files:**
- Modify: `cli/src/main.rs`

**Interfaces:**
- Consumes: the new gRPC `CreateSchedule`/`ListSchedules`/`DeleteSchedule` RPCs (Task 7).
- Produces: `chopflow_cli schedule create --name X --task T (--cron C | --eta E) [--tags a,b] [--resources cpu:2] [--max-retries N] [--overlap skip|coalesce|allow]`, `schedule list`, `schedule delete <id>`.

- [ ] **Step 1: Add the subcommand + handlers**

In `cli/src/main.rs`, extend the `Commands` enum and `main` match. Add a `Schedule` variant with nested subcommands:

```rust
#[derive(Subcommand)]
enum Commands {
    // ... existing Enqueue, Status ...
    /// Manage schedules
    Schedule {
        #[command(subcommand)]
        action: ScheduleCmd,
    },
}

#[derive(Subcommand)]
enum ScheduleCmd {
    /// Create a schedule
    Create {
        #[arg(long)] name: String,
        #[arg(long)] task: String,
        #[arg(long)] cron: Option<String>,
        #[arg(long)] eta: Option<String>,
        #[arg(long, default_value = "{}")] payload: String,
        #[arg(long, default_value = "")] tags: String,
        #[arg(long, default_value = "")] resources: String,
        #[arg(long, default_value_t = 3)] max_retries: u32,
        #[arg(long, default_value = "skip")] overlap: String,
    },
    /// List schedules
    List,
    /// Delete a schedule
    Delete { id: String },
}
```

- [ ] **Step 2: Implement the handlers**

```rust
async fn schedule_create(broker: String, name: String, task: String, cron: Option<String>, eta: Option<String>, payload: String, tags: String, resources: String, max_retries: u32, overlap: String) -> Result<()> {
    let mut client = connect_to_broker(&broker).await?;
    let payload_val: serde_json::Value = serde_json::from_str(&payload)
        .map_err(|e| chopflow_core::error::ChopFlowError::SerializationError(e.to_string()))?;
    let tags_vec: Vec<String> = if tags.is_empty() { vec![] } else { tags.split(',').map(|s| s.trim().to_string()).collect() };
    let mut res_map = std::collections::HashMap::new();
    for entry in resources.split(',') {
        let e = entry.trim();
        if e.is_empty() { continue; }
        let parts: Vec<&str> = e.split(':').collect();
        if parts.len() == 2 { res_map.insert(parts[0].to_string(), parts[1].parse().unwrap_or(0)); }
    }
    let kind = match (cron, eta) {
        (Some(c), None) => Some(chopflow::schedule_kind::Kind::Cron(c)),
        (None, Some(e)) => {
            let dt = chrono::DateTime::parse_from_rfc3339(&e).map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?.with_timezone(&chrono::Utc);
            Some(chopflow::schedule_kind::Kind::Eta(prost_types::Timestamp { seconds: dt.timestamp(), nanos: dt.timestamp_subsec_nanos() as i32 }))
        }
        _ => return Err(chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!("exactly one of --cron or --eta is required"))),
    };
    let overlap_enum = match overlap.as_str() {
        "skip" => chopflow::OverlapPolicy::OverlapSkip,
        "coalesce" => chopflow::OverlapPolicy::OverlapCoalesce,
        "allow" => chopflow::OverlapPolicy::OverlapAllow,
        _ => return Err(chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!("invalid overlap (skip|coalesce|allow)"))),
    };
    let schedule = chopflow::Schedule {
        id: String::new(), name,
        task_template: Some(chopflow::TaskTemplate { name: task, payload: serde_json::to_string(&payload_val).unwrap_or_default(), tags: tags_vec, resources: res_map, max_retries }),
        kind: Some(chopflow::ScheduleKind { kind }),
        overlap_policy: overlap_enum as i32, enabled: true,
        last_fired: None, next_fire: None, created_at: None,
    };
    let resp = client.create_schedule(chopflow::CreateScheduleRequest { schedule: Some(schedule) }).await
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;
    println!("Created schedule: {}", resp.get_ref().schedule_id);
    Ok(())
}

async fn schedule_list(broker: String) -> Result<()> {
    let mut client = connect_to_broker(&broker).await?;
    let resp = client.list_schedules(chopflow::ListSchedulesRequest {}).await
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;
    for s in &resp.get_ref().schedules {
        let kind = match &s.kind { Some(k) => match &k.kind { Some(chopflow::schedule_kind::Kind::Cron(c)) => format!("cron {}", c), Some(chopflow::schedule_kind::Kind::Eta(_)) => "oneshot".into(), None => "?".into() }, None => "?".into() };
        println!("  {} [{}] {} ({})", s.id, if s.enabled { "on" } else { "off" }, s.name, kind);
    }
    Ok(())
}

async fn schedule_delete(broker: String, id: String) -> Result<()> {
    let mut client = connect_to_broker(&broker).await?;
    client.delete_schedule(chopflow::DeleteScheduleRequest { id }).await
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;
    println!("Deleted.");
    Ok(())
}
```

Wire the `Commands::Schedule { action }` arm in `main` to call these.

- [ ] **Step 3: Build**

Run: `cargo build -p chopflow_cli`
Expected: PASS. (Note: proto enum variant names depend on what tonic-build generates — `OverlapSkip` vs `OVERLAP_SKIP`. If the names differ, adjust to what `cargo build` reports; the build error will name the exact variants.)

- [ ] **Step 4: Manual smoke (optional, only if a broker is running)**

If a broker is up: `cargo run -p chopflow_cli -- schedule create --name demo --task echo --cron "*/2 * * * *" --tags demo` then `schedule list`. Otherwise skip — the integration is covered by the build compiling against the generated proto.

- [ ] **Step 5: Commit**

```bash
git add cli/src/main.rs
git commit -m ":sparkles: Add: CLI schedule subcommand (create/list/delete)

Refs: SCHEDULE-1"
```

---

### Task 9: Dashboard UI — Schedules view + drawer + api/hooks

**Files:**
- Create: `broker/ui/src/components/SchedulesView.tsx`, `broker/ui/src/components/ScheduleDrawer.tsx`
- Modify: `broker/ui/src/lib/api.ts`, `broker/ui/src/hooks/useChopFlow.ts`, `broker/ui/src/components/Sidebar.tsx`, `broker/ui/src/App.tsx`

**Interfaces:**
- Consumes: `/api/schedules*` HTTP routes (Task 6), `/api/stats` `schedules` field.
- Produces: a `SchedulesView` table + `ScheduleDrawer` (Run now / Enable toggle / Delete), a `"schedules"` sidebar view, and `useSchedules`/`useCreateSchedule`/`useDeleteSchedule`/`usePatchSchedule` hooks.

- [ ] **Step 1: Add Schedule types + API methods to `broker/ui/src/lib/api.ts`**

Append:

```ts
export type OverlapPolicy = "skip" | "coalesce" | "allow";

export type ScheduleKind =
  | { type: "cron"; cron: string }
  | { type: "oneshot"; eta: string };

export interface TaskTemplate {
  name: string;
  payload: unknown;
  tags: string[];
  resources: Record<string, number>;
  max_retries: number;
}

export interface Schedule {
  id: string;
  name: string;
  task_template: TaskTemplate;
  kind: ScheduleKind;
  overlap_policy: OverlapPolicy;
  enabled: boolean;
  last_fired: number | null;
  next_fire: number;
  created_at: number;
}

export interface CreateScheduleBody {
  name: string;
  task_template: TaskTemplate;
  kind: ScheduleKind;
  overlap_policy: OverlapPolicy;
}
```

Extend the `api` object:

```ts
  listSchedules: () => json<Schedule[]>("/schedules"),
  createSchedule: (body: CreateScheduleBody) =>
    json<{ schedule_id: string }>("/schedules", { method: "POST", body: JSON.stringify(body) }),
  patchSchedule: (id: string, body: Record<string, unknown>) =>
    json<Schedule>(`/schedules/${id}`, { method: "PATCH", body: JSON.stringify(body) }),
  deleteSchedule: (id: string) =>
    json<{ success: boolean }>(`/schedules/${id}`, { method: "DELETE" }),
```

Also add `schedule_id: string | null;` to the `Task` interface (and `schedules: number;` to `Stats`).

- [ ] **Step 2: Add hooks to `broker/ui/src/hooks/useChopFlow.ts`**

```ts
import { api, type CreateScheduleBody } from "../lib/api";

export const qkSchedules = ["schedules"] as const;
export function useSchedules() {
  return useQuery({ queryKey: qkSchedules, queryFn: api.listSchedules, refetchInterval: REFETCH_MS });
}
export function useCreateSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: CreateScheduleBody) => api.createSchedule(body),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["schedules"] }); qc.invalidateQueries({ queryKey: ["stats"] }); },
  });
}
export function usePatchSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, body }: { id: string; body: Record<string, unknown> }) => api.patchSchedule(id, body),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["schedules"] }); qc.invalidateQueries({ queryKey: ["stats"] }); },
  });
}
export function useDeleteSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.deleteSchedule(id),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["schedules"] }); qc.invalidateQueries({ queryKey: ["stats"] }); },
  });
}
```

- [ ] **Step 3: Add `"schedules"` to the Sidebar view + nav**

In `broker/ui/src/components/Sidebar.tsx`: change `export type View = "tasks" | "workers";` to `export type View = "tasks" | "schedules" | "workers";`. Add a `ScheduleIcon` import from `./Icons` (add a simple calendar/clock icon to `Icons.tsx` if one doesn't exist). Add to the `nav` array, between tasks and workers:

```ts
    { key: "schedules", label: "Schedules", icon: ScheduleIcon, count: scheduleCount },
```

Add `scheduleCount: number;` to `Props` and pass `scheduleCount={stats?.schedules ?? 0}` from `App.tsx` (App already has `stats`). Actually App passes explicit props — add `scheduleCount` to the `Sidebar` props in App and pass it.

- [ ] **Step 4: Create `broker/ui/src/components/SchedulesView.tsx`**

A table mirroring `TasksView.tsx`'s structure (view-head + filter chips + table). Columns: Name / Kind / Trigger / Overlap / Last fired / Next fire / Enabled. Filter chips: All / Enabled / One-shot / Cron / Disabled. Row click → opens `ScheduleDrawer`. Reuse `StatusBadge`-style chips (`stbadge` data attributes) for Kind (cron=warn, oneshot=info). Next-fire cell shows a live "time-until" using the existing in-place pattern (a `data-next` attribute + a 1s interval, same technique the tasks view uses for enqueued time). Keep it simple: render `new Date(next_fire).toLocaleString()` plus a relative hint.

The component receives `schedules: Schedule[]`, `isLoading`, `error`, `onOpen: (s: Schedule) => void`. Mirror the prop shape of `TasksView`.

- [ ] **Step 5: Create `broker/ui/src/components/ScheduleDrawer.tsx`**

A right-side drawer (mirror `TaskDrawer.tsx`'s motion/framer pattern). Summary section: template name/payload/tags/resources, next/last fire, overlap policy. Actions:
- **Run now** → `api.enqueue({ name: tmpl.name, payload: tmpl.payload, tags: tmpl.tags, resources: tmpl.resources, max_retries: tmpl.max_retries })` (reuses the existing enqueue path; `schedule_id` unset). Use `useEnqueue`.
- **Enable/Disable** toggle → `usePatchSchedule({ id, body: { enabled: !s.enabled } })`.
- **Delete** → `useDeleteSchedule(id)`, then close.

- [ ] **Step 6: Wire into `App.tsx`**

Import `SchedulesView` + `ScheduleDrawer` + `useSchedules`. Add `const [schedSelected, setSchedSelected] = useState<Schedule | null>(null);`. In `App`, fetch schedules: `const schedQ = useSchedules();`. Render `view === "schedules" ? <SchedulesView ... onOpen={setSchedSelected} /> : ...` (add the branch between tasks and workers). Render `<ScheduleDrawer schedule={schedSelected} onClose={() => setSchedSelected(null)} />` alongside the task drawer.

- [ ] **Step 7: Build the UI**

Run: `pnpm --dir broker/ui build`
Expected: build succeeds (TS compiles). Fix any type errors (e.g. missing `ScheduleIcon` export).

- [ ] **Step 8: Re-embed + build broker**

Run: `touch broker/src/http.rs && cargo build -p chopflow_broker`
Expected: PASS (rust-embed re-reads `ui/dist`).

- [ ] **Step 9: Commit**

```bash
git add broker/ui/src broker/ui/dist
git commit -m ":sparkles: Add: Schedules view + drawer in dashboard UI

Refs: SCHEDULE-1"
```

---

### Task 10: Dashboard UI — Enqueue modal Schedule toggle + lineage

**Files:**
- Modify: `broker/ui/src/components/EnqueueDialog.tsx`, `broker/ui/src/components/TasksView.tsx`, `broker/ui/src/components/TaskDrawer.tsx`

**Interfaces:**
- Consumes: `useCreateSchedule` (Task 9), `Task.schedule_id` (Task 2 → DTO in Task 6).

- [ ] **Step 1: Add the Schedule toggle to `EnqueueDialog.tsx`**

Add a segmented control at the top of the form: **Run immediately** (default → existing `useEnqueue` → `POST /api/tasks`) vs **Schedule** (→ `useCreateSchedule` → `POST /api/schedules`). When Schedule is chosen, reveal:
- A radio: One-shot vs Cron.
- One-shot: `<input type="datetime-local">` → convert to UTC RFC3339 before sending.
- Cron: a text field for the cron expression; show a client-side "next fire" preview by calling a tiny local helper (a 5-field→6-field prepend + a JS cron lib is overkill — instead show the raw expression with a hint, or compute next minute boundary for `*/N` patterns; keep it a static hint label to avoid a new dep).
- Overlap dropdown: Skip / Coalesce / Allow (default Skip).

On submit, branch: if "Schedule", build a `CreateScheduleBody` with the same name/payload/tags/resources/max_retries as the template, the chosen kind, and the overlap; call `useCreateSchedule`. Else existing path.

- [ ] **Step 2: Add lineage indicator to `TasksView.tsx`**

On task rows where `task.schedule_id` is non-null, render a small schedule icon (reuse `ScheduleIcon`) with a `title="from schedule"` tooltip. Place it next to the task name.

- [ ] **Step 3: Add "Spawned by schedule" note to `TaskDrawer.tsx`**

In the Lifecycle/Summary tab, when `task.schedule_id` is present, show a note: `Spawned by schedule {schedule_id}` (a small muted line near the Created node / summary).

- [ ] **Step 4: Build + re-embed**

Run: `pnpm --dir broker/ui build && touch broker/src/http.rs && cargo build -p chopflow_broker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add broker/ui/src broker/ui/dist
git commit -m ":sparkles: Add: schedule toggle in enqueue modal + task lineage UI

Refs: SCHEDULE-1"
```

---

### Task 11: Demos crate — demo worker + 4 handlers

**Files:**
- Create: `demos/Cargo.toml`, `demos/build.rs`, `demos/src/main.rs`, `demos/src/handlers.rs`
- Modify: `Cargo.toml` (workspace — add `demos` member + `image`/`nalgebra`/`rand`/`reqwest` workspace deps)

**Interfaces:**
- Consumes: the broker gRPC API (compiled from `../broker/proto/chopflow.proto`), `chopflow_core` error types.
- Produces: `chopflow_demo_worker` binary — a drop-in worker registering `resize_image`, `batch_compute`, `simulate_pipeline`, `flaky_handler` (+ `default` → echo). Showcases code; not unit-tested.

- [ ] **Step 1: Add workspace deps + member**

In root `Cargo.toml`, add to `[workspace.dependencies]`:

```toml
image = "0.25"
nalgebra = "0.33"
rand = "0.8"
reqwest = { version = "0.12", features = ["json"] }
```

Add `"demos"` to `members`.

- [ ] **Step 2: Create `demos/Cargo.toml`**

```toml
[package]
name = "chopflow_demos"
version.workspace = true
edition.workspace = true
authors.workspace = true
description = "Demo handlers + seeding tool for ChopFlow"
license.workspace = true

[dependencies]
chopflow_core = { path = "../core" }
tokio.workspace = true
anyhow.workspace = true
serde.workspace = true
serde_json.workspace = true
chrono.workspace = true
uuid = { workspace = true, features = ["v4", "serde"] }
clap.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
tonic.workspace = true
prost.workspace = true
prost-types = "0.12"
rand = "0.8"
image = "0.25"
nalgebra = "0.33"
reqwest = { version = "0.12", features = ["json"] }

[build-dependencies]
tonic-build = "0.10"
```

- [ ] **Step 3: Create `demos/build.rs`**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../broker/proto/chopflow.proto");
    tonic_build::compile_protos("../broker/proto/chopflow.proto")?;
    Ok(())
}
```

- [ ] **Step 4: Create `demos/src/handlers.rs`** — the 4 handlers

```rust
use chopflow_core::error::{ChopFlowError, Result};
use rand::Rng;
use std::collections::HashMap;

type Handler = fn(serde_json::Value) -> Result<serde_json::Value>;

pub fn registry() -> HashMap<&'static str, Handler> {
    let mut m: HashMap<&'static str, Handler> = HashMap::new();
    m.insert("resize_image", resize_image);
    m.insert("batch_compute", batch_compute);
    m.insert("simulate_pipeline", simulate_pipeline);
    m.insert("flaky_handler", flaky_handler);
    m.insert("echo", echo);
    m.insert("default", echo);
    m
}

fn echo(p: serde_json::Value) -> Result<serde_json::Value> {
    Ok(serde_json::json!({ "status": "ok", "echo": p }))
}

/// Generate a synthetic gradient PNG, resize to payload dims, return size + dims.
fn resize_image(payload: serde_json::Value) -> Result<serde_json::Value> {
    let w = payload.get("width").and_then(|v| v.as_u64()).unwrap_or(64) as u32;
    let h = payload.get("height").and_then(|v| v.as_u64()).unwrap_or(64) as u32;
    let mut img = image::ImageBuffer::new(w.max(1), h.max(1));
    for (x, y, pixel) in img.enumerate_pixels_mut() {
        let r = ((x as f32 / w.max(1) as f32) * 255.0) as u8;
        let g = ((y as f32 / h.max(1) as f32) * 255.0) as u8;
        *pixel = image::Rgb([r, g, 128]);
    }
    let resized = image::imageops::resize(&img, (w / 2).max(1), (h / 2).max(1), image::imageops::FilterType::Nearest);
    Ok(serde_json::json!({
        "status": "ok",
        "input_dims": [w, h],
        "output_dims": [resized.width(), resized.height()],
        "output_pixels": resized.width() * resized.height(),
    }))
}

/// CPU-bound matrix multiply over random f64 matrices sized by payload.
fn batch_compute(payload: serde_json::Value) -> Result<serde_json::Value> {
    let n = payload.get("n").and_then(|v| v.as_usize()).unwrap_or(64).max(2).min(256);
    let a = nalgebra::DMatrix::<f64>::new_random(n, n);
    let b = nalgebra::DMatrix::<f64>::new_random(n, n);
    let start = std::time::Instant::now();
    let c = &a * &b;
    let elapsed = start.elapsed();
    let checksum: f64 = c.iter().sum();
    Ok(serde_json::json!({
        "status": "ok", "matrix_size": n, "elapsed_ms": elapsed.as_millis() as u64, "checksum": checksum,
    }))
}

/// Multi-stage simulated pipeline with staged sleeps + per-stage timings.
fn simulate_pipeline(payload: serde_json::Value) -> Result<serde_json::Value> {
    let stages = payload.get("stages").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(3).max(1);
    let mut timings = Vec::new();
    for i in 0..stages {
        let start = std::time::Instant::now();
        let ms = 200 + (i as u64 * 150);
        std::thread::sleep(std::time::Duration::from_millis(ms));
        timings.push(serde_json::json!({ "stage": i, "name": ["download","process","upload"][i.min(2)], "ms": start.elapsed().as_millis() as u64 }));
    }
    Ok(serde_json::json!({ "status": "ok", "stages": timings }))
}

/// Fails ~30% of the time (seeded by payload) to exercise retries.
fn flaky_handler(payload: serde_json::Value) -> Result<serde_json::Value> {
    let seed = payload.get("seed").and_then(|v| v.as_u64()).unwrap_or(0);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    if rng.gen_bool(0.3) {
        return Err(ChopFlowError::Other(anyhow::anyhow!("flaky failure (simulated)")));
    }
    Ok(serde_json::json!({ "status": "ok", "seed": seed }))
}
```

- [ ] **Step 5: Create `demos/src/main.rs`** — adapt the worker pull loop

Copy the structure from `worker/src/main.rs`: clap CLI (`--broker`, `--tags`, `--resources`, `--heartbeat-interval`), `connect_and_register` with backoff, heartbeat loop, fetch→execute→ack loop. Replace `WorkerState::new`'s registry with `handlers::registry()`. Handler lookup: `registry.get(&task.name).or_else(|| registry.get("default"))`. Keep `execute_task` + `send_task_acknowledgment` + `fetch_tasks` + `send_heartbeat` essentially as-is (adapt imports to the demos crate's own `chopflow` module via `tonic::include_proto!("chopflow")`). The handler type is `fn(serde_json::Value) -> Result<serde_json::Value>` from `chopflow_core::error::Result`.

Bin name: set `[[bin]] name = "chopflow_demo_worker"` path `src/main.rs` in `demos/Cargo.toml` (or rely on the default bin = crate name `chopflow_demos`; explicit `[[bin]]` is clearer — add it).

- [ ] **Step 6: Build**

Run: `cargo build -p chopflow_demos`
Expected: PASS. Fix proto enum variant name mismatches as the compiler reports.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml demos
git commit -m":sparkles: Add: demos crate with 4 simulated demo handlers + worker

Refs: SCHEDULE-1"
```

---

### Task 12: Demos seed binary + run.sh + README

**Files:**
- Create: `demos/src/bin/seed.rs`, `demos/run.sh`
- Modify: `demos/Cargo.toml` (add `[[bin]]` for seed), `README.md`

**Interfaces:**
- Consumes: the broker HTTP API (Task 6) — `POST /api/tasks` + `POST /api/schedules`.
- Produces: `chopflow_demo_seed` binary + a one-command demo run script + README docs.

- [ ] **Step 1: Add the seed bin to `demos/Cargo.toml`**

```toml
[[bin]]
name = "chopflow_demo_worker"
path = "src/main.rs"

[[bin]]
name = "chopflow_demo_seed"
path = "src/bin/seed.rs"
```

- [ ] **Step 2: Create `demos/src/bin/seed.rs`**

Uses `reqwest` (async) to hit the broker HTTP API. Enqueues one of each handler + creates 2 schedules:

```rust
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let broker = std::env::args().nth(1).unwrap_or_else(|| "http://localhost:8080".to_string());
    let api = format!("{}/api", broker);
    let client = reqwest::Client::new();

    let post = |path: &str, body: serde_json::Value| {
        let c = client.clone();
        let url = format!("{}{}", api, path);
        async move {
            let r = c.post(&url).json(&body).send().await?;
            println!("POST {} -> {}", path, r.status());
            r.json::<serde_json::Value>().await
        }
    };

    // One of each handler.
    post("/tasks", json!({ "name": "resize_image", "payload": { "width": 128, "height": 96 }, "tags": ["demo"], "resources": { "cpu": 1 } })).await.ok();
    post("/tasks", json!({ "name": "batch_compute", "payload": { "n": 96 }, "tags": ["demo"], "resources": { "cpu": 2 } })).await.ok();
    post("/tasks", json!({ "name": "simulate_pipeline", "payload": { "stages": ["download","process","upload"] }, "tags": ["demo"], "resources": { "cpu": 1 } })).await.ok();
    post("/tasks", json!({ "name": "flaky_handler", "payload": { "seed": 7 }, "tags": ["demo"], "max_retries": 5, "resources": { "cpu": 1 } })).await.ok();

    // A cron schedule every 2 minutes.
    post("/schedules", json!({
        "name": "every-2min-compute",
        "task_template": { "name": "batch_compute", "payload": { "n": 64 }, "tags": ["demo"], "resources": { "cpu": 2 }, "max_retries": 3 },
        "kind": { "type": "cron", "cron": "*/2 * * * *" },
        "overlap_policy": "skip"
    })).await.ok();

    // A one-shot 5 minutes out.
    let in5 = chrono::Utc::now() + chrono::Duration::minutes(5);
    post("/schedules", json!({
        "name": "one-off-pipeline",
        "task_template": { "name": "simulate_pipeline", "payload": { "stages": ["download","process","upload"] }, "tags": ["demo"], "resources": { "cpu": 1 }, "max_retries": 1 },
        "kind": { "type": "oneshot", "eta": in5.to_rfc3339() },
        "overlap_policy": "allow"
    })).await.ok();

    println!("Seed complete. Open the dashboard at {} — tasks flowing + 2 schedules ticking.", broker);
    Ok(())
}
```

- [ ] **Step 3: Create `demos/run.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
# Demo run story: broker + demo worker + seed → live dashboard.
cargo build
./target/debug/chopflow_broker start --http-port 8080 --port 8000 --storage memory --open &
BROKER_PID=$!
sleep 1
./target/debug/chopflow_demo_worker --broker http://localhost:8000 --tags demo,ml --resources cpu:4 &
WORKER_PID=$!
sleep 1
./target/debug/chopflow_demo_seed http://localhost:8080
echo "Broker: $BROKER_PID, Worker: $WORKER_PID. Ctrl+C to stop."
trap 'kill $BROKER_PID $WORKER_PID 2>/dev/null || true' EXIT
wait
```

`chmod +x demos/run.sh`.

- [ ] **Step 4: Build**

Run: `cargo build -p chopflow_demos`
Expected: both bins compile.

- [ ] **Step 5: Add README sections**

In `README.md`, after the dashboard section, add a `## Scheduled tasks` section (cron + one-shot, overlap policy, UTC, CLI `schedule` subcommand example, HTTP `/api/schedules` table) and a `## Demo handlers` section (the 4 handlers table + `demos/run.sh` run story from spec section 8.3).

- [ ] **Step 6: Commit**

```bash
git add demos README.md
git commit -m ":sparkles: Add: demo seed binary + run script + README sections

Refs: SCHEDULE-1"
```

---

### Task 13: End-to-end verification + final integration test

**Files:**
- Extend: `broker/tests/grpc_flow.rs` (optional schedule RPC coverage)

- [ ] **Step 1: Build + test the whole workspace**

Run: `cargo build --workspace && cargo test --workspace`
Expected: everything compiles (including `demos`) and all tests pass.

- [ ] **Step 2: (Optional) Extend `broker/tests/grpc_flow.rs` with a schedule RPC test**

Add a test that calls `create_schedule` (cron every second, overlap Allow) over gRPC, waits ~2s, and asserts `list_tasks` shows a task with the template name and `schedule_id` set. If time-boxed, the ticker tests in `schedule_ticker.rs` (Task 5) already cover the materialization logic — this step is a nice-to-have.

- [ ] **Step 3: Manual demo run**

Run: `bash demos/run.sh`
Expected (per spec section 12.3): browser shows 4 tasks flowing through distinct lifecycles + 2 schedules ticking; `flaky_handler` visibly retrying. Ctrl+C to stop. Document any deviations.

- [ ] **Step 4: Final commit (if any test edits)**

```bash
git add broker/tests/grpc_flow.rs
git commit -m ":white_check_mark: Add: gRPC schedule materialization integration test

Refs: SCHEDULE-1"
```

---

## Self-Review

**1. Spec coverage:**
- §3 data model (Schedule/ScheduleKind/OverlapPolicy/TaskTemplate/Schedule + Task.schedule_id) → Tasks 1, 2. ✓
- §4 Storage (6 methods, schedules table, in_flight scan) → Tasks 3, 4. ✓
- §5 broker ticker + reconciliation → Task 5. ✓ (tick_once + reconcile_schedules; overlap-skip advances cron next_fire; one-shot self-disables.)
- §6 HTTP `/api/schedules*` + `stats.schedules` + cron validation → Task 6. ✓
- §6 gRPC Schedule RPCs + `Task.schedule_id` proto → Task 7. ✓
- §6 CLI `schedule` subcommand → Task 8. ✓
- §7 UI Schedules view + drawer + Run-now + modal Schedule toggle + lineage → Tasks 9, 10. ✓ (adapted from the deleted `dashboard.html` to the real React components.)
- §8 demos crate (4 handlers + worker + seed + run.sh + deps) → Tasks 11, 12. ✓
- §9 edge cases (invalid cron → 400, missed fires no-backfill via reconcile, overlap skip advances cron, ticker resilience via per-schedule warn+continue, schedule deletion doesn't cancel spawned tasks) → covered in Tasks 5, 6. ✓
- §10 testing (schedule.rs unit, storage both backends, ticker integration, overlap) → Tasks 1, 3, 4, 5, 6. ✓ (demos explicitly not unit-tested per spec §10.)
- §11 files touched → matches File Structure above. ✓
- §12 verification → Task 13. ✓

**2. Placeholder scan:** No "TBD"/"add error handling" without code. Cron next-fire preview in the modal is intentionally a static hint (a JS cron lib is out of scope / YAGNI) — stated explicitly, not hand-waved. Demo worker `main.rs` says "copy the structure from worker/src/main.rs" with the concrete diff (registry swap) — the executor reads both; the pull-loop code already exists verbatim in the repo and is referenced by file, not re-pasted (DRY within the plan).

**3. Type consistency:** `Schedule`, `ScheduleKind`, `OverlapPolicy`, `TaskTemplate` defined in Task 1, used identically in Tasks 3–7. `tick_once`/`spawn_schedule_ticker`/`reconcile_schedules` defined in Task 5, called in Task 5 Step 4. `ScheduleDto`/`CreateScheduleBody`/`ScheduleKindDto` defined in Task 6, mirrored in api.ts (Task 9). `schedule_id` flows Task 2 → proto (Task 7) → TaskDto (Task 6, implicit via `From<Task>` — the field must be added to `TaskDto` too: **correction below**).

**Correction found during review:** `TaskDto` in `broker/src/http.rs` (Task 6) must also gain `schedule_id: Option<String>` so the UI lineage (Task 10) has the field. Add to the `TaskDto` struct and its `From<Task>` impl: `schedule_id: t.schedule_id.map(|u| u.to_string()),`. This is folded into Task 6 Step 3 implicitly (the DTO block) — the executor should add it there. Noted here so it isn't missed; no separate task needed.
