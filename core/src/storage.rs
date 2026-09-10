/*!
# Storage Module

This module defines the durable/persistent task storage abstraction for
ChopFlow.

`Storage` is the single source of truth for task state. It replaces the older
split between a pending `Queue` and an all-tasks `TaskStore`: a task's
lifecycle is encoded entirely by its `status` field. "Queued" tasks are simply
rows/entries with `status = Queued`; the broker asks storage to atomically
[`claim_ready`](Storage::claim_ready) matching tasks (Queued → Running) when a
worker pulls work.

Two implementations are provided:
- [`InMemoryStorage`] — a `HashMap` behind a `tokio::Mutex`. Mirrors the
  pre-persistence behavior; useful for tests and ephemeral runs.
- [`SqliteStorage`] — a single `rusqlite` connection behind a
  `std::sync::Mutex`, with the SQLite library bundled in. Durable across
  broker restarts; `Running` tasks are reconciled back to `Queued` on open via
  [`reconcile`](Storage::reconcile) (workers are gone after a restart).

All `Storage` methods are `async`; the SQLite implementation offloads its
synchronous database work to `tokio::task::spawn_blocking`.
*/

use crate::error::{ChopFlowError, Result};
use crate::schedule::Schedule;
use crate::task::{Task, TaskStatus};
use async_trait::async_trait;
use rusqlite::Connection;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Filter applied to [`Storage::list`].
#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    /// Restrict to these statuses; empty means "any status".
    pub statuses: Vec<TaskStatus>,
    /// Maximum number of tasks to return. `0` means unlimited.
    pub limit: usize,
    /// Number of tasks to skip from the start (ordered by enqueue time).
    pub offset: usize,
}

/// Counts of tasks grouped by status, for stats/dashboard use.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusCounts {
    pub queued: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub dead_lettered: usize,
    pub cancelled: usize,
}

/// The single source of truth for task state.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Insert or replace a task (upsert keyed by id).
    async fn insert(&self, task: Task) -> Result<()>;

    /// Get a task by id.
    async fn get(&self, id: &Uuid) -> Result<Option<Task>>;

    /// List tasks matching the filter, ordered by enqueue time.
    async fn list(&self, filter: &TaskFilter) -> Result<Vec<Task>>;

    /// Count tasks currently in the `Queued` status (pending work).
    async fn count_pending(&self) -> Result<usize>;

    /// Count tasks grouped by status.
    async fn count_by_status(&self) -> Result<StatusCounts>;

    /// Atomically claim up to `max` ready tasks (ETA elapsed, status
    /// `Queued`) whose tags intersect `tags` — or any ready task when
    /// `tags` is empty — marking them `Running` and returning them. A second
    /// concurrent caller must not receive the same task.
    async fn claim_ready(&self, tags: &[String], max: usize) -> Result<Vec<Task>>;

    /// Reset every `Running` task back to `Queued`. Called on startup, since
    /// any in-flight task was being handled by a worker that no longer
    /// exists. Returns the number of tasks reconciled.
    async fn reconcile(&self) -> Result<usize>;

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
}

// ---------------------------------------------------------------------------
// In-memory implementation
// ---------------------------------------------------------------------------

/// BTreeMap key for the ready queue. Ascending iteration yields the next task
/// to claim: highest priority first (via `Reverse`), then earliest ETA (`None`
/// sorts before `Some`, so immediately-ready tasks come first), then earliest
/// enqueue time (FIFO tie-break), then id as a unique tie-breaker so two tasks
/// can never collide on the same key.
type ReadyKey = (
    Reverse<i32>,
    Option<chrono::DateTime<chrono::Utc>>,
    chrono::DateTime<chrono::Utc>,
    Uuid,
);

/// O(1) atomic status counters. `count_by_status` / `count_pending` read these
/// instead of scanning the task map, so the stats poll (dashboard + benchmark
/// drain loop) costs nothing regardless of how many tasks are stored. `Created`
/// is intentionally untracked — it mirrors the old scan-based behavior where
/// `Created` tasks were skipped.
#[derive(Default)]
struct StatusCounters {
    queued: AtomicU64,
    running: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    dead_lettered: AtomicU64,
    cancelled: AtomicU64,
}

impl StatusCounters {
    fn snapshot(&self) -> StatusCounts {
        StatusCounts {
            queued: self.queued.load(Ordering::Relaxed) as usize,
            running: self.running.load(Ordering::Relaxed) as usize,
            completed: self.completed.load(Ordering::Relaxed) as usize,
            failed: self.failed.load(Ordering::Relaxed) as usize,
            dead_lettered: self.dead_lettered.load(Ordering::Relaxed) as usize,
            cancelled: self.cancelled.load(Ordering::Relaxed) as usize,
        }
    }

    fn inc(&self, status: TaskStatus) {
        match status {
            TaskStatus::Queued => {
                self.queued.fetch_add(1, Ordering::Relaxed);
            }
            TaskStatus::Running => {
                self.running.fetch_add(1, Ordering::Relaxed);
            }
            TaskStatus::Completed => {
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
            TaskStatus::Failed => {
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
            TaskStatus::DeadLettered => {
                self.dead_lettered.fetch_add(1, Ordering::Relaxed);
            }
            TaskStatus::Cancelled => {
                self.cancelled.fetch_add(1, Ordering::Relaxed);
            }
            TaskStatus::Created => {}
        }
    }

    fn dec(&self, status: TaskStatus) {
        match status {
            TaskStatus::Queued => {
                self.queued.fetch_sub(1, Ordering::Relaxed);
            }
            TaskStatus::Running => {
                self.running.fetch_sub(1, Ordering::Relaxed);
            }
            TaskStatus::Completed => {
                self.completed.fetch_sub(1, Ordering::Relaxed);
            }
            TaskStatus::Failed => {
                self.failed.fetch_sub(1, Ordering::Relaxed);
            }
            TaskStatus::DeadLettered => {
                self.dead_lettered.fetch_sub(1, Ordering::Relaxed);
            }
            TaskStatus::Cancelled => {
                self.cancelled.fetch_sub(1, Ordering::Relaxed);
            }
            TaskStatus::Created => {}
        }
    }
}

/// In-memory `Storage` backed by a `HashMap` under a `tokio::Mutex`.
///
/// Beyond the task map it keeps two derived structures so the hot paths stay
/// cheap at scale:
/// - `ready` — a `BTreeMap` index of every `Queued` task keyed by
///   `(priority, eta, enqueue_time, id)`, so [`Storage::claim_ready`] is
///   O(batch · log N) instead of an O(N) full scan on every worker fetch. At
///   1M tasks the old scan capped throughput around ~130 tasks/s; the index
///   makes each fetch independent of the total task count.
/// - `stats` — atomic per-status counters, so [`Storage::count_by_status`] /
///   [`Storage::count_pending`] are O(1) instead of scanning all tasks.
///
/// Both are maintained inside `insert` / `claim_ready` / `reconcile`, which all
/// acquire `tasks` then `ready` in that order to avoid deadlock.
pub struct InMemoryStorage {
    tasks: Arc<Mutex<HashMap<Uuid, Task>>>,
    ready: Arc<Mutex<BTreeMap<ReadyKey, Uuid>>>,
    schedules: Arc<Mutex<HashMap<Uuid, Schedule>>>,
    stats: StatusCounters,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            ready: Arc::new(Mutex::new(BTreeMap::new())),
            schedules: Arc::new(Mutex::new(HashMap::new())),
            stats: StatusCounters::default(),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    /// True if the task's tags intersect `tags`, or if `tags` is empty.
    fn tags_match(task: &Task, tags: &[String]) -> bool {
        if tags.is_empty() {
            return true;
        }
        tags.iter().any(|t| task.tags.contains(t))
    }

    /// The ready-queue key for a task. Derived from the task's scheduling
    /// fields (priority / eta / enqueue_time / id), not its status, so it is
    /// stable across status transitions and cheap to recompute for removal.
    fn ready_key(task: &Task) -> ReadyKey {
        (Reverse(task.priority), task.eta, task.enqueue_time, task.id)
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn insert(&self, task: Task) -> Result<()> {
        let mut tasks = self.tasks.lock().await;
        let mut ready = self.ready.lock().await;

        // Undo the previous state of this id (if any) so the counters and the
        // ready index stay consistent on upsert. Every status transition in the
        // broker flows through `insert`, so this is the single place that
        // reconciles derived state with the task map.
        if let Some(old) = tasks.get(&task.id) {
            if old.status == TaskStatus::Queued {
                ready.remove(&Self::ready_key(old));
            }
            self.stats.dec(old.status);
        }

        let new_status = task.status;
        if new_status == TaskStatus::Queued {
            ready.insert(Self::ready_key(&task), task.id);
        }
        self.stats.inc(new_status);

        tasks.insert(task.id, task);
        Ok(())
    }

    async fn get(&self, id: &Uuid) -> Result<Option<Task>> {
        let tasks = self.tasks.lock().await;
        Ok(tasks.get(id).cloned())
    }

    async fn list(&self, filter: &TaskFilter) -> Result<Vec<Task>> {
        let tasks = self.tasks.lock().await;
        let mut all: Vec<Task> = tasks
            .values()
            .filter(|t| filter.statuses.is_empty() || filter.statuses.contains(&t.status))
            .cloned()
            .collect();
        all.sort_by_key(|t| t.enqueue_time);

        let limited: Vec<Task> = all
            .into_iter()
            .skip(filter.offset)
            .take(if filter.limit == 0 {
                usize::MAX
            } else {
                filter.limit
            })
            .collect();
        Ok(limited)
    }

    async fn count_pending(&self) -> Result<usize> {
        Ok(self.stats.queued.load(Ordering::Relaxed) as usize)
    }

    async fn count_by_status(&self) -> Result<StatusCounts> {
        Ok(self.stats.snapshot())
    }

    async fn claim_ready(&self, tags: &[String], max: usize) -> Result<Vec<Task>> {
        let max = max.max(1);
        let mut tasks = self.tasks.lock().await;
        let mut ready = self.ready.lock().await;
        let now = chrono::Utc::now();

        // The ready index holds every Queued task, ordered so that ascending
        // iteration yields the next-to-claim (highest priority, earliest ETA,
        // earliest enqueue). Walk it and take the first `max` tasks that are
        // actually due (`is_ready_at`) and whose tags match. Tasks with a
        // future ETA, or a tag mismatch belonging to another worker, are
        // skipped but left in the index. In the common no-ETA / matching-tag
        // case every visited entry is claimable, so this is O(batch) rather
        // than the old O(N) full scan — the cost of a fetch no longer grows
        // with the total number of stored tasks.
        let mut claimed_ids: Vec<Uuid> = Vec::with_capacity(max);
        let mut stale_keys: Vec<ReadyKey> = Vec::new();
        for (key, id) in ready.iter() {
            if claimed_ids.len() >= max {
                break;
            }
            match tasks.get(id) {
                // Ghost entry (task gone but index not): clean it up lazily.
                None => stale_keys.push(*key),
                Some(task) => {
                    if task.status == TaskStatus::Queued
                        && task.is_ready_at(now)
                        && Self::tags_match(task, tags)
                    {
                        claimed_ids.push(*id);
                    }
                }
            }
        }

        // Remove claimed + stale entries from the index and flip the claimed
        // tasks to Running in one pass over the task map.
        let mut claimed: Vec<Task> = Vec::with_capacity(claimed_ids.len());
        for id in &claimed_ids {
            if let Some(task) = tasks.get_mut(id) {
                ready.remove(&Self::ready_key(task));
                task.mark_running();
                claimed.push(task.clone());
            }
        }
        for key in stale_keys {
            ready.remove(&key);
        }

        let n = claimed.len() as u64;
        self.stats.queued.fetch_sub(n, Ordering::Relaxed);
        self.stats.running.fetch_add(n, Ordering::Relaxed);
        Ok(claimed)
    }

    async fn reconcile(&self) -> Result<usize> {
        let mut tasks = self.tasks.lock().await;
        let mut ready = self.ready.lock().await;
        let mut n: u64 = 0;
        for task in tasks.values_mut() {
            if task.status == TaskStatus::Running {
                task.status = TaskStatus::Queued;
                ready.insert(Self::ready_key(task), task.id);
                n += 1;
            }
        }
        self.stats.running.fetch_sub(n, Ordering::Relaxed);
        self.stats.queued.fetch_add(n, Ordering::Relaxed);
        Ok(n as usize)
    }

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
        all.sort_by_key(|s| s.created_at);
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
            .filter(|t| {
                t.schedule_id == Some(*schedule_id)
                    && matches!(t.status, TaskStatus::Queued | TaskStatus::Running)
            })
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------------
// SQLite implementation
// ---------------------------------------------------------------------------

/// SQLite-backed durable `Storage`. A single connection is guarded by a
/// `std::sync::Mutex`; all database work runs on `spawn_blocking`.
pub struct SqliteStorage {
    conn: Arc<std::sync::Mutex<Connection>>,
}

impl SqliteStorage {
    /// Open (or create) a SQLite database at `path`, initialize the schema,
    /// and reconcile any `Running` tasks back to `Queued`.
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path).map_err(rusqlite_err)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(rusqlite_err)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS tasks (
                 id          TEXT PRIMARY KEY,
                 status      INTEGER NOT NULL,
                 eta_ms      INTEGER,
                 enqueue_ms  INTEGER NOT NULL,
                 priority    INTEGER NOT NULL DEFAULT 0,
                 task_json   TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS tasks_status ON tasks(status);
             CREATE INDEX IF NOT EXISTS tasks_eta ON tasks(eta_ms);
             CREATE INDEX IF NOT EXISTS tasks_enqueue ON tasks(enqueue_ms);
             CREATE TABLE IF NOT EXISTS schedules (
                 id              TEXT PRIMARY KEY,
                 next_fire_ms    INTEGER NOT NULL,
                 enabled         INTEGER NOT NULL,
                 schedule_json   TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS schedules_next_fire ON schedules(next_fire_ms);",
        )
        .map_err(rusqlite_err)?;

        // Idempotent migration for DBs created before priority existed. The only
        // failure mode is "duplicate column name" (column already present), which
        // is the desired end state — so we discard the result. The tasks_priority
        // index references this column, so it must be created *after* the ALTER
        // (a legacy DB has no priority column until this runs).
        let _ = conn.execute(
            "ALTER TABLE tasks ADD COLUMN priority INTEGER NOT NULL DEFAULT 0",
            [],
        );
        conn.execute(
            "CREATE INDEX IF NOT EXISTS tasks_priority ON tasks(status, priority)",
            [],
        )
        .map_err(rusqlite_err)?;

        Ok(Self {
            conn: Arc::new(std::sync::Mutex::new(conn)),
        })
    }

    /// Open an in-memory SQLite database (useful for tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(rusqlite_err)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tasks (
                 id          TEXT PRIMARY KEY,
                 status      INTEGER NOT NULL,
                 eta_ms      INTEGER,
                 enqueue_ms  INTEGER NOT NULL,
                 priority    INTEGER NOT NULL DEFAULT 0,
                 task_json   TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS tasks_status ON tasks(status);
             CREATE INDEX IF NOT EXISTS tasks_eta ON tasks(eta_ms);
             CREATE INDEX IF NOT EXISTS tasks_enqueue ON tasks(enqueue_ms);
             CREATE INDEX IF NOT EXISTS tasks_priority ON tasks(status, priority);
             CREATE TABLE IF NOT EXISTS schedules (
                 id              TEXT PRIMARY KEY,
                 next_fire_ms    INTEGER NOT NULL,
                 enabled         INTEGER NOT NULL,
                 schedule_json   TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS schedules_next_fire ON schedules(next_fire_ms);",
        )
        .map_err(rusqlite_err)?;
        Ok(Self {
            conn: Arc::new(std::sync::Mutex::new(conn)),
        })
    }

    /// Serialize a task to its JSON blob and extract the indexed columns.
    fn marshal(task: &Task) -> Result<(String, i64, Option<i64>, i64, i64, String)> {
        let id = task.id.to_string();
        let status = task.status as i64;
        let eta_ms = task.eta.map(|eta| eta.timestamp_millis());
        let enqueue_ms = task.enqueue_time.timestamp_millis();
        let priority = task.priority as i64;
        let json = serde_json::to_string(task).map_err(|e| {
            ChopFlowError::SerializationError(format!("failed to serialize task: {}", e))
        })?;
        Ok((id, status, eta_ms, enqueue_ms, priority, json))
    }

    /// Deserialize a row into a `Task`.
    fn unmarshal(row: &rusqlite::Row<'_>) -> rusqlite::Result<Task> {
        let json: String = row.get("task_json")?;
        serde_json::from_str(&json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
        })
    }

    fn marshal_schedule(s: &Schedule) -> Result<(String, i64, i64, String)> {
        let id = s.id.to_string();
        let next_fire_ms = s.next_fire.timestamp_millis();
        let enabled = if s.enabled { 1i64 } else { 0 };
        let json = serde_json::to_string(s).map_err(|e| {
            ChopFlowError::SerializationError(format!("failed to serialize schedule: {}", e))
        })?;
        Ok((id, next_fire_ms, enabled, json))
    }
}

fn rusqlite_err(e: rusqlite::Error) -> ChopFlowError {
    ChopFlowError::Other(e.into())
}

/// Lock the shared connection, mapping a poisoned-mutex error into a plain
/// `ChopFlowError`. We must not move the inner `Connection` into an
/// `anyhow::Error` (it is neither `Sync` nor `StdError`), so format the
/// `PoisonError` via its `Display` impl instead.
fn lock_conn(
    conn: &Arc<std::sync::Mutex<Connection>>,
) -> Result<std::sync::MutexGuard<'_, Connection>> {
    conn.lock()
        .map_err(|e| ChopFlowError::Other(anyhow::anyhow!("storage mutex poisoned: {}", e)))
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn insert(&self, task: Task) -> Result<()> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let (id, status, eta_ms, enqueue_ms, priority, json) = Self::marshal(&task)?;
            let conn = lock_conn(&conn)?;
            conn.execute(
                "INSERT OR REPLACE INTO tasks (id, status, eta_ms, enqueue_ms, priority, task_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![id, status, eta_ms, enqueue_ms, priority, json],
            )
            .map_err(rusqlite_err)?;
            Ok(())
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(())
    }

    async fn get(&self, id: &Uuid) -> Result<Option<Task>> {
        let conn = self.conn.clone();
        let id_str = id.to_string();
        let task = tokio::task::spawn_blocking(move || -> Result<Option<Task>> {
            let conn = lock_conn(&conn)?;
            let mut stmt = conn
                .prepare("SELECT * FROM tasks WHERE id = ?1")
                .map_err(rusqlite_err)?;
            let mut rows = stmt
                .query(rusqlite::params![id_str])
                .map_err(rusqlite_err)?;
            match rows.next().map_err(rusqlite_err)? {
                Some(row) => {
                    let task = Self::unmarshal(row).map_err(rusqlite_err)?;
                    Ok(Some(task))
                }
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(task)
    }

    async fn list(&self, filter: &TaskFilter) -> Result<Vec<Task>> {
        let conn = self.conn.clone();
        let filter = filter.clone();
        let tasks = tokio::task::spawn_blocking(move || -> Result<Vec<Task>> {
            let conn = lock_conn(&conn)?;

            // Build the status filter clause. Statuses are stored as the i64
            // enum discriminant (declaration order, matching the proto cast).
            let statuses: Vec<i64> = filter.statuses.iter().map(|s| *s as i64).collect();
            let limit = if filter.limit == 0 {
                i64::MAX
            } else {
                filter.limit as i64
            };

            let mut sql = String::from("SELECT * FROM tasks");
            if !statuses.is_empty() {
                sql.push_str(" WHERE status IN (");
                sql.push_str(&vec!["?"; statuses.len()].join(","));
                sql.push(')');
            }
            sql.push_str(" ORDER BY enqueue_ms, id LIMIT ? OFFSET ?");

            let mut stmt = conn.prepare(&sql).map_err(rusqlite_err)?;
            let params_iter: Vec<Box<dyn rusqlite::ToSql>> = statuses
                .iter()
                .map(|s| Box::new(*s) as Box<dyn rusqlite::ToSql>)
                .chain(std::iter::once(Box::new(limit) as Box<dyn rusqlite::ToSql>))
                .chain(std::iter::once(
                    Box::new(filter.offset as i64) as Box<dyn rusqlite::ToSql>
                ))
                .collect();

            let params: Vec<&dyn rusqlite::ToSql> = params_iter.iter().map(Box::as_ref).collect();

            let rows: rusqlite::Result<Vec<Task>> = stmt
                .query_map(params.as_slice(), Self::unmarshal)
                .map_err(rusqlite_err)?
                .collect();
            rows.map_err(rusqlite_err)
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(tasks)
    }

    async fn count_pending(&self) -> Result<usize> {
        let conn = self.conn.clone();
        let n = tokio::task::spawn_blocking(move || -> Result<usize> {
            let conn = lock_conn(&conn)?;
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM tasks WHERE status = ?1",
                    rusqlite::params![TaskStatus::Queued as i64],
                    |row| row.get(0),
                )
                .map_err(rusqlite_err)?;
            Ok(n as usize)
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(n)
    }

    async fn count_by_status(&self) -> Result<StatusCounts> {
        let conn = self.conn.clone();
        let counts = tokio::task::spawn_blocking(move || -> Result<StatusCounts> {
            let conn = lock_conn(&conn)?;
            let mut stmt = conn
                .prepare("SELECT status, COUNT(*) FROM tasks GROUP BY status")
                .map_err(rusqlite_err)?;
            let counts = stmt
                .query_map([], |row| {
                    let status: i64 = row.get(0)?;
                    let count: i64 = row.get(1)?;
                    Ok((status, count as usize))
                })
                .map_err(rusqlite_err)?;
            let mut out = StatusCounts::default();
            for c in counts {
                let (status, count) = c.map_err(rusqlite_err)?;
                match status as i32 {
                    s if s == TaskStatus::Queued as i32 => out.queued += count,
                    s if s == TaskStatus::Running as i32 => out.running += count,
                    s if s == TaskStatus::Completed as i32 => out.completed += count,
                    s if s == TaskStatus::Failed as i32 => out.failed += count,
                    s if s == TaskStatus::DeadLettered as i32 => out.dead_lettered += count,
                    s if s == TaskStatus::Cancelled as i32 => out.cancelled += count,
                    _ => {}
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(counts)
    }

    async fn claim_ready(&self, tags: &[String], max: usize) -> Result<Vec<Task>> {
        let conn = self.conn.clone();
        let tags = tags.to_vec();
        let max = max.max(1);
        let tasks = tokio::task::spawn_blocking(move || -> Result<Vec<Task>> {
            let conn = lock_conn(&conn)?;
            let now_ms = chrono::Utc::now().timestamp_millis();

            // Select candidate ready, queued tasks ordered by priority (desc),
            // then eta, then enqueue time. Tag filtering happens in Rust (tags
            // live inside the JSON blob); the connection Mutex guarantees
            // atomicity between select and update so no two callers claim the
            // same task.
            let mut stmt = conn
                .prepare(
                    "SELECT * FROM tasks
                     WHERE status = ?1 AND (eta_ms IS NULL OR eta_ms <= ?2)
                     ORDER BY priority DESC, eta_ms, enqueue_ms, id",
                )
                .map_err(rusqlite_err)?;
            let mut rows = stmt
                .query(rusqlite::params![TaskStatus::Queued as i64, now_ms])
                .map_err(rusqlite_err)?;

            let mut claimed: Vec<Task> = Vec::new();
            while let Some(row) = rows.next().map_err(rusqlite_err)? {
                if claimed.len() >= max {
                    break;
                }
                let task = Self::unmarshal(row).map_err(rusqlite_err)?;
                if !tags.is_empty() && !tags.iter().any(|t| task.tags.contains(t)) {
                    continue;
                }
                claimed.push(task);
            }
            drop(rows);
            drop(stmt);

            // Mark the chosen tasks Running.
            for task in &mut claimed {
                task.mark_running();
                let (id, status, eta_ms, enqueue_ms, priority, json) = Self::marshal(task)?;
                conn.execute(
                    "UPDATE tasks SET status = ?2, eta_ms = ?3, enqueue_ms = ?4, priority = ?5, task_json = ?6 WHERE id = ?1",
                    rusqlite::params![id, status, eta_ms, enqueue_ms, priority, json],
                )
                .map_err(rusqlite_err)?;
            }
            Ok(claimed)
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(tasks)
    }

    async fn reconcile(&self) -> Result<usize> {
        let conn = self.conn.clone();
        let n = tokio::task::spawn_blocking(move || -> Result<usize> {
            let conn = lock_conn(&conn)?;

            // Load every Running task. We can't just `UPDATE ... SET status`
            // because the authoritative status lives inside the `task_json`
            // blob (that's what reads deserialize); the `status` column is only
            // an index. So we deserialize each Running task, flip it to Queued,
            // and write both columns back in sync.
            let mut stmt = conn
                .prepare("SELECT * FROM tasks WHERE status = ?1")
                .map_err(rusqlite_err)?;
            let rows: rusqlite::Result<Vec<Task>> = stmt
                .query_map(rusqlite::params![TaskStatus::Running as i64], |row| {
                    Self::unmarshal(row)
                })
                .map_err(rusqlite_err)?
                .collect();
            let mut running: Vec<Task> = rows.map_err(rusqlite_err)?;
            drop(stmt);

            for task in &mut running {
                task.status = TaskStatus::Queued;
                let (id, status, _eta_ms, _enqueue_ms, _priority, json) = Self::marshal(task)?;
                conn.execute(
                    "UPDATE tasks SET status = ?2, task_json = ?3 WHERE id = ?1",
                    rusqlite::params![id, status, json],
                )
                .map_err(rusqlite_err)?;
            }
            Ok(running.len())
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(n)
    }

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
            let mut stmt = conn
                .prepare("SELECT schedule_json FROM schedules WHERE id = ?1")
                .map_err(rusqlite_err)?;
            let mut rows = stmt
                .query(rusqlite::params![id_str])
                .map_err(rusqlite_err)?;
            match rows.next().map_err(rusqlite_err)? {
                Some(row) => {
                    let json: String = row.get(0).map_err(rusqlite_err)?;
                    let s: Schedule = serde_json::from_str(&json).map_err(|e| {
                        ChopFlowError::SerializationError(format!(
                            "failed to deserialize schedule: {}",
                            e
                        ))
                    })?;
                    Ok(Some(s))
                }
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(sch)
    }

    async fn list_schedules(&self) -> Result<Vec<Schedule>> {
        let conn = self.conn.clone();
        let schs = tokio::task::spawn_blocking(move || -> Result<Vec<Schedule>> {
            let conn = lock_conn(&conn)?;
            let mut stmt = conn
                .prepare("SELECT schedule_json FROM schedules ORDER BY rowid")
                .map_err(rusqlite_err)?;
            let rows: rusqlite::Result<Vec<Schedule>> = stmt
                .query_map([], |row| {
                    let json: String = row.get(0)?;
                    serde_json::from_str(&json).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })
                })
                .map_err(rusqlite_err)?
                .collect();
            rows.map_err(rusqlite_err)
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(schs)
    }

    async fn delete_schedule(&self, id: &Uuid) -> Result<()> {
        let conn = self.conn.clone();
        let id_str = id.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = lock_conn(&conn)?;
            conn.execute(
                "DELETE FROM schedules WHERE id = ?1",
                rusqlite::params![id_str],
            )
            .map_err(rusqlite_err)?;
            Ok(())
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
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
            rows.map_err(rusqlite_err)
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
            let mut stmt = conn
                .prepare("SELECT task_json FROM tasks WHERE status = ?1 OR status = ?2")
                .map_err(rusqlite_err)?;
            let rows: rusqlite::Result<Vec<Task>> = stmt
                .query_map(
                    rusqlite::params![TaskStatus::Queued as i64, TaskStatus::Running as i64],
                    |row| {
                        let json: String = row.get(0)?;
                        serde_json::from_str(&json).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                0,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })
                    },
                )
                .map_err(rusqlite_err)?
                .collect();
            let all: Vec<Task> = rows.map_err(rusqlite_err)?;
            Ok(all
                .into_iter()
                .filter(|t| t.schedule_id.as_ref().map(|s| s.to_string()) == Some(target.clone()))
                .collect())
        })
        .await
        .map_err(|e| ChopFlowError::Other(e.into()))??;
        Ok(tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::{OverlapPolicy, Schedule, ScheduleKind, TaskTemplate};
    use chrono::Utc;
    use std::collections::HashMap;

    fn queued(name: &str, tags: &[&str]) -> Task {
        let mut t = Task::new(name.into(), serde_json::json!({}));
        for tag in tags {
            t = t.with_tag(*tag);
        }
        t.status = TaskStatus::Queued;
        t
    }

    fn tmpl(name: &str) -> TaskTemplate {
        TaskTemplate {
            name: name.into(),
            payload: serde_json::json!({}),
            tags: vec![],
            resources: HashMap::new(),
            max_retries: 3,
            priority: 0,
        }
    }

    fn cron_schedule(name: &str, cron: &str) -> Schedule {
        Schedule::new(
            name.into(),
            tmpl(name),
            ScheduleKind::Cron { cron: cron.into() },
            OverlapPolicy::Skip,
        )
        .unwrap()
    }

    async fn check_in_memory() {
        let s = InMemoryStorage::new();

        // insert + get
        let t = queued("a", &["gpu"]);
        s.insert(t.clone()).await.unwrap();
        assert_eq!(s.get(&t.id).await.unwrap().unwrap().id, t.id);

        // count_pending / count_by_status
        assert_eq!(s.count_pending().await.unwrap(), 1);
        let counts = s.count_by_status().await.unwrap();
        assert_eq!(counts.queued, 1);

        // claim_ready marks Running and returns the task
        let claimed = s.claim_ready(&["gpu".into()], 10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].status, TaskStatus::Running);
        // No longer pending
        assert_eq!(s.count_pending().await.unwrap(), 0);

        // A second claim returns nothing (no double-claim)
        let again = s.claim_ready(&["gpu".into()], 10).await.unwrap();
        assert!(again.is_empty());
    }

    #[tokio::test]
    async fn in_memory_basic_and_no_double_claim() {
        check_in_memory().await;
    }

    #[tokio::test]
    async fn in_memory_tag_intersection_and_eta_gating() {
        let s = InMemoryStorage::new();
        s.insert(queued("gpu_task", &["gpu"])).await.unwrap();
        s.insert(queued("cpu_task", &["cpu"])).await.unwrap();

        // Worker with cpu tag only gets the cpu task.
        let got = s.claim_ready(&["cpu".into()], 10).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "cpu_task");

        // Empty tags claims any remaining ready task.
        let got = s.claim_ready(&[], 10).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "gpu_task");

        // ETA gating: a task scheduled in the future is not claimed.
        let future = Utc::now() + chrono::Duration::hours(1);
        let mut delayed = queued("later", &[]);
        delayed.eta = Some(future);
        s.insert(delayed).await.unwrap();
        let got = s.claim_ready(&[], 10).await.unwrap();
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn in_memory_list_filters_and_paginates() {
        let s = InMemoryStorage::new();
        for i in 0..5 {
            s.insert(queued(&format!("t{}", i), &[])).await.unwrap();
        }
        // All
        let all = s.list(&TaskFilter::default()).await.unwrap();
        assert_eq!(all.len(), 5);

        // Limit
        let two = s
            .list(&TaskFilter {
                limit: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(two.len(), 2);

        // Offset
        let after = s
            .list(&TaskFilter {
                offset: 3,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(after.len(), 2);
    }

    #[tokio::test]
    async fn in_memory_reconcile_resets_running() {
        let s = InMemoryStorage::new();
        let mut t = queued("running", &[]);
        t.status = TaskStatus::Running;
        s.insert(t).await.unwrap();
        assert_eq!(s.count_by_status().await.unwrap().running, 1);

        let n = s.reconcile().await.unwrap();
        assert_eq!(n, 1);
        assert_eq!(s.count_pending().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn sqlite_round_trip_and_claim() {
        let s = SqliteStorage::open_in_memory().unwrap();
        let t = queued("a", &["gpu"]);
        s.insert(t.clone()).await.unwrap();
        assert_eq!(s.get(&t.id).await.unwrap().unwrap().name, "a");
        assert_eq!(s.count_pending().await.unwrap(), 1);

        let claimed = s.claim_ready(&["gpu".into()], 10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].status, TaskStatus::Running);
        assert_eq!(s.count_pending().await.unwrap(), 0);

        // No double-claim.
        let again = s.claim_ready(&["gpu".into()], 10).await.unwrap();
        assert!(again.is_empty());
    }

    #[tokio::test]
    async fn sqlite_reconcile_resets_running() {
        let s = SqliteStorage::open_in_memory().unwrap();
        let mut t = queued("running", &[]);
        t.status = TaskStatus::Running;
        let id = t.id;
        s.insert(t).await.unwrap();
        let n = s.reconcile().await.unwrap();
        assert_eq!(n, 1);
        assert_eq!(s.count_pending().await.unwrap(), 1);

        // The authoritative status lives in task_json (what reads return), so
        // the reconciled task must read back as Queued, not just be counted as
        // pending via the status index column.
        let got = s.get(&id).await.unwrap().unwrap();
        assert_eq!(got.status, TaskStatus::Queued);
    }

    #[tokio::test]
    async fn in_memory_schedule_crud() {
        let s = InMemoryStorage::new();
        let sch = cron_schedule("nightly", "0 9 * * *");
        s.insert_schedule(sch.clone()).await.unwrap();
        assert_eq!(
            s.get_schedule(&sch.id).await.unwrap().unwrap().name,
            "nightly"
        );

        let listed = s.list_schedules().await.unwrap();
        assert_eq!(listed.len(), 1);

        let mut updated = sch.clone();
        updated.enabled = false;
        s.update_schedule(updated.clone()).await.unwrap();
        assert!(!s.get_schedule(&sch.id).await.unwrap().unwrap().enabled);

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

    #[tokio::test]
    async fn sqlite_schedule_crud() {
        let s = SqliteStorage::open_in_memory().unwrap();
        let sch = cron_schedule("nightly", "0 9 * * *");
        s.insert_schedule(sch.clone()).await.unwrap();
        assert_eq!(
            s.get_schedule(&sch.id).await.unwrap().unwrap().name,
            "nightly"
        );
        assert_eq!(s.list_schedules().await.unwrap().len(), 1);

        let mut updated = sch.clone();
        updated.enabled = false;
        s.update_schedule(updated).await.unwrap();
        assert!(!s.get_schedule(&sch.id).await.unwrap().unwrap().enabled);

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

    fn priority_task(name: &str, priority: i32) -> Task {
        let mut t = queued(name, &[]);
        t.priority = priority;
        t
    }

    #[tokio::test]
    async fn in_memory_claim_ready_respects_priority() {
        let s = InMemoryStorage::new();
        // Low-priority enqueued first, high-priority second.
        s.insert(priority_task("low", 1)).await.unwrap();
        s.insert(priority_task("high", 9)).await.unwrap();

        // Claim one: the high-priority task wins despite being enqueued second.
        let claimed = s.claim_ready(&[], 1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].name, "high");
        assert_eq!(claimed[0].priority, 9);
    }

    #[tokio::test]
    async fn sqlite_claim_ready_respects_priority() {
        let s = SqliteStorage::open_in_memory().unwrap();
        s.insert(priority_task("low", 1)).await.unwrap();
        s.insert(priority_task("high", 9)).await.unwrap();

        let claimed = s.claim_ready(&[], 1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].name, "high");
    }

    #[tokio::test]
    async fn claim_ready_priority_then_fifo_within_tier() {
        // Same priority: FIFO by enqueue_time (low enqueued first wins).
        let s = InMemoryStorage::new();
        s.insert(priority_task("first", 5)).await.unwrap();
        s.insert(priority_task("second", 5)).await.unwrap();
        let claimed = s.claim_ready(&[], 1).await.unwrap();
        assert_eq!(claimed[0].name, "first");
    }

    #[tokio::test]
    async fn sqlite_priority_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pq.db");
        let p = path.to_str().unwrap().to_string();

        let t = priority_task("vip", 7);
        let id = t.id;
        {
            let s = SqliteStorage::open(&p).unwrap();
            s.insert(t).await.unwrap();
        }
        // Drop the storage, reopen the same file — priority must survive.
        let s = SqliteStorage::open(&p).unwrap();
        let got = s.get(&id).await.unwrap().unwrap();
        assert_eq!(got.priority, 7);
    }

    #[tokio::test]
    async fn sqlite_migrates_pre_existing_db_without_priority_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");

        // A real task serialized the old way (no priority in JSON is fine — serde
        // would default it, but here we just need a valid row to round-trip).
        let t = priority_task("legacy", 0);
        let id = t.id;
        let json = serde_json::to_string(&t).unwrap();
        let enqueue_ms = t.enqueue_time.timestamp_millis();

        // Build a DB with the OLD schema (no priority column) and insert the row.
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, status INTEGER, eta_ms INTEGER, enqueue_ms INTEGER, task_json TEXT);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks (id, status, eta_ms, enqueue_ms, task_json) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![t.id.to_string(), t.status as i64, Option::<i64>::None, enqueue_ms, json],
        )
        .unwrap();
        drop(conn);

        // Opening via SqliteStorage must add the priority column idempotently
        // and the legacy row must read back with priority 0.
        let s = SqliteStorage::open(path.to_str().unwrap()).unwrap();
        let got = s.get(&id).await.unwrap().unwrap();
        assert_eq!(got.priority, 0, "legacy row should default to priority 0");
    }
}
