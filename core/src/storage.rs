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
use std::collections::HashMap;
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

/// In-memory `Storage` backed by a `HashMap` under a `tokio::Mutex`.
pub struct InMemoryStorage {
    tasks: Arc<Mutex<HashMap<Uuid, Task>>>,
    schedules: Arc<Mutex<HashMap<Uuid, Schedule>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            schedules: Arc::new(Mutex::new(HashMap::new())),
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
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn insert(&self, task: Task) -> Result<()> {
        let mut tasks = self.tasks.lock().await;
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
        let tasks = self.tasks.lock().await;
        Ok(tasks.values().filter(|t| t.status == TaskStatus::Queued).count())
    }

    async fn count_by_status(&self) -> Result<StatusCounts> {
        let tasks = self.tasks.lock().await;
        let mut counts = StatusCounts::default();
        for t in tasks.values() {
            match t.status {
                TaskStatus::Queued => counts.queued += 1,
                TaskStatus::Running => counts.running += 1,
                TaskStatus::Completed => counts.completed += 1,
                TaskStatus::Failed => counts.failed += 1,
                TaskStatus::DeadLettered => counts.dead_lettered += 1,
                TaskStatus::Cancelled => counts.cancelled += 1,
                TaskStatus::Created => {}
            }
        }
        Ok(counts)
    }

    async fn claim_ready(&self, tags: &[String], max: usize) -> Result<Vec<Task>> {
        let max = max.max(1);
        let mut tasks = self.tasks.lock().await;
        let now = chrono::Utc::now();

        // Collect ids of ready, matching, queued tasks (must collect first
        // because we can't mutate while iterating the borrow).
        let ids: Vec<Uuid> = tasks
            .values()
            .filter(|t| t.status == TaskStatus::Queued && t.is_ready_at(now) && Self::tags_match(t, tags))
            .take(max)
            .map(|t| t.id)
            .collect();

        let mut claimed = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(task) = tasks.get_mut(&id) {
                task.mark_running();
                claimed.push(task.clone());
            }
        }
        Ok(claimed)
    }

    async fn reconcile(&self) -> Result<usize> {
        let mut tasks = self.tasks.lock().await;
        let mut n = 0;
        for task in tasks.values_mut() {
            if task.status == TaskStatus::Running {
                task.status = TaskStatus::Queued;
                n += 1;
            }
        }
        Ok(n)
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
            .filter(|t| t.schedule_id == Some(*schedule_id)
                && matches!(t.status, TaskStatus::Queued | TaskStatus::Running))
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
        Ok(Self {
            conn: Arc::new(std::sync::Mutex::new(conn)),
        })
    }

    /// Serialize a task to its JSON blob and extract the indexed columns.
    fn marshal(task: &Task) -> Result<(String, i64, Option<i64>, i64, String)> {
        let id = task.id.to_string();
        let status = task.status as i64;
        let eta_ms = task.eta.map(|eta| eta.timestamp_millis());
        let enqueue_ms = task.enqueue_time.timestamp_millis();
        let json = serde_json::to_string(task).map_err(|e| {
            ChopFlowError::SerializationError(format!("failed to serialize task: {}", e))
        })?;
        Ok((id, status, eta_ms, enqueue_ms, json))
    }

    /// Deserialize a row into a `Task`.
    fn unmarshal(row: &rusqlite::Row<'_>) -> rusqlite::Result<Task> {
        let json: String = row.get("task_json")?;
        serde_json::from_str(&json).map_err(|e| rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            Box::new(e),
        ))
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
            let (id, status, eta_ms, enqueue_ms, json) = Self::marshal(&task)?;
            let conn = lock_conn(&conn)?;
            conn.execute(
                "INSERT OR REPLACE INTO tasks (id, status, eta_ms, enqueue_ms, task_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![id, status, eta_ms, enqueue_ms, json],
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
            let mut rows = stmt.query(rusqlite::params![id_str]).map_err(rusqlite_err)?;
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
            let limit = if filter.limit == 0 { i64::MAX } else { filter.limit as i64 };

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
                .chain(std::iter::once(Box::new(filter.offset as i64) as Box<dyn rusqlite::ToSql>))
                .collect();

            let params: Vec<&dyn rusqlite::ToSql> =
                params_iter.iter().map(Box::as_ref).collect();

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

            // Select candidate ready, queued tasks ordered by enqueue time.
            // Tag filtering happens in Rust (tags live inside the JSON blob);
            // the connection Mutex guarantees atomicity between select and
            // update so no two callers claim the same task.
            let mut stmt = conn
                .prepare(
                    "SELECT * FROM tasks
                     WHERE status = ?1 AND (eta_ms IS NULL OR eta_ms <= ?2)
                     ORDER BY enqueue_ms, id",
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
                let (id, status, eta_ms, enqueue_ms, json) = Self::marshal(task)?;
                conn.execute(
                    "UPDATE tasks SET status = ?2, eta_ms = ?3, enqueue_ms = ?4, task_json = ?5 WHERE id = ?1",
                    rusqlite::params![id, status, eta_ms, enqueue_ms, json],
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
                let (id, status, _eta_ms, _enqueue_ms, json) = Self::marshal(task)?;
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
            let mut stmt = conn.prepare("SELECT schedule_json FROM schedules WHERE id = ?1").map_err(rusqlite_err)?;
            let mut rows = stmt.query(rusqlite::params![id_str]).map_err(rusqlite_err)?;
            match rows.next().map_err(rusqlite_err)? {
                Some(row) => {
                    let json: String = row.get(0).map_err(rusqlite_err)?;
                    let s: Schedule = serde_json::from_str(&json).map_err(|e| ChopFlowError::SerializationError(format!("failed to deserialize schedule: {}", e)))?;
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
            rows.map_err(rusqlite_err)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use crate::schedule::{OverlapPolicy, Schedule, ScheduleKind, TaskTemplate};
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
        }
    }

    fn cron_schedule(name: &str, cron: &str) -> Schedule {
        Schedule::new(name.into(), tmpl(name), ScheduleKind::Cron { cron: cron.into() }, OverlapPolicy::Skip).unwrap()
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
}
