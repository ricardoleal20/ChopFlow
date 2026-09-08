/*!
# HTTP / JSON API + embedded dashboard

A thin REST layer that shares the broker's live [`BrokerState`] with the gRPC
service, plus a static-file server for the embedded web UI. This is what the
ChopFlow dashboard talks to: tasks enqueued from the UI are immediately
visible to gRPC workers (and vice versa) because both read the same storage.

## Endpoints
- `GET  /api/stats`            — queue length, processing/completed/failed counts, active workers
- `GET  /api/tasks`            — list tasks (`?status=&limit=&offset=`)
- `GET  /api/tasks/:id`        — single task
- `POST /api/tasks`            — enqueue a task
- `POST /api/tasks/:id/cancel` — cancel a non-terminal task
- `GET  /api/workers`          — registered workers + liveness

Everything else under `/` serves the embedded SPA from `ui/dist`.
*/

use crate::BrokerState;
use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chopflow_core::schedule::{next_fire, OverlapPolicy, Schedule, ScheduleKind, TaskTemplate};
use chopflow_core::storage::TaskFilter;
use chopflow_core::task::{Task, TaskStatus};
use chopflow_core::Dispatcher;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Embedded build output of the Vite app in `broker/ui/dist`.
#[derive(RustEmbed)]
#[folder = "ui/dist"]
struct UiAsset;

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

/// A task as the dashboard sees it. Status is a lowercase string (not the raw
/// enum discriminant) so the frontend never has to know the numeric mapping.
#[derive(Debug, Serialize)]
struct TaskDto {
    id: String,
    name: String,
    payload: serde_json::Value,
    tags: Vec<String>,
    status: String,
    retry_count: u32,
    max_retries: u32,
    priority: i32,
    enqueue_time: i64,
    eta: Option<i64>,
    resources: HashMap<String, u32>,
    result: Option<String>,
    schedule_id: Option<String>,
}

impl From<Task> for TaskDto {
    fn from(t: Task) -> Self {
        Self {
            id: t.id.to_string(),
            name: t.name,
            payload: t.payload,
            tags: t.tags,
            status: status_str(t.status).to_string(),
            retry_count: t.retry_count,
            max_retries: t.max_retries,
            priority: t.priority,
            enqueue_time: t.enqueue_time.timestamp_millis(),
            eta: t.eta.map(|e| e.timestamp_millis()),
            resources: t.resources,
            result: t.result,
            schedule_id: t.schedule_id.map(|u| u.to_string()),
        }
    }
}

fn status_str(s: TaskStatus) -> &'static str {
    match s {
        TaskStatus::Created => "created",
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::DeadLettered => "dead-lettered",
        TaskStatus::Cancelled => "cancelled",
    }
}

fn parse_status(s: &str) -> Option<TaskStatus> {
    match s.to_ascii_lowercase().as_str() {
        "created" => Some(TaskStatus::Created),
        "queued" => Some(TaskStatus::Queued),
        "running" => Some(TaskStatus::Running),
        "completed" => Some(TaskStatus::Completed),
        "failed" => Some(TaskStatus::Failed),
        "dead-lettered" | "deadlettered" => Some(TaskStatus::DeadLettered),
        "cancelled" => Some(TaskStatus::Cancelled),
        _ => None,
    }
}

#[derive(Debug, Serialize)]
struct StatsDto {
    queue_length: usize,
    tasks_processing: usize,
    tasks_completed: usize,
    tasks_failed: usize,
    active_workers: usize,
    total_tasks: usize,
    schedules: usize,
}

#[derive(Debug, Serialize)]
struct WorkerDto {
    id: String,
    address: String,
    tags: Vec<String>,
    alive: bool,
    assigned_tasks: usize,
    resources_total: HashMap<String, u32>,
    resources_available: HashMap<String, u32>,
    last_heartbeat: i64,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    status: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct EnqueueBody {
    name: String,
    payload: serde_json::Value,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    max_retries: u32,
    #[serde(default)]
    resources: HashMap<String, u32>,
    #[serde(default)]
    priority: i32,
}

#[derive(Debug, Serialize)]
struct EnqueueResponse {
    task_id: String,
}

#[derive(Debug, Serialize)]
struct TaskListResponse {
    tasks: Vec<TaskDto>,
    total: usize,
}

#[derive(Debug, Serialize)]
struct CancelResponse {
    success: bool,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

/// API error response with an HTTP status code.
type ApiError = (StatusCode, Json<ErrorResponse>);

// ---------------------------------------------------------------------------
// Schedule DTOs
// ---------------------------------------------------------------------------

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
                let _ = next_fire(
                    &ScheduleKind::Cron { cron: cron.clone() },
                    chrono::Utc::now(),
                )
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
            ScheduleKind::OneShot { eta } => ScheduleKindDto::Oneshot {
                eta: eta.to_rfc3339(),
            },
        };
        Self {
            id: s.id.to_string(),
            name: s.name,
            task_template: s.task_template,
            overlap_policy: match s.overlap_policy {
                OverlapPolicy::Skip => "skip",
                OverlapPolicy::Coalesce => "coalesce",
                OverlapPolicy::Allow => "allow",
            }
            .to_string(),
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
fn default_overlap() -> String {
    "skip".into()
}

#[derive(Debug, Deserialize)]
struct PatchScheduleBody {
    enabled: Option<bool>,
    overlap_policy: Option<String>,
    cron: Option<String>,
}

#[derive(Debug, Serialize)]
struct ScheduleIdResponse {
    schedule_id: String,
}
#[derive(Debug, Serialize)]
struct SuccessResponse {
    success: bool,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the HTTP router (API + embedded UI) over shared broker state.
pub fn router(state: BrokerState) -> Router {
    let api = Router::new()
        .route("/stats", get(stats))
        .route("/tasks", get(list_tasks).post(enqueue))
        .route("/tasks/:id", get(get_task))
        .route("/tasks/:id/cancel", post(cancel_task))
        .route("/workers", get(list_workers))
        .route("/schedules", get(list_schedules).post(create_schedule))
        .route(
            "/schedules/:id",
            get(get_schedule)
                .patch(patch_schedule)
                .delete(delete_schedule),
        );

    Router::new()
        .nest("/api", api)
        .route("/healthz", get(|| async { "ok" }))
        .fallback(static_handler)
        .with_state(Arc::new(state))
}

/// Serve the embedded SPA. Any path that isn't an API route and isn't a real
/// asset falls back to `index.html` (client-side routing).
async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');

    // Try the exact file first (e.g. /assets/index-abc123.js).
    if let Some(asset) = UiAsset::get(path) {
        return asset_response(path, asset);
    }

    // SPA fallback.
    if let Some(asset) = UiAsset::get("index.html") {
        return asset_response("index.html", asset);
    }

    (
        StatusCode::SERVICE_UNAVAILABLE,
        "ChopFlow UI not built. Run: pnpm --dir broker/ui install && pnpm --dir broker/ui build",
    )
        .into_response()
}

fn asset_response(path: &str, asset: rust_embed::EmbeddedFile) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let body = asset.data.into_owned();
    let mut resp = (StatusCode::OK, body).into_response();
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, mime.as_ref().parse().unwrap());
    resp
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

type SharedState = State<Arc<BrokerState>>;

async fn stats(State(state): SharedState) -> Result<Json<StatsDto>, ApiError> {
    let counts = state.storage.count_by_status().await.map_err(internal)?;

    let workers = state
        .dispatcher
        .lock()
        .await
        .list_workers()
        .await
        .map_err(internal)?;

    let tasks_processing: usize = workers.iter().map(|w| w.assigned_tasks.len()).sum();
    let active_workers = workers.iter().filter(|w| w.is_alive()).count();
    // Derive total_tasks from the status counts we already have, rather than
    // calling `list()` (which clones every task into a Vec just to count them).
    // At 100k+ tasks the clone was the dominant cost in /api/stats.
    let total_tasks = counts.queued
        + counts.running
        + counts.completed
        + counts.failed
        + counts.dead_lettered
        + counts.cancelled;
    let schedules = state
        .storage
        .list_schedules()
        .await
        .map_err(internal)?
        .iter()
        .filter(|s| s.enabled)
        .count();

    Ok(Json(StatsDto {
        queue_length: counts.queued,
        tasks_processing,
        tasks_completed: counts.completed,
        tasks_failed: counts.failed + counts.dead_lettered,
        active_workers,
        total_tasks,
        schedules,
    }))
}

async fn list_tasks(
    State(state): SharedState,
    Query(q): Query<ListQuery>,
) -> Result<Json<TaskListResponse>, ApiError> {
    let statuses = q
        .status
        .as_deref()
        .and_then(parse_status)
        .into_iter()
        .collect::<Vec<_>>();
    let filter = TaskFilter {
        statuses,
        limit: q.limit.unwrap_or(0),
        offset: q.offset.unwrap_or(0),
    };

    let tasks = state.storage.list(&filter).await.map_err(internal)?;
    let total = state
        .storage
        .list(&TaskFilter::default())
        .await
        .map_err(internal)?
        .len();

    let tasks: Vec<TaskDto> = tasks.into_iter().map(TaskDto::from).collect();
    Ok(Json(TaskListResponse { tasks, total }))
}

async fn get_task(
    State(state): SharedState,
    Path(id): Path<String>,
) -> Result<Json<TaskDto>, ApiError> {
    let uuid = Uuid::parse_str(&id).map_err(|_| bad("Invalid task ID format"))?;
    let task = state
        .storage
        .get(&uuid)
        .await
        .map_err(internal)?
        .ok_or_else(|| not_found(&format!("Task not found: {id}")))?;
    Ok(Json(TaskDto::from(task)))
}

async fn enqueue(
    State(state): SharedState,
    Json(body): Json<EnqueueBody>,
) -> Result<(StatusCode, Json<EnqueueResponse>), ApiError> {
    let mut task = Task::new(body.name, body.payload).with_tags(body.tags);
    if body.max_retries > 0 {
        task = task.with_max_retries(body.max_retries);
    }
    for (resource, amount) in body.resources {
        task = task.with_resource(resource, amount);
    }
    task = task.with_priority(body.priority);
    task.status = TaskStatus::Queued;

    state.storage.insert(task.clone()).await.map_err(internal)?;

    Ok((
        StatusCode::CREATED,
        Json(EnqueueResponse {
            task_id: task.id.to_string(),
        }),
    ))
}

async fn cancel_task(
    State(state): SharedState,
    Path(id): Path<String>,
) -> Result<Json<CancelResponse>, ApiError> {
    let task_id = Uuid::parse_str(&id).map_err(|_| bad("Invalid task ID format"))?;

    let Some(mut task) = state.storage.get(&task_id).await.map_err(internal)? else {
        return Ok(Json(CancelResponse { success: false }));
    };

    let is_terminal = matches!(
        task.status,
        TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::DeadLettered
            | TaskStatus::Cancelled
    );
    if is_terminal {
        return Ok(Json(CancelResponse { success: false }));
    }

    let was_running = task.status == TaskStatus::Running;
    task.mark_cancelled();
    state.storage.insert(task.clone()).await.map_err(internal)?;

    if was_running {
        let workers = state
            .dispatcher
            .lock()
            .await
            .list_workers()
            .await
            .map_err(internal)?;
        for worker in workers {
            if worker.assigned_tasks.contains(&task_id) {
                state
                    .dispatcher
                    .lock()
                    .await
                    .release_task(&worker.id, &task)
                    .await
                    .map_err(internal)?;
                break;
            }
        }
    }

    Ok(Json(CancelResponse { success: true }))
}

async fn list_workers(State(state): SharedState) -> Result<Json<Vec<WorkerDto>>, ApiError> {
    let workers = state
        .dispatcher
        .lock()
        .await
        .list_workers()
        .await
        .map_err(internal)?;
    let dtos = workers
        .into_iter()
        .map(|w| {
            let alive = w.is_alive();
            let assigned_tasks = w.assigned_tasks.len();
            let last_heartbeat = w.last_heartbeat.timestamp_millis();
            let resources_total = w.resources.total.clone();
            let resources_available = w.resources.available.clone();
            WorkerDto {
                id: w.id.to_string(),
                address: w.address,
                tags: w.tags,
                alive,
                assigned_tasks,
                resources_total,
                resources_available,
                last_heartbeat,
            }
        })
        .collect();
    Ok(Json(dtos))
}

// ---------------------------------------------------------------------------
// Schedule handlers
// ---------------------------------------------------------------------------

async fn list_schedules(State(state): SharedState) -> Result<Json<Vec<ScheduleDto>>, ApiError> {
    let schs = state.storage.list_schedules().await.map_err(internal)?;
    Ok(Json(schs.into_iter().map(ScheduleDto::from).collect()))
}

async fn get_schedule(
    State(state): SharedState,
    Path(id): Path<String>,
) -> Result<Json<ScheduleDto>, ApiError> {
    let uuid = Uuid::parse_str(&id).map_err(|_| bad("Invalid schedule ID format"))?;
    let s = state
        .storage
        .get_schedule(&uuid)
        .await
        .map_err(internal)?
        .ok_or_else(|| not_found(&format!("Schedule not found: {id}")))?;
    Ok(Json(ScheduleDto::from(s)))
}

fn parse_overlap(s: &str) -> Result<OverlapPolicy, ApiError> {
    match s {
        "skip" => Ok(OverlapPolicy::Skip),
        "coalesce" => Ok(OverlapPolicy::Coalesce),
        "allow" => Ok(OverlapPolicy::Allow),
        other => Err(bad(&format!(
            "invalid overlap_policy: {} (skip|coalesce|allow)",
            other
        ))),
    }
}

async fn create_schedule(
    State(state): SharedState,
    Json(body): Json<CreateScheduleBody>,
) -> Result<(StatusCode, Json<ScheduleIdResponse>), ApiError> {
    let kind: ScheduleKind = match body.kind.try_into() {
        Ok(k) => k,
        Err(e) => return Err(bad(&e)),
    };
    let overlap = parse_overlap(&body.overlap_policy)?;
    let schedule = Schedule::new(body.name, body.task_template, kind, overlap)
        .map_err(|e| bad(&e.to_string()))?;
    let id = schedule.id;
    state
        .storage
        .insert_schedule(schedule)
        .await
        .map_err(internal)?;
    Ok((
        StatusCode::CREATED,
        Json(ScheduleIdResponse {
            schedule_id: id.to_string(),
        }),
    ))
}

async fn patch_schedule(
    State(state): SharedState,
    Path(id): Path<String>,
    Json(body): Json<PatchScheduleBody>,
) -> Result<Json<ScheduleDto>, ApiError> {
    let uuid = Uuid::parse_str(&id).map_err(|_| bad("Invalid schedule ID format"))?;
    let mut s = state
        .storage
        .get_schedule(&uuid)
        .await
        .map_err(internal)?
        .ok_or_else(|| not_found(&format!("Schedule not found: {id}")))?;
    if let Some(enabled) = body.enabled {
        s.enabled = enabled;
    }
    if let Some(op) = body.overlap_policy {
        s.overlap_policy = parse_overlap(&op)?;
    }
    if let Some(cron) = body.cron {
        let kind = ScheduleKind::Cron { cron: cron.clone() };
        let _ = next_fire(&kind, chrono::Utc::now())
            .map_err(|e| bad(&format!("invalid cron: {}", e)))?;
        s.kind = kind;
        s.next_fire = chopflow_core::schedule::next_fire(&s.kind, chrono::Utc::now())
            .map_err(|e| bad(&e.to_string()))?;
    }
    state
        .storage
        .update_schedule(s.clone())
        .await
        .map_err(internal)?;
    Ok(Json(ScheduleDto::from(s)))
}

async fn delete_schedule(
    State(state): SharedState,
    Path(id): Path<String>,
) -> Result<Json<SuccessResponse>, ApiError> {
    let uuid = Uuid::parse_str(&id).map_err(|_| bad("Invalid schedule ID format"))?;
    state
        .storage
        .delete_schedule(&uuid)
        .await
        .map_err(internal)?;
    Ok(Json(SuccessResponse { success: true }))
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn internal<E: std::fmt::Display>(e: E) -> ApiError {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: format!("internal error: {e}"),
        }),
    )
}

fn bad(msg: &str) -> ApiError {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

fn not_found(msg: &str) -> ApiError {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}
