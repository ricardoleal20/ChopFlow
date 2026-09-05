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
    enqueue_time: i64,
    eta: Option<i64>,
    resources: HashMap<String, u32>,
    result: Option<String>,
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
            enqueue_time: t.enqueue_time.timestamp_millis(),
            eta: t.eta.map(|e| e.timestamp_millis()),
            resources: t.resources,
            result: t.result,
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
        .route("/workers", get(list_workers));

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
    resp.headers_mut().insert(header::CONTENT_TYPE, mime.as_ref().parse().unwrap());
    resp
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

type SharedState = State<Arc<BrokerState>>;

async fn stats(State(state): SharedState) -> Result<Json<StatsDto>, Json<ErrorResponse>> {
    let counts = state
        .storage
        .count_by_status()
        .await
        .map_err(|e| internal(e))?;

    let workers = state
        .dispatcher
        .lock()
        .await
        .list_workers()
        .await
        .map_err(|e| internal(e))?;

    let tasks_processing: usize = workers.iter().map(|w| w.assigned_tasks.len()).sum();
    let active_workers = workers.iter().filter(|w| w.is_alive()).count();
    let total_tasks = state.storage.list(&TaskFilter::default()).await.map_err(|e| internal(e))?.len();

    Ok(Json(StatsDto {
        queue_length: counts.queued,
        tasks_processing,
        tasks_completed: counts.completed,
        tasks_failed: counts.failed + counts.dead_lettered,
        active_workers,
        total_tasks,
    }))
}

async fn list_tasks(
    State(state): SharedState,
    Query(q): Query<ListQuery>,
) -> Result<Json<TaskListResponse>, Json<ErrorResponse>> {
    let statuses = q.status.as_deref().and_then(parse_status).into_iter().collect::<Vec<_>>();
    let filter = TaskFilter {
        statuses,
        limit: q.limit.unwrap_or(0),
        offset: q.offset.unwrap_or(0),
    };

    let tasks = state.storage.list(&filter).await.map_err(|e| internal(e))?;
    let total = state
        .storage
        .list(&TaskFilter::default())
        .await
        .map_err(|e| internal(e))?
        .len();

    let tasks: Vec<TaskDto> = tasks.into_iter().map(TaskDto::from).collect();
    Ok(Json(TaskListResponse { tasks, total }))
}

async fn get_task(
    State(state): SharedState,
    Path(id): Path<String>,
) -> Result<Json<TaskDto>, Json<ErrorResponse>> {
    let uuid = Uuid::parse_str(&id).map_err(|_| bad("Invalid task ID format"))?;
    let task = state
        .storage
        .get(&uuid)
        .await
        .map_err(|e| internal(e))?
        .ok_or_else(|| not_found(&format!("Task not found: {id}")))?;
    Ok(Json(TaskDto::from(task)))
}

async fn enqueue(
    State(state): SharedState,
    Json(body): Json<EnqueueBody>,
) -> Result<(StatusCode, Json<EnqueueResponse>), Json<ErrorResponse>> {
    let mut task = Task::new(body.name, body.payload).with_tags(body.tags);
    if body.max_retries > 0 {
        task = task.with_max_retries(body.max_retries);
    }
    for (resource, amount) in body.resources {
        task = task.with_resource(resource, amount);
    }
    task.status = TaskStatus::Queued;

    state.storage.insert(task.clone()).await.map_err(|e| internal(e))?;

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
) -> Result<Json<CancelResponse>, Json<ErrorResponse>> {
    let task_id = Uuid::parse_str(&id).map_err(|_| bad("Invalid task ID format"))?;

    let Some(mut task) = state.storage.get(&task_id).await.map_err(|e| internal(e))? else {
        return Ok(Json(CancelResponse { success: false }));
    };

    let is_terminal = matches!(
        task.status,
        TaskStatus::Completed | TaskStatus::Failed | TaskStatus::DeadLettered | TaskStatus::Cancelled
    );
    if is_terminal {
        return Ok(Json(CancelResponse { success: false }));
    }

    let was_running = task.status == TaskStatus::Running;
    task.mark_cancelled();
    state.storage.insert(task.clone()).await.map_err(|e| internal(e))?;

    if was_running {
        let workers = state.dispatcher.lock().await.list_workers().await.map_err(|e| internal(e))?;
        for worker in workers {
            if worker.assigned_tasks.contains(&task_id) {
                state
                    .dispatcher
                    .lock()
                    .await
                    .release_task(&worker.id, &task)
                    .await
                    .map_err(|e| internal(e))?;
                break;
            }
        }
    }

    Ok(Json(CancelResponse { success: true }))
}

async fn list_workers(State(state): SharedState) -> Result<Json<Vec<WorkerDto>>, Json<ErrorResponse>> {
    let workers = state.dispatcher.lock().await.list_workers().await.map_err(|e| internal(e))?;
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
// Error helpers
// ---------------------------------------------------------------------------

fn internal<E: std::fmt::Display>(e: E) -> Json<ErrorResponse> {
    Json(ErrorResponse {
        error: format!("internal error: {e}"),
    })
}

fn bad(msg: &str) -> Json<ErrorResponse> {
    Json(ErrorResponse {
        error: msg.to_string(),
    })
}

fn not_found(msg: &str) -> Json<ErrorResponse> {
    Json(ErrorResponse {
        error: msg.to_string(),
    })
}
