/*!
# ChopFlow Broker library

The broker is the central coordination point for ChopFlow: it owns the durable
task state ([`chopflow_core::Storage`]), registers workers, and dispatches
tasks to them on demand (pull model via `FetchTasks`).

This crate is a library so that the service can be exercised by integration
tests in `broker/tests/`. The `main.rs` binary is a thin CLI wrapper.
*/

use chopflow_core::dispatcher::{Dispatcher, InMemoryDispatcher, Worker};
use chopflow_core::resources::ResourceAvailability;
use chopflow_core::retry::RetryPolicy;
use chopflow_core::schedule::{next_fire, Schedule, ScheduleKind, OverlapPolicy, TaskTemplate};
use chopflow_core::storage::{Storage, TaskFilter};
use chopflow_core::task::{Task, TaskStatus};

use clap::{Parser, Subcommand};
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info, warn};
use uuid::Uuid;
use std::time::Duration;
use tokio::time::sleep;

// Generate code from protobuf definitions
pub mod chopflow {
    tonic::include_proto!("chopflow");
}

/// HTTP / JSON API + embedded dashboard UI.
pub mod http;

use chopflow::{
    AcknowledgeTaskRequest,
    AcknowledgeTaskResponse,
    CancelTaskRequest,
    CancelTaskResponse,
    CreateScheduleRequest,
    CreateScheduleResponse,
    DeleteScheduleRequest,
    DeleteScheduleResponse,
    EnqueueTaskRequest,
    EnqueueTaskResponse,
    FetchTasksRequest,
    FetchTasksResponse,
    GetQueueStatsRequest,
    GetQueueStatsResponse,
    GetTaskStatusRequest,
    GetTaskStatusResponse,
    ListSchedulesRequest,
    ListSchedulesResponse,
    ListTasksRequest,
    ListTasksResponse,
    ListWorkersResponse,
    OverlapPolicy as ProtoOverlapPolicy,
    RegisterWorkerRequest,
    RegisterWorkerResponse,
    ResourceAvailability as ProtoResourceAvailability,
    Schedule as ProtoSchedule,
    ScheduleKind as ProtoScheduleKind,
    Task as ProtoTask,
    TaskStatus as ProtoTaskStatus,
    TaskTemplate as ProtoTaskTemplate,
    Worker as ProtoWorker,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
};

use chopflow::chop_flow_broker_server::{ChopFlowBroker, ChopFlowBrokerServer};

/// ChopFlow Broker CLI.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the broker
    Start {
        /// Path to config file
        #[arg(long, short, default_value = "config/broker.yml")]
        config: String,

        /// Host to bind to
        #[arg(long, short = 'H', default_value = "127.0.0.1")]
        host: String,

        /// Port to listen on (gRPC)
        #[arg(long, short, default_value = "8000")]
        port: u16,

        /// Port for the HTTP/JSON API + embedded dashboard UI
        #[arg(long, default_value = "8080")]
        http_port: u16,

        /// Task storage backend
        #[arg(long, default_value = "sqlite")]
        storage: String,

        /// Path to the SQLite database file (only used when --storage sqlite)
        #[arg(long, default_value = "chopflow.db")]
        db_path: String,

        /// Open the dashboard UI in the default browser on startup
        #[arg(long, default_value_t = false)]
        open: bool,
    },
}

/// Which storage backend to construct.
pub enum StorageBackend {
    Memory,
    Sqlite { path: String },
}

/// Build a `Storage` from a backend choice.
pub fn build_storage(backend: &StorageBackend) -> std::result::Result<Arc<dyn Storage>, String> {
    match backend {
        StorageBackend::Memory => Ok(Arc::new(chopflow_core::InMemoryStorage::new())),
        StorageBackend::Sqlite { path } => {
            let store = chopflow_core::SqliteStorage::open(path)
                .map_err(|e| format!("failed to open sqlite storage at {}: {}", path, e))?;
            Ok(Arc::new(store))
        }
    }
}

// Conversions between core and proto types
impl From<Task> for ProtoTask {
    fn from(task: Task) -> Self {
        ProtoTask {
            id: task.id.to_string(),
            name: task.name,
            payload: serde_json::to_string(&task.payload).unwrap_or_default(),
            tags: task.tags,
            enqueue_time: Some(prost_types::Timestamp {
                seconds: task.enqueue_time.timestamp(),
                nanos: task.enqueue_time.timestamp_subsec_nanos() as i32,
            }),
            eta: task.eta.map(|eta| prost_types::Timestamp {
                seconds: eta.timestamp(),
                nanos: eta.timestamp_subsec_nanos() as i32,
            }),
            retry_count: task.retry_count,
            max_retries: task.max_retries,
            status: task.status as i32,
            resources: task.resources,
            result: task.result.unwrap_or_default(),
            schedule_id: task.schedule_id.map(|u| u.to_string()).unwrap_or_default(),
            priority: task.priority,
        }
    }
}

impl From<ProtoTaskStatus> for TaskStatus {
    fn from(status: ProtoTaskStatus) -> Self {
        match status {
            ProtoTaskStatus::Created => TaskStatus::Created,
            ProtoTaskStatus::Queued => TaskStatus::Queued,
            ProtoTaskStatus::Running => TaskStatus::Running,
            ProtoTaskStatus::Completed => TaskStatus::Completed,
            ProtoTaskStatus::Failed => TaskStatus::Failed,
            ProtoTaskStatus::Deadlettered => TaskStatus::DeadLettered,
            ProtoTaskStatus::Cancelled => TaskStatus::Cancelled,
        }
    }
}

impl From<Worker> for ProtoWorker {
    fn from(worker: Worker) -> Self {
        ProtoWorker {
            id: worker.id.to_string(),
            address: worker.address,
            tags: worker.tags,
            resources: Some(ProtoResourceAvailability {
                available: worker.resources.available.clone(),
                total: worker.resources.total.clone(),
            }),
            assigned_tasks: worker
                .assigned_tasks
                .iter()
                .map(|id| id.to_string())
                .collect(),
            last_heartbeat: Some(prost_types::Timestamp {
                seconds: worker.last_heartbeat.timestamp(),
                nanos: worker.last_heartbeat.timestamp_subsec_nanos() as i32,
            }),
        }
    }
}

/// Convert a proto `TaskTemplate` into the core type. The proto payload is a
/// JSON string; we parse it into a `serde_json::Value` here.
fn proto_to_template(p: ProtoTaskTemplate) -> std::result::Result<TaskTemplate, Status> {
    let payload: serde_json::Value = serde_json::from_str(&p.payload)
        .map_err(|e| Status::invalid_argument(format!("invalid template payload: {}", e)))?;
    Ok(TaskTemplate { name: p.name, payload, tags: p.tags, resources: p.resources, max_retries: p.max_retries, priority: p.priority })
}

/// Convert a proto `Schedule` into the core `Schedule` (computing `next_fire`
/// via [`Schedule::new`]).
fn proto_to_schedule(p: ProtoSchedule) -> std::result::Result<Schedule, Status> {
    let kind = match p.kind {
        Some(ProtoScheduleKind { kind: Some(chopflow::schedule_kind::Kind::Cron(c)) }) => ScheduleKind::Cron { cron: c },
        Some(ProtoScheduleKind { kind: Some(chopflow::schedule_kind::Kind::Eta(ts)) }) => {
            let eta = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                .ok_or_else(|| Status::invalid_argument("invalid eta"))?;
            ScheduleKind::OneShot { eta }
        }
        _ => return Err(Status::invalid_argument("schedule kind required")),
    };
    let overlap = ProtoOverlapPolicy::try_from(p.overlap_policy)
        .map_err(|_| Status::invalid_argument("invalid overlap_policy"))?;
    let overlap = match overlap {
        ProtoOverlapPolicy::OverlapSkip => OverlapPolicy::Skip,
        ProtoOverlapPolicy::OverlapCoalesce => OverlapPolicy::Coalesce,
        ProtoOverlapPolicy::OverlapAllow => OverlapPolicy::Allow,
    };
    let tmpl = proto_to_template(p.task_template.ok_or_else(|| Status::invalid_argument("task_template required"))?)?;
    let s = Schedule::new(p.name, tmpl, kind, overlap).map_err(|e| Status::invalid_argument(e.to_string()))?;
    Ok(s)
}

/// Convert a core `Schedule` into the proto representation.
fn schedule_to_proto(s: Schedule) -> ProtoSchedule {
    let kind = Some(ProtoScheduleKind {
        kind: Some(match s.kind {
            ScheduleKind::Cron { cron } => chopflow::schedule_kind::Kind::Cron(cron),
            ScheduleKind::OneShot { eta } => chopflow::schedule_kind::Kind::Eta(prost_types::Timestamp {
                seconds: eta.timestamp(), nanos: eta.timestamp_subsec_nanos() as i32,
            }),
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
            tags: s.task_template.tags, resources: s.task_template.resources, max_retries: s.task_template.max_retries, priority: s.task_template.priority,
        }),
        kind, overlap_policy: overlap as i32, enabled: s.enabled,
        last_fired: s.last_fired.map(ts), next_fire: Some(ts(s.next_fire)), created_at: Some(ts(s.created_at)),
    }
}

/// Shared, cheaply-clonable broker state. Both the gRPC service
/// ([`ChopFlowBrokerService`]) and the HTTP/JSON layer ([`http`] module) hold
/// one of these, so a task enqueued via gRPC is immediately visible to the
/// dashboard and vice versa.
#[derive(Clone)]
pub struct BrokerState {
    /// The single source of truth for task state.
    pub storage: Arc<dyn Storage>,
    /// Ephemeral worker registry + resource allocator.
    pub dispatcher: Arc<tokio::sync::Mutex<InMemoryDispatcher>>,
}

impl BrokerState {
    /// Construct shared state backed by the given storage and a fresh
    /// in-memory dispatcher.
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            dispatcher: Arc::new(tokio::sync::Mutex::new(InMemoryDispatcher::new())),
        }
    }
}

/// The gRPC broker service. Task state lives in `state.storage` (the single
/// source of truth); the dispatcher only tracks workers and their resources.
#[derive(Clone)]
pub struct ChopFlowBrokerService {
    state: BrokerState,
}

impl ChopFlowBrokerService {
    /// Construct a service backed by the given storage.
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            state: BrokerState::new(storage),
        }
    }

    /// Construct a service that shares an existing [`BrokerState`] (used to
    /// keep the gRPC service and HTTP layer over the same live state).
    pub fn from_state(state: BrokerState) -> Self {
        Self { state }
    }

    /// Borrow the shared state (used by the HTTP layer).
    pub fn state(&self) -> &BrokerState {
        &self.state
    }

    /// Build the tonic server wrapper.
    pub fn into_server(self) -> ChopFlowBrokerServer<Self> {
        ChopFlowBrokerServer::new(self)
    }

    /// Spawn the background task that fails tasks stuck in `Running` for too
    /// long. Kept best-effort; reconciliation on startup is the main safety
    /// net for crashed workers.
    pub fn spawn_timeout_monitor(&self) {
        let service_clone = self.clone();
        tokio::spawn(async move {
            service_clone.handle_task_timeouts().await;
        });
    }

    /// Spawn the background schedule ticker. Every second it materializes a
    /// Task for each due schedule (subject to the overlap policy) and advances
    /// the schedule. A single bad schedule is logged and skipped.
    pub fn spawn_schedule_ticker(&self) {
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
                    let sid = schedule.id;
                    if let Err(e) = self.state.storage.update_schedule(schedule).await {
                        warn!("failed to update skipped schedule {}: {}", sid, e);
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
            task = task.with_priority(t.priority);
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
            let sid = schedule.id;
            if let Err(e) = self.state.storage.update_schedule(schedule).await {
                warn!("failed to advance schedule {}: {}", sid, e);
            }
        }
        Ok(())
    }

    /// Helper method to handle task timeouts for running tasks.
    async fn handle_task_timeouts(&self) {
        loop {
            sleep(Duration::from_secs(30)).await; // Check every 30 seconds

            let workers = {
                let dispatcher = self.state.dispatcher.lock().await;
                dispatcher.list_workers().await.unwrap_or_default()
            };

            // Collect the set of task ids currently assigned to a worker so
            // we can scan storage for stale running tasks.
            let assigned: Vec<Uuid> = workers
                .iter()
                .flat_map(|w| w.assigned_tasks.clone())
                .collect();

            for task_id in assigned {
                let Some(task) = self.state.storage.get(&task_id).await.unwrap_or(None) else {
                    continue;
                };
                if task.status != TaskStatus::Running {
                    continue;
                }

                // Check if task has been running for too long (e.g., 1 hour)
                let running_time = chrono::Utc::now() - task.enqueue_time;
                if running_time > chrono::Duration::hours(1) {
                    warn!(
                        "Task {} has been running for too long ({}), marking as failed",
                        task_id, running_time
                    );

                    let mut task = task;
                    task.mark_failed();
                    self.finish_failure(&mut task).await;

                    // Release worker resources for the stale task.
                    if let Some(worker) =
                        workers.iter().find(|w| w.assigned_tasks.contains(&task_id))
                    {
                        let mut dispatcher = self.state.dispatcher.lock().await;
                        if let Err(e) = dispatcher.release_task(&worker.id, &task).await {
                            warn!("Failed to release timed-out task resources: {}", e);
                        }
                    }
                }
            }
        }
    }

    /// Apply the retry/dead-letter policy to a failed task and persist it via
    /// storage. Retries re-store the task with a backoff ETA so it won't be
    /// claimed again until the backoff elapses.
    async fn finish_failure(&self, task: &mut Task) {
        if task.status == TaskStatus::DeadLettered {
            warn!(
                "Task {} dead-lettered after {} retries",
                task.id, task.retry_count
            );
            if let Err(e) = self.state.storage.insert(task.clone()).await {
                warn!("Failed to persist dead-lettered task {}: {}", task.id, e);
            }
            return;
        }

        // Retryable: schedule a backoff and re-store as Queued.
        let policy = RetryPolicy::exponential_backoff(task.max_retries);
        if let Some(next) = policy.next_retry_time(task.retry_count) {
            task.eta = Some(next);
        }
        task.status = TaskStatus::Queued;

        info!(
            "Task {} failed, re-queuing for retry (attempt {}/{})",
            task.id, task.retry_count, task.max_retries
        );

        if let Err(e) = self.state.storage.insert(task.clone()).await {
            warn!("Failed to re-enqueue task {} for retry: {}", task.id, e);
        }
    }
}

#[tonic::async_trait]
impl ChopFlowBroker for ChopFlowBrokerService {
    async fn enqueue_task(
        &self,
        request: Request<EnqueueTaskRequest>,
    ) -> std::result::Result<Response<EnqueueTaskResponse>, Status> {
        let req = request.into_inner();

        let payload: serde_json::Value = serde_json::from_str(&req.payload)
            .map_err(|e| Status::invalid_argument(format!("Invalid payload JSON: {}", e)))?;

        let mut task = Task::new(req.name, payload);
        task = task.with_tags(req.tags);

        if let Some(eta_proto) = req.eta {
            let eta_chrono =
                chrono::DateTime::from_timestamp(eta_proto.seconds, eta_proto.nanos as u32)
                    .ok_or_else(|| Status::invalid_argument("Invalid ETA timestamp"))?;
            task = task.with_eta(eta_chrono);
        }

        if req.max_retries > 0 {
            task = task.with_max_retries(req.max_retries);
        }

        for (resource, amount) in req.resources {
            task = task.with_resource(resource, amount);
        }

        task = task.with_priority(req.priority);

        // A single insert is both "store" and "enqueue": queued tasks are
        // just tasks with status Queued, claimable by workers via FetchTasks.
        task.status = TaskStatus::Queued;
        self.state.storage
            .insert(task.clone())
            .await
            .map_err(|e| Status::internal(format!("Failed to enqueue task: {}", e)))?;

        info!(
            "Enqueued task {} (name={}, tags={:?}, resources={:?})",
            task.id, task.name, task.tags, task.resources
        );

        Ok(Response::new(EnqueueTaskResponse {
            task_id: task.id.to_string(),
        }))
    }

    async fn get_task_status(
        &self,
        request: Request<GetTaskStatusRequest>,
    ) -> std::result::Result<Response<GetTaskStatusResponse>, Status> {
        let req = request.into_inner();
        let task_id = Uuid::parse_str(&req.task_id)
            .map_err(|_| Status::invalid_argument("Invalid task ID format"))?;

        let task = self
            .state.storage
            .get(&task_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get task: {}", e)))?
            .ok_or_else(|| Status::not_found(format!("Task not found: {}", task_id)))?;

        Ok(Response::new(GetTaskStatusResponse {
            task: Some(ProtoTask::from(task)),
        }))
    }

    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> std::result::Result<Response<CancelTaskResponse>, Status> {
        let req = request.into_inner();
        let task_id = Uuid::parse_str(&req.task_id)
            .map_err(|_| Status::invalid_argument("Invalid task ID format"))?;

        let Some(mut task) = self
            .state.storage
            .get(&task_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get task: {}", e)))?
        else {
            warn!("Attempted to cancel non-existent task {}", task_id);
            return Ok(Response::new(CancelTaskResponse { success: false }));
        };

        let was_running = task.status == TaskStatus::Running;
        let is_terminal = matches!(
            task.status,
            TaskStatus::Completed
                | TaskStatus::Failed
                | TaskStatus::DeadLettered
                | TaskStatus::Cancelled
        );

        if is_terminal {
            warn!(
                "Task {} cannot be cancelled due to its status: {:?}",
                task_id, task.status
            );
            return Ok(Response::new(CancelTaskResponse { success: false }));
        }

        task.mark_cancelled();
        self.state.storage
            .insert(task.clone())
            .await
            .map_err(|e| Status::internal(format!("Failed to update task: {}", e)))?;

        // If the task was running, release the worker's resources.
        if was_running {
            let workers = {
                let dispatcher = self.state.dispatcher.lock().await;
                dispatcher.list_workers().await.unwrap_or_default()
            };
            for worker in workers {
                if worker.assigned_tasks.contains(&task_id) {
                    let mut dispatcher = self.state.dispatcher.lock().await;
                    if let Err(e) = dispatcher.release_task(&worker.id, &task).await {
                        warn!("Failed to release cancelled task from worker: {}", e);
                    }
                    break;
                }
            }
        }

        info!("Cancelled task {}", task_id);
        Ok(Response::new(CancelTaskResponse { success: true }))
    }

    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> std::result::Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();

        if req.resources.is_empty() {
            return Err(Status::invalid_argument("Worker must specify at least one resource"));
        }

        let worker = Worker {
            id: Uuid::new_v4(),
            address: req.address,
            tags: req.tags,
            resources: ResourceAvailability {
                available: req.resources.clone(),
                total: req.resources,
            },
            assigned_tasks: Vec::new(),
            last_heartbeat: chrono::Utc::now(),
        };

        let worker_id = worker.id;
        let mut dispatcher = self.state.dispatcher.lock().await;
        dispatcher
            .register_worker(worker.clone())
            .await
            .map_err(|e| Status::internal(format!("Failed to register worker: {}", e)))?;

        info!(
            "Registered worker {} at {} with resources: {:?}",
            worker_id, worker.address, worker.resources
        );

        Ok(Response::new(RegisterWorkerResponse {
            worker_id: worker_id.to_string(),
        }))
    }

    async fn worker_heartbeat(
        &self,
        request: Request<WorkerHeartbeatRequest>,
    ) -> std::result::Result<Response<WorkerHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let worker_id = Uuid::parse_str(&req.worker_id)
            .map_err(|_| Status::invalid_argument("Invalid worker ID format"))?;

        let mut dispatcher = self.state.dispatcher.lock().await;
        match dispatcher.heartbeat(&worker_id).await {
            Ok(_) => {
                debug!("Received heartbeat from worker {}", worker_id);
                Ok(Response::new(WorkerHeartbeatResponse { success: true }))
            }
            Err(e) => {
                warn!("Failed to update heartbeat for worker {}: {}", worker_id, e);
                Ok(Response::new(WorkerHeartbeatResponse { success: false }))
            }
        }
    }

    async fn fetch_tasks(
        &self,
        request: Request<FetchTasksRequest>,
    ) -> std::result::Result<Response<FetchTasksResponse>, Status> {
        let req = request.into_inner();
        let worker_id = Uuid::parse_str(&req.worker_id)
            .map_err(|_| Status::invalid_argument("Invalid worker ID format"))?;
        let max_tasks = req.max_tasks.max(1) as usize;

        let worker = {
            let dispatcher = self.state.dispatcher.lock().await;
            dispatcher
                .get_worker(&worker_id)
                .await
                .map_err(|e| Status::internal(format!("Failed to get worker: {}", e)))?
                .ok_or_else(|| Status::not_found(format!("Worker not found: {}", worker_id)))?
        };

        if !worker.is_alive() {
            return Err(Status::failed_precondition(format!(
                "Worker {} is not alive (stale heartbeat)",
                worker_id
            )));
        }

        // Atomically claim ready matching tasks (Queued → Running).
        let claimed = self
            .state.storage
            .claim_ready(&worker.tags, max_tasks)
            .await
            .map_err(|e| Status::internal(format!("Failed to claim tasks: {}", e)))?;

        let mut dispatched = Vec::new();
        for mut task in claimed {
            // Allocate the task's resources on the worker. If the worker
            // can't satisfy them, put the task back to Queued and stop.
            let assign_result = {
                let mut dispatcher = self.state.dispatcher.lock().await;
                dispatcher.assign_task(&worker_id, &task).await
            };

            if let Err(e) = assign_result {
                debug!(
                    "Worker {} cannot take task {} ({}); re-queuing",
                    worker_id, task.id, e
                );
                task.status = TaskStatus::Queued;
                if let Err(e) = self.state.storage.insert(task.clone()).await {
                    warn!("Failed to re-queue task {}: {}", task.id, e);
                }
                break;
            }

            info!(
                "Dispatched task {} to worker {} (resources: {:?})",
                task.id, worker_id, task.resources
            );
            dispatched.push(ProtoTask::from(task));
        }

        Ok(Response::new(FetchTasksResponse { tasks: dispatched }))
    }

    async fn acknowledge_task(
        &self,
        request: Request<AcknowledgeTaskRequest>,
    ) -> std::result::Result<Response<AcknowledgeTaskResponse>, Status> {
        let req = request.into_inner();
        let worker_id = Uuid::parse_str(&req.worker_id)
            .map_err(|_| Status::invalid_argument("Invalid worker ID format"))?;
        let task_id = Uuid::parse_str(&req.task_id)
            .map_err(|_| Status::invalid_argument("Invalid task ID format"))?;

        let mut task = self
            .state.storage
            .get(&task_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get task: {}", e)))?
            .ok_or_else(|| Status::not_found(format!("Task not found: {}", task_id)))?;

        // Release the worker's resources for this task regardless of outcome.
        {
            let mut dispatcher = self.state.dispatcher.lock().await;
            if let Err(e) = dispatcher.release_task(&worker_id, &task).await {
                warn!("Failed to release task {} resources: {}", task_id, e);
            }
        }

        if req.success {
            task.mark_completed_with_result(req.result);
            self.state.storage
                .insert(task.clone())
                .await
                .map_err(|e| Status::internal(format!("Failed to update task: {}", e)))?;
            info!("Task {} completed successfully by worker {}", task_id, worker_id);
        } else {
            task.result = Some(req.result.clone());
            task.mark_failed();
            self.finish_failure(&mut task).await;
        }

        Ok(Response::new(AcknowledgeTaskResponse { success: true }))
    }

    async fn get_queue_stats(
        &self,
        _: Request<GetQueueStatsRequest>,
    ) -> std::result::Result<Response<GetQueueStatsResponse>, Status> {
        let queue_length = self
            .state.storage
            .count_pending()
            .await
            .map_err(|e| Status::internal(format!("Failed to count pending: {}", e)))?;

        let counts = self
            .state.storage
            .count_by_status()
            .await
            .map_err(|e| Status::internal(format!("Failed to count by status: {}", e)))?;

        let workers = {
            let dispatcher = self.state.dispatcher.lock().await;
            dispatcher
                .list_workers()
                .await
                .map_err(|e| Status::internal(format!("Failed to list workers: {}", e)))?
        };

        let active_workers = workers.iter().filter(|w| w.is_alive()).count() as u32;
        let tasks_processing = workers
            .iter()
            .map(|w| w.assigned_tasks.len())
            .sum::<usize>() as u32;

        Ok(Response::new(GetQueueStatsResponse {
            queue_length: queue_length as u32,
            tasks_processing,
            tasks_completed: counts.completed as u32,
            tasks_failed: (counts.failed + counts.dead_lettered) as u32,
            active_workers,
        }))
    }

    async fn list_tasks(
        &self,
        request: Request<ListTasksRequest>,
    ) -> std::result::Result<Response<ListTasksResponse>, Status> {
        let req = request.into_inner();

        // Map proto status filters (raw i32) to core statuses.
        let statuses: Vec<TaskStatus> = req
            .filter_status
            .into_iter()
            .filter_map(|s| ProtoTaskStatus::try_from(s).ok())
            .map(TaskStatus::from)
            .collect();

        let filter = TaskFilter {
            statuses,
            limit: req.limit as usize,
            offset: req.offset as usize,
        };

        let tasks = self
            .state.storage
            .list(&filter)
            .await
            .map_err(|e| Status::internal(format!("Failed to list tasks: {}", e)))?;

        // total_count is the unfiltered total in the store.
        let total = self
            .state.storage
            .list(&TaskFilter::default())
            .await
            .map_err(|e| Status::internal(format!("Failed to count tasks: {}", e)))?
            .len() as u32;

        let tasks: Vec<ProtoTask> = tasks.into_iter().map(ProtoTask::from).collect();

        Ok(Response::new(ListTasksResponse {
            tasks,
            total_count: total,
        }))
    }

    async fn list_workers(
        &self,
        _: Request<()>,
    ) -> std::result::Result<Response<ListWorkersResponse>, Status> {
        let dispatcher = self.state.dispatcher.lock().await;
        let workers = dispatcher
            .list_workers()
            .await
            .map_err(|e| Status::internal(format!("Failed to list workers: {}", e)))?;
        let proto_workers = workers.into_iter().map(ProtoWorker::from).collect();
        Ok(Response::new(ListWorkersResponse {
            workers: proto_workers,
        }))
    }

    async fn create_schedule(
        &self,
        request: Request<CreateScheduleRequest>,
    ) -> std::result::Result<Response<CreateScheduleResponse>, Status> {
        let schedule = proto_to_schedule(
            request.into_inner().schedule.ok_or_else(|| Status::invalid_argument("schedule required"))?,
        )?;
        let id = schedule.id;
        self.state
            .storage
            .insert_schedule(schedule)
            .await
            .map_err(|e| Status::internal(format!("insert schedule: {}", e)))?;
        Ok(Response::new(CreateScheduleResponse { schedule_id: id.to_string() }))
    }

    async fn list_schedules(
        &self,
        _: Request<ListSchedulesRequest>,
    ) -> std::result::Result<Response<ListSchedulesResponse>, Status> {
        let schs = self
            .state
            .storage
            .list_schedules()
            .await
            .map_err(|e| Status::internal(format!("list schedules: {}", e)))?;
        let schedules = schs.into_iter().map(schedule_to_proto).collect();
        Ok(Response::new(ListSchedulesResponse { schedules }))
    }

    async fn delete_schedule(
        &self,
        request: Request<DeleteScheduleRequest>,
    ) -> std::result::Result<Response<DeleteScheduleResponse>, Status> {
        let id = Uuid::parse_str(&request.into_inner().id)
            .map_err(|_| Status::invalid_argument("invalid schedule id"))?;
        self.state
            .storage
            .delete_schedule(&id)
            .await
            .map_err(|e| Status::internal(format!("delete schedule: {}", e)))?;
        Ok(Response::new(DeleteScheduleResponse { success: true }))
    }
}

/// Serve the broker on an already-bound TCP listener. Returns the bound
/// address (useful when binding to port 0 in tests).
pub async fn serve_with_listener(
    service: ChopFlowBrokerService,
    listener: tokio::net::TcpListener,
) -> std::result::Result<std::net::SocketAddr, Box<dyn std::error::Error>> {
    let addr = listener.local_addr()?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    Server::builder()
        .add_service(service.into_server())
        .serve_with_incoming(incoming)
        .await?;
    Ok(addr)
}

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
