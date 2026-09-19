/*!
# ChopFlow Worker

The worker executable for ChopFlow, a distributed task queue.

Workers are responsible for:
- Registering with the broker
- Declaring their capabilities (tags) and resources (static or replenishing)
- Executing assigned tasks (plain sync handlers, or context-aware async
  handlers that can persist pipeline checkpoints and resume on retry)
- Reporting task results back to the broker
- Sending regular heartbeats to indicate health

This crate is a library plus a thin binary (`main.rs`). The library exposes
the worker loop, handler registry, and parsing helpers so they can be unit-
and integration-tested; the binary only wires up CLI parsing + tracing.
*/

use chopflow_core::error::{ChopFlowError, Result};
use chopflow_core::resources::{parse_resources_ext, RefillSpec, ResourceAvailability};
use chopflow_core::Checkpoint;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tracing::{error, info};
use uuid::Uuid;

// Re-export the generated gRPC types from the shared `chopflow-proto` crate.
// The proto is compiled once there (not per consumer), which keeps this crate
// crates.io-publishable: `cargo publish` verifies the tarball in isolation and
// would reject a `build.rs` pointing at `../broker/proto/...`.
pub use chopflow_proto::chopflow;

use chopflow::{
    chop_flow_broker_client::ChopFlowBrokerClient, AcknowledgeTaskRequest, FetchTasksRequest,
    GetCheckpointsRequest, RegisterWorkerRequest,
    ResourceAvailability as ProtoResourceAvailability, ResourceSpec as ProtoResourceSpec,
    SaveCheckpointRequest, Task as ProtoTask, WorkerHeartbeatRequest,
};

#[derive(Clone)]
pub struct WorkerState {
    pub id: String,
    pub broker_address: String,
    /// A persistent gRPC channel to the broker, created once at startup and
    /// reused for every heartbeat / fetch / ack. Reconnecting per call (the
    /// old behavior) exhausted the ephemeral port range over a long run —
    /// each call left a socket in TIME_WAIT, and at 100k+ tasks the worker
    /// alone opened tens of thousands of connections.
    pub channel: Channel,
    pub resources: ResourceAvailability,
    pub tags: Vec<String>,
    pub assigned_tasks: HashMap<String, ProtoTask>,
    pub task_registry: TaskRegistry,
    /// Maximum number of tasks this worker executes at once. Sized by the
    /// declared resources (see [`derive_concurrency`]) or overridden via
    /// `--concurrency`. The broker's resource accounting may further restrict
    /// dispatch for tasks that declare their own resource requirements.
    pub concurrency: usize,
}

/// Build a lazy (non-blocking) gRPC channel to `broker_address`. The channel
/// connects on first use and reconnects automatically on failure, so it is
/// cheap to create even before the broker is reachable.
fn broker_channel(broker_address: &str) -> Result<Channel> {
    let endpoint = Endpoint::from_shared(broker_address.to_string())
        .map_err(|e| ChopFlowError::NetworkError(format!("invalid broker address: {e}")))?;
    Ok(endpoint.connect_lazy())
}

impl WorkerState {
    pub fn new(
        id: String,
        broker_address: String,
        resources: ResourceAvailability,
        tags: Vec<String>,
        concurrency: usize,
    ) -> Result<Self> {
        let mut registry = TaskRegistry::new();

        // Register a built-in `echo` handler and a `default` fallback so the
        // worker can execute tasks out of the box. Real deployments register
        // their own handlers (e.g. loaded from a plugin/WASM module).
        registry.register("echo", echo_handler);
        registry.register("default", echo_handler);

        let channel = broker_channel(&broker_address)?;

        Ok(Self {
            id,
            broker_address,
            channel,
            resources,
            tags,
            assigned_tasks: HashMap::new(),
            task_registry: registry,
            concurrency: concurrency.max(1),
        })
    }

    /// Build a worker state with a caller-supplied handler registry (used by
    /// the demos crate and other embedders that ship their own handlers).
    pub fn with_registry(
        id: String,
        broker_address: String,
        resources: ResourceAvailability,
        tags: Vec<String>,
        concurrency: usize,
        registry: TaskRegistry,
    ) -> Result<Self> {
        let channel = broker_channel(&broker_address)?;
        Ok(Self {
            id,
            broker_address,
            channel,
            resources,
            tags,
            assigned_tasks: HashMap::new(),
            task_registry: registry,
            concurrency: concurrency.max(1),
        })
    }
}

/// Derive a concurrency limit from the worker's declared resources.
///
/// For a **single** declared resource the limit is that resource's total
/// (e.g. `cpu:4` → 4, `gpu:1` → 1): a worker can run up to that many
/// unit-sized tasks of that kind at once.
///
/// For **multiple** declared resources (e.g. `ram:16,cpu:4`) no meaningful
/// count can be derived without knowing each task's per-resource footprint —
/// summing the totals mixes incompatible units (GB + cores), and the minimum
/// total is just a guess that can over- or under-shoot. We default to 1
/// (sequential) and let the operator set `--concurrency` explicitly.
///
/// Correctness is never at stake either way: the broker's `assign_task` is the
/// hard per-resource gate that prevents over-dispatch. This only chooses the
/// fetch batch size so the worker doesn't request more in-flight tasks than it
/// can plausibly run (which would otherwise churn tasks Queued→Running→Queued
/// each poll).
pub fn derive_concurrency(resources: &ResourceAvailability) -> usize {
    match resources.total.len() {
        // No resources declared — fall back to sequential.
        0 => 1,
        // One resource kind: its total is a sensible unit count.
        1 => {
            let total: u32 = resources.total.values().sum();
            (total as usize).max(1)
        }
        // Heterogeneous resources: can't sum incompatible units. Let the
        // operator decide via --concurrency.
        _ => 1,
    }
}

/// Built-in task handler: echoes the payload back as the result. Useful as a
/// smoke test and as the default when no specific handler is registered.
pub fn echo_handler(payload: serde_json::Value) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "status": "ok",
        "echo": payload,
    }))
}

/// Execution context handed to a context-aware (async) task handler.
///
/// A `TaskCtx` carries the task's identity, its declared pipeline stages
/// (if any), the checkpoints recorded by previous attempts of the same task
/// (fetched before the handler runs), and a cheap handle for saving new
/// checkpoints: [`TaskCtx::checkpoint`] upserts a `(task_id, stage)` record on
/// the broker, so a retried task resumes from the last completed stage
/// instead of restarting (Temporal-style durable execution without a
/// workflow engine).
///
/// The broker channel is the same persistent channel the fetch/ack loop
/// holds; a client is cloned off it per checkpoint save, exactly like
/// [`send_task_acknowledgment`] and [`fetch_tasks`] do.
pub struct TaskCtx {
    /// ID of the task being executed
    pub task_id: String,

    /// Name of the task being executed
    pub task_name: String,

    /// The task's declared pipeline stages (`None` when the task declares
    /// none — a plain, non-checkpointed task)
    pub stages: Option<Vec<String>>,

    /// Checkpoints recorded by previous attempts of this task (empty on a
    /// first attempt, or when the task declares no stages)
    pub checkpoints: Vec<Checkpoint>,

    /// The worker's persistent broker channel, cloned per checkpoint save
    channel: Channel,
}

impl TaskCtx {
    /// Persist (upsert) a checkpoint for `stage` on the broker.
    ///
    /// The payload is serialized to a JSON string and stored keyed by
    /// `(task_id, stage)` — saving the same stage again overwrites it. Errors
    /// are returned as readable strings (never panics), so a handler can
    /// propagate them and let the broker retry the task with its checkpoints
    /// intact.
    pub async fn checkpoint(
        &self,
        stage: &str,
        payload: serde_json::Value,
    ) -> std::result::Result<(), String> {
        let payload = serde_json::to_string(&payload).map_err(|e| {
            format!("failed to serialize checkpoint payload for stage '{stage}': {e}")
        })?;

        let mut client = ChopFlowBrokerClient::new(self.channel.clone());

        client
            .save_checkpoint(Request::new(SaveCheckpointRequest {
                task_id: self.task_id.clone(),
                stage: stage.to_string(),
                payload,
            }))
            .await
            .map(|_| ())
            .map_err(|e| {
                format!(
                    "failed to save checkpoint for stage '{}' of task {}: {e}",
                    stage, self.task_id
                )
            })
    }

    /// The checkpoint recorded for `stage`, if any. Checkpoints are upserted
    /// per stage, so at most one exists per stage name.
    pub fn stage_checkpoint(&self, stage: &str) -> Option<&Checkpoint> {
        self.checkpoints.iter().find(|c| c.stage == stage)
    }

    /// Distinct stage names that have a checkpoint, in pipeline order.
    ///
    /// When the task declares its stages, the declared order is the
    /// authoritative one — checkpoint listing order (`recorded_at`) must not
    /// be relied on because the broker re-orders it on upsert. Without
    /// declared stages the first-appearance order in the checkpoint list is
    /// used.
    pub fn completed_stages(&self) -> Vec<String> {
        completed_stages(self.stages.as_deref(), &self.checkpoints)
    }
}

/// Distinct stage names that have a checkpoint, ordered by their first
/// appearance in the declared `stages` when available (falling back to
/// first-appearance order in `checkpoints` otherwise).
///
/// Free-function form of [`TaskCtx::completed_stages`] so the ordering logic
/// is unit-testable without a broker channel.
pub fn completed_stages(stages: Option<&[String]>, checkpoints: &[Checkpoint]) -> Vec<String> {
    let completed: HashSet<&str> = checkpoints.iter().map(|c| c.stage.as_str()).collect();

    match stages {
        // Declared stages exist: keep their (authoritative) order.
        Some(stages) => stages
            .iter()
            .filter(|stage| completed.contains(stage.as_str()))
            .cloned()
            .collect(),
        // No declared stages: fall back to first-appearance order.
        None => {
            let mut seen = HashSet::new();
            checkpoints
                .iter()
                .map(|c| c.stage.clone())
                .filter(|stage| seen.insert(stage.clone()))
                .collect()
        }
    }
}

/// The boxed future returned by a context-aware task handler.
pub type CtxHandlerFuture =
    futures::future::BoxFuture<'static, std::result::Result<serde_json::Value, String>>;

/// A context-aware (async) task handler: receives the [`TaskCtx`] (task
/// identity, declared stages, prior checkpoints, checkpoint sink) plus the
/// deserialized payload, and returns the task result — or a readable error
/// string that is acked back to the broker as a failure.
pub type CtxHandler = Arc<dyn Fn(TaskCtx, serde_json::Value) -> CtxHandlerFuture + Send + Sync>;

/// A function that can handle a task
pub type TaskHandlerFn = fn(serde_json::Value) -> Result<serde_json::Value>;

/// Registry for task handlers
#[derive(Clone)]
pub struct TaskRegistry {
    handlers: HashMap<String, TaskHandlerFn>,
    ctx_handlers: HashMap<String, CtxHandler>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            ctx_handlers: HashMap::new(),
        }
    }

    /// Register a new task handler
    pub fn register(&mut self, task_name: &str, handler: TaskHandlerFn) {
        self.handlers.insert(task_name.to_string(), handler);
    }

    /// Register a context-aware (async) task handler. The handler receives a
    /// [`TaskCtx`] — task identity, declared stages, prior checkpoints, and
    /// the [`TaskCtx::checkpoint`] sink — so it can implement durable,
    /// resumable pipelines. A plain [`TaskRegistry::register`] handler for
    /// the same name is unaffected: context-aware handlers take precedence
    /// at dispatch.
    ///
    /// Accepts any async function/closure of `(TaskCtx, Value)`; it is boxed
    /// into the shared [`CtxHandler`] shape internally.
    pub fn register_ctx<F, Fut>(&mut self, task_name: &str, handler: F)
    where
        F: Fn(TaskCtx, serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<serde_json::Value, String>> + Send + 'static,
    {
        self.ctx_handlers.insert(
            task_name.to_string(),
            Arc::new(move |ctx, payload| Box::pin(handler(ctx, payload))),
        );
    }

    /// Get a handler for a task
    pub fn get(&self, task_name: &str) -> Option<&TaskHandlerFn> {
        self.handlers.get(task_name)
    }

    /// Get a context-aware handler for a task (cloned `Arc` handle)
    pub fn get_ctx(&self, task_name: &str) -> Option<CtxHandler> {
        self.ctx_handlers.get(task_name).cloned()
    }

    /// Number of registered handlers (plain + context-aware).
    pub fn len(&self) -> usize {
        self.handlers.len() + self.ctx_handlers.len()
    }

    /// Whether any handlers are registered.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty() && self.ctx_handlers.is_empty()
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a comma-separated tag list into owned, trimmed `String`s.
pub fn parse_tags(tags_str: &str) -> Vec<String> {
    tags_str.split(',').map(|s| s.trim().to_string()).collect()
}

/// Parse a comma-separated `name:amount` resource list into a map.
///
/// Each entry is `name:amount`. A malformed entry is an error, not a silent
/// skip — otherwise the worker would register with no resources and the broker
/// would reject it with a confusing message. An empty result (no entries) is
/// also an error: a worker must declare at least one resource.
pub fn parse_resources(resources_str: &str) -> Result<HashMap<String, u32>> {
    let mut resource_map = HashMap::new();
    for resource_str in resources_str.split(',') {
        let entry = resource_str.trim();
        if entry.is_empty() {
            continue;
        }
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 2 {
            return Err(chopflow_core::error::ChopFlowError::NetworkError(format!(
                "invalid resource '{}': expected 'name:amount' (e.g. cpu:2)",
                entry
            )));
        }
        let resource_name = parts[0].trim().to_string();
        let amount = parts[1].trim().parse::<u32>().map_err(|e| {
            chopflow_core::error::ChopFlowError::NetworkError(format!(
                "invalid resource amount in '{}': {}",
                entry, e
            ))
        })?;
        resource_map.insert(resource_name, amount);
    }

    if resource_map.is_empty() {
        return Err(chopflow_core::error::ChopFlowError::NetworkError(
            "a worker must declare at least one resource (use -r, e.g. -r cpu:1)".into(),
        ));
    }

    Ok(resource_map)
}

/// Build the worker's [`ResourceAvailability`] from an extended
/// `--resources` declaration: every entry contributes its capacity, and
/// replenishing entries (`name:capacity@refill_amount/period_secs`) declare a
/// lazy token bucket on top.
pub fn resources_from_declarations(
    capacities: &HashMap<String, u32>,
    refills: &HashMap<String, RefillSpec>,
) -> ResourceAvailability {
    let mut resources = ResourceAvailability::from_capacities(capacities.clone());
    for (name, spec) in refills {
        // Every refill entry also carries a capacity in the parsed map.
        let capacity = capacities.get(name).copied().unwrap_or(1);
        resources.add_replenishing_resource(name.clone(), capacity, spec.amount, spec.period_secs);
    }
    resources
}

/// Build the gRPC `map<String, ResourceSpec>` registration payload. A
/// replenishing entry gets its refill fields; a static entry declares
/// `refill_amount == 0` (the broker treats that as a plain static resource).
pub fn resource_specs_proto(
    capacities: &HashMap<String, u32>,
    refills: &HashMap<String, RefillSpec>,
) -> HashMap<String, ProtoResourceSpec> {
    capacities
        .iter()
        .map(|(name, capacity)| {
            let refill = refills.get(name);
            (
                name.clone(),
                ProtoResourceSpec {
                    capacity: *capacity,
                    refill_amount: refill.map(|s| s.amount).unwrap_or(0),
                    refill_period_secs: refill.map(|s| s.period_secs).unwrap_or(0),
                },
            )
        })
        .collect()
}

pub async fn start_worker(
    broker_address: String,
    tags_str: String,
    resources_str: String,
    heartbeat_interval: u64,
    concurrency: Option<usize>,
) -> Result<()> {
    // Parse tags
    let tags = parse_tags(&tags_str);

    // Parse resources: `name:capacity` (static) or
    // `name:capacity@refill_amount/period_secs` (replenishing).
    let (capacities, refills) = parse_resources_ext(&resources_str)?;

    let resources = resources_from_declarations(&capacities, &refills);

    let concurrency = concurrency.unwrap_or_else(|| derive_concurrency(&resources));

    info!("Worker configured with tags: {:?}", tags);
    info!("Worker resources: {:?}", resources);
    info!("Worker concurrency: {}", concurrency);

    // Connect to the broker and register. We retry with backoff so the worker
    // can be started before the broker, or survive a broker restart, instead
    // of dying on the first failed connection with a cryptic "transport error".
    let worker_id =
        connect_and_register_with_refills(&broker_address, &tags, &capacities, &refills).await?;

    info!("Worker registered with ID: {}", worker_id);

    // Create shared worker state with the built-in echo/default registry.
    let worker_state = Arc::new(Mutex::new(WorkerState::new(
        worker_id.clone(),
        broker_address,
        resources,
        tags,
        concurrency,
    )?));

    run_worker(worker_state, heartbeat_interval).await
}

/// Start a worker with a caller-supplied handler registry (used by the demos
/// crate and other embedders). The registry replaces the built-in echo/default
/// handlers, so callers that want those should register them themselves.
pub async fn start_worker_with_registry(
    broker_address: String,
    tags_str: String,
    resources_str: String,
    heartbeat_interval: u64,
    concurrency: Option<usize>,
    registry: TaskRegistry,
) -> Result<()> {
    let tags = parse_tags(&tags_str);

    let (capacities, refills) = parse_resources_ext(&resources_str)?;

    let resources = resources_from_declarations(&capacities, &refills);

    let concurrency = concurrency.unwrap_or_else(|| derive_concurrency(&resources));

    info!("Worker configured with tags: {:?}", tags);
    info!("Worker resources: {:?}", resources);
    info!("Worker concurrency: {}", concurrency);

    let worker_id =
        connect_and_register_with_refills(&broker_address, &tags, &capacities, &refills).await?;
    info!("Worker registered with ID: {}", worker_id);

    let worker_state = Arc::new(Mutex::new(WorkerState::with_registry(
        worker_id.clone(),
        broker_address,
        resources,
        tags,
        concurrency,
        registry,
    )?));

    run_worker(worker_state, heartbeat_interval).await
}

/// Shared run loop: spawn the heartbeat + task-processing loops on a built
/// `WorkerState`, await Ctrl+C, then shut both down. Used by both
/// [`start_worker`] and [`start_worker_with_registry`].
async fn run_worker(worker_state: Arc<Mutex<WorkerState>>, heartbeat_interval: u64) -> Result<()> {
    // Start heartbeat loop
    let heartbeat_state = worker_state.clone();
    let heartbeat_handle = tokio::spawn(async move {
        let heartbeat_interval = Duration::from_secs(heartbeat_interval);
        let mut interval = time::interval(heartbeat_interval);

        loop {
            interval.tick().await;
            if let Err(e) = send_heartbeat(&heartbeat_state).await {
                error!("Failed to send heartbeat: {}", e);
            }
        }
    });

    // Start task processing loop
    let process_state = worker_state.clone();
    let process_handle = tokio::spawn(async move {
        if let Err(e) = start_task_processing(&process_state).await {
            error!("Task processing loop failed: {}", e);
        }
    });

    info!("Worker is running. Press Ctrl+C to exit.");

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await.map_err(|e| {
        error!("Failed to listen for Ctrl+C: {}", e);
        chopflow_core::error::ChopFlowError::Other(e.into())
    })?;

    info!("Shutting down worker...");
    heartbeat_handle.abort();
    process_handle.abort();

    Ok(())
}

/// Connect to the broker and register the worker with static resources only,
/// retrying with backoff until it succeeds. Kept for backward compatibility;
/// see [`connect_and_register_with_refills`] for the full declaration.
pub async fn connect_and_register(
    broker_address: &str,
    tags: &[String],
    resources: &HashMap<String, u32>,
) -> Result<String> {
    connect_and_register_with_refills(broker_address, tags, resources, &HashMap::new()).await
}

/// Connect to the broker and register the worker, retrying with backoff until
/// it succeeds. This makes startup resilient to the broker not being ready yet
/// (or restarting) — instead of exiting with a bare "transport error".
///
/// `capacities` holds every declared resource's capacity; `refills` holds the
/// replenishing subset (`name:capacity@refill_amount/period_secs` entries).
pub async fn connect_and_register_with_refills(
    broker_address: &str,
    tags: &[String],
    capacities: &HashMap<String, u32>,
    refills: &HashMap<String, RefillSpec>,
) -> Result<String> {
    info!("Connecting to broker at {}", broker_address);

    let mut backoff = Duration::from_millis(500);
    const MAX_BACKOFF: Duration = Duration::from_secs(5);

    loop {
        match try_connect_and_register(broker_address, tags, capacities, refills).await {
            Ok(id) => return Ok(id),
            Err(e) => {
                error!(
                    "Could not reach broker at {} ({}). Retrying in {:?}.",
                    broker_address, e, backoff
                );
                info!(
                    "Hint: the broker's gRPC port is --port (default 8000). The dashboard/HTTP \
                     port --http-port (default 8080) is not a gRPC endpoint."
                );
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
            }
        }
    }
}

/// A single best-effort attempt to connect + register.
pub async fn try_connect_and_register(
    broker_address: &str,
    tags: &[String],
    capacities: &HashMap<String, u32>,
    refills: &HashMap<String, RefillSpec>,
) -> Result<String> {
    let mut client = ChopFlowBrokerClient::connect(broker_address.to_string())
        .await
        .map_err(|e| chopflow_core::error::ChopFlowError::NetworkError(e.to_string()))?;

    let register_request = Request::new(RegisterWorkerRequest {
        address: "localhost".to_string(), // In production, this would be the actual address
        tags: tags.to_vec(),
        // Static entries declare refill_amount = 0; replenishing entries
        // carry their refill rate so the broker builds a token bucket.
        resources: resource_specs_proto(capacities, refills),
    });

    let response = client
        .register_worker(register_request)
        .await
        .map_err(|e| chopflow_core::error::ChopFlowError::NetworkError(e.to_string()))?;

    Ok(response.into_inner().worker_id)
}

pub async fn send_heartbeat(worker_state: &Arc<Mutex<WorkerState>>) -> Result<()> {
    let state = worker_state.lock().await;

    let mut client = ChopFlowBrokerClient::new(state.channel.clone());

    let heartbeat_request = Request::new(WorkerHeartbeatRequest {
        worker_id: state.id.clone(),
        resources: Some(ProtoResourceAvailability {
            available: state.resources.available.clone(),
            total: state.resources.total.clone(),
        }),
    });

    let _ = client
        .worker_heartbeat(heartbeat_request)
        .await
        .map_err(|e| {
            error!("Failed to send heartbeat: {}", e);
            chopflow_core::error::ChopFlowError::NetworkError(e.to_string())
        })?;

    info!("Heartbeat sent successfully");
    Ok(())
}

/// Which kind of handler a task resolved to, and what it needs to run.
enum HandlerKind {
    /// A context-aware (async) handler plus the broker channel for its [`TaskCtx`].
    Ctx(CtxHandler, Channel),
    /// A plain (sync) `fn` pointer, executed on the blocking pool.
    Plain(TaskHandlerFn),
}

/// Fetch a task's recorded checkpoints from the broker (recorded_at order),
/// converting the proto records into core [`Checkpoint`]s.
async fn fetch_checkpoints(channel: &Channel, task_id: &str) -> Result<Vec<Checkpoint>> {
    let mut client = ChopFlowBrokerClient::new(channel.clone());

    let response = client
        .get_checkpoints(Request::new(GetCheckpointsRequest {
            task_id: task_id.to_string(),
        }))
        .await
        .map_err(|e| {
            ChopFlowError::NetworkError(format!(
                "failed to fetch checkpoints for task {task_id}: {e}"
            ))
        })?
        .into_inner();

    let mut checkpoints = Vec::with_capacity(response.checkpoints.len());
    for cp in response.checkpoints {
        let task_uuid = Uuid::parse_str(&cp.task_id).map_err(|e| {
            ChopFlowError::Other(anyhow::anyhow!(
                "invalid task id in checkpoint for task {task_id}: {e}"
            ))
        })?;
        let recorded_at = chrono::DateTime::parse_from_rfc3339(&cp.recorded_at)
            .map_err(|e| {
                ChopFlowError::Other(anyhow::anyhow!(
                    "invalid recorded_at in checkpoint for task {task_id}: {e}"
                ))
            })?
            .with_timezone(&chrono::Utc);
        checkpoints.push(Checkpoint {
            task_id: task_uuid,
            stage: cp.stage,
            payload: cp.payload,
            recorded_at,
        });
    }

    Ok(checkpoints)
}

// This function would execute a task and send the result back to the broker
pub async fn execute_task(worker_state: &Arc<Mutex<WorkerState>>, task: ProtoTask) -> Result<()> {
    let task_id = task.id.clone();
    let task_name = task.name.clone();
    info!("Executing task {}: {}", task_id, task_name);

    // Deserialize the task payload
    let payload: serde_json::Value = match serde_json::from_str(&task.payload) {
        Ok(payload) => payload,
        Err(e) => {
            error!("Failed to deserialize task payload: {}", e);

            // Send failure acknowledgment
            send_task_acknowledgment(
                worker_state,
                task_id.clone(),
                false,
                serde_json::json!({
                    "status": "error",
                    "message": format!("Failed to deserialize payload: {}", e),
                }),
            )
            .await?;

            return Err(chopflow_core::error::ChopFlowError::Other(
                anyhow::Error::new(e),
            ));
        }
    };

    // Look up the appropriate handler for the task name: a context-aware
    // (async) handler registered for the exact name wins, then a plain one,
    // then the "default" fallback of either kind.
    let handler = {
        // Use a block to ensure the lock is released after we get the handler
        let state = worker_state.lock().await;
        let registry = &state.task_registry;

        if let Some(ctx_handler) = registry.get_ctx(&task_name) {
            Some(HandlerKind::Ctx(ctx_handler, state.channel.clone()))
        } else if let Some(handler) = registry.get(&task_name) {
            Some(HandlerKind::Plain(*handler))
        } else if let Some(ctx_handler) = registry.get_ctx("default") {
            Some(HandlerKind::Ctx(ctx_handler, state.channel.clone()))
        } else {
            registry
                .get("default")
                .map(|handler| HandlerKind::Plain(*handler))
        }
    };

    // If no handler found, acknowledge failure
    let Some(handler) = handler else {
        error!("No handler found for task: {}", task_name);
        send_task_acknowledgment(
            worker_state,
            task_id.clone(),
            false,
            serde_json::json!({
                "status": "error",
                "message": format!("Unknown task type: {}", task_name),
            }),
        )
        .await?;

        info!("Task {} failed: no handler found", task_id);
        return Ok(());
    };

    // Run the handler. Plain handlers run on the blocking pool so a
    // long-running or CPU-bound handler can't stall the async runtime's
    // worker threads (the handler is a plain `fn` pointer and the payload is
    // `Send`, so the closure is `Send + 'static` as `spawn_blocking`
    // requires). Context-aware handlers are async and run on the runtime
    // directly; before invoking one we fetch the task's prior checkpoints
    // (only when the task declares stages — otherwise the RPC would only
    // ever return an empty list) so the handler can resume where the
    // previous attempt left off.
    let outcome: std::result::Result<serde_json::Value, String> = match handler {
        HandlerKind::Plain(handler_fn) => {
            match tokio::task::spawn_blocking(move || {
                handler_fn(payload).map_err(|e| e.to_string())
            })
            .await
            {
                Ok(outcome) => outcome,
                // A panicked handler is a task failure (acked, then retried by
                // the broker) rather than a worker error, so the task never
                // gets stuck in Running.
                Err(join_err) => Err(format!("handler panicked: {join_err}")),
            }
        }
        HandlerKind::Ctx(ctx_handler, channel) => {
            let checkpoints = if task.stages.is_empty() {
                Vec::new()
            } else {
                fetch_checkpoints(&channel, &task_id).await?
            };
            let stages = if task.stages.is_empty() {
                None
            } else {
                Some(task.stages.clone())
            };
            let ctx = TaskCtx {
                task_id: task_id.clone(),
                task_name: task_name.clone(),
                stages,
                checkpoints,
                channel,
            };
            (ctx_handler)(ctx, payload).await
        }
    };

    let result = match outcome {
        Ok(result) => {
            info!("Task {} executed successfully", task_id);
            send_task_acknowledgment(worker_state, task_id.clone(), true, result).await?
        }
        Err(e) => {
            error!("Task {} failed: {}", task_id, e);
            send_task_acknowledgment(
                worker_state,
                task_id.clone(),
                false,
                serde_json::json!({
                    "status": "error",
                    "message": format!("Task execution failed: {}", e),
                }),
            )
            .await?
        }
    };

    info!("Task {} acknowledged: {}", task_id, result);
    Ok(())
}

/// Helper function to send task acknowledgment to the broker
pub async fn send_task_acknowledgment(
    worker_state: &Arc<Mutex<WorkerState>>,
    task_id: String,
    success: bool,
    result: serde_json::Value,
) -> Result<String> {
    let state = worker_state.lock().await;
    let mut client = ChopFlowBrokerClient::new(state.channel.clone());

    let result_json = serde_json::to_string(&result).unwrap_or_else(|_| {
        r#"{"status": "error", "message": "Failed to serialize result"}"#.to_string()
    });

    let ack_request = Request::new(AcknowledgeTaskRequest {
        worker_id: state.id.clone(),
        task_id,
        success,
        result: result_json.clone(),
    });

    client.acknowledge_task(ack_request).await.map_err(|e| {
        error!("Failed to acknowledge task: {}", e);
        chopflow_core::error::ChopFlowError::NetworkError(e.to_string())
    })?;

    Ok(result_json)
}

/// Start processing tasks.
///
/// Polls the broker for tasks this worker can execute (pull model) and runs
/// them through a **bounded concurrency pool** sized by
/// [`WorkerState::concurrency`]. Each fetched task is spawned onto a
/// `JoinSet`; backpressure prevents fetching more than the worker can run at
/// once. The (sync) handler itself runs via `tokio::task::spawn_blocking`
/// inside [`execute_task`] so CPU-bound handlers don't stall the runtime.
pub async fn start_task_processing(worker_state: &Arc<Mutex<WorkerState>>) -> Result<()> {
    let concurrency = {
        let state = worker_state.lock().await;
        state.concurrency
    };
    info!(
        "Starting task processing loop (concurrency={})",
        concurrency
    );

    // When the queue is empty, re-poll quickly rather than sleeping for whole
    // seconds. The old 2s sleep dominated small-batch latency (a 1k run spent
    // most of its wall-clock asleep on the first empty fetch) and added a long
    // tail at the end of every run. With the broker's ready index a fetch is
    // cheap, so a short idle poll keeps the worker responsive without burning
    // CPU.
    let poll_interval = Duration::from_millis(100);
    let mut in_flight: JoinSet<()> = JoinSet::new();

    loop {
        // Reap any tasks that have finished (non-blocking).
        while in_flight.try_join_next().is_some() {}

        // Backpressure: if we're at capacity, wait for at least one task to
        // finish before fetching more.
        if in_flight.len() >= concurrency {
            let _ = in_flight.join_next().await;
            continue;
        }

        // Only fetch as many tasks as we have free capacity for.
        let want = (concurrency - in_flight.len()) as u32;
        match fetch_tasks(worker_state, want).await {
            Ok(tasks) if !tasks.is_empty() => {
                for task in tasks {
                    let state = worker_state.clone();
                    in_flight.spawn(async move {
                        if let Err(e) = execute_task(&state, task).await {
                            error!("Failed to execute task: {}", e);
                        }
                    });
                }
            }
            Ok(_) => {
                // No tasks available — wait before polling again.
                tokio::time::sleep(poll_interval).await;
            }
            Err(e) => {
                error!("Failed to fetch tasks: {}", e);
                // Back off on errors to avoid hammering the broker.
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
}

/// Pull a batch of ready tasks from the broker for this worker. At most
/// `max_tasks` are requested; the broker may return fewer (e.g. when its
/// resource accounting can't satisfy all of them).
pub async fn fetch_tasks(
    worker_state: &Arc<Mutex<WorkerState>>,
    max_tasks: u32,
) -> Result<Vec<ProtoTask>> {
    let (channel, worker_id) = {
        let state = worker_state.lock().await;
        (state.channel.clone(), state.id.clone())
    };

    let mut client = ChopFlowBrokerClient::new(channel);

    let request = Request::new(FetchTasksRequest {
        worker_id,
        max_tasks: max_tasks.max(1),
    });

    let response = client
        .fetch_tasks(request)
        .await
        .map_err(|e| {
            error!("Failed to fetch tasks: {}", e);
            chopflow_core::error::ChopFlowError::NetworkError(e.to_string())
        })?
        .into_inner();

    Ok(response.tasks)
}
