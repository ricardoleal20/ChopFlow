/*!
# ChopFlow Worker

The worker executable for ChopFlow, a distributed task queue.

Workers are responsible for:
- Registering with the broker
- Declaring their capabilities (tags) and resources
- Executing assigned tasks
- Reporting task results back to the broker
- Sending regular heartbeats to indicate health

This crate is a library plus a thin binary (`main.rs`). The library exposes
the worker loop, handler registry, and parsing helpers so they can be unit-
and integration-tested; the binary only wires up CLI parsing + tracing.
*/

use chopflow_core::error::{Result, ChopFlowError};
use chopflow_core::resources::ResourceAvailability;

use anyhow;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time;
use tonic::Request;
use tracing::{error, info};

// Generate code from protobuf definitions
pub mod chopflow {
    tonic::include_proto!("chopflow");
}

use chopflow::{
    chop_flow_broker_client::ChopFlowBrokerClient, AcknowledgeTaskRequest, FetchTasksRequest,
    RegisterWorkerRequest, ResourceAvailability as ProtoResourceAvailability,
    Task as ProtoTask, WorkerHeartbeatRequest,
};

#[derive(Clone)]
pub struct WorkerState {
    pub id: String,
    pub broker_address: String,
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

impl WorkerState {
    pub fn new(
        id: String,
        broker_address: String,
        resources: ResourceAvailability,
        tags: Vec<String>,
        concurrency: usize,
    ) -> Self {
        let mut registry = TaskRegistry::new();

        // Register a built-in `echo` handler and a `default` fallback so the
        // worker can execute tasks out of the box. Real deployments register
        // their own handlers (e.g. loaded from a plugin/WASM module).
        registry.register("echo", echo_handler);
        registry.register("default", echo_handler);

        Self {
            id,
            broker_address,
            resources,
            tags,
            assigned_tasks: HashMap::new(),
            task_registry: registry,
            concurrency: concurrency.max(1),
        }
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
    ) -> Self {
        Self {
            id,
            broker_address,
            resources,
            tags,
            assigned_tasks: HashMap::new(),
            task_registry: registry,
            concurrency: concurrency.max(1),
        }
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

/// A function that can handle a task
pub type TaskHandlerFn = fn(serde_json::Value) -> Result<serde_json::Value>;

/// Registry for task handlers
#[derive(Clone)]
pub struct TaskRegistry {
    handlers: HashMap<String, TaskHandlerFn>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register a new task handler
    pub fn register(&mut self, task_name: &str, handler: TaskHandlerFn) {
        self.handlers.insert(task_name.to_string(), handler);
    }

    /// Get a handler for a task
    pub fn get(&self, task_name: &str) -> Option<&TaskHandlerFn> {
        self.handlers.get(task_name)
    }

    /// Number of registered handlers.
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Whether any handlers are registered.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a comma-separated tag list into owned, trimmed `String`s.
pub fn parse_tags(tags_str: &str) -> Vec<String> {
    tags_str
        .split(',')
        .map(|s| s.trim().to_string())
        .collect()
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

pub async fn start_worker(
    broker_address: String,
    tags_str: String,
    resources_str: String,
    heartbeat_interval: u64,
    concurrency: Option<usize>,
) -> Result<()> {
    // Parse tags
    let tags = parse_tags(&tags_str);

    // Parse resources.
    let resource_map = parse_resources(&resources_str)?;

    let resources = ResourceAvailability {
        available: resource_map.clone(),
        total: resource_map.clone(),
    };

    let concurrency =
        concurrency.unwrap_or_else(|| derive_concurrency(&resources));

    info!("Worker configured with tags: {:?}", tags);
    info!("Worker resources: {:?}", resources);
    info!("Worker concurrency: {}", concurrency);

    // Connect to the broker and register. We retry with backoff so the worker
    // can be started before the broker, or survive a broker restart, instead
    // of dying on the first failed connection with a cryptic "transport error".
    let worker_id = connect_and_register(&broker_address, &tags, &resource_map).await?;

    info!("Worker registered with ID: {}", worker_id);

    // Create shared worker state with the built-in echo/default registry.
    let worker_state = Arc::new(Mutex::new(WorkerState::new(
        worker_id.clone(),
        broker_address,
        resources,
        tags,
        concurrency,
    )));

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
    let resource_map = parse_resources(&resources_str)?;

    let resources = ResourceAvailability {
        available: resource_map.clone(),
        total: resource_map.clone(),
    };

    let concurrency =
        concurrency.unwrap_or_else(|| derive_concurrency(&resources));

    info!("Worker configured with tags: {:?}", tags);
    info!("Worker resources: {:?}", resources);
    info!("Worker concurrency: {}", concurrency);

    let worker_id = connect_and_register(&broker_address, &tags, &resource_map).await?;
    info!("Worker registered with ID: {}", worker_id);

    let worker_state = Arc::new(Mutex::new(WorkerState::with_registry(
        worker_id.clone(),
        broker_address,
        resources,
        tags,
        concurrency,
        registry,
    )));

    run_worker(worker_state, heartbeat_interval).await
}

/// Shared run loop: spawn the heartbeat + task-processing loops on a built
/// `WorkerState`, await Ctrl+C, then shut both down. Used by both
/// [`start_worker`] and [`start_worker_with_registry`].
async fn run_worker(
    worker_state: Arc<Mutex<WorkerState>>,
    heartbeat_interval: u64,
) -> Result<()> {
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

/// Connect to the broker and register the worker, retrying with backoff until
/// it succeeds. This makes startup resilient to the broker not being ready yet
/// (or restarting) — instead of exiting with a bare "transport error".
pub async fn connect_and_register(
    broker_address: &str,
    tags: &[String],
    resources: &HashMap<String, u32>,
) -> Result<String> {
    info!("Connecting to broker at {}", broker_address);

    let mut backoff = Duration::from_millis(500);
    const MAX_BACKOFF: Duration = Duration::from_secs(5);

    loop {
        match try_connect_and_register(broker_address, tags, resources).await {
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
    resources: &HashMap<String, u32>,
) -> Result<String> {
    let mut client = ChopFlowBrokerClient::connect(broker_address.to_string())
        .await
        .map_err(|e| chopflow_core::error::ChopFlowError::NetworkError(e.to_string()))?;

    let register_request = Request::new(RegisterWorkerRequest {
        address: "localhost".to_string(), // In production, this would be the actual address
        tags: tags.to_vec(),
        resources: resources.clone(),
    });

    let response = client
        .register_worker(register_request)
        .await
        .map_err(|e| chopflow_core::error::ChopFlowError::NetworkError(e.to_string()))?;

    Ok(response.into_inner().worker_id)
}

pub async fn send_heartbeat(worker_state: &Arc<Mutex<WorkerState>>) -> Result<()> {
    let state = worker_state.lock().await;

    let mut client = ChopFlowBrokerClient::connect(state.broker_address.clone())
        .await
        .map_err(|e| {
            error!("Failed to connect to broker for heartbeat: {}", e);
            chopflow_core::error::ChopFlowError::NetworkError(e.to_string())
        })?;

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

    // Look up the appropriate handler for the task name
    let handler_fn = {
        // Use a block to ensure the lock is released after we get the handler
        let state = worker_state.lock().await;

        match state
            .task_registry
            .get(&task_name)
            .or_else(|| state.task_registry.get("default"))
        {
            Some(handler) => {
                // Clone the function pointer so we can release the lock
                let handler_clone = *handler;
                Some(handler_clone)
            }
            None => None,
        }
    };

    // If no handler found, acknowledge failure
    if handler_fn.is_none() {
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
    }

    // Safe to unwrap: we returned above when no handler was found.
    let handler_fn = handler_fn.unwrap();

    // Execute the handler on the blocking pool so a long-running or CPU-bound
    // handler can't stall the async runtime's worker threads. The handler is a
    // plain `fn` pointer (Send + 'static) and the payload is `Send`, so the
    // closure is `Send + 'static` as `spawn_blocking` requires.
    let outcome = tokio::task::spawn_blocking(move || handler_fn(payload))
        .await
        .map_err(|join_err| ChopFlowError::Other(anyhow::Error::new(join_err)))?;

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
    let mut client = ChopFlowBrokerClient::connect(state.broker_address.clone())
        .await
        .map_err(|e| {
            error!("Failed to connect to broker for task acknowledgment: {}", e);
            chopflow_core::error::ChopFlowError::NetworkError(e.to_string())
        })?;

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
    info!("Starting task processing loop (concurrency={})", concurrency);

    let poll_interval = Duration::from_secs(2);
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
    let (broker_address, worker_id) = {
        let state = worker_state.lock().await;
        (state.broker_address.clone(), state.id.clone())
    };

    let mut client = ChopFlowBrokerClient::connect(broker_address)
        .await
        .map_err(|e| {
            error!("Failed to connect to broker for fetch: {}", e);
            chopflow_core::error::ChopFlowError::NetworkError(e.to_string())
        })?;

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
