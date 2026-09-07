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

use chopflow_core::error::Result;
use chopflow_core::resources::ResourceAvailability;

use anyhow;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
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
}

impl WorkerState {
    pub fn new(
        id: String,
        broker_address: String,
        resources: ResourceAvailability,
        tags: Vec<String>,
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
        }
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
) -> Result<()> {
    // Parse tags
    let tags = parse_tags(&tags_str);

    // Parse resources.
    let resource_map = parse_resources(&resources_str)?;

    let resources = ResourceAvailability {
        available: resource_map.clone(),
        total: resource_map.clone(),
    };

    info!("Worker configured with tags: {:?}", tags);
    info!("Worker resources: {:?}", resources);

    // Connect to the broker and register. We retry with backoff so the worker
    // can be started before the broker, or survive a broker restart, instead
    // of dying on the first failed connection with a cryptic "transport error".
    let worker_id = connect_and_register(&broker_address, &tags, &resource_map).await?;

    info!("Worker registered with ID: {}", worker_id);

    // Create shared worker state
    let worker_state = Arc::new(Mutex::new(WorkerState::new(
        worker_id.clone(),
        broker_address,
        resources,
        tags,
    )));

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

    // Execute the handler
    let result = match handler_fn.unwrap()(payload) {
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
/// Polls the broker for tasks this worker can execute (pull model). Each
/// fetched task is executed and acknowledged. Tasks run sequentially for
/// now; a future version will dispatch them to a bounded concurrency pool
/// sized by the worker's declared resources.
pub async fn start_task_processing(worker_state: &Arc<Mutex<WorkerState>>) -> Result<()> {
    info!("Starting task processing loop");

    let poll_interval = Duration::from_secs(2);

    loop {
        // Fetch a batch of tasks from the broker.
        match fetch_tasks(worker_state).await {
            Ok(tasks) if !tasks.is_empty() => {
                for task in tasks {
                    if let Err(e) = execute_task(worker_state, task).await {
                        error!("Failed to execute task: {}", e);
                    }
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

/// Pull a batch of ready tasks from the broker for this worker.
pub async fn fetch_tasks(worker_state: &Arc<Mutex<WorkerState>>) -> Result<Vec<ProtoTask>> {
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
        max_tasks: 4,
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
