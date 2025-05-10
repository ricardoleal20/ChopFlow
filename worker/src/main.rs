/*!
# ChopFlow Worker

This is the worker executable for ChopFlow, a distributed task queue.

Workers are responsible for:
- Registering with the broker
- Declaring their capabilities (tags) and resources
- Executing assigned tasks
- Reporting task results back to the broker
- Sending regular heartbeats to indicate health

This implementation provides:
- Command-line configuration via clap
- Resource and tag declaration
- gRPC client connection to broker
- Task execution and result reporting
- Heartbeat mechanism
- Signal handling for graceful shutdown
*/

use chopflow_core::error::Result;
use chopflow_core::resources::ResourceAvailability;

use anyhow;
use clap::{Parser, Subcommand};
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
    chop_flow_broker_client::ChopFlowBrokerClient, AcknowledgeTaskRequest, RegisterWorkerRequest,
    ResourceAvailability as ProtoResourceAvailability, Task as ProtoTask, WorkerHeartbeatRequest,
};

/// ChopFlow Worker - Task Executor
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a worker
    Start {
        /// Broker address
        #[arg(long, short, default_value = "http://localhost:8000")]
        broker: String,

        /// Tags to subscribe to (comma-separated)
        #[arg(long, short, default_value = "default")]
        tags: String,

        /// Resources available (format: resource:amount,resource:amount)
        #[arg(long, short, default_value = "cpu:1")]
        resources: String,

        /// Heartbeat interval in seconds
        #[arg(long, default_value = "30")]
        heartbeat_interval: u64,
    },
}

#[derive(Clone)]
struct WorkerState {
    id: String,
    broker_address: String,
    resources: ResourceAvailability,
    tags: Vec<String>,
    assigned_tasks: HashMap<String, ProtoTask>,
    task_registry: TaskRegistry,
}

impl WorkerState {
    fn new(
        id: String,
        broker_address: String,
        resources: ResourceAvailability,
        tags: Vec<String>,
    ) -> Self {
        let mut registry = TaskRegistry::new();

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

/// A function that can handle a task
type TaskHandlerFn = fn(serde_json::Value) -> Result<serde_json::Value>;

/// Registry for task handlers
#[derive(Clone)]
struct TaskRegistry {
    handlers: HashMap<String, TaskHandlerFn>,
}

impl TaskRegistry {
    fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register a new task handler
    fn register(&mut self, task_name: &str, handler: TaskHandlerFn) {
        self.handlers.insert(task_name.to_string(), handler);
    }

    /// Get a handler for a task
    fn get(&self, task_name: &str) -> Option<&TaskHandlerFn> {
        self.handlers.get(task_name)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            broker,
            tags,
            resources,
            heartbeat_interval,
        } => {
            info!("Starting ChopFlow worker connected to {}", broker);
            start_worker(broker, tags, resources, heartbeat_interval).await?;
        }
    }

    Ok(())
}

async fn start_worker(
    broker_address: String,
    tags_str: String,
    resources_str: String,
    heartbeat_interval: u64,
) -> Result<()> {
    // Parse tags
    let tags: Vec<String> = tags_str.split(',').map(|s| s.trim().to_string()).collect();

    // Parse resources
    let mut resource_map = HashMap::new();
    for resource_str in resources_str.split(',') {
        let parts: Vec<&str> = resource_str.split(':').collect();
        if parts.len() == 2 {
            let resource_name = parts[0].trim().to_string();
            if let Ok(amount) = parts[1].trim().parse::<u32>() {
                resource_map.insert(resource_name, amount);
            }
        }
    }

    let resources = ResourceAvailability {
        available: resource_map.clone(),
        total: resource_map.clone(),
    };

    info!("Worker configured with tags: {:?}", tags);
    info!("Worker resources: {:?}", resources);

    // Connect to broker
    let mut client = ChopFlowBrokerClient::connect(broker_address.clone())
        .await
        .map_err(|e| {
            error!("Failed to connect to broker: {}", e);
            chopflow_core::error::ChopFlowError::NetworkError(e.to_string())
        })?;

    // Register worker with broker
    let register_request = Request::new(RegisterWorkerRequest {
        address: "localhost".to_string(), // In production, this would be the actual address
        tags: tags.clone(),
        resources: resource_map,
    });

    let response = client
        .register_worker(register_request)
        .await
        .map_err(|e| {
            error!("Failed to register worker: {}", e);
            chopflow_core::error::ChopFlowError::NetworkError(e.to_string())
        })?;

    let worker_id = response.into_inner().worker_id;
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

async fn send_heartbeat(worker_state: &Arc<Mutex<WorkerState>>) -> Result<()> {
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
async fn execute_task(worker_state: &Arc<Mutex<WorkerState>>, task: ProtoTask) -> Result<()> {
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
async fn send_task_acknowledgment(
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

/// Start processing tasks
/// * NOTE (WIP):
/// In the final system, this would fetch tasks from the broker using
/// either polling or a streaming connection
async fn start_task_processing(worker_state: &Arc<Mutex<WorkerState>>) -> Result<()> {
    info!("Starting task processing loop");

    // * NOTE (WIP):
    // In the final system, this would:
    // 1. Poll for available tasks or maintain a streaming connection
    // 2. Dispatch tasks to worker threads for parallel execution
    // 3. Manage task timeouts and retries
    // 4. Handle worker shutdown gracefully

    let mut interval = time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        info!("Polling for tasks...");

        // * NOTE (WIP):
        // In the final system, this would fetch tasks from the broker
        let example_task = ProtoTask {
            id: format!("test-task-{}", uuid::Uuid::new_v4()),
            name: "example_task".to_string(),
            payload: r#"{"param1": "value1", "param2": 42}"#.to_string(),
            tags: vec!["test".to_string()],
            enqueue_time: None,
            eta: None,
            retry_count: 0,
            max_retries: 3,
            status: 0, // CREATED
            resources: HashMap::new(),
        };

        // Execute the task
        if let Err(e) = execute_task(worker_state, example_task).await {
            error!("Failed to execute task: {}", e);
        }

        // * NOTE (WIP):
        // In the final system, this would be more sophisticated:
        // - Poll multiple tasks
        // - Track running tasks
        // - Manage concurrency/parallelism
        // - Handle failures and retries

        // For the MVP purposes, sleep for 30 seconds before polling again
        // This prevents excessive log spam
        tokio::time::sleep(Duration::from_secs(30)).await;
    }

    // Note: This is unreachable in practice since the loop is infinite
    // * NOTE (WIP):
    // In the final system, we'd have a proper shutdown mechanism
    #[allow(unreachable_code)]
    Ok(())
}
