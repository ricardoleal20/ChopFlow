/*!
# ChopFlow Demo Worker

A drop-in worker that registers the four demo handlers (`resize_image`,
`batch_compute`, `simulate_pipeline`, `flaky_handler`) plus an `echo` and a
`default` fallback. Structurally identical to `worker/src/main.rs` — the only
difference is the handler registry, which comes from `handlers::registry()`.

Run it against a broker:

```bash
cargo run -p chopflow_demos --bin chopflow_demo_worker -- start \
    --broker http://localhost:8000 --tags demo,ml --resources cpu:4
```
*/

use chopflow_core::error::Result;
use chopflow_core::resources::ResourceAvailability;

use anyhow;
use clap::{Parser, Subcommand};
use handlers::Handler;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tonic::Request;
use tracing::{error, info};

mod handlers;

// Generate code from protobuf definitions (same proto as worker/cli).
pub mod chopflow {
    tonic::include_proto!("chopflow");
}

use chopflow::{
    chop_flow_broker_client::ChopFlowBrokerClient, AcknowledgeTaskRequest, FetchTasksRequest,
    RegisterWorkerRequest, ResourceAvailability as ProtoResourceAvailability,
    Task as ProtoTask, WorkerHeartbeatRequest,
};

/// ChopFlow Demo Worker - Task Executor
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a demo worker
    Start {
        /// Broker address
        #[arg(long, short, default_value = "http://localhost:8000")]
        broker: String,

        /// Tags to subscribe to (comma-separated)
        #[arg(long, short, default_value = "demo")]
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
    task_registry: HashMap<&'static str, Handler>,
}

impl WorkerState {
    fn new(
        id: String,
        broker_address: String,
        resources: ResourceAvailability,
        tags: Vec<String>,
    ) -> Self {
        // The only difference from `worker/src/main.rs`: pull in the demo
        // handler registry instead of an echo-only one.
        Self {
            id,
            broker_address,
            resources,
            tags,
            assigned_tasks: HashMap::new(),
            task_registry: handlers::registry(),
        }
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
            info!("Starting ChopFlow demo worker connected to {}", broker);
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

    // Parse resources. Each entry is `name:amount`. A malformed entry is an
    // error, not a silent skip — otherwise the worker would register with no
    // resources and the broker would reject it with a confusing message.
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

    info!("Demo worker is running. Press Ctrl+C to exit.");

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
async fn connect_and_register(
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
async fn try_connect_and_register(
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

        // Try the exact task name, then the `default` fallback.
        state
            .task_registry
            .get(task_name.as_str())
            .or_else(|| state.task_registry.get("default"))
            .copied()
    };

    // If no handler found, acknowledge failure
    let handler_fn = match handler_fn {
        Some(h) => h,
        None => {
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
    };

    // Execute the handler
    let result = match handler_fn(payload) {
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

/// Start processing tasks.
///
/// Polls the broker for tasks this worker can execute (pull model). Each
/// fetched task is executed and acknowledged. Tasks run sequentially for
/// now; a future version will dispatch them to a bounded concurrency pool
/// sized by the worker's declared resources.
async fn start_task_processing(worker_state: &Arc<Mutex<WorkerState>>) -> Result<()> {
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
async fn fetch_tasks(worker_state: &Arc<Mutex<WorkerState>>) -> Result<Vec<ProtoTask>> {
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
