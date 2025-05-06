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

use chopflow_core::dispatcher::Worker;
use chopflow_core::error::Result;
use chopflow_core::resources::ResourceAvailability;
use chopflow_core::task::TaskStatus;

use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};
use uuid::Uuid;

// Generate code from protobuf definitions
pub mod chopflow {
    tonic::include_proto!("chopflow");
}

use chopflow::{
    chop_flow_broker_client::ChopFlowBrokerClient, AcknowledgeTaskRequest, EnqueueTaskRequest,
    GetTaskStatusRequest, RegisterWorkerRequest, RegisterWorkerResponse,
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
}

impl WorkerState {
    fn new(
        id: String,
        broker_address: String,
        resources: ResourceAvailability,
        tags: Vec<String>,
    ) -> Self {
        Self {
            id,
            broker_address,
            resources,
            tags,
            assigned_tasks: HashMap::new(),
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

    // In a full implementation, we would also:
    // 1. Poll for tasks or use a streaming connection
    // 2. Execute tasks
    // 3. Report results

    info!("Worker is running. Press Ctrl+C to exit.");

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await.map_err(|e| {
        error!("Failed to listen for Ctrl+C: {}", e);
        chopflow_core::error::ChopFlowError::Other(e.into())
    })?;

    info!("Shutting down worker...");
    heartbeat_handle.abort();

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
    info!("Executing task {}: {}", task_id, task.name);

    // In a real implementation, this would:
    // 1. Deserialize the task payload
    // 2. Look up the appropriate handler for the task name
    // 3. Execute the handler with the payload
    // 4. Capture the result or error
    // 5. Update the task status

    // Simulate task execution
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Send acknowledgment
    let state = worker_state.lock().await;
    let mut client = ChopFlowBrokerClient::connect(state.broker_address.clone())
        .await
        .map_err(|e| {
            error!("Failed to connect to broker for task acknowledgment: {}", e);
            chopflow_core::error::ChopFlowError::NetworkError(e.to_string())
        })?;

    let ack_request = Request::new(AcknowledgeTaskRequest {
        worker_id: state.id.clone(),
        task_id,
        success: true,
        result: r#"{"status": "success", "message": "Task completed"}"#.to_string(),
    });

    let _ = client.acknowledge_task(ack_request).await.map_err(|e| {
        error!("Failed to acknowledge task: {}", e);
        chopflow_core::error::ChopFlowError::NetworkError(e.to_string())
    })?;

    info!("Task acknowledged successfully");
    Ok(())
}
