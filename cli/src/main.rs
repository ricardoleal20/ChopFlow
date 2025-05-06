/*!
# ChopFlow CLI

This is the command-line interface for ChopFlow, a distributed task queue.

The CLI provides tools for:
- Enqueueing tasks from JSON files
- Checking task status and results
- Viewing queue statistics
- Managing workers and brokers
- Testing and debugging the system

In a full implementation, the CLI would offer more advanced features like
worker management, task cancellation, and queue introspection tools.
*/

use chopflow_core::error::Result;
use chopflow_core::task::Task;

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tonic::transport::Channel;
use tracing::{error, info, warn};

// Generate code from protobuf definitions
pub mod chopflow {
    // The generated code from the build script will be in OUT_DIR
    tonic::include_proto!("chopflow");
}

use chopflow::{
    chop_flow_broker_client::ChopFlowBrokerClient, EnqueueTaskRequest, EnqueueTaskResponse,
    GetQueueStatsRequest, GetQueueStatsResponse, GetTaskStatusRequest, GetTaskStatusResponse,
};

/// ChopFlow CLI - Task Queue Client
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Broker address
    #[arg(long, short, default_value = "http://localhost:8000")]
    broker: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Enqueue a task
    Enqueue {
        /// Path to task JSON file
        #[arg(long, short = 'f')]
        task: PathBuf,

        /// Task name
        #[arg(long, short)]
        name: Option<String>,

        /// Tags (comma-separated)
        #[arg(long, short = 'g')]
        tags: Option<String>,

        /// ETA (earliest time of arrival) in ISO 8601 format
        #[arg(long)]
        eta: Option<String>,
    },

    /// Get task status
    Status {
        /// Task ID
        #[arg(long, short)]
        id: Option<String>,

        /// Show all tasks
        #[arg(long, short)]
        all: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Enqueue {
            task,
            name,
            tags,
            eta,
        } => {
            enqueue_task(cli.broker, task, name, tags, eta).await?;
        }
        Commands::Status { id, all } => {
            get_status(cli.broker, id, all).await?;
        }
    }

    Ok(())
}

async fn enqueue_task(
    broker_address: String,
    task_path: PathBuf,
    name: Option<String>,
    tags_str: Option<String>,
    eta_str: Option<String>,
) -> Result<()> {
    info!("Reading task from {:?}", task_path);

    // Read task file
    let task_json = std::fs::read_to_string(task_path)
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;

    // Parse task
    let payload: serde_json::Value = serde_json::from_str(&task_json)
        .map_err(|e| chopflow_core::error::ChopFlowError::SerializationError(e.to_string()))?;

    // Create task
    let task_name = name.unwrap_or_else(|| "default".to_string());
    let mut task = Task::new(task_name, payload);

    // Add tags if provided
    if let Some(tags) = tags_str {
        let tags_vec: Vec<String> = tags.split(',').map(|s| s.trim().to_string()).collect();
        task = task.with_tags(tags_vec);
    }

    // Parse ETA if provided
    if let Some(eta) = eta_str {
        let eta_time = DateTime::parse_from_rfc3339(&eta)
            .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?
            .with_timezone(&Utc);
        task = task.with_eta(eta_time);
    }

    // Show the task details
    info!("Enqueueing task:");
    println!("Task ID: {}", task.id);
    println!("Name: {}", task.name);
    println!("Tags: {:?}", task.tags);
    if let Some(eta) = task.eta {
        println!("ETA: {}", eta);
    }

    // Connect to the broker and send the task
    info!("Connecting to broker at {}", broker_address);
    let mut client = connect_to_broker(&broker_address).await?;

    // Prepare the request
    let mut request = EnqueueTaskRequest {
        name: task.name,
        tags: task.tags,
        payload: serde_json::to_string(&task.payload).unwrap_or_default(),
        max_retries: task.max_retries,
        resources: task.resources,
        eta: None,
    };

    // Add ETA if present
    if let Some(eta) = task.eta {
        request.eta = Some(prost_types::Timestamp {
            seconds: eta.timestamp(),
            nanos: eta.timestamp_subsec_nanos() as i32,
        });
    }

    // Send the request
    match client.enqueue_task(request).await {
        Ok(response) => {
            info!("Task enqueued successfully");
            println!("Broker assigned task ID: {}", response.get_ref().task_id);
            Ok(())
        }
        Err(status) => {
            error!("Failed to enqueue task: {}", status);
            Err(chopflow_core::error::ChopFlowError::Other(status.into()))
        }
    }
}

async fn get_status(broker_address: String, id: Option<String>, all: bool) -> Result<()> {
    if let Some(task_id) = id {
        info!("Getting status for task {}", task_id);

        // Connect to the broker
        let mut client = connect_to_broker(&broker_address).await?;

        // Get the task status
        let request = GetTaskStatusRequest {
            task_id: task_id.clone(),
        };

        match client.get_task_status(request).await {
            Ok(response) => {
                let status_response = response.get_ref();
                if let Some(task) = &status_response.task {
                    println!("Task ID: {}", task.id);
                    println!("Name: {}", task.name);
                    println!("Status: {:?}", task.status);
                    println!("Tags: {:?}", task.tags);
                    if let Some(eta) = &task.eta {
                        println!("ETA: {}s {}ns", eta.seconds, eta.nanos);
                    }
                } else {
                    println!("Task not found: {}", task_id);
                }
                Ok(())
            }
            Err(status) => {
                error!("Failed to get task status: {}", status);
                Err(chopflow_core::error::ChopFlowError::Other(status.into()))
            }
        }
    } else if all {
        info!("Listing all tasks");

        // Not implemented yet, would use the list_tasks endpoint
        println!("Task listing not implemented");
        Ok(())
    } else {
        // Show queue stats
        info!("Getting queue stats");

        // Connect to the broker
        let mut client = connect_to_broker(&broker_address).await?;

        // Get queue stats
        let request = GetQueueStatsRequest {};

        match client.get_queue_stats(request).await {
            Ok(response) => {
                let stats = response.get_ref();
                println!("Queue length: {}", stats.queue_length);
                println!("Tasks processing: {}", stats.tasks_processing);
                println!("Tasks completed: {}", stats.tasks_completed);
                println!("Tasks failed: {}", stats.tasks_failed);
                println!("Active workers: {}", stats.active_workers);
                Ok(())
            }
            Err(status) => {
                error!("Failed to get queue stats: {}", status);
                Err(chopflow_core::error::ChopFlowError::Other(status.into()))
            }
        }
    }
}

// Helper function to connect to the broker
async fn connect_to_broker(broker_address: &str) -> Result<ChopFlowBrokerClient<Channel>> {
    match ChopFlowBrokerClient::connect(broker_address.to_string()).await {
        Ok(client) => Ok(client),
        Err(e) => {
            error!("Failed to connect to broker at {}: {}", broker_address, e);
            Err(chopflow_core::error::ChopFlowError::Other(e.into()))
        }
    }
}
