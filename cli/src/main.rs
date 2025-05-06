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
use tracing::{error, info, warn};

/// ChopFlow CLI - Task Queue Client
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Broker address
    #[arg(long, short, default_value = "http://localhost:7575")]
    broker: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Enqueue a task
    Enqueue {
        /// Path to task JSON file
        #[arg(long, short)]
        task: PathBuf,

        /// Task name
        #[arg(long, short)]
        name: Option<String>,

        /// Tags (comma-separated)
        #[arg(long, short)]
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

    // In a real implementation, we would send the task to the broker here
    // For the MVP, we'll just print the task details
    info!("Task would be enqueued:");
    println!("Task ID: {}", task.id);
    println!("Name: {}", task.name);
    println!("Tags: {:?}", task.tags);
    if let Some(eta) = task.eta {
        println!("ETA: {}", eta);
    }

    Ok(())
}

async fn get_status(broker_address: String, id: Option<String>, all: bool) -> Result<()> {
    if let Some(task_id) = id {
        info!("Getting status for task {}", task_id);

        // In a real implementation, we would fetch the task status from the broker
        // For the MVP, we'll just print a placeholder
        println!("Task ID: {}", task_id);
        println!("Status: Unknown (not implemented)");
    } else if all {
        info!("Listing all tasks");

        // In a real implementation, we would fetch all tasks from the broker
        // For the MVP, we'll just print a placeholder
        println!("Task listing not implemented");
    } else {
        // Show queue stats
        info!("Getting queue stats");

        // In a real implementation, we would fetch queue stats from the broker
        // For the MVP, we'll just print a placeholder
        println!("Queue length: 0 (not implemented)");
        println!("Processed tasks: 0 (not implemented)");
        println!("Failed tasks: 0 (not implemented)");
    }

    Ok(())
}
