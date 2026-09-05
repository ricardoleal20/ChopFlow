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
use tracing::{error, info};

// Generate code from protobuf definitions
pub mod chopflow {
    // Include the generated code from the build script
    include!(concat!(env!("OUT_DIR"), "/chopflow.rs"));
}

use chopflow::{
    chop_flow_broker_client::ChopFlowBrokerClient, EnqueueTaskRequest, GetQueueStatsRequest,
    GetTaskStatusRequest,
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

    /// Manage schedules
    Schedule {
        #[command(subcommand)]
        action: ScheduleCmd,
    },
}

#[derive(Subcommand)]
enum ScheduleCmd {
    /// Create a schedule
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        task: String,
        #[arg(long)]
        cron: Option<String>,
        #[arg(long)]
        eta: Option<String>,
        #[arg(long, default_value = "{}")]
        payload: String,
        #[arg(long, default_value = "")]
        tags: String,
        #[arg(long, default_value = "")]
        resources: String,
        #[arg(long, default_value_t = 3)]
        max_retries: u32,
        #[arg(long, default_value = "skip")]
        overlap: String,
    },
    /// List schedules
    List,
    /// Delete a schedule
    Delete { id: String },
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
        Commands::Schedule { action } => match action {
            ScheduleCmd::Create {
                name,
                task,
                cron,
                eta,
                payload,
                tags,
                resources,
                max_retries,
                overlap,
            } => {
                schedule_create(
                    cli.broker,
                    name,
                    task,
                    cron,
                    eta,
                    payload,
                    tags,
                    resources,
                    max_retries,
                    overlap,
                )
                .await?;
            }
            ScheduleCmd::List => schedule_list(cli.broker).await?,
            ScheduleCmd::Delete { id } => schedule_delete(cli.broker, id).await?,
        },
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
                    println!("Status: {}", status_name(task.status));
                    println!("Tags: {:?}", task.tags);
                    println!("Retries: {}/{}", task.retry_count, task.max_retries);
                    if let Some(eta) = &task.eta {
                        println!("ETA: {}s {}ns", eta.seconds, eta.nanos);
                    }
                    if !task.result.is_empty() {
                        println!("Result: {}", task.result);
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

        let mut client = connect_to_broker(&broker_address).await?;
        let request = chopflow::ListTasksRequest {
            limit: 50,
            offset: 0,
            filter_status: Vec::new(),
        };

        match client.list_tasks(request).await {
            Ok(response) => {
                let resp = response.get_ref();
                println!("Tasks ({} total):", resp.total_count);
                for task in &resp.tasks {
                    println!(
                        "  {} [{}] {} (retries {}/{})",
                        task.id,
                        status_name(task.status),
                        task.name,
                        task.retry_count,
                        task.max_retries
                    );
                }
                Ok(())
            }
            Err(status) => {
                error!("Failed to list tasks: {}", status);
                Err(chopflow_core::error::ChopFlowError::Other(status.into()))
            }
        }
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

async fn schedule_create(
    broker: String,
    name: String,
    task: String,
    cron: Option<String>,
    eta: Option<String>,
    payload: String,
    tags: String,
    resources: String,
    max_retries: u32,
    overlap: String,
) -> Result<()> {
    let mut client = connect_to_broker(&broker).await?;
    let payload_val: serde_json::Value = serde_json::from_str(&payload)
        .map_err(|e| chopflow_core::error::ChopFlowError::SerializationError(e.to_string()))?;
    let tags_vec: Vec<String> = if tags.is_empty() {
        vec![]
    } else {
        tags.split(',').map(|s| s.trim().to_string()).collect()
    };
    let mut res_map = std::collections::HashMap::new();
    for entry in resources.split(',') {
        let e = entry.trim();
        if e.is_empty() {
            continue;
        }
        let parts: Vec<&str> = e.split(':').collect();
        if parts.len() == 2 {
            res_map.insert(parts[0].to_string(), parts[1].parse().unwrap_or(0));
        }
    }
    let kind = match (cron, eta) {
        (Some(c), None) => Some(chopflow::schedule_kind::Kind::Cron(c)),
        (None, Some(e)) => {
            let dt = chrono::DateTime::parse_from_rfc3339(&e)
                .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?
                .with_timezone(&chrono::Utc);
            Some(chopflow::schedule_kind::Kind::Eta(prost_types::Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }))
        }
        _ => {
            return Err(chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!(
                "exactly one of --cron or --eta is required"
            )))
        }
    };
    let overlap_enum = match overlap.as_str() {
        "skip" => chopflow::OverlapPolicy::OverlapSkip,
        "coalesce" => chopflow::OverlapPolicy::OverlapCoalesce,
        "allow" => chopflow::OverlapPolicy::OverlapAllow,
        _ => {
            return Err(chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!(
                "invalid overlap (skip|coalesce|allow)"
            )))
        }
    };
    let schedule = chopflow::Schedule {
        id: String::new(),
        name,
        task_template: Some(chopflow::TaskTemplate {
            name: task,
            payload: serde_json::to_string(&payload_val).unwrap_or_default(),
            tags: tags_vec,
            resources: res_map,
            max_retries,
        }),
        kind: Some(chopflow::ScheduleKind { kind }),
        overlap_policy: overlap_enum as i32,
        enabled: true,
        last_fired: None,
        next_fire: None,
        created_at: None,
    };
    let resp = client
        .create_schedule(chopflow::CreateScheduleRequest {
            schedule: Some(schedule),
        })
        .await
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;
    println!("Created schedule: {}", resp.get_ref().schedule_id);
    Ok(())
}

async fn schedule_list(broker: String) -> Result<()> {
    let mut client = connect_to_broker(&broker).await?;
    let resp = client
        .list_schedules(chopflow::ListSchedulesRequest {})
        .await
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;
    for s in &resp.get_ref().schedules {
        let kind = match &s.kind {
            Some(k) => match &k.kind {
                Some(chopflow::schedule_kind::Kind::Cron(c)) => format!("cron {}", c),
                Some(chopflow::schedule_kind::Kind::Eta(_)) => "oneshot".into(),
                None => "?".into(),
            },
            None => "?".into(),
        };
        println!(
            "  {} [{}] {} ({})",
            s.id,
            if s.enabled { "on" } else { "off" },
            s.name,
            kind
        );
    }
    Ok(())
}

async fn schedule_delete(broker: String, id: String) -> Result<()> {
    let mut client = connect_to_broker(&broker).await?;
    client
        .delete_schedule(chopflow::DeleteScheduleRequest { id })
        .await
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;
    println!("Deleted.");
    Ok(())
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

/// Map a proto task status value to a human-readable name.
fn status_name(status: i32) -> &'static str {
    match status {
        0 => "CREATED",
        1 => "QUEUED",
        2 => "RUNNING",
        3 => "COMPLETED",
        4 => "FAILED",
        5 => "DEADLETTERED",
        6 => "CANCELLED",
        _ => "UNKNOWN",
    }
}
