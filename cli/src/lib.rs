/*!
# ChopFlow CLI

The command-line interface for ChopFlow, a distributed task queue.

The CLI provides tools for:
- Enqueueing tasks from JSON files
- Checking task status and results
- Viewing queue statistics
- Managing schedules

This crate is a library plus a thin binary (`main.rs`). The library exposes the
per-subcommand handlers and the pure validation/parsing helpers so they can be
unit- and integration-tested; the binary only wires up CLI parsing + tracing.
*/

use chopflow_core::error::Result;
use chopflow_core::task::Task;

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use tonic::transport::Channel;
use tracing::{error, info};

// Generate code from protobuf definitions.
// The generated gRPC methods return `Result<_, tonic::Status>` and the oneof
// enums are large, which trips `result_large_err` / `large_enum_variant`. The
// types come from tonic/prost, so we allow these two lints on the module.
//
// The generated code is re-exported from the shared `chopflow-proto` crate so
// this crate stays crates.io-publishable (no `build.rs` pointing outside it).
pub use chopflow_proto::chopflow;

use chopflow::{
    chop_flow_broker_client::ChopFlowBrokerClient, EnqueueTaskRequest, GetQueueStatsRequest,
    GetTaskStatusRequest,
};

pub async fn enqueue_task(
    broker_address: String,
    task_path: PathBuf,
    name: Option<String>,
    tags_str: Option<String>,
    eta_str: Option<String>,
    priority: i32,
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
        priority,
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

pub async fn get_status(broker_address: String, id: Option<String>, all: bool) -> Result<()> {
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

// TODO: collapse these fields into a `ScheduleCreateArgs` struct to drop below
// the 7-argument threshold and remove this allow. Left for a focused refactor —
// the call sites mirror the clap `--flag` surface and would change together.
#[allow(clippy::too_many_arguments)]
pub async fn schedule_create(
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
    priority: i32,
) -> Result<()> {
    // Validate + build the schedule before connecting, so misconfiguration
    // fails fast (and is unit-testable without a broker).
    let schedule = build_schedule(
        name,
        task,
        cron,
        eta,
        payload,
        tags,
        resources,
        max_retries,
        overlap,
        priority,
    )?;

    let mut client = connect_to_broker(&broker).await?;
    let resp = client
        .create_schedule(chopflow::CreateScheduleRequest {
            schedule: Some(schedule),
        })
        .await
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;
    println!("Created schedule: {}", resp.get_ref().schedule_id);
    Ok(())
}

/// Parse a comma-separated `name:amount` resource string into a map. Malformed
/// entries are silently skipped (a CLI affordance — the worker is stricter).
pub fn parse_resource_map(resources: &str) -> HashMap<String, u32> {
    let mut res_map = HashMap::new();
    for entry in resources.split(',') {
        let e = entry.trim();
        if e.is_empty() {
            continue;
        }
        let parts: Vec<&str> = e.split(':').collect();
        if parts.len() == 2 {
            res_map.insert(
                parts[0].trim().to_string(),
                parts[1].trim().parse().unwrap_or(0),
            );
        }
    }
    res_map
}

/// Parse the overlap-policy CLI string into the proto enum.
pub fn parse_overlap(overlap: &str) -> Result<chopflow::OverlapPolicy> {
    match overlap {
        "skip" => Ok(chopflow::OverlapPolicy::OverlapSkip),
        "coalesce" => Ok(chopflow::OverlapPolicy::OverlapCoalesce),
        "allow" => Ok(chopflow::OverlapPolicy::OverlapAllow),
        _ => Err(chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!(
            "invalid overlap (skip|coalesce|allow)"
        ))),
    }
}

/// Build the proto `ScheduleKind.kind` oneof from exactly one of `cron`/`eta`.
/// Returns an error if neither or both are set.
pub fn build_schedule_kind(
    cron: Option<String>,
    eta: Option<String>,
) -> Result<Option<chopflow::schedule_kind::Kind>> {
    match (cron, eta) {
        (Some(c), None) => Ok(Some(chopflow::schedule_kind::Kind::Cron(c))),
        (None, Some(e)) => {
            let dt = chrono::DateTime::parse_from_rfc3339(&e)
                .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?
                .with_timezone(&chrono::Utc);
            Ok(Some(chopflow::schedule_kind::Kind::Eta(
                prost_types::Timestamp {
                    seconds: dt.timestamp(),
                    nanos: dt.timestamp_subsec_nanos() as i32,
                },
            )))
        }
        _ => Err(chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!(
            "exactly one of --cron or --eta is required"
        ))),
    }
}

/// Assemble a full `chopflow::Schedule` from CLI arguments, validating the
/// overlap policy, the cron/eta exclusivity, and the payload JSON. Pure — no
/// network — so it can be unit-tested directly.
#[allow(clippy::too_many_arguments)]
pub fn build_schedule(
    name: String,
    task: String,
    cron: Option<String>,
    eta: Option<String>,
    payload: String,
    tags: String,
    resources: String,
    max_retries: u32,
    overlap: String,
    priority: i32,
) -> Result<chopflow::Schedule> {
    let payload_val: serde_json::Value = serde_json::from_str(&payload)
        .map_err(|e| chopflow_core::error::ChopFlowError::SerializationError(e.to_string()))?;
    let tags_vec: Vec<String> = if tags.is_empty() {
        vec![]
    } else {
        tags.split(',').map(|s| s.trim().to_string()).collect()
    };
    let res_map = parse_resource_map(&resources);
    let kind = build_schedule_kind(cron, eta)?;
    let overlap_enum = parse_overlap(&overlap)?;
    Ok(chopflow::Schedule {
        id: String::new(),
        name,
        task_template: Some(chopflow::TaskTemplate {
            name: task,
            payload: serde_json::to_string(&payload_val).unwrap_or_default(),
            tags: tags_vec,
            resources: res_map,
            max_retries,
            priority,
        }),
        kind: Some(chopflow::ScheduleKind { kind }),
        overlap_policy: overlap_enum as i32,
        enabled: true,
        last_fired: None,
        next_fire: None,
        created_at: None,
    })
}

pub async fn schedule_list(broker: String) -> Result<()> {
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

pub async fn schedule_delete(broker: String, id: String) -> Result<()> {
    let mut client = connect_to_broker(&broker).await?;
    client
        .delete_schedule(chopflow::DeleteScheduleRequest { id })
        .await
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;
    println!("Deleted.");
    Ok(())
}

// Helper function to connect to the broker
pub async fn connect_to_broker(broker_address: &str) -> Result<ChopFlowBrokerClient<Channel>> {
    match ChopFlowBrokerClient::connect(broker_address.to_string()).await {
        Ok(client) => Ok(client),
        Err(e) => {
            error!("Failed to connect to broker at {}: {}", broker_address, e);
            Err(chopflow_core::error::ChopFlowError::Other(e.into()))
        }
    }
}

/// Map a proto task status value to a human-readable name.
pub fn status_name(status: i32) -> &'static str {
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

// ---- CLI definition + entry point ----------------------------------------
//
// The clap structs live in the library (not `main.rs`) so the `chopflow`
// umbrella crate's `chopflow-cli` binary can call `chopflow_cli::run()`
// directly, and so the command surface can be unit-tested.

use clap::{Parser, Subcommand};

/// ChopFlow CLI - Task Queue Client
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Broker address
    #[arg(long, short, default_value = "http://localhost:8000")]
    pub broker: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
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

        /// Dispatch priority (higher = claimed first). Default 0.
        #[arg(long, default_value_t = 0)]
        priority: i32,
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
pub enum ScheduleCmd {
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
        /// Dispatch priority for materialized tasks (higher = first). Default 0.
        #[arg(long, default_value_t = 0)]
        priority: i32,
    },
    /// List schedules
    List,
    /// Delete a schedule
    Delete { id: String },
}

/// CLI entry point. Initialize tracing, parse arguments, and dispatch to the
/// per-subcommand handler. The `chopflow-cli` binary's `main.rs` is a thin
/// wrapper around this; the `chopflow` umbrella crate calls it too.
pub async fn run() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Enqueue {
            task,
            name,
            tags,
            eta,
            priority,
        } => {
            enqueue_task(cli.broker, task, name, tags, eta, priority).await?;
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
                priority,
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
                    priority,
                )
                .await?;
            }
            ScheduleCmd::List => schedule_list(cli.broker).await?,
            ScheduleCmd::Delete { id } => schedule_delete(cli.broker, id).await?,
        },
    }

    Ok(())
}
