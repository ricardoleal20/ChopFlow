/*!
# ChopFlow CLI

The command-line interface for ChopFlow, a distributed task queue.

The CLI provides tools for:
- Enqueueing tasks from JSON files (optionally staged pipelines with an
  idempotency key)
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

// ---- Task options (stages + idempotency key) ------------------------------

/// Task-level options carried outside the payload: the declared pipeline
/// stages and the submit-time idempotency key. Sourced from CLI flags
/// (authoritative) or from top-level fields in the task JSON file.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TaskOptions {
    /// Declared pipeline stages, e.g. `["chunk", "embed", "index"]`. Workers
    /// checkpoint per stage and resume from the last completed stage on retry.
    pub stages: Option<Vec<String>>,
    /// Submit-time idempotency key: submitting the same key again returns the
    /// existing task instead of creating a duplicate.
    pub idempotency_key: Option<String>,
}

/// Parse a comma-separated stage list (`--stages "chunk,embed,index"`),
/// trimming whitespace around each entry. Empty entries (including an
/// entirely empty flag value) are rejected so a typo cannot silently declare
/// a nameless stage.
pub fn parse_stages(stages: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in stages.split(',') {
        let stage = entry.trim();
        if stage.is_empty() {
            return Err(chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!(
                "--stages must be a comma-separated list of non-empty stage names, got '{}'",
                stages
            )));
        }
        out.push(stage.to_string());
    }
    Ok(out)
}

/// Extract task-level metadata (`stages`, `idempotency_key`) from the
/// top-level object of a task JSON file, removing the keys from the payload —
/// they describe the task, not the handler's input. Non-object payloads carry
/// no task options. An error is returned when a reserved key is present with
/// the wrong shape (`stages` must be an array of strings, `idempotency_key`
/// a string).
pub fn extract_task_options(payload: &mut serde_json::Value) -> Result<TaskOptions> {
    let Some(obj) = payload.as_object_mut() else {
        return Ok(TaskOptions::default());
    };

    let stages = match obj.remove("stages") {
        None => None,
        Some(serde_json::Value::Array(items)) => {
            let mut stages = Vec::with_capacity(items.len());
            for item in items {
                let stage = item.as_str().ok_or_else(|| {
                    chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!(
                        "task file field \"stages\" must be an array of stage names"
                    ))
                })?;
                stages.push(stage.to_string());
            }
            // An empty array declares nothing.
            (!stages.is_empty()).then_some(stages)
        }
        Some(_) => {
            return Err(chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!(
                "task file field \"stages\" must be an array of stage names"
            )))
        }
    };

    let idempotency_key = match obj.remove("idempotency_key") {
        None => None,
        Some(serde_json::Value::String(key)) => (!key.is_empty()).then_some(key),
        Some(_) => {
            return Err(chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!(
                "task file field \"idempotency_key\" must be a string"
            )))
        }
    };

    Ok(TaskOptions {
        stages,
        idempotency_key,
    })
}

/// Merge enqueue options: CLI flags win over values declared inside the task
/// JSON file when both are present.
pub fn merge_task_options(flags: TaskOptions, file: TaskOptions) -> TaskOptions {
    TaskOptions {
        stages: flags.stages.or(file.stages),
        idempotency_key: flags.idempotency_key.or(file.idempotency_key),
    }
}

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
    options: TaskOptions,
) -> Result<()> {
    info!("Reading task from {:?}", task_path);

    // Read task file
    let task_json = std::fs::read_to_string(task_path)
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;

    // Parse task
    let mut payload: serde_json::Value = serde_json::from_str(&task_json)
        .map_err(|e| chopflow_core::error::ChopFlowError::SerializationError(e.to_string()))?;

    // Task-level metadata declared inside the file (stripped from the
    // payload); CLI flags win over the file when both are present.
    let file_options = extract_task_options(&mut payload)?;
    let options = merge_task_options(options, file_options);

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
    if let Some(stages) = &options.stages {
        println!("Stages: {:?}", stages);
    }
    if let Some(key) = &options.idempotency_key {
        println!("Idempotency key: {}", key);
    }
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
        // Empty = not declared (the proto treats empty as absent).
        stages: options.stages.unwrap_or_default(),
        idempotency_key: options.idempotency_key.unwrap_or_default(),
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
                    if !task.stages.is_empty() {
                        println!("Stages: {:?}", task.stages);
                    }
                    if !task.idempotency_key.is_empty() {
                        println!("Idempotency key: {}", task.idempotency_key);
                    }
                    if let Some(eta) = &task.eta {
                        println!("ETA: {}s {}ns", eta.seconds, eta.nanos);
                    }
                    if !task.result.is_empty() {
                        println!("Result: {}", task.result);
                    }

                    // Pipeline checkpoints. The status `Task` message does not
                    // carry them, so they are fetched via the dedicated
                    // GetCheckpoints RPC on the same connection.
                    match client
                        .get_checkpoints(chopflow::GetCheckpointsRequest {
                            task_id: task_id.clone(),
                        })
                        .await
                    {
                        Ok(checkpoints) => {
                            let checkpoints = &checkpoints.get_ref().checkpoints;
                            if !checkpoints.is_empty() {
                                println!("Checkpoints:");
                                for checkpoint in checkpoints {
                                    println!(
                                        "  {} ({}): {}",
                                        checkpoint.stage,
                                        checkpoint.recorded_at,
                                        checkpoint_payload_preview(&checkpoint.payload, 80)
                                    );
                                }
                            }
                        }
                        // The status output above is already complete; a
                        // checkpoint fetch failure degrades to a warning
                        // rather than failing the whole command.
                        Err(status) => {
                            error!(
                                "Failed to fetch checkpoints for task {}: {}",
                                task_id, status
                            );
                        }
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

/// Resolve the broker gRPC address. When `env` is `Some(name)`, the address is
/// looked up by name in the `environments.yml` catalog at `env_config` and its
/// `grpc_url` returned (overriding `default_broker`). When `env` is `None`,
/// `default_broker` is returned unchanged. Pure + synchronous so it can be
/// unit-tested without a broker.
pub fn resolve_broker(default_broker: &str, env: Option<&str>, env_config: &str) -> Result<String> {
    let Some(name) = env else {
        return Ok(default_broker.to_string());
    };

    let cfg = chopflow_core::config::EnvironmentsConfig::load(env_config)
        .map_err(|e| chopflow_core::error::ChopFlowError::Other(e.into()))?;
    let entry = cfg.find(name).ok_or_else(|| {
        chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!(
            "environment '{}' not found in {} (known: {})",
            name,
            env_config,
            cfg.environments
                .iter()
                .map(|e| e.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })?;
    if entry.grpc_url.is_empty() {
        return Err(chopflow_core::error::ChopFlowError::Other(anyhow::anyhow!(
            "environment '{}' has no grpc_url in {}",
            name,
            env_config
        )));
    }
    Ok(entry.grpc_url.clone())
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

/// One-line preview of a checkpoint payload for `status get`: collapses all
/// whitespace and elides to at most `max_chars` characters (on a char
/// boundary), appending an ellipsis when truncated. Empty payloads render as
/// `(empty)`.
pub fn checkpoint_payload_preview(payload: &str, max_chars: usize) -> String {
    let collapsed = payload.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return "(empty)".to_string();
    }
    if collapsed.chars().count() <= max_chars {
        return collapsed;
    }
    if max_chars == 0 {
        return String::new();
    }
    let mut preview: String = collapsed.chars().take(max_chars - 1).collect();
    preview.push('…');
    preview
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
    /// Broker address (gRPC). Ignored when `--env` is given — the address is
    /// then resolved from `--env-config` (`environments.yml`).
    #[arg(long, short, default_value = "http://localhost:8000")]
    pub broker: String,

    /// Target a named environment from `environments.yml` (e.g. `prod`,
    /// `staging`). Resolves the broker's gRPC URL from the catalog, overriding
    /// `--broker`.
    #[arg(long)]
    pub env: Option<String>,

    /// Path to the fleet catalog used to resolve `--env`.
    #[arg(long, default_value = "config/environments.yml")]
    pub env_config: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Enqueue a task, optionally as a staged pipeline with an idempotency key
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

        /// Pipeline stage names (comma-separated), e.g. "chunk,embed,index".
        /// Workers checkpoint progress per stage and resume from the last
        /// completed stage on retry. May also be declared as a top-level
        /// "stages" array in the task JSON file; this flag wins when both
        /// are present.
        #[arg(long, value_name = "STAGES")]
        stages: Option<String>,

        /// Idempotency key: submitting the same key again returns the
        /// existing task instead of creating a duplicate. May also be
        /// declared as "idempotency_key" in the task JSON file; this flag
        /// wins when both are present.
        #[arg(long, value_name = "KEY")]
        idempotency_key: Option<String>,
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

/// CLI entry point. Initialize tracing and dispatch to the per-subcommand
/// handler. Accepts an already-parsed [`Cli`] so the `chopflow` umbrella
/// binary can route its flat CLI verbs (`chopflow enqueue`, `chopflow status`,
/// `chopflow schedule`) into this same dispatch; the standalone `chopflow-cli`
/// binary just passes `Cli::parse()` through.
pub async fn run(cli: Cli) -> Result<()> {
    tracing_subscriber::fmt::init();

    let broker = resolve_broker(&cli.broker, cli.env.as_deref(), &cli.env_config)?;

    match cli.command {
        Commands::Enqueue {
            task,
            name,
            tags,
            eta,
            priority,
            stages,
            idempotency_key,
        } => {
            let options = TaskOptions {
                stages: stages.map(|s| parse_stages(&s)).transpose()?,
                idempotency_key: idempotency_key.filter(|k| !k.trim().is_empty()),
            };
            enqueue_task(broker, task, name, tags, eta, priority, options).await?;
        }
        Commands::Status { id, all } => {
            get_status(broker, id, all).await?;
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
                    broker,
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
            ScheduleCmd::List => schedule_list(broker).await?,
            ScheduleCmd::Delete { id } => schedule_delete(broker, id).await?,
        },
    }

    Ok(())
}
