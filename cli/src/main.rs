//! ChopFlow CLI binary — clap wiring + tracing around the [`chopflow_cli`] library.

use chopflow_core::error::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
            chopflow_cli::enqueue_task(cli.broker, task, name, tags, eta).await?;
        }
        Commands::Status { id, all } => {
            chopflow_cli::get_status(cli.broker, id, all).await?;
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
                chopflow_cli::schedule_create(
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
            ScheduleCmd::List => chopflow_cli::schedule_list(cli.broker).await?,
            ScheduleCmd::Delete { id } => chopflow_cli::schedule_delete(cli.broker, id).await?,
        },
    }

    Ok(())
}
