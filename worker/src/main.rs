//! ChopFlow worker binary — CLI wiring + tracing around the [`chopflow_worker`]
//! library.

use chopflow_core::error::Result;
use clap::{Parser, Subcommand};
use tracing::info;

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
            chopflow_worker::start_worker(broker, tags, resources, heartbeat_interval).await?;
        }
    }

    Ok(())
}
