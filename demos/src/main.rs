/*!
# ChopFlow Demo Worker

A drop-in worker that registers the four demo handlers (`resize_image`,
`batch_compute`, `simulate_pipeline`, `flaky_handler`) plus an `echo` and a
`default` fallback. The worker loop, concurrency, heartbeats, and gRPC wiring
all come from the [`chopflow_worker`] crate — this binary only wires up CLI
parsing and the demo handler registry, so demos get bounded concurrency for
free and stay in lock-step with the generic worker.

Run it against a broker:

```bash
cargo run -p chopflow_demos --bin chopflow_demo_worker -- start \
    --broker http://localhost:8000 --tags demo,ml --resources cpu:4
```
*/

use chopflow_core::error::Result;
use chopflow_worker::TaskRegistry;
use clap::{Parser, Subcommand};
use tracing::info;

mod handlers;

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

        /// Max tasks to run at once. Defaults to the sum of declared resources
        /// (e.g. `cpu:4` → 4). Override when you want more parallelism than the
        /// resource totals imply.
        #[arg(long)]
        concurrency: Option<usize>,
    },
}

/// Build the worker-crate [`TaskRegistry`] from the demo handler map.
fn build_registry() -> TaskRegistry {
    let mut registry = TaskRegistry::new();
    for (name, handler) in handlers::registry() {
        registry.register(name, handler);
    }
    registry
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            broker,
            tags,
            resources,
            heartbeat_interval,
            concurrency,
        } => {
            info!("Starting ChopFlow demo worker connected to {}", broker);
            chopflow_worker::start_worker_with_registry(
                broker,
                tags,
                resources,
                heartbeat_interval,
                concurrency,
                build_registry(),
            )
            .await?;
        }
    }

    Ok(())
}
