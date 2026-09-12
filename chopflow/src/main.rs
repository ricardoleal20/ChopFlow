//! ChopFlow — unified command-line entry point.
//!
//! One `chopflow` binary with subcommands for every role:
//!
//! ```text
//! chopflow broker start     the broker (gRPC + HTTP/dashboard server)
//! chopflow mcp              the MCP server (AI tools over the broker HTTP API, stdio)
//! chopflow enqueue -f …     enqueue a task from a JSON file
//! chopflow status [–id|–all]   task status, list tasks, or queue stats
//! chopflow schedule …       manage schedules (create | list | delete)
//! ```
//!
//! Each subcommand delegates to the matching library crate's `run()`; this
//! binary only adds the top-level clap dispatch. The standalone binaries
//! (`chopflow-broker`, `chopflow-cli`, `chopflow-mcp`) remain available when
//! building those crates directly, but `cargo install chopflow` gives you this
//! single unified command.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// Shared options for the CLI verbs (enqueue / status / schedule). These talk
/// gRPC to the broker, so `--broker` is the gRPC address (default port 8000) —
/// distinct from `chopflow mcp`'s `--broker`, which is the HTTP base URL.
#[derive(Args)]
struct CliOpts {
    /// Broker gRPC address.
    #[arg(long, short, default_value = "http://localhost:8000")]
    broker: String,
}

#[derive(Parser)]
#[command(
    name = "chopflow",
    author,
    version,
    about = "ChopFlow — a durable distributed task queue (unified command)",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the broker (gRPC + HTTP/JSON API + embedded dashboard).
    Broker {
        #[command(subcommand)]
        command: BrokerCmd,
    },

    /// Run the MCP server (exposes the broker as AI-friendly tools over stdio).
    Mcp {
        /// Broker HTTP base URL (e.g. http://127.0.0.1:8080). Overrides CHOPFLOW_HTTP_URL.
        #[arg(long)]
        broker: Option<String>,
    },

    /// Enqueue a task from a JSON file.
    Enqueue {
        #[command(flatten)]
        opts: CliOpts,

        /// Path to task JSON file.
        #[arg(long, short = 'f')]
        task: PathBuf,

        /// Task name.
        #[arg(long, short)]
        name: Option<String>,

        /// Tags (comma-separated).
        #[arg(long, short = 'g')]
        tags: Option<String>,

        /// ETA (earliest time of arrival) in ISO 8601 format.
        #[arg(long)]
        eta: Option<String>,

        /// Dispatch priority (higher = claimed first). Default 0.
        #[arg(long, default_value_t = 0)]
        priority: i32,
    },

    /// Get task status, list tasks, or show queue stats.
    Status {
        #[command(flatten)]
        opts: CliOpts,

        /// Task ID.
        #[arg(long, short)]
        id: Option<String>,

        /// Show all tasks.
        #[arg(long, short)]
        all: bool,
    },

    /// Manage schedules (create | list | delete).
    Schedule {
        #[command(flatten)]
        opts: CliOpts,

        #[command(subcommand)]
        action: chopflow_cli::ScheduleCmd,
    },
}

#[derive(Subcommand)]
enum BrokerCmd {
    /// Start the broker server.
    Start {
        /// Path to config file.
        #[arg(long, short, default_value = "config/broker.yml")]
        config: String,

        /// Host to bind to.
        #[arg(long, short = 'H', default_value = "127.0.0.1")]
        host: String,

        /// Port to listen on (gRPC).
        #[arg(long, short, default_value = "8000")]
        port: u16,

        /// Port for the HTTP/JSON API + embedded dashboard UI.
        #[arg(long, default_value = "8080")]
        http_port: u16,

        /// Task storage backend.
        #[arg(long, default_value = "sqlite")]
        storage: String,

        /// Path to the SQLite database file (only used when --storage sqlite).
        #[arg(long, default_value = "chopflow.db")]
        db_path: String,

        /// Open the dashboard UI in the default browser on startup.
        #[arg(long, default_value_t = false)]
        open: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Broker {
            command:
                BrokerCmd::Start {
                    config,
                    host,
                    port,
                    http_port,
                    storage,
                    db_path,
                    open,
                },
        } => {
            let broker_cli = chopflow_broker::Cli {
                command: chopflow_broker::Commands::Start {
                    config,
                    host,
                    port,
                    http_port,
                    storage,
                    db_path,
                    open,
                },
            };
            chopflow_broker::run(broker_cli)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }

        Command::Mcp { broker } => {
            chopflow_mcp::run(chopflow_mcp::Cli { broker }).await?;
        }

        Command::Enqueue {
            opts,
            task,
            name,
            tags,
            eta,
            priority,
        } => {
            let cli_cli = chopflow_cli::Cli {
                broker: opts.broker,
                command: chopflow_cli::Commands::Enqueue {
                    task,
                    name,
                    tags,
                    eta,
                    priority,
                },
            };
            chopflow_cli::run(cli_cli)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }

        Command::Status { opts, id, all } => {
            let cli_cli = chopflow_cli::Cli {
                broker: opts.broker,
                command: chopflow_cli::Commands::Status { id, all },
            };
            chopflow_cli::run(cli_cli)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }

        Command::Schedule { opts, action } => {
            let cli_cli = chopflow_cli::Cli {
                broker: opts.broker,
                command: chopflow_cli::Commands::Schedule { action },
            };
            chopflow_cli::run(cli_cli)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    Ok(())
}
