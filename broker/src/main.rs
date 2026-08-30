/*!
# ChopFlow Broker

Binary entry point for the ChopFlow broker. The service implementation lives in
the library crate (`chopflow_broker`); this binary just parses CLI arguments,
constructs the storage backend, reconciles in-flight tasks, and serves gRPC.
*/

use chopflow_broker::{
    build_storage, ChopFlowBrokerService, Cli, Commands, StorageBackend,
};
use clap::Parser;
use tonic::transport::Server;
use tracing::info;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let Commands::Start {
        config,
        host,
        port,
        storage,
        db_path,
    } = cli.command;

    // `config` is accepted for forward-compat (e.g. loading broker.yml) but
    // not yet consumed.
    let _ = config;

    let backend = match storage.as_str() {
        "memory" => StorageBackend::Memory,
        "sqlite" => {
            info!("Persistence: sqlite ({})", db_path);
            StorageBackend::Sqlite { path: db_path }
        }
        other => {
            return Err(format!("unknown storage backend: {} (use memory|sqlite)", other).into());
        }
    };

    let storage = build_storage(&backend)?;

    // Reconcile in-flight tasks: any task left Running from a previous run is
    // reset to Queued (its worker is gone after a restart).
    if let Err(e) = storage.reconcile().await {
        tracing::warn!("storage reconcile failed: {}", e);
    }

    let service = ChopFlowBrokerService::new(storage);
    service.spawn_timeout_monitor();

    let addr: std::net::SocketAddr = format!("{}:{}", host, port).parse()?;
    info!("Starting ChopFlow broker on {}", addr);

    Server::builder()
        .add_service(service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
