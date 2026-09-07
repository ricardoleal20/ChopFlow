/*!
# ChopFlow Broker

Binary entry point for the ChopFlow broker. The service implementation lives in
the library crate (`chopflow_broker`); this binary just parses CLI arguments,
constructs the storage backend, reconciles in-flight tasks, and serves both
gRPC (for workers / CLI) and HTTP/JSON + the embedded dashboard UI (for the
browser) over a single shared [`BrokerState`].
*/

use chopflow_broker::{
    build_storage, BrokerState, ChopFlowBrokerService, Cli, Commands, StorageBackend,
};
use clap::Parser;
use std::time::Duration;
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
        http_port,
        storage,
        db_path,
        open,
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

    // One shared state object backs both the gRPC service and the HTTP layer,
    // so the dashboard sees live updates from workers and vice versa.
    let state = BrokerState::new(storage);
    let service = ChopFlowBrokerService::from_state(state.clone());
    service.spawn_timeout_monitor();

    // Recompute schedule next_fire times (skip missed cron runs), then spawn
    // the 1s schedule ticker.
    if let Err(e) = chopflow_broker::reconcile_schedules(state.storage.as_ref()).await {
        tracing::warn!("schedule reconcile failed: {}", e);
    }
    service.spawn_schedule_ticker();

    let grpc_addr: std::net::SocketAddr = format!("{}:{}", host, port).parse()?;
    let http_addr: std::net::SocketAddr = format!("{}:{}", host, http_port).parse()?;

    info!("ChopFlow gRPC  on {}", grpc_addr);
    info!(
        "ChopFlow HTTP/ on {}  (dashboard: http://{})",
        http_addr, http_addr
    );

    // Spawn the HTTP server alongside gRPC. Both run until either errors.
    let http_router = chopflow_broker::http::router(state);
    let http_task = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(http_addr)
            .await
            .expect("failed to bind HTTP port");
        axum::serve(listener, http_router)
            .await
            .expect("HTTP server error");
    });

    // Optionally open the dashboard in the default browser. We wait a beat for
    // the HTTP listener to bind so the page is actually ready. Failures (e.g.
    // a headless server with no browser) are logged, not fatal.
    if open {
        let url = format!("http://{}", http_addr);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            match open_browser(&url) {
                Ok(()) => info!("Opened dashboard in browser: {}", url),
                Err(e) => info!("Could not open browser ({}). Open manually: {}", e, url),
            }
        });
    }

    Server::builder()
        .add_service(service.into_server())
        .serve(grpc_addr)
        .await?;

    // If gRPC returns, don't leave the HTTP task dangling.
    http_task.abort();
    Ok(())
}

/// Open `url` in the platform's default browser. Cross-platform: macOS `open`,
/// Windows `start`, Linux `xdg-open`. Non-fatal if no DE/browser is present.
fn open_browser(url: &str) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    let (cmd, args) = ("open", vec![url]);
    #[cfg(target_os = "windows")]
    let (cmd, args) = ("cmd", vec!["/C", "start", "", url]);
    #[cfg(all(unix, not(target_os = "macos")))]
    let (cmd, args) = ("xdg-open", vec![url]);

    std::process::Command::new(cmd)
        .args(&args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    Ok(())
}
