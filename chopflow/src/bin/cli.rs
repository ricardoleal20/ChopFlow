//! `chopflow-cli` — ChopFlow CLI binary (umbrella build).
//!
//! Thin wrapper over [`chopflow_cli::run`]. Identical behavior to the
//! `chopflow-cli` binary built from the `chopflow_cli` crate.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(chopflow_cli::run().await?)
}
