//! `chopflow-broker` — ChopFlow broker binary (umbrella build).
//!
//! Thin wrapper over [`chopflow_broker::run`]. Identical behavior to the
//! `chopflow-broker` binary built from the `chopflow_broker` crate; this one
//! ships from the `chopflow` umbrella crate so `cargo install chopflow` gives
//! you the broker, CLI, and MCP server in one install.

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    chopflow_broker::run().await
}
