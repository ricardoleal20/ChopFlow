//! `chopflow-mcp` — ChopFlow MCP server binary (umbrella build).
//!
//! Thin wrapper over [`chopflow_mcp::run`]. Identical behavior to the
//! `chopflow-mcp` binary built from the `chopflow-mcp` crate.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(chopflow_mcp::run().await?)
}
