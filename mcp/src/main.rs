//! ChopFlow MCP server binary — thin entry point.
//!
//! All implementation lives in [`chopflow_mcp`] (`lib.rs`); this binary just
//! delegates to `chopflow_mcp::run()`. The umbrella `chopflow` crate's
//! `chopflow-mcp` binary calls the same function, so both stay in lockstep.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    chopflow_mcp::run().await
}
