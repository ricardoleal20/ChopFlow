//! ChopFlow MCP server binary — thin entry point.
//!
//! All implementation lives in [`chopflow_mcp`] (`lib.rs`); this binary just
//! parses args and delegates to `chopflow_mcp::run()`. The umbrella `chopflow`
//! crate's `chopflow mcp` subcommand calls the same function.

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    chopflow_mcp::run(chopflow_mcp::Cli::parse()).await
}
