//! ChopFlow CLI binary — thin entry point.
//!
//! All command definition and dispatch lives in [`chopflow_cli`] (`lib.rs`);
//! this binary just parses args and delegates to `chopflow_cli::run()`. The
//! `chopflow` umbrella crate routes its flat CLI verbs through the same function.

use clap::Parser;

#[tokio::main]
async fn main() -> chopflow_core::error::Result<()> {
    chopflow_cli::run(chopflow_cli::Cli::parse()).await
}
