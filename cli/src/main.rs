//! ChopFlow CLI binary — thin entry point.
//!
//! All command definition and dispatch lives in [`chopflow_cli`] (`lib.rs`);
//! this binary just delegates to `chopflow_cli::run()`. The `chopflow` umbrella
//! crate's `chopflow-cli` binary calls the same function.

#[tokio::main]
async fn main() -> chopflow_core::error::Result<()> {
    chopflow_cli::run().await
}
