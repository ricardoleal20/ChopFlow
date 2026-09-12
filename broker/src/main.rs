/*!
# ChopFlow Broker

Binary entry point for the ChopFlow broker. The service implementation — CLI
parsing, storage construction, reconciliation, and gRPC + HTTP serving — lives
in [`chopflow_broker::run`]; this binary just parses args and delegates. The
`chopflow` umbrella crate's unified `chopflow broker` subcommand calls the same
function.
*/

use clap::Parser;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    chopflow_broker::run(chopflow_broker::Cli::parse()).await
}
