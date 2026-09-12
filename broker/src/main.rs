/*!
# ChopFlow Broker

Binary entry point for the ChopFlow broker. The service implementation — CLI
parsing, storage construction, reconciliation, and gRPC + HTTP serving — lives
in [`chopflow_broker::run`]; this binary just delegates to it. The `chopflow`
umbrella crate's `chopflow-broker` binary calls the same function.
*/

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    chopflow_broker::run().await
}
