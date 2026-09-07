//! ChopFlow demo seed binary.
//!
//! Hits the broker HTTP API to enqueue one of each demo handler and create two
//! schedules (a `*/2 * * * *` cron with overlap `skip`, and a one-shot 5
//! minutes out with overlap `allow`). Run it after the broker + demo worker
//! are up — `demos/run.sh` does all three in order.
//!
//! ```bash
//! cargo run -p chopflow_demos --bin chopflow_demo_seed -- [broker_base_url]
//! # broker_base_url defaults to http://localhost:8080
//! ```

use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let broker = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost:8080".to_string());
    let api = format!("{}/api", broker);
    let client = reqwest::Client::new();

    let post = |path: &str, body: serde_json::Value| {
        let c = client.clone();
        let url = format!("{}{}", api, path);
        let path = path.to_string();
        async move {
            let r = c.post(&url).json(&body).send().await?;
            println!("POST {} -> {}", path, r.status());
            r.json::<serde_json::Value>().await
        }
    };

    // One of each handler.
    post("/tasks", json!({ "name": "resize_image", "payload": { "width": 128, "height": 96 }, "tags": ["demo"], "resources": { "cpu": 1 } })).await.ok();
    post("/tasks", json!({ "name": "batch_compute", "payload": { "n": 96 }, "tags": ["demo"], "resources": { "cpu": 2 } })).await.ok();
    post("/tasks", json!({ "name": "simulate_pipeline", "payload": { "stages": ["download","process","upload"] }, "tags": ["demo"], "resources": { "cpu": 1 } })).await.ok();
    post("/tasks", json!({ "name": "flaky_handler", "payload": { "seed": 7 }, "tags": ["demo"], "max_retries": 5, "resources": { "cpu": 1 } })).await.ok();

    // A cron schedule every 2 minutes.
    post("/schedules", json!({
        "name": "every-2min-compute",
        "task_template": { "name": "batch_compute", "payload": { "n": 64 }, "tags": ["demo"], "resources": { "cpu": 2 }, "max_retries": 3 },
        "kind": { "type": "cron", "cron": "*/2 * * * *" },
        "overlap_policy": "skip"
    })).await.ok();

    // A one-shot 5 minutes out.
    let in5 = chrono::Utc::now() + chrono::Duration::minutes(5);
    post("/schedules", json!({
        "name": "one-off-pipeline",
        "task_template": { "name": "simulate_pipeline", "payload": { "stages": ["download","process","upload"] }, "tags": ["demo"], "resources": { "cpu": 1 }, "max_retries": 1 },
        "kind": { "type": "oneshot", "eta": in5.to_rfc3339() },
        "overlap_policy": "allow"
    })).await.ok();

    println!(
        "Seed complete. Open the dashboard at {} — tasks flowing + 2 schedules ticking.",
        broker
    );
    Ok(())
}
