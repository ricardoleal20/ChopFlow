//! ChopFlow demo seed binary.
//!
//! Hits the broker HTTP API to enqueue one of each demo handler, the durable
//! `rag.ingest` pipeline (submitted twice with the same idempotency key to
//! demonstrate dedup), and two schedules (a `*/2 * * * *` cron with overlap
//! `skip`, and a one-shot 5 minutes out with overlap `allow`). Run it after
//! the broker + demo worker are up — `demos/run.sh` and `demos/rag.sh` do all
//! three in order.
//!
//! ```bash
//! cargo run -p chopflow_demos --bin chopflow_demo_seed -- [broker_base_url]
//! # broker_base_url defaults to http://localhost:8080
//! ```

use serde_json::json;

/// The sample document for the `rag.ingest` durable-pipeline demo.
const RAG_DOCUMENT: &str = "Task queues sit at the heart of every data pipeline that \
needs to survive crashes. A producer submits a unit of work to a broker, which \
persists it and waits. Workers declare what they can do and pull work when \
they are ready, so a slow or crashed worker never blocks the producers behind \
it.

Retries are where naive pipelines fall over. If a worker dies halfway through \
a multi-stage job, restarting the whole job wastes the work that already \
succeeded and can even corrupt downstream state when earlier stages run a \
second time. Checkpointing fixes this: each stage persists its intermediate \
output, and a retry resumes from the last checkpoint instead of the beginning.

Rate limits are the other recurring pain point. AI workloads in particular \
burn through provider quotas, and a burst of tasks can exhaust a quota for \
the next minute or hour. Treating a quota as a replenishing resource, \
refilled at a fixed rate, lets the queue wait for tokens instead of failing.

Put together, a task queue with durable checkpoints, idempotent submits, and \
rate-limited resources gives AI agents a substrate they can trust: submit \
work with a key, watch the stages progress, and know that a crash anywhere \
along the way costs only the unfinished tail.";

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

    // The durable-pipeline showcase: rag.ingest declares its stages, requires
    // a replenishing llm.rpm token, and carries an idempotency key. Submitting
    // the same key twice must return the SAME task id (the second response
    // carries `deduplicated: true`) — proving both dedup and the pipeline.
    let rag_task = json!({
        "name": "rag.ingest",
        "payload": { "document": RAG_DOCUMENT, "chunk_size": 64 },
        "tags": ["demo"],
        "resources": { "llm.rpm": 1 },
        "stages": ["chunk", "embed", "index"],
        "idempotency_key": "rag-demo-doc-1",
    });
    let first = post("/tasks", rag_task.clone()).await.ok();
    let second = post("/tasks", rag_task).await.ok();
    if let (Some(first), Some(second)) = (first, second) {
        let first_id = first["task_id"].as_str().unwrap_or("<no id>");
        let second_id = second["task_id"].as_str().unwrap_or("<no id>");
        if first_id == second_id {
            println!(
                "Idempotent submit: both rag.ingest submits returned the same task id {} (deduplicated: {})",
                first_id,
                second["deduplicated"].as_bool().unwrap_or(false)
            );
        } else {
            println!(
                "WARNING: rag.ingest submits returned different ids ({} vs {}) — dedup did not apply",
                first_id, second_id
            );
        }
    }

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
