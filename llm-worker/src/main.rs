//! # ChopFlow LLM Worker
//!
//! A ChopFlow worker whose handlers call an LLM. It registers with the broker
//! like any worker, polls for tasks, and dispatches `llm.complete` /
//! `llm.chat` tasks to an OpenAI-compatible chat-completions endpoint — turning
//! ChopFlow into an orchestrator for agent/LLM jobs. An agent (via the MCP
//! server) enqueues an `llm.complete` task and polls until the result lands.
//!
//! This is MCP Phase 2: the "AI worker handler" — ChopFlow drives the LLM.
//!
//! The loop mirrors `worker/src/main.rs` (connect-with-backoff, heartbeat,
//! pull-model fetch, ack). It diverges in two ways: handlers are **async**
//! (LLM calls are network I/O), and tasks run with **bounded concurrency**
//! (LLM work is I/O-bound, so one worker can pipeline several requests). The
//! generic worker-concurrency + async-handler unification is ROADMAP #2; this
//! crate is intentionally self-contained until that refactor lands.
//!
//! ## Configuration
//! - `--broker` (default `http://localhost:8000`) — broker gRPC address.
//! - `--tags` (default `llm`) — subscription tags; tasks must carry one to route here.
//! - `--resources` (default `llm:1`) — declared resources.
//! - `--concurrency` (default `4`) — max in-flight LLM calls.
//! - `--api-base` / `OPENAI_BASE_URL` (default `https://api.openai.com/v1`).
//! - `--model` / `LLM_MODEL` (default `gpt-4o-mini`).
//! - `--api-key` / `OPENAI_API_KEY` (required).

use anyhow::Result;
use chopflow_core::error::ChopFlowError;
use chopflow_core::resources::ResourceAvailability;
use clap::Parser;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time;
use tonic::Request;
use tracing::{error, info, warn};

// Generate code from protobuf definitions (same proto as the broker/worker).
pub mod chopflow {
    tonic::include_proto!("chopflow");
}

use chopflow::{
    chop_flow_broker_client::ChopFlowBrokerClient, AcknowledgeTaskRequest, FetchTasksRequest,
    RegisterWorkerRequest, ResourceAvailability as ProtoResourceAvailability, Task as ProtoTask,
    WorkerHeartbeatRequest,
};

/// CLI configuration.
#[derive(Parser, Debug)]
#[command(
    name = "chopflow-llm-worker",
    about = "ChopFlow worker that executes LLM tasks (llm.complete / llm.chat)"
)]
struct Cli {
    /// Broker gRPC address.
    #[arg(long, default_value = "http://localhost:8000")]
    broker: String,

    /// Tags to subscribe to (comma-separated). Tasks must carry one to route here.
    #[arg(long, short, default_value = "llm")]
    tags: String,

    /// Resources available (format: resource:amount,resource:amount).
    #[arg(long, short, default_value = "llm:1")]
    resources: String,

    /// Heartbeat interval in seconds.
    #[arg(long, default_value = "30")]
    heartbeat_interval: u64,

    /// Max in-flight LLM calls (I/O-bound, so pipelining helps throughput).
    #[arg(long, default_value = "4")]
    concurrency: usize,

    /// OpenAI-compatible API base URL. Overrides OPENAI_BASE_URL.
    #[arg(long)]
    api_base: Option<String>,

    /// Default model for completions. Overrides LLM_MODEL.
    #[arg(long)]
    model: Option<String>,

    /// API key. Overrides OPENAI_API_KEY.
    #[arg(long)]
    api_key: Option<String>,
}

/// LLM endpoint configuration, shared across all in-flight handlers.
#[derive(Clone)]
struct LlmConfig {
    api_base: String,
    model: String,
    api_key: String,
    http: reqwest::Client,
}

/// Shared worker state: identity, broker address, declared resources.
#[derive(Clone)]
struct WorkerState {
    id: String,
    broker_address: String,
    resources: ResourceAvailability,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let cli = Cli::parse();

    let api_key = cli
        .api_key
        .or_else(|| std::env::var("OPENAI_API_KEY").ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no API key: set --api-key or OPENAI_API_KEY. \
                 (For a non-OpenAI provider, also set --api-base / OPENAI_BASE_URL.)"
            )
        })?;
    let api_base = cli
        .api_base
        .or_else(|| std::env::var("OPENAI_BASE_URL").ok())
        .unwrap_or_else(|| "https://api.openai.com/v1".to_string());
    let api_base = api_base.trim_end_matches('/').to_string();
    let model = cli
        .model
        .or_else(|| std::env::var("LLM_MODEL").ok())
        .unwrap_or_else(|| "gpt-4o-mini".to_string());

    let llm = LlmConfig {
        api_base: api_base.clone(),
        model: model.clone(),
        api_key,
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?,
    };

    let tags: Vec<String> = cli.tags.split(',').map(|s| s.trim().to_string()).collect();

    let resource_map = parse_resources(&cli.resources)?;
    let resources = ResourceAvailability {
        available: resource_map.clone(),
        total: resource_map.clone(),
    };

    info!(
        "ChopFlow LLM worker: broker={}, tags={:?}, model={}, api_base={}, concurrency={}",
        cli.broker, tags, model, api_base, cli.concurrency
    );

    // Connect + register, retrying with backoff so the worker can start before
    // the broker and survive broker restarts.
    let worker_id = connect_and_register(&cli.broker, &tags, &resource_map).await?;
    info!("Worker registered with ID: {}", worker_id);

    let state = Arc::new(WorkerState {
        id: worker_id,
        broker_address: cli.broker,
        resources,
    });
    let state_hb = state.clone();

    // Heartbeat loop.
    let heartbeat_handle = tokio::spawn(async move {
        let interval = Duration::from_secs(cli.heartbeat_interval);
        let mut ticker = time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(e) = send_heartbeat(&state_hb).await {
                error!("heartbeat failed: {}", e);
            }
        }
    });

    // Fetch + dispatch loop with bounded concurrency.
    let fetch_state = state.clone();
    let fetch_llm = llm.clone();
    let fetch_handle = tokio::spawn(async move {
        if let Err(e) = run_fetch_loop(fetch_state, fetch_llm, cli.concurrency).await {
            error!("fetch loop failed: {}", e);
        }
    });

    info!("LLM worker is running. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");
    heartbeat_handle.abort();
    fetch_handle.abort();
    Ok(())
}

/// Parse `name:amount,...` into a resource map. Malformed entries are errors.
fn parse_resources(s: &str) -> Result<HashMap<String, u32>> {
    let mut map = HashMap::new();
    for entry in s.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 2 {
            anyhow::bail!("invalid resource '{}': expected 'name:amount' (e.g. llm:1)", entry);
        }
        let amount = parts[1].trim().parse::<u32>().map_err(|e| {
            anyhow::anyhow!("invalid resource amount in '{}': {}", entry, e)
        })?;
        map.insert(parts[0].trim().to_string(), amount);
    }
    if map.is_empty() {
        anyhow::bail!("a worker must declare at least one resource (e.g. -r llm:1)");
    }
    Ok(map)
}

/// Connect to the broker and register, retrying with backoff until success.
async fn connect_and_register(
    broker_address: &str,
    tags: &[String],
    resources: &HashMap<String, u32>,
) -> Result<String> {
    let mut backoff = Duration::from_millis(500);
    const MAX_BACKOFF: Duration = Duration::from_secs(5);
    loop {
        match try_connect_and_register(broker_address, tags, resources).await {
            Ok(id) => return Ok(id),
            Err(e) => {
                error!(
                    "could not reach broker at {} ({}). Retrying in {:?}.",
                    broker_address, e, backoff
                );
                time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
            }
        }
    }
}

async fn try_connect_and_register(
    broker_address: &str,
    tags: &[String],
    resources: &HashMap<String, u32>,
) -> Result<String> {
    let mut client = ChopFlowBrokerClient::connect(broker_address.to_string())
        .await
        .map_err(|e| ChopFlowError::NetworkError(e.to_string()))?;
    let resp = client
        .register_worker(Request::new(RegisterWorkerRequest {
            address: "localhost".to_string(),
            tags: tags.to_vec(),
            resources: resources.clone(),
        }))
        .await
        .map_err(|e| ChopFlowError::NetworkError(e.to_string()))?;
    Ok(resp.into_inner().worker_id)
}

async fn send_heartbeat(state: &Arc<WorkerState>) -> Result<()> {
    let mut client = ChopFlowBrokerClient::connect(state.broker_address.clone())
        .await
        .map_err(|e| ChopFlowError::NetworkError(e.to_string()))?;
    client
        .worker_heartbeat(Request::new(WorkerHeartbeatRequest {
            worker_id: state.id.clone(),
            resources: Some(ProtoResourceAvailability {
                available: state.resources.available.clone(),
                total: state.resources.total.clone(),
            }),
        }))
        .await
        .map_err(|e| ChopFlowError::NetworkError(e.to_string()))?;
    Ok(())
}

/// Pull tasks and dispatch them to async LLM handlers with bounded concurrency.
async fn run_fetch_loop(state: Arc<WorkerState>, llm: LlmConfig, concurrency: usize) -> Result<()> {
    let poll_interval = Duration::from_secs(2);
    // A JoinSet lets us run up to `concurrency` LLM calls concurrently while
    // still acknowledging each as it finishes. A semaphore caps in-flight work.
    let mut in_flight: JoinSet<()> = JoinSet::new();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));

    loop {
        // Reap finished tasks so the set doesn't grow unbounded.
        while in_flight.try_join_next().is_some() {}

        // If we're at capacity, wait for a slot before fetching more.
        if in_flight.len() >= semaphore.available_permits() && in_flight.len() >= concurrency {
            in_flight.join_next().await.transpose()?;
        }

        match fetch_tasks(&state).await {
            Ok(tasks) if !tasks.is_empty() => {
                for task in tasks {
                    let state = state.clone();
                    let llm = llm.clone();
                    let permit = semaphore.clone();
                    in_flight.spawn(async move {
                        // Acquire a permit so we never exceed `concurrency` LLM calls.
                        let _permit = match permit.acquire_owned().await {
                            Ok(p) => p,
                            Err(e) => {
                                error!("semaphore closed: {e}");
                                return;
                            }
                        };
                        if let Err(e) = execute_and_ack(&state, &llm, task).await {
                            error!("task failed: {e}");
                        }
                    });
                }
            }
            Ok(_) => time::sleep(poll_interval).await,
            Err(e) => {
                error!("fetch failed: {}", e);
                time::sleep(poll_interval).await;
            }
        }
    }
}

async fn fetch_tasks(state: &Arc<WorkerState>) -> Result<Vec<ProtoTask>> {
    let mut client = ChopFlowBrokerClient::connect(state.broker_address.clone())
        .await
        .map_err(|e| ChopFlowError::NetworkError(e.to_string()))?;
    let resp = client
        .fetch_tasks(Request::new(FetchTasksRequest {
            worker_id: state.id.clone(),
            max_tasks: 4,
        }))
        .await
        .map_err(|e| ChopFlowError::NetworkError(e.to_string()))?;
    Ok(resp.into_inner().tasks)
}

/// Execute one task through an LLM handler and acknowledge the result.
async fn execute_and_ack(state: &Arc<WorkerState>, llm: &LlmConfig, task: ProtoTask) -> Result<()> {
    let task_id = task.id.clone();
    let name = task.name.clone();
    info!("Executing task {} ({})", task_id, name);

    let payload: Value = match serde_json::from_str(&task.payload) {
        Ok(v) => v,
        Err(e) => {
            ack(state, &task_id, false, error_json("invalid payload", &e.to_string())).await?;
            return Ok(());
        }
    };

    let outcome = match name.as_str() {
        "llm.complete" | "complete" => llm_complete(llm, payload).await,
        "llm.chat" | "chat" => llm_chat(llm, payload).await,
        other => {
            warn!("no LLM handler for '{}', falling back to llm.complete", other);
            llm_complete(llm, payload).await
        }
    };

    match outcome {
        Ok(result) => ack(state, &task_id, true, result).await?,
        Err(e) => {
            error!("task {} handler error: {}", task_id, e);
            ack(state, &task_id, false, error_json("llm call failed", &e.to_string())).await?;
        }
    }
    Ok(())
}

/// `llm.complete`: payload `{ prompt, model?, temperature?, max_tokens? }` → `{ text, model, usage }`.
async fn llm_complete(llm: &LlmConfig, payload: Value) -> Result<Value> {
    let prompt = payload
        .get("prompt")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("llm.complete requires a string `prompt` field"))?
        .to_string();
    let messages = Value::Array(vec![json!({ "role": "user", "content": prompt })]);
    chat_completions(llm, payload, messages).await
}

/// `llm.chat`: payload `{ messages: [{role, content}], model?, temperature?, max_tokens? }` → `{ text, model, usage }`.
async fn llm_chat(llm: &LlmConfig, payload: Value) -> Result<Value> {
    let messages = payload
        .get("messages")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("llm.chat requires a `messages` array"))?;
    chat_completions(llm, payload, messages).await
}

/// Shared chat-completions call. Pulls optional `model`/`temperature`/`max_tokens`
/// from the task payload, falling back to the worker's configured defaults.
async fn chat_completions(llm: &LlmConfig, payload: Value, messages: Value) -> Result<Value> {
    let model = payload
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(&llm.model)
        .to_string();

    let mut body = json!({
        "model": model,
        "messages": messages,
    });
    if let Some(t) = payload.get("temperature") {
        body["temperature"] = t.clone();
    }
    if let Some(m) = payload.get("max_tokens") {
        body["max_tokens"] = m.clone();
    }

    let url = format!("{}/chat/completions", llm.api_base);
    let resp = llm
        .http
        .post(&url)
        .bearer_auth(&llm.api_key)
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("HTTP request to LLM failed: {e}"))?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("LLM API returned {}: {}", status.as_u16(), text);
    }

    let v: Value = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("could not parse LLM response as JSON: {e}"))?;

    let content = v["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let returned_model = v["model"].as_str().unwrap_or(&model).to_string();
    let usage = v.get("usage").cloned().unwrap_or(json!({}));

    Ok(json!({
        "text": content,
        "model": returned_model,
        "usage": usage,
    }))
}

/// Acknowledge a task: success/failure + JSON result body.
async fn ack(
    state: &Arc<WorkerState>,
    task_id: &str,
    success: bool,
    result: Value,
) -> Result<()> {
    let mut client = ChopFlowBrokerClient::connect(state.broker_address.clone())
        .await
        .map_err(|e| ChopFlowError::NetworkError(e.to_string()))?;
    let result_json = serde_json::to_string(&result).unwrap_or_else(|_| {
        r#"{"status":"error","message":"failed to serialize result"}"#.to_string()
    });
    client
        .acknowledge_task(Request::new(AcknowledgeTaskRequest {
            worker_id: state.id.clone(),
            task_id: task_id.to_string(),
            success,
            result: result_json,
        }))
        .await
        .map_err(|e| ChopFlowError::NetworkError(e.to_string()))?;
    info!("task {} acknowledged (success={})", task_id, success);
    Ok(())
}

fn error_json(message: &str, detail: &str) -> Value {
    json!({ "status": "error", "message": message, "detail": detail })
}
