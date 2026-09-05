//! # ChopFlow MCP Server
//!
//! A [Model Context Protocol](https://modelcontextprotocol.io) server that exposes
//! the ChopFlow broker as AI-friendly tools. An AI assistant (Claude Desktop,
//! Cursor, Cherry, …) connects to this server over stdio and can enqueue tasks,
//! inspect their lifecycle, manage schedules, and watch queue health — turning
//! ChopFlow into a system an agent can operate with no glue code.
//!
//! The server is a thin wrapper over the broker's HTTP/JSON API
//! (`broker/src/http.rs`); it adds no new broker surface. Configure the broker
//! URL with `--broker` or `CHOPFLOW_HTTP_URL` (default `http://127.0.0.1:8080`).
//!
//! All tool results are the broker's JSON response body as a string. Non-2xx
//! responses are surfaced as tool errors carrying the `{"error": "..."}` body.

use clap::Parser;
use rmcp::{
    handler::server::wrapper::Parameters, schemars, tool, tool_router, ServiceExt,
    transport::stdio,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

/// Entry point. Parse config, build the server, serve over stdio.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs go to stderr — stdout is reserved for the MCP JSON-RPC protocol.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let cli = Cli::parse();
    let base = cli
        .broker
        .or_else(|| std::env::var("CHOPFLOW_HTTP_URL").ok())
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string());
    let base = base.trim_end_matches('/').to_string();

    tracing::info!("ChopFlow MCP server targeting {}", base);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let service = ChopFlowMcp { client, base };
    let running = service.serve(stdio()).await?;
    running.waiting().await?;
    Ok(())
}

/// CLI config. Kept minimal — the only knob is the broker HTTP URL.
#[derive(Parser, Debug)]
#[command(
    name = "chopflow-mcp",
    about = "MCP server exposing the ChopFlow broker as AI-friendly tools"
)]
struct Cli {
    /// Broker HTTP base URL (e.g. http://127.0.0.1:8080). Overrides CHOPFLOW_HTTP_URL.
    #[arg(long)]
    broker: Option<String>,
}

/// The MCP server. Holds a reusable HTTP client and the broker base URL.
#[derive(Clone)]
struct ChopFlowMcp {
    client: reqwest::Client,
    base: String,
}

impl ChopFlowMcp {
    /// Issue a request to the broker and return the response body as a string.
    /// Non-2xx responses become tool errors carrying the broker's `{"error":...}` body.
    async fn http(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<String, String> {
        let url = format!("{}{}", self.base, path);
        let mut req = self.client.request(method.clone(), &url);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("request to broker failed: {e}"))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| format!("reading broker response failed: {e}"))?;
        if status.is_success() {
            Ok(text)
        } else {
            Err(format!("broker returned {}: {}", status.as_u16(), text))
        }
    }
}

// ---- Tool parameter types -------------------------------------------------

/// No-op marker for tools that take no parameters.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct NoParams {}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListTasksParams {
    /// Filter by status: created, queued, running, completed, failed, dead-lettered, cancelled.
    #[serde(default)]
    status: Option<String>,
    /// Max number of tasks to return (0 = no limit).
    #[serde(default)]
    limit: Option<u32>,
    /// Number of tasks to skip.
    #[serde(default)]
    offset: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct IdParams {
    /// The task or schedule UUID.
    id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct EnqueueTaskParams {
    /// Task name — matched by workers against their registered handlers.
    name: String,
    /// JSON-serializable task payload. Passed to the handler verbatim.
    payload: Value,
    /// Tags the worker must be subscribed to in order to pick this task up.
    #[serde(default)]
    tags: Vec<String>,
    /// Max retry attempts before the task is dead-lettered (0 = no retries).
    #[serde(default)]
    max_retries: Option<u32>,
    /// Resource requirements, e.g. {"gpu": 1}.
    #[serde(default)]
    resources: std::collections::BTreeMap<String, u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct CreateScheduleParams {
    /// Human-readable schedule name.
    name: String,
    /// The task template the schedule materializes on each fire.
    task_template: TaskTemplateInput,
    /// When/how the schedule fires.
    kind: ScheduleKindInput,
    /// Overlap policy: skip (default), coalesce, or allow.
    #[serde(default)]
    overlap_policy: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct TaskTemplateInput {
    /// Task name for materialized tasks.
    name: String,
    /// JSON payload for materialized tasks.
    payload: Value,
    /// Tags for materialized tasks.
    #[serde(default)]
    tags: Vec<String>,
    /// Resource requirements for materialized tasks.
    #[serde(default)]
    resources: std::collections::BTreeMap<String, u32>,
    /// Max retries for materialized tasks.
    #[serde(default)]
    max_retries: Option<u32>,
}

/// Schedule kind, mirroring the broker's tagged union.
/// Use `{"type":"cron","cron":"*/5 * * * *"}` for recurring, or
/// `{"type":"oneshot","eta":"2026-09-05T12:00:00Z"}` for a one-time fire.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ScheduleKindInput {
    #[serde(rename = "type")]
    kind_type: String,
    /// Cron expression (5-field). Required when type is "cron".
    #[serde(default)]
    cron: Option<String>,
    /// RFC3339 timestamp. Required when type is "oneshot".
    #[serde(default)]
    eta: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct UpdateScheduleParams {
    /// The schedule UUID.
    id: String,
    /// Enable or disable the schedule.
    #[serde(default)]
    enabled: Option<bool>,
    /// New overlap policy: skip, coalesce, or allow.
    #[serde(default)]
    overlap_policy: Option<String>,
    /// New cron expression (switches the schedule to Cron and recomputes next_fire).
    #[serde(default)]
    cron: Option<String>,
}

// ---- Tools ----------------------------------------------------------------

#[tool_router(server_handler)]
impl ChopFlowMcp {
    #[tool(description = "Get aggregate queue, worker, and schedule counters from the ChopFlow broker.")]
    async fn get_stats(&self, _p: Parameters<NoParams>) -> Result<String, String> {
        self.http(reqwest::Method::GET, "/api/stats", None).await
    }

    #[tool(description = "List tasks, optionally filtered by status with limit/offset pagination. Statuses: created, queued, running, completed, failed, dead-lettered, cancelled.")]
    async fn list_tasks(
        &self,
        Parameters(p): Parameters<ListTasksParams>,
    ) -> Result<String, String> {
        let mut q: Vec<String> = Vec::new();
        if let Some(s) = &p.status {
            q.push(format!("status={}", urlencoding::encode_simple(s)));
        }
        if let Some(l) = p.limit {
            q.push(format!("limit={l}"));
        }
        if let Some(o) = p.offset {
            q.push(format!("offset={o}"));
        }
        let path = if q.is_empty() {
            "/api/tasks".to_string()
        } else {
            format!("/api/tasks?{}", q.join("&"))
        };
        self.http(reqwest::Method::GET, &path, None).await
    }

    #[tool(description = "Fetch a single task by its UUID, including status, retries, result, and schedule lineage.")]
    async fn get_task(
        &self,
        Parameters(p): Parameters<IdParams>,
    ) -> Result<String, String> {
        self.http(
            reqwest::Method::GET,
            &format!("/api/tasks/{}", urlencoding::encode_simple(&p.id)),
            None,
        )
        .await
    }

    #[tool(description = "Enqueue a task for asynchronous execution. Workers subscribed to the task's tags pick it up. Returns the new task UUID.")]
    async fn enqueue_task(
        &self,
        Parameters(p): Parameters<EnqueueTaskParams>,
    ) -> Result<String, String> {
        let mut body = json!({
            "name": p.name,
            "payload": p.payload,
            "tags": p.tags,
        });
        if let Some(mr) = p.max_retries {
            body["max_retries"] = json!(mr);
        }
        if !p.resources.is_empty() {
            body["resources"] = json!(p.resources);
        }
        self.http(reqwest::Method::POST, "/api/tasks", Some(body)).await
    }

    #[tool(description = "Cancel a non-terminal task (queued or running) by its UUID. Terminal tasks (completed/failed/dead-lettered/cancelled) cannot be cancelled.")]
    async fn cancel_task(
        &self,
        Parameters(p): Parameters<IdParams>,
    ) -> Result<String, String> {
        self.http(
            reqwest::Method::POST,
            &format!("/api/tasks/{}/cancel", urlencoding::encode_simple(&p.id)),
            None,
        )
        .await
    }

    #[tool(description = "List registered workers with their tags, liveness, assigned-task count, and resource availability.")]
    async fn list_workers(&self, _p: Parameters<NoParams>) -> Result<String, String> {
        self.http(reqwest::Method::GET, "/api/workers", None).await
    }

    #[tool(description = "List all schedules (enabled and disabled), each with its kind (cron/oneshot), overlap policy, and next_fire time.")]
    async fn list_schedules(&self, _p: Parameters<NoParams>) -> Result<String, String> {
        self.http(reqwest::Method::GET, "/api/schedules", None).await
    }

    #[tool(description = "Fetch a single schedule by its UUID.")]
    async fn get_schedule(
        &self,
        Parameters(p): Parameters<IdParams>,
    ) -> Result<String, String> {
        self.http(
            reqwest::Method::GET,
            &format!("/api/schedules/{}", urlencoding::encode_simple(&p.id)),
            None,
        )
        .await
    }

    #[tool(description = "Create a schedule. kind is a tagged union: {\"type\":\"cron\",\"cron\":\"*/5 * * * *\"} (5-field cron) or {\"type\":\"oneshot\",\"eta\":\"2026-09-05T12:00:00Z\"} (RFC3339). overlap_policy: skip (default), coalesce, or allow. Returns the new schedule UUID.")]
    async fn create_schedule(
        &self,
        Parameters(p): Parameters<CreateScheduleParams>,
    ) -> Result<String, String> {
        let mut template = json!({
            "name": p.task_template.name,
            "payload": p.task_template.payload,
            "tags": p.task_template.tags,
        });
        if !p.task_template.resources.is_empty() {
            template["resources"] = json!(p.task_template.resources);
        }
        if let Some(mr) = p.task_template.max_retries {
            template["max_retries"] = json!(mr);
        }

        let mut kind = json!({ "type": p.kind.kind_type });
        match p.kind.kind_type.as_str() {
            "cron" => {
                kind["cron"] = json!(p.kind.cron.ok_or_else(|| {
                    "kind.cron is required when type is \"cron\"".to_string()
                })?);
            }
            "oneshot" => {
                kind["eta"] = json!(p.kind.eta.ok_or_else(|| {
                    "kind.eta is required when type is \"oneshot\"".to_string()
                })?);
            }
            other => {
                return Err(format!(
                    "kind.type must be \"cron\" or \"oneshot\", got \"{other}\""
                ));
            }
        }

        let mut body = json!({
            "name": p.name,
            "task_template": template,
            "kind": kind,
        });
        if let Some(op) = p.overlap_policy {
            body["overlap_policy"] = json!(op);
        }
        self.http(reqwest::Method::POST, "/api/schedules", Some(body))
            .await
    }

    #[tool(description = "Partially update a schedule. Any of enabled, overlap_policy, and cron can be set; unspecified fields are left as-is. Setting cron switches the schedule to Cron and recomputes next_fire.")]
    async fn update_schedule(
        &self,
        Parameters(p): Parameters<UpdateScheduleParams>,
    ) -> Result<String, String> {
        let mut body = json!({});
        if let Some(en) = p.enabled {
            body["enabled"] = json!(en);
        }
        if let Some(op) = p.overlap_policy {
            body["overlap_policy"] = json!(op);
        }
        if let Some(c) = p.cron {
            body["cron"] = json!(c);
        }
        self.http(
            reqwest::Method::PATCH,
            &format!("/api/schedules/{}", urlencoding::encode_simple(&p.id)),
            Some(body),
        )
        .await
    }

    #[tool(description = "Delete a schedule by its UUID.")]
    async fn delete_schedule(
        &self,
        Parameters(p): Parameters<IdParams>,
    ) -> Result<String, String> {
        self.http(
            reqwest::Method::DELETE,
            &format!("/api/schedules/{}", urlencoding::encode_simple(&p.id)),
            None,
        )
        .await
    }
}

// ---- helpers --------------------------------------------------------------

/// Minimal percent-encoding for URL path/query segments (no extra dependency).
mod urlencoding {
    pub fn encode_simple(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{:02X}", b)),
            }
        }
        out
    }
}
