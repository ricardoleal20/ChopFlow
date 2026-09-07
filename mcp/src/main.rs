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
    ServerHandler, ServiceExt, handler::server::wrapper::Parameters, model, schemars, tool,
    tool_handler, tool_router, transport::stdio,
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
    /// Dispatch priority (higher = claimed first). Default 0.
    #[serde(default)]
    priority: Option<i32>,
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
    /// Dispatch priority for materialized tasks (higher = first). Default 0.
    #[serde(default)]
    priority: Option<i32>,
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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct WaitForTaskParams {
    /// The task UUID.
    id: String,
    /// Max seconds to wait before giving up (default 120). The call blocks,
    /// polling the broker, until the task reaches a terminal status or the timeout.
    #[serde(default)]
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RunLlmTaskParams {
    /// The prompt to send to the LLM.
    prompt: String,
    /// Override the LLM worker's default model for this call.
    #[serde(default)]
    model: Option<String>,
    /// Sampling temperature (0.0–2.0).
    #[serde(default)]
    temperature: Option<f64>,
    /// Max tokens to generate.
    #[serde(default)]
    max_tokens: Option<u32>,
    /// Tags to route the task. Defaults to ["llm"] so an LLM worker picks it up.
    #[serde(default)]
    tags: Vec<String>,
}

// ---- Tools ----------------------------------------------------------------

#[tool_router]
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
        if let Some(pr) = p.priority {
            body["priority"] = json!(pr);
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
        if let Some(pr) = p.task_template.priority {
            template["priority"] = json!(pr);
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

    #[tool(description = "Block until a task reaches a terminal status (completed, failed, dead-lettered, or cancelled), then return the full task JSON. Useful for getting a synchronous-style answer from an async task. Polls the broker roughly twice per second.")]
    async fn wait_for_task(
        &self,
        Parameters(p): Parameters<WaitForTaskParams>,
    ) -> Result<String, String> {
        self.wait_for_task_inner(
            &p.id,
            Duration::from_secs(p.timeout_seconds.unwrap_or(120)),
        )
        .await
    }

    #[tool(description = "Run an LLM completion end-to-end: enqueue an `llm.complete` task (routed to an LLM worker via the `llm` tag), wait for it to finish, and return the final task JSON (with the model's text in `result.text`). Requires an LLM worker (chopflow-llm-worker) to be running and subscribed to the `llm` tag. This is MCP Phase 2 — ChopFlow driving an LLM.")]
    async fn run_llm_task(
        &self,
        Parameters(p): Parameters<RunLlmTaskParams>,
    ) -> Result<String, String> {
        // Route to the LLM worker unless the caller specified explicit tags.
        let tags = if p.tags.is_empty() {
            vec!["llm".to_string()]
        } else {
            p.tags
        };

        let mut payload = json!({ "prompt": p.prompt });
        if let Some(m) = p.model {
            payload["model"] = json!(m);
        }
        if let Some(t) = p.temperature {
            payload["temperature"] = json!(t);
        }
        if let Some(mt) = p.max_tokens {
            payload["max_tokens"] = json!(mt);
        }

        let body = json!({ "name": "llm.complete", "payload": payload, "tags": tags });
        let resp = self
            .http(reqwest::Method::POST, "/api/tasks", Some(body))
            .await?;
        let v: Value = serde_json::from_str(&resp)
            .map_err(|e| format!("could not parse enqueue response: {e}"))?;
        let id = v
            .get("task_id")
            .and_then(|t| t.as_str())
            .ok_or_else(|| format!("broker did not return a task_id: {resp}"))?;

        // LLM calls can take a while; allow up to 5 minutes by default.
        self.wait_for_task_inner(id, Duration::from_secs(300)).await
    }

    /// Poll `GET /api/tasks/:id` until the task is terminal or the timeout
    /// elapses. Returns the full task JSON on success. Non-private to the
    /// tool methods above; not exposed as an MCP tool itself (use
    /// `wait_for_task` for that).
    async fn wait_for_task_inner(
        &self,
        id: &str,
        timeout: Duration,
    ) -> Result<String, String> {
        let start = std::time::Instant::now();
        let poll = Duration::from_millis(500);
        let path = format!("/api/tasks/{}", urlencoding::encode_simple(id));
        loop {
            let body = self.http(reqwest::Method::GET, &path, None).await?;
            let status = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v.get("status").and_then(|s| s.as_str()).map(str::to_string))
                .unwrap_or_default();
            if matches!(
                status.as_str(),
                "completed" | "failed" | "dead-lettered" | "cancelled"
            ) {
                return Ok(body);
            }
            if start.elapsed() >= timeout {
                return Err(format!(
                    "timed out after {:?} waiting for task {} (last status: {})",
                    timeout, id, status
                ));
            }
            tokio::time::sleep(poll).await;
        }
    }
}

// ---- ServerHandler: tools (via tool_handler) + resources + prompts --------

// `#[tool_handler]` generates `call_tool`/`list_tools`/`get_tool` from the
// tool_router, and leaves our manual overrides below (resources, prompts,
// get_info) untouched. We provide `get_info` ourselves so the server
// advertises tools + resources + prompts capabilities.
#[tool_handler]
impl ServerHandler for ChopFlowMcp {
    fn get_info(&self) -> model::ServerInfo {
        model::ServerInfo::new(
            model::ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
        )
        .with_server_info(model::Implementation::from_build_env())
    }

    // -- Resources: live cluster state the agent can read without calling tools --

    async fn list_resources(
        &self,
        _request: Option<model::PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<model::ListResourcesResult, rmcp::ErrorData> {
        let resources = vec![
            model::Resource::new("chopflow://stats", "cluster-stats")
                .with_description("Live aggregate queue, worker, and schedule counters.")
                .with_mime_type("application/json"),
            model::Resource::new("chopflow://workers", "workers")
                .with_description("Live list of registered workers, their tags, and resources.")
                .with_mime_type("application/json"),
            model::Resource::new("chopflow://tasks/recent", "recent-tasks")
                .with_description("The 20 most recent tasks across all statuses.")
                .with_mime_type("application/json"),
            model::Resource::new("chopflow://guide", "task-guide")
                .with_description(
                    "How to construct ChopFlow tasks: names, payloads, tags, and the LLM worker.",
                )
                .with_mime_type("text/plain"),
        ];
        Ok(model::ListResourcesResult {
            resources,
            ..Default::default()
        })
    }

    async fn read_resource(
        &self,
        request: model::ReadResourceRequestParams,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<model::ReadResourceResponse, rmcp::ErrorData> {
        let uri = request.uri.clone();
        let text = match uri.as_str() {
            "chopflow://stats" => self.http(reqwest::Method::GET, "/api/stats", None).await,
            "chopflow://workers" => self.http(reqwest::Method::GET, "/api/workers", None).await,
            "chopflow://tasks/recent" => {
                self.http(reqwest::Method::GET, "/api/tasks?limit=20", None).await
            }
            "chopflow://guide" => Ok(TASK_GUIDE.to_string()),
            other => {
                return Err(rmcp::ErrorData::resource_not_found(
                    format!("unknown resource uri: {other}"),
                    None,
                ))
            }
        }
        .map_err(|e| rmcp::ErrorData::internal_error(format!("broker read failed: {e}"), None))?;

        let contents = vec![model::ResourceContents::TextResourceContents {
            uri,
            mime_type: Some("application/json".to_string()),
            text,
            meta: None,
        }];
        Ok(model::ReadResourceResult::new(contents).into())
    }

    // -- Prompts: ready-made agent workflows --

    async fn list_prompts(
        &self,
        _request: Option<model::PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<model::ListPromptsResult, rmcp::ErrorData> {
        let prompts = vec![
            model::Prompt::new(
                "run-llm-completion",
                Some("Run a single LLM completion through ChopFlow and return the answer."),
                Some(vec![model::PromptArgument::new("prompt")
                    .with_description("The prompt to send to the LLM.")
                    .with_required(true)]),
            ),
            model::Prompt::new(
                "process-image-batch",
                Some("Enqueue a batch of image-resize tasks and summarize their results."),
                Some(vec![model::PromptArgument::new("count")
                    .with_description("Number of resize_image tasks to enqueue (default 5).")
                    .with_required(false)]),
            ),
            model::Prompt::new(
                "debug-stuck-tasks",
                Some("Inspect failed and dead-lettered tasks and propose fixes."),
                None,
            ),
            model::Prompt::new(
                "schedule-recurring",
                Some("Create a cron schedule that fires a task on a recurring schedule."),
                Some(vec![
                    model::PromptArgument::new("cron")
                        .with_description("5-field cron expression (e.g. '*/5 * * * *').")
                        .with_required(true),
                    model::PromptArgument::new("task")
                        .with_description("Task name to fire (e.g. resize_image, llm.complete).")
                        .with_required(true),
                ]),
            ),
        ];
        Ok(model::ListPromptsResult {
            prompts,
            ..Default::default()
        })
    }

    async fn get_prompt(
        &self,
        request: model::GetPromptRequestParams,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<model::GetPromptResponse, rmcp::ErrorData> {
        // Helper to pull a string argument out of the optional arguments map.
        let arg = |key: &str| -> Option<String> {
            request
                .arguments
                .as_ref()
                .and_then(|a| a.get(key))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        };

        let messages = match request.name.as_str() {
            "run-llm-completion" => {
                let prompt = arg("prompt").unwrap_or_else(|| "(no prompt provided)".into());
                vec![user_msg(format!(
                    "Use the `run_llm_task` tool to run this LLM completion and return only the \
                     model's text:\n\n{prompt}"
                ))]
            }
            "process-image-batch" => {
                let count = arg("count").unwrap_or_else(|| "5".into());
                vec![user_msg(format!(
                    "Enqueue {count} `resize_image` tasks (payload {{{{\"width\":128,\"height\":128}}}}, \
                     tags [\"image\"]) using `enqueue_task`, then poll `get_task` for each until they \
                     reach a terminal status. Summarize how many completed vs failed and the \
                     typical result."
                ))]
            }
            "debug-stuck-tasks" => {
                vec![user_msg(
                    "Use `list_tasks` with status filters `failed` and `dead-lettered` to find \
                     stuck tasks. For each, use `get_task` to read its result/error and retry \
                     count. Propose concrete fixes (e.g. bad payload, handler missing, resource \
                     shortage) and, where appropriate, re-enqueue a corrected task with \
                     `enqueue_task`."
                    .to_string(),
                )]
            }
            "schedule-recurring" => {
                let cron = arg("cron").unwrap_or_else(|| "*/5 * * * *".into());
                let task = arg("task").unwrap_or_else(|| "resize_image".into());
                vec![user_msg(format!(
                    "Use `create_schedule` to create a cron schedule with cron = `{cron}` that \
                     fires the `{task}` task on a recurring schedule. Use overlap_policy `skip`. \
                     Confirm by listing schedules with `list_schedules`."
                ))]
            }
            other => {
                return Err(rmcp::ErrorData::invalid_params(
                    format!("unknown prompt: {other}"),
                    None,
                ))
            }
        };

        Ok(model::GetPromptResult::new(messages)
            .with_description(format!("ChopFlow prompt: {}", request.name))
            .into())
    }
}

/// Static guide resource: teaches the agent how to construct valid ChopFlow tasks.
const TASK_GUIDE: &str = "\
ChopFlow task guide
===================

A task is enqueued with: name, payload (JSON), tags[], and optional max_retries/resources.
Workers subscribe by tag and pick up tasks whose tags they match (empty tags = default routing).

Common task names (depend on which workers are running):
  - echo                 echoes the payload (built-in, always available)
  - resize_image         payload {\"width\":N,\"height\":N} -> synthetic image resize (demos worker, tag: image)
  - batch_compute        CPU-bound matrix multiply (demos worker, tag: cpu)
  - simulate_pipeline    multi-stage sleep pipeline (demos worker)
  - llm.complete         payload {\"prompt\":\"...\", \"model\"?, \"temperature\"?, \"max_tokens\"?}
                         -> {text, model, usage}  (LLM worker, tag: llm)
  - llm.chat             payload {\"messages\":[{\"role\",\"content\"}]} -> {text, model, usage}

Tips:
  - To get a synchronous LLM answer, use the `run_llm_task` tool (it enqueues llm.complete and waits).
  - To wait on any task, use `wait_for_task` with the task UUID.
  - Schedules fire tasks on cron or one-shot ETA; create them with `create_schedule`.
  - Task statuses: created, queued, running, completed, failed, dead-lettered, cancelled.
";

/// Build a user-role prompt message from a string.
fn user_msg(text: String) -> model::PromptMessage {
    model::PromptMessage::new_text(model::Role::User, text)
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
