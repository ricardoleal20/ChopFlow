# ChopFlow MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io) server that exposes the
ChopFlow broker as AI-friendly tools. An AI assistant (Claude Desktop, Cursor, Cherry,
…) connects to this server over stdio and can enqueue tasks, inspect their lifecycle,
manage schedules, and watch queue health — turning ChopFlow into a system an agent can
operate with no glue code.

The server exposes the broker through all three MCP primitives: **tools** (actions),
**resources** (live, readable cluster state), and **prompts** (ready-made agent
workflows). It is a thin wrapper over the broker's HTTP/JSON API (`broker/src/http.rs`)
and adds no new broker surface.

## Tools

Thirteen tools. Eleven wrap a broker HTTP route 1:1; two are higher-level helpers for
LLM workflows:

| Tool | What it does |
| --- | --- |
| `get_stats` | Aggregate queue / worker / schedule counters. |
| `list_tasks` | List tasks, optional status filter + pagination. |
| `get_task` | Fetch one task by UUID (status, retries, result, lineage). |
| `enqueue_task` | Enqueue a task for async execution; returns the task UUID. |
| `cancel_task` | Cancel a non-terminal task. |
| `list_workers` | List registered workers + resource availability. |
| `list_schedules` | List all schedules (cron + oneshot). |
| `get_schedule` | Fetch one schedule by UUID. |
| `create_schedule` | Create a cron or one-shot schedule. |
| `update_schedule` | Patch a schedule (enable/disable, overlap policy, cron). |
| `delete_schedule` | Delete a schedule. |
| `wait_for_task` | Block (polling) until a task reaches a terminal status, then return its JSON. Gives a synchronous-style answer from an async task. |
| `run_llm_task` | **Phase 2.** Enqueue an `llm.complete` task (routed to an LLM worker via the `llm` tag), wait for completion, and return the task JSON with the model's text in `result.text`. Requires an LLM worker (`chopflow-llm-worker`) to be running. |

All tool results are the broker's JSON response body as text. Non-2xx broker responses
surface as tool errors carrying the `{"error": "..."}` body.

## Resources

Live, readable cluster state — an agent reads these to understand the system without
firing tools blindly. `resources/read` fetches fresh data from the broker at read time.

| URI | What it returns |
| --- | --- |
| `chopflow://stats` | Live aggregate counters (queue depth, processing, completed/failed, workers, schedules). |
| `chopflow://workers` | Live list of registered workers, tags, and resource availability. |
| `chopflow://tasks/recent` | The 20 most recent tasks across all statuses. |
| `chopflow://guide` | A static text guide: task names, payload shapes, tag routing, and the LLM worker. |

## Prompts

Ready-made workflows the agent can invoke with `prompts/get`. Each returns a seeded
user message describing a multi-step task.

| Prompt | Arguments | What it sets up |
| --- | --- | --- |
| `run-llm-completion` | `prompt` (required) | Run a single LLM completion via `run_llm_task` and return the answer. |
| `process-image-batch` | `count` (optional, default 5) | Enqueue N `resize_image` tasks, wait, summarize results. |
| `debug-stuck-tasks` | — | List failed/dead-lettered tasks, inspect, propose fixes. |
| `schedule-recurring` | `cron` (required), `task` (required) | Create a cron schedule firing a recurring task. |

## Configure

The broker URL is set at server launch (not per-tool):

- `--broker http://127.0.0.1:8080` CLI flag, or
- `CHOPFLOW_HTTP_URL` env var, or
- defaults to `http://127.0.0.1:8080`.

## Run

```bash
cargo run -p chopflow-mcp            # defaults to http://127.0.0.1:8080
CHOPFLOW_HTTP_URL=http://broker:8080 cargo run -p chopflow-mcp
```

## Wire into a client

### Claude Desktop / Cursor (`claude_desktop_config.json`)

```json
{
  "mcpServers": {
    "chopflow": {
      "command": "/path/to/chopflow-mcp",
      "args": ["--broker", "http://127.0.0.1:8080"]
    }
  }
}
```

### Smoke test by hand

The server speaks JSON-RPC over stdio. Start the broker, then:

```bash
{
  printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}'
  printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/initialized"}'
  printf '%s\n' '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"enqueue_task","arguments":{"name":"echo","payload":{"hello":"world"}}}}'
  sleep 1
} | cargo run -q -p chopflow-mcp
```

## Implementation

Built with the official Rust MCP SDK ([`rmcp`](https://crates.io/crates/rmcp)). Tools
are `#[tool]`-annotated async methods on a `#[tool_router]` inherent impl; resources
and prompts are manual `ServerHandler` trait overrides in a `#[tool_handler]` block.
The server advertises all three capabilities (`tools`, `resources`, `prompts`) in
`get_info`. Tool input schemas are derived from typed param structs
(`schemars::JsonSchema`), so clients see rich, typed arguments with no manual schema
work.

`run_llm_task` is MCP Phase 2: it turns ChopFlow into an LLM-job orchestrator. The
[`chopflow-llm-worker`](../llm-worker) crate provides the worker that executes
`llm.complete` / `llm.chat` tasks against an OpenAI-compatible endpoint.

## License

Apache-2.0, same as the rest of ChopFlow.
