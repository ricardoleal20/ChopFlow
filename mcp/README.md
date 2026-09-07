# ChopFlow MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io) server that exposes the
ChopFlow broker as AI-friendly tools. An AI assistant (Claude Desktop, Cursor, Cherry,
…) connects to this server over stdio and can enqueue tasks, inspect their lifecycle,
manage schedules, and watch queue health — turning ChopFlow into a system an agent can
operate with no glue code.

The server is a thin wrapper over the broker's HTTP/JSON API (`broker/src/http.rs`); it
adds no new broker surface.

## Tools

Eleven tools, one per broker HTTP route:

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

All tool results are the broker's JSON response body as text. Non-2xx broker responses
surface as tool errors carrying the `{"error": "..."}` body.

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

Built with the official Rust MCP SDK ([`rmcp`](https://crates.io/crates/rmcp)). Each
tool is a `#[tool]`-annotated async method that calls the broker via `reqwest` and
returns the JSON body. Tool input schemas are derived from typed param structs
(`schemars::JsonSchema`), so clients see rich, typed arguments with no manual schema
work.

## License

Apache-2.0, same as the rest of ChopFlow.
