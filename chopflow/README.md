# ChopFlow

> A durable distributed task queue built in Rust.

This is the **unified binary crate**. One install gives you a single `chopflow`
command covering the broker, the CLI, and the MCP server:

| Subcommand              | What it is                                                     |
| ----------------------- | -------------------------------------------------------------- |
| `chopflow broker start` | The broker — gRPC + HTTP/JSON API and the embedded dashboard.  |
| `chopflow mcp`          | The MCP server — exposes the broker as AI-friendly tools.      |
| `chopflow enqueue`      | Enqueue a task from a JSON file.                               |
| `chopflow status`       | Task status, list tasks, or queue stats.                       |
| `chopflow schedule`     | Manage schedules (create / list / delete).                     |

## Install

```sh
cargo install chopflow
```

Workers ship separately (they run your handler code):

```sh
cargo install chopflow_worker
```

## Quick start

```sh
# 1. Start the broker (gRPC on :8000, HTTP/dashboard on :8080, SQLite by default)
chopflow broker start

# 2. Enqueue a task
chopflow enqueue -f task.json --name echo

# 3. Point an AI assistant at the broker over MCP
chopflow mcp --broker http://127.0.0.1:8080
```

## Homebrew

```sh
brew tap ricardoleal20/chopflow
brew install chopflow
```

## What's inside

The `chopflow` binary is a thin clap dispatcher over the library crates'
`run()` entry points — `chopflow_broker::run`, `chopflow_cli::run`,
`chopflow_mcp::run` — so each subcommand behaves identically to the matching
standalone binary. The unified command just bundles them so a single
`cargo install` (or `brew install`) gets you everything.

License: Apache-2.0.
