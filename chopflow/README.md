# ChopFlow

> A durable distributed task queue built in Rust.

This is the **all-in-one umbrella crate**. One install gives you the three
ChopFlow binaries:

| Binary           | What it is                                                    |
| ---------------- | ------------------------------------------------------------- |
| `chopflow-broker`| The broker — gRPC + HTTP/JSON API and the embedded dashboard. |
| `chopflow-cli`   | The CLI client — enqueue tasks, check status, manage schedules. |
| `chopflow-mcp`   | The MCP server — exposes the broker as AI-friendly tools.     |

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
chopflow-broker start

# 2. Enqueue a task
chopflow-cli enqueue -f task.json --name echo

# 3. Point an AI assistant at the broker over MCP
chopflow-mcp --broker http://127.0.0.1:8080
```

## Homebrew

```sh
brew tap ricardoleal20/chopflow
brew install chopflow
```

## What's inside

Each binary is a thin wrapper over the matching library crate's `run()` entry
point — `chopflow_broker`, `chopflow_cli`, `chopflow_mcp` — so behavior is
identical to installing those crates individually. The umbrella just bundles
them so a single `cargo install` (or `brew install`) gets you everything.

License: Apache-2.0.
