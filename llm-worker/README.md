# ChopFlow LLM Worker

A ChopFlow worker whose handlers call an LLM. This is **MCP Phase 2**: ChopFlow
drives the model. An agent (via the [`mcp`](../mcp) server) enqueues an `llm.complete`
task; this worker picks it up, calls an OpenAI-compatible chat-completions endpoint,
and acknowledges the result back into the queue — so the agent can poll and read the
answer.

The broker, scheduling, retries, and result storage all stay in Rust. This worker is
just another worker, registered with tags and resources like any other — except its
handlers do inference instead of CPU work.

## Tasks it runs

| Task name | Payload | Result |
| --- | --- | --- |
| `llm.complete` | `{ "prompt": "...", "model"?, "temperature"?, "max_tokens"? }` | `{ "text", "model", "usage" }` |
| `llm.chat` | `{ "messages": [{"role","content"}], "model"?, ... }` | `{ "text", "model", "usage" }` |

Unknown task names fall back to `llm.complete`. Optional `model`/`temperature`/`max_tokens`
in the payload override the worker's defaults per-task.

## Configure

| Flag | Env | Default | What |
| --- | --- | --- | --- |
| `--broker` | — | `http://localhost:8000` | Broker gRPC address. |
| `--tags` | — | `llm` | Subscription tags; tasks must carry one to route here. |
| `--resources` | — | `llm:1` | Declared resources. |
| `--concurrency` | — | `4` | Max in-flight LLM calls (I/O-bound, so pipelining helps). |
| `--api-base` | `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible endpoint. |
| `--model` | `LLM_MODEL` | `gpt-4o-mini` | Default model. |
| `--api-key` | `OPENAI_API_KEY` | _(required)_ | API key. |

## Run

```bash
# 1. Start the broker (HTTP :8080, gRPC :8000).
cargo run -p chopflow_broker -- start

# 2. Start the LLM worker.
export OPENAI_API_KEY=sk-...
cargo run -p chopflow-llm-worker

# 3. From an MCP client (or the HTTP API), enqueue an llm.complete task:
#    POST /api/tasks  { "name": "llm.complete", "payload": { "prompt": "Say hi" }, "tags": ["llm"] }
```

Any OpenAI-compatible endpoint works (OpenAI, Ollama's `/v1`, OpenRouter, a local
vLLM, etc.) — point `--api-base` at it.

## How it differs from the generic worker

The generic `worker` crate runs sync handlers sequentially. LLM calls are async network
I/O, so this worker uses **async handlers** and a **bounded concurrency pool** (a
`JoinSet` + `Semaphore`) to pipeline several inferences per worker. The loop otherwise
mirrors `worker/src/main.rs` (connect-with-backoff, heartbeat, pull-model fetch, ack).
The ROADMAP (#2) tracks unifying the two behind a shared async-handler worker library.

## License

Apache-2.0, same as the rest of ChopFlow.
