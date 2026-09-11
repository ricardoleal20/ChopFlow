# ChopFlow Python Client

A Python worker + producer SDK for [ChopFlow](../../README.md), the distributed task
queue in Rust. The broker and all execution logic live in Rust; this SDK lets you
**define and run task handlers in Python** and **enqueue tasks from Python**, speaking
gRPC to the broker over the contract in [`proto/proto/chopflow.proto`](../../proto/proto/chopflow.proto).

> **Build status:** Verified — `pytest` passes (Python 3.12 + grpcio 1.83) and the
> examples round-trip end-to-end against the Rust broker (Python producer enqueues an
> echo task → Python worker acks → `AsyncResult.get()` returns `COMPLETED`).

## Requirements

- **Python 3.10+**
- The Rust broker binary (`chopflow_broker`) running somewhere reachable. Build it with
  `cargo build -p chopflow_broker` from the repo root.

## Install

The SDK is not yet published to PyPI; install it editable from source:

```bash
cd clients/python
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

The generated gRPC stubs (`src/chopflow/_generated/`) are committed, so **no `protoc`
toolchain is required** to install or use the SDK. Regenerate them only when the proto
changes:

```bash
python generate.py        # reads ../../proto/proto/chopflow.proto
```

## Usage

### Worker — define and run handlers in Python

```python
from chopflow import ChopFlowWorker, task


@task("resize_image")
def resize(payload):
    # ...your logic; return something JSON-serializable...
    return {"status": "ok", "resized": payload}


worker = (
    ChopFlowWorker.builder()
    .broker("localhost:8000")
    .tags("image")
    .resources("cpu", 2)
    .build()
)
# `resize_image` was registered via the @task decorator above.
worker.register("ping", lambda p: {"pong": True})  # or register callables directly
worker.start_and_await()  # blocks until Ctrl+C
```

The worker registers with the broker, sends heartbeats, polls `FetchTasks`, executes
the matching handler, and acknowledges each task. A handler that raises is acked as a
failure and the broker applies its retry/dead-letter policy. Tasks with no registered
handler fall back to the worker's `default` handler (override via
`worker.register_default(...)`).

### Producer — enqueue tasks and await results

```python
from chopflow import ChopFlowClient

with ChopFlowClient.connect("localhost:8000") as client:
    result = (
        client.enqueue("resize_image")
        .payload({"path": "/img/a.png", "w": 128})
        .tags("image")
        .max_retries(2)
        .priority(5)  # higher = claimed before lower (default 0)
        .enqueue()
    )
    task = result.get(timeout=60)  # blocks until terminal
    print(task.status_name, task.result)
```

`AsyncResult.get()` polls `GetTaskStatus` until the task reaches a terminal state
(`COMPLETED`, `FAILED`, `DEADLETTERED`, `CANCELLED`). By default it raises
`TaskFailedError` if the task did not complete; pass `raise_on_failure=False` to get
the terminal `Task` back instead. It raises `TaskTimeoutError` if the deadline elapses.

### Schedules

```python
from chopflow import ChopFlowClient
from chopflow.models import OverlapPolicy

with ChopFlowClient.connect("localhost:8000") as client:
    sid = client.create_cron_schedule(
        "nightly", "build", "0 9 * * *", tags=["ci"], overlap=OverlapPolicy.OVERLAP_SKIP
    )
    print(client.list_schedules())
    client.delete_schedule(sid)
```

## Examples

- `examples/producer.py` — enqueue an echo task and print the result.
- `examples/echo_worker.py` — register an `echo` handler and run until Ctrl+C.

## Tests

```bash
cd clients/python
pip install -e ".[dev]"
pytest                    # builds the broker once, then runs 14 integration tests
```

The test suite builds the Rust broker on an ephemeral port with in-memory storage and
exercises the full SDK: enqueue/get, polling, cancellation, failure dead-lettering,
schedule CRUD, and worker handler dispatch.
