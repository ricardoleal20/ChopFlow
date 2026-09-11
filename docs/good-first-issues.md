# Good first issues (drafts)

These are ready-to-paste issue bodies for the good-first-issue / help-wanted
issues the launch-readiness feedback recommends. They are **not** created on
GitHub automatically (that's an outward-facing write the maintainer should do
explicitly). To create them, run the `gh issue create` commands at the bottom
of this file, or paste each body into the GitHub UI.

Labels to use: `good first issue`, `help wanted`, plus the component label.
Component labels (`benchmark`, `observability`, `worker`, `broker`,
`client-library`) already exist on the repo.

---

## 1. Add human-readable worker list to the CLI

**Labels:** `good first issue`, `help wanted`, `worker`

### Summary

The CLI (`cli/`) currently has `enqueue`, `status`, and `schedule`
subcommands but no way to list connected workers. The broker already exposes
this via `GET /api/workers` (see `broker/src/http.rs`), so this is a thin
client-side addition.

### What to build

A new `chopflow_cli workers` subcommand that calls `GET /api/workers` and
prints a human-readable table:

```text
ID            TAGS        RESOURCES (total → available)    ASSIGNED  LAST HEARTBEAT
e71fc29f…     gpu,ml      cpu:8→8 gpu:1→1                 0         2s ago
0082749c…     cpu         cpu:4→3                          1         1s ago
```

### Suggested steps

1. Add a `Workers` variant to the `Commands` enum in `cli/src/main.rs`.
2. Reuse the existing `reqwest` client to `GET {broker}/api/workers`.
3. Print a table (the `cli` already depends on `serde_json`; a plain formatted
   print is fine, no new deps needed).
4. Add a test that hits a mock/fake broker response.

### Where to look

- `cli/src/main.rs` — the clap CLI definition and existing subcommands.
- `broker/src/http.rs` — the `/api/workers` handler and its JSON shape.
- `CONTRIBUTING.md` — branch naming, commit format, PR rules.

### Good first issue because

Self-contained, touches only `cli/`, no broker/core changes, and the data is
already available over HTTP.

---

## 2. Expose per-task duration on the task object (enables real latency benchmarks)

**Labels:** `good first issue`, `help wanted`, `broker`, `benchmark`

### Summary

The benchmark harness (`bench/bench.py`) currently can't measure true
execution latency because the task object returned by `GET /api/tasks/:id`
has `enqueue_time` but no `completed_time` or `duration_ms`. Latency has to be
inferred from harness-side polling, which is coarse.

### What to build

- Record a `completed_time` (and/or `started_time`) on `Task` in
  `core/src/task.rs`, persisted in the SQLite schema and the in-memory store.
- Surface it in the HTTP API and gRPC proto (`proto/proto/chopflow.proto`).
- Update `BENCHMARKS.md` once the harness can read `duration_ms`.

### Good first issue because

Well-scoped: add two timestamps, thread them through storage + serialization,
and update the proto. High value: unblocks accurate latency benchmarks.

---

## 3. Python client library over the existing gRPC proto

**Labels:** `help wanted`, `client-library`

### Summary

The Python client is on the roadmap and the gRPC contract it targets is
already stable in `proto/proto/chopflow.proto`. This is the largest of the
"good first" set but a great contribution for someone who wants to own a
client SDK.

### Target ergonomics

```python
from chopflow import task, Client

@task(tags=["ml"], resources={"gpu": 1})
def train_model(dataset, hyperparams):
    return {"accuracy": 0.95}

result = train_model.delay("imagenet", {"lr": 0.001})
output = result.get(timeout=3600)
```

### Suggested steps

See `CONTRIBUTING.md` §8 (Adding a new client library). Lives under `clients/python/`.

---

## 4. Java client library over the existing gRPC proto

**Labels:** `help wanted`, `client-library`

### Summary

Same as the Python client but for Java. The proto already declares
`java_package = "dev.ricardoleal20.chopflow.grpc"`. See `CONTRIBUTING.md` §8.
Lives under `clients/java/`.

---

## 5. Add a `chopflow_cli cancel` subcommand

**Labels:** `good first issue`, `help wanted`, `cli`

### Summary

The broker supports `POST /api/tasks/:id/cancel` and the gRPC `CancelTask` RPC,
but the CLI has no `cancel` subcommand. Add `chopflow_cli cancel <task-id>`
that calls the HTTP endpoint.

### Where to look

- `cli/src/main.rs` — existing `enqueue`/`status` subcommands as a pattern.
- `broker/src/http.rs` — the `/api/tasks/:id/cancel` handler.

---

## 6. Additional demo handler (e.g. `word_count`)

**Labels:** `good first issue`, `help wanted`

### Summary

Add a new handler to `demos/src/handlers.rs` to broaden the demo story — e.g.
a `word_count` handler that takes text and returns counts, or a `sort`
handler. Register it in the demo worker and add a seeded task in
`demos/src/bin/seed.rs`.

### Good first issue because

Contained to `demos/`, follows an existing handler pattern, and is visible in
the dashboard immediately.

---

## Create them all (run from repo root)

```bash
gh issue create -R ricardoleal20/ChopFlow \
  --title "Add human-readable worker list to the CLI" \
  --label "good first issue,help wanted,worker" \
  --body-file docs/good-first-issues.md   # paste section 1 body instead

gh issue create -R ricardoleal20/ChopFlow \
  --title "Expose per-task duration on the task object" \
  --label "good first issue,help wanted,broker,benchmark" \
  --body "<paste section 2>"

# …etc. (Each section above is a self-contained body.)
```

Tip: `gh issue create --body-file` takes one file, so copy each section into a
temp file, or just paste into the GitHub UI.
