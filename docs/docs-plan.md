# ChopFlow Documentation — Implementation Plan

> Goal: ship a complete, accurate documentation site for everything ChopFlow
> has today, with deliberate placeholder space for upcoming client libraries.
> The site shell and visual design are produced by OpenDesign
> (`docs/design/chopflow-docs.html`); this plan covers the **content** that
> fills that shell, page by page, in shipping order.

The design contract is [`docs/design/DESIGN.md`](design/DESIGN.md). All status
semantics use **dot + label, never color alone**; `Running` pulses.

---

## 0. Source of truth per page

Every docs page must be written from the actual code, not from memory. The
authoritative files per topic:

| Topic | Source files |
|---|---|
| Task model & statuses | `core/src/task.rs` |
| Task priority & ordering | `core/src/task.rs`, `core/src/storage.rs` |
| Queue | `core/src/queue.rs` |
| Dispatcher (pull model) | `core/src/dispatcher.rs` |
| Retry & dead-letter | `core/src/retry.rs` |
| Scheduling | `core/src/schedule.rs` |
| Storage backends | `core/src/storage.rs` |
| Resources & tags | `core/src/resources.rs` |
| Broker gRPC service | `broker/src/lib.rs`, `proto/proto/chopflow.proto` |
| Broker HTTP API + embedded UI | `broker/src/http.rs`, `broker/src/main.rs` |
| Worker | `worker/src/main.rs` |
| CLI | `cli/src/main.rs` |
| Demos | `demos/src/`, `demos/run.sh` |

---

## 1. Information architecture (nav tree)

The nav is fixed by the OpenDesign shell. Group order and page ownership:

### Getting Started
1. **Overview** — what ChopFlow is, the workspace crates, the architecture
   diagram, "task queue not workflow engine" positioning. *(filled in v1)*
2. **Quickstart** — build, start broker, start worker, enqueue via CLI. *(filled in v1)*
3. **Installation** — prerequisites, `cargo build --release`, binary names,
   where the dashboard lives. *(filled in v1)*

### Concepts
4. **Task Lifecycle** — the 6-state walkthrough (enqueue → Queued →
   FetchTasks → Running → AcknowledgeTask → Completed) with the animated
   diagram from the landing page as a static reference. *(filled in v1)*
5. **Task States & Statuses** — the `TaskStatus` enum table. *(filled in v1)*
6. **Scheduling & Recurrence** — cron vs one-shot, overlap policies, UTC
   semantics, no backfill. *(filled in v1)*
7. **Retry & Dead-Letter** — `RetryPolicy`, exponential backoff, computed
   ETA, terminal dead-letter. *(filled in v1)*
7b. **Priority & Ordering** — `Task.priority` (default 0, higher = claimed
    first), the `claim_ready` ordering key `(priority DESC, eta ASC,
    enqueue_time ASC)`, FIFO within a tier, SQLite index + migration. *(filled in v1)*
8. **Resources & Tags** — `ResourceRequirements`, `ResourceAvailability`,
   tag matching, pull-based dispatch. *(filled in v1)*

### Components
9. **Core (Rust library)** — public API surface (`Task`, `Queue`,
   `Dispatcher`, `RetryPolicy`, `Schedule`, `Storage`, re-exports). *(filled in v1)*
10. **Broker** — gRPC + HTTP, `BrokerState`, embedded UI, `--open`, restart
    reconcile. *(filled in v1)*
11. **Worker** — registration, tags, resources, lease, ack loop. *(filled in v1)*
12. **CLI** — `enqueue`, `status`, `schedule create/list/delete`. *(filled in v1)*
13. **Embedded Dashboard** — what the UI shows, poll cadence, Tauri app. *(filled in v1)*
14. **Demos** — the four handlers, `run.sh`, seed tool. *(filled in v1)*
14b. **LLM Worker** — `chopflow-llm-worker`, async + bounded concurrency,
    `llm.complete` / `llm.chat`, OpenAI-compatible config. *(filled in v1)*

### Client Libraries
15. **Rust** — in-process / gRPC client usage. *(filled in v1)*
16. **Python** — `@task`, `AsyncResult`, `.delay()`. *(placeholder: client not
    yet landed; show intended ergonomics + proto reference)*
17. **Java** — intended gRPC client. *(placeholder: client not yet landed)*
18. **More clients soon** — permanent placeholder node. Leave this empty
    page with the "coming soon" callout. It is the deliberate space for
    future clients (e.g. Go, Node/TS).

### HTTP API Reference
19. **Stats** — `GET /api/stats`. *(filled in v1)*
20. **Tasks** — `GET/POST /api/tasks`, `GET /api/tasks/:id`,
    `POST /api/tasks/:id/cancel`. *(filled in v1)*
21. **Schedules** — `GET/POST /api/schedules`,
    `GET/PATCH/DELETE /api/schedules/:id`. *(filled in v1)*
22. **Workers** — `GET /api/workers`, `GET /healthz`. *(filled in v1)*

### gRPC Reference
23. **gRPC service** — methods from `chopflow.proto` (FetchTasks,
    AcknowledgeTask, register, …). *(filled in v1)*

### Storage Backends
24. **SQLite** — default, `--db-path`, reconcile on boot. *(filled in v1)*
25. **In-Memory** — testing/demo backend. *(filled in v1)*

### Architecture
26. **Architecture** — the 7-crate map, single-binary rationale, gRPC vs HTTP
    split, shared `BrokerState`. *(filled in v1)*

### FAQ
27. **FAQ** — reuse the landing page's 7 items, extended with docs-specific
    questions. *(filled in v1)*

---

## 2. Content per filled page (spec)

Each filled page follows the docs shell template:

- **H1** + one-sentence lede
- Prose body (2–4 short paragraphs)
- Code block(s) with file tab + copy button
- Tables where the data is tabular (statuses, endpoints, overlap policies)
- Callouts (`info` / `success` / `warn` / `danger`) for gotchas
- Anchor-linked headings

### Page 1 — Overview
Lede: "ChopFlow is a durable distributed task queue with a Rust core,
multi-language clients, and an embedded operations dashboard." Render the
7-crate architecture map (reuse the landing page's `architecture` block
markup). State the category positioning vs Temporal. List the workspace
crates with one-line roles.

### Page 2 — Quickstart
Four terminal blocks (mono, `$` prompt, command + output):
1. `cargo build --release`
2. `./target/release/chopflow_broker start --host 127.0.0.1 --port 8000 --http-port 8080 --open` + real log lines (Persistence: sqlite; reconciled in-flight tasks; gRPC on :8000; HTTP/ on :8080; Opened dashboard).
3. `./target/release/chopflow_worker start --broker http://localhost:8000 --tags gpu,ml --resources gpu:1,cpu:4`
4. `./target/release/chopflow_cli enqueue --task task.json --name training --tags gpu,ml`

End with a "next steps" callout linking to Concepts and the HTTP API.

### Page 5 — Task States & Statuses
A status table rendered from `TaskStatus`:

| Status | Dot color | Meaning |
|---|---|---|
| Created | slate | Task constructed, not yet enqueued |
| Queued | warn | Broker persisting, awaiting eligibility |
| Running | info (pulses) | Worker claimed it, lease active |
| Completed | success | Terminal — result persisted |
| Failed | danger | Retries remain |
| DeadLettered | purple | Terminal — retries exhausted |
| Cancelled | slate | Terminal — cancelled before completion |

Gotcha callout: "Status is dot + label, never color alone."

### Pages 19–22 — HTTP API Reference
One endpoint table per page, with method badge + mono path + purpose + a
request/response example block. Methods: `GET /api/stats`, `GET /api/tasks`
(with `?status=&limit=&offset=`), `GET /api/tasks/:id`, `POST /api/tasks`,
`POST /api/tasks/:id/cancel`, `GET /api/workers`, `GET /healthz`,
`GET/POST /api/schedules`, `GET/PATCH/DELETE /api/schedules/:id`.

### Pages 16–18 — Client libraries
- **Python/Java pages:** show the intended ergonomics (the `@task` /
  `AsyncResult` example), then a clearly-marked `warn` callout: "This client
  is on the roadmap. The gRPC contract it targets is stable in
  `proto/proto/chopflow.proto`." Link to CONTRIBUTING §8.
- **"More clients soon" page:** single `info` callout, no content. This is
  the reserved space.

---

## 3. Placeholder convention

Any page not filled in v1 must render the docs shell with:
- its real H1 + lede (so the nav isn't lying about what the page is), and
- a `warn` callout: "This section is a placeholder — content coming soon."

No page is a 404 and no page is silently empty. The nav tree is complete;
the content fills in over time.

---

## 4. Shipping order

**Phase 1 — shell + Getting Started + key references (this is what OpenDesign
produces):** Overview, Quickstart, Installation, Task States & Statuses,
HTTP API Reference. All other pages ship as honest placeholders.

**Phase 2 — Concepts + Components:** fill pages 4, 6, 7, 8, 9, 10, 11, 12,
13, 14. These are the "how it works" depth.

**Phase 3 — gRPC reference + Storage + Architecture + FAQ:** pages 23, 24,
25, 26, 27.

**Phase 4 — Clients:** land the Rust client docs (page 15). Pages 16/17 stay
placeholder until the Python/Java clients ship; page 18 stays as the
permanent future-clients space.

---

## 5. Maintenance rules

- Docs live in `docs/`; the rendered site is `docs/design/chopflow-docs.html`
  (single self-contained file, same design system).
- When a status, endpoint, or CLI flag changes in code, update the matching
  docs page in the same PR. The same applies to `Task.priority` / the
  `claim_ready` ordering key — the "Priority & Ordering" concept page and the
  enqueue examples (CLI `--priority`, HTTP/gRPC `priority`, client `.priority()`)
  are the canonical user-facing reference and must track
  `core/src/task.rs` + `core/src/storage.rs`.
- Keep the "More clients soon" placeholder — do not delete it when adding a
  client; instead graduate that client to its own filled page and leave the
  placeholder for the next one.
- Run the Pages deploy workflow (`deploy-pages.yml`) only ships the landing
  page today; extend it to ship `chopflow-docs.html` when the docs are ready
  to publish.
