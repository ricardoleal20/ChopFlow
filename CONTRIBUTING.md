# Contributing to ChopFlow

Thanks for your interest in ChopFlow. This document is the working agreement for
everyone touching the repo — keep it readable, keep it honest, and keep the
voice consistent with [docs/design/DESIGN.md](docs/design/DESIGN.md).

---

## 1. Project context

ChopFlow is a durable distributed **task queue** (Celery / Ray / Dask lineage)
with a Rust core, multi-language client libraries, and an embedded operations
dashboard. It is **not** a workflow engine like Temporal — see the FAQ. Keep
comparisons and copy in that category.

### Workspace layout

| Crate | Role |
|---|---|
| `core` | Rust library: `Task`, `Queue`, `Dispatcher`, `RetryPolicy`, `Schedule`, `Storage`, resources |
| `broker` | gRPC + HTTP/JSON server, embeds the dashboard UI, owns `BrokerState` |
| `worker` | Pulls work from the broker by tag + resource match, acks results |
| `cli` | `chopflow_cli` — enqueue, status, schedule management |
| `demos` | Example handlers, seed tooling, run scripts |

Client libraries live alongside (Python, Java) and call the broker over gRPC.
**Leave structural space for additional clients** — see §6.

---

## 2. Development setup

Requirements: Rust (2021 edition), Cargo, Python 3.8+ (for the Python client).

```bash
git clone https://github.com/ricardoleal20/ChopFlow.git
cd ChopFlow
cargo build --release

# Run a broker (gRPC :8000, dashboard + HTTP API :8080)
./target/release/chopflow_broker start --host 127.0.0.1 --port 8000 --http-port 8080 --open

# In another shell, start a worker
./target/release/chopflow_worker start --broker http://localhost:8000 --tags gpu,ml --resources gpu:1,cpu:4
```

For iterative dashboard work, point a browser at `broker/ui/dashboard.html`
directly while the broker serves the API on `:8080`. After editing the UI:

```bash
cp broker/ui/dashboard.html broker/ui/dist/index.html
touch broker/src/http.rs   # force rust-embed to re-read ui/dist
cargo build
```

---

## 3. Branching

- Branch from `main` (or the relevant feature branch it depends on).
- Branch naming: `ricardo/{issue-or-topic}-{what-it-solves}` — e.g.
  `ricardo/scheduled-tasks-and-demos`, `ricardo/landing-page`.
- `main` is fast-forward only. Land work via squash-merge PR; do not push
  merge commits to protected branches.

---

## 4. Commit format

Organization convention is **gitmoji + Action**: `<gitmoji> <Action>: <summary>`.

- The gitmoji is a literal token (`:sparkles:`, `:bug:`, `:memo:`, `:wrench:`,
  `:white_check_mark:`, `:zap:`, `:lock:`, `:fire:`, `:lipstick:`).
- The Action is a capitalized verb in **imperative mood** (Add, Update, Fix,
  Remove, Refactor, Document).
- Summary is concise — soft target ≤72 characters.

Examples:

```
:sparkles: Add: uv workspace scaffold
:bug: Fix: idempotency-key day-bucket collision
:memo: Update: AGENTS.md autonomy boundary wording
```

The gitmoji + Action prefix is **non-optional** — every commit on this repo
carries one.

### Linear trailer (soft default)

Commits SHOULD include a Linear trailer as the LAST line of the message body:

- `Refs: <TICKET>-<n>` — references a ticket without completing it.
- `Closes: <TICKET>-<n>` — completes the ticket (auto-closes on merge).
- Multiple: `Refs: ABC-8, ABC-9`.

### Co-authorship

**Do not add `Co-Authored-By: Claude` or any AI-attribution trailer.** This is
a project-level prohibition. Genuine human co-authorship MAY use
`Co-Authored-By: <Name> <email>` at the author's discretion.

### Never commit

- Secrets or credentials — use a local gitignored `.env` and read from the
  process environment.
- Large generated artifacts — use `.gitignore`; if a file must be tracked,
  document why in the commit body.
- Merge commits on protected branches.

---

## 5. Pull requests

Every PR MUST have an assignee (assign yourself) and at least one label.

**Title:** `<TICKET> :: <one-line summary>` — no emoji, no gitmoji, no action
verb prefix. Soft target ≤72 chars, imperative mood.
Example: `ABC-52 :: Add idempotent ticket creation`.
Multi-ticket: `ABC-52, OPS-3438 :: Add X`.

**Description sections** (normative, in this order):

1. `## Summary` — what changes and why; one paragraph.
2. `## Changes` — bulleted list of files/areas touched.
3. `## Test plan` — explicit test commands (`cargo test -q`, …), manual
   verification steps, and any out-of-band checks.
4. `## Refs` — Linear ticket links (`<TICKET>-<n>`), FR/NFR IDs, `DEC-<slug>`
   decision IDs.

---

## 6. Design system & UI work

`docs/design/DESIGN.md` is the **canonical visual contract**. When it and a
shipping file disagree, **DESIGN.md wins for token values**. Key rules:

- Dark-first; persisted light/dark toggle (`localStorage` key `chopflow-theme`).
- Status semantics: **dot + label, never color alone**; `Running` pulses.
- Fonts: Inter (sans) + JetBrains Mono (mono / data / IDs).
- One accent (`--primary #4060ff`); no decorative gradients on content
  surfaces; no emoji as icons.
- `prefers-reduced-motion` compliance is **mandatory**.
- Single self-contained HTML files (inline CSS+JS; Google Fonts via CDN; no
  build step) unless a component crate genuinely needs a bundler.

When adding a new client library, add a nav entry and a docs page under
**Client Libraries**; leave the "More clients soon" placeholder node intact.

---

## 7. Testing

- `cargo test` — unit + integration tests across the workspace.
- `cargo clippy -- -D warnings` — keep the tree clean.
- `cargo fmt --check` — formatting is enforced.
- For UI changes, verify both dark and light themes, and at ≥375px mobile +
  desktop widths.

---

## 8. Adding a new client library

1. Create the client package under its own directory (e.g. `clients/<lang>`).
2. Speak gRPC to the broker; reuse the proto definitions.
3. Mirror the Python client's ergonomics where idiomatic (`@task`-equivalent,
   async result handle).
4. Add a docs page under Client Libraries and a row in the architecture map.
5. Update the landing page's "Multi-language Interface" copy if the supported
   set changed.

---

## 9. Questions / conduct

Be precise, engineering-focused, and calm — the same voice as the product.
Open an issue for bugs, feature requests, or design questions before opening
a PR for non-trivial work.
