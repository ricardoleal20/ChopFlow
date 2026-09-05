# Demos

Scripts that exercise ChopFlow end-to-end against a live broker. Each one
builds the workspace (debug), starts a broker, and tears down on Ctrl+C.

## `run.sh` — full demo

The headline demo: a broker with `--open`, one demo worker wired to four
showcase handlers, and a seed of one task per handler plus a cron and a
one-shot schedule. Watch four tasks flow through distinct lifecycles in the
dashboard, including a visibly retrying `flaky_handler`.

```bash
bash demos/run.sh
```

## `multi-worker.sh` — tag-based routing

Two workers with different tags and resources (`gpu,ml` vs `cpu`), and a mix
of tasks seeded so work routes to the matching worker. Open the dashboard's
Workers view to see each worker's tags and live resource meters.

```bash
bash demos/multi-worker.sh
```

## `worker-failure.sh` — at-least-once execution

Enqueues a long-running `simulate_pipeline` task, waits until it's `Running`,
then kills that worker mid-flight. The broker's reconcile/timeout path
requeues the task and the surviving worker claims and completes it — a live
demonstration of at-least-once delivery under worker failure.

```bash
bash demos/worker-failure.sh
```

## Demo handlers

The demo worker (`chopflow_demo_worker`) registers these handlers:

| Handler            | What it does                                                        |
|--------------------|---------------------------------------------------------------------|
| `resize_image`     | Generates a synthetic gradient PNG and resizes it (image-rs).        |
| `batch_compute`    | CPU-bound `n x n` f64 matrix multiply (nalgebra), reports timings.  |
| `simulate_pipeline`| Multi-stage pipeline (download/process/upload) with staged sleeps. |
| `flaky_handler`    | Fails ~30% of the time (seeded) to exercise the retry policy.        |
| `echo` / `default` | Echoes the payload back — useful as a smoke test.                    |

## Seeding a running broker manually

```bash
cargo run -p chopflow_demos --bin chopflow_demo_seed -- [broker_base_url]
# broker_base_url defaults to http://localhost:8080
```
