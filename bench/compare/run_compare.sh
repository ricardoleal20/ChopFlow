#!/usr/bin/env bash
# Head-to-head comparison runner: Celery + Temporal + BullMQ + Ray vs ChopFlow.
#
# Starts the dependencies each system needs (Redis for Celery+BullMQ, Temporal
# dev server for Temporal, a local Ray cluster for Ray), runs the same sweep
# through each driver, and prints RESULT lines in the shared machine-readable
# format. ChopFlow's own numbers come from bench/run.sh — run that first.
#
# Same fairness contract for every system (see README.md):
#   - same machine, same workload, same sweep
#   - one worker process, 4 execution slots
#   - end-to-end throughput (submit → last completion) + sampled p50/p95/p99
#   - 1M time budget honored honestly ("did not finish" if exceeded)
#
# Usage:
#   bash bench/compare/run_compare.sh                      # 1k/10k/100k, echo
#   TASKS="1000 10000 100000 1000000" bash bench/compare/run_compare.sh  # +1M
#   WORKLOAD=resize bash bench/compare/run_compare.sh      # real-work axis
#   BUDGET=1200 bash bench/compare/run_compare.sh          # 1M capped at 20min
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/bench/compare"

TASKS="${TASKS:-1000 10000 100000}"
CONC="${CONC:-32}"
SAMPLE="${SAMPLE:-500}"
BUDGET="${BUDGET:-0}"
WORKLOAD="${WORKLOAD:-echo}"
PY="${PY:-python3}"
NODE="${NODE:-node}"

# Prefer the dedicated venv if present.
if [ -x .venv/bin/python ]; then PY=".venv/bin/python"; fi

cleanup() {
  echo "▶ tearing down…"
  pkill -f "redis-server .*6379" 2>/dev/null || true
  pkill -f "temporal.*start-dev" 2>/dev/null || true
  pkill -f "celery -A echo_celery worker" 2>/dev/null || true
  pkill -f "node .*echo_bullmq/worker.mjs" 2>/dev/null || true
}
trap cleanup EXIT

# Redis is shared by Celery and BullMQ. Start it once up front.
if command -v redis-server >/dev/null 2>&1; then
  echo "▶ starting Redis (Celery + BullMQ)…"
  redis-server --daemonize yes --port 6379 --save "" --appendonly no 2>/dev/null || true
else
  echo "✗ redis-server not found. Install: brew install redis"
  echo "  skipping Celery + BullMQ comparisons."
  SKIP_CELERY=1; SKIP_BULLMQ=1
fi

# ── Celery (needs Redis + a Celery worker) ────────────────────────────────
if [ -z "${SKIP_CELERY:-}" ]; then
  sleep 1
  echo "▶ starting Celery worker (concurrency=4, prefork)…"
  "$PY" -m celery -A echo_celery worker --concurrency=4 --loglevel=warning --pool=prefork \
      > /tmp/chopflow_bench_celery_worker.log 2>&1 &
  CELERY_WORKER_PID=$!
  sleep 4
  for n in $TASKS; do
    echo "════ celery: ${n} ${WORKLOAD} ════"
    "$PY" echo_celery.py --tasks "$n" --concurrency "$CONC" \
        --sample-size "$SAMPLE" --workload "$WORKLOAD" --time-budget "$BUDGET" || true
    redis-cli flushdb >/dev/null 2>&1 || true
    echo
  done
  kill "$CELERY_WORKER_PID" 2>/dev/null || true
  wait "$CELERY_WORKER_PID" 2>/dev/null || true
fi

# ── BullMQ (Node.js + Redis Streams, separate worker process) ─────────────
if [ -z "${SKIP_BULLMQ:-}" ]; then
  if [ -d echo_bullmq/node_modules ]; then
    echo "▶ starting BullMQ worker (concurrency=4)…"
    "$NODE" echo_bullmq/worker.mjs --workload "$WORKLOAD" \
        > /tmp/chopflow_bench_bullmq_worker.log 2>&1 &
    BULLMQ_WORKER_PID=$!
    sleep 2
    for n in $TASKS; do
      echo "════ bullmq: ${n} ${WORKLOAD} ════"
      redis-cli del bench:completed bench:failures bench:latency >/dev/null 2>&1 || true
      "$NODE" echo_bullmq/driver.mjs --tasks "$n" --concurrency "$CONC" \
          --sample-size "$SAMPLE" --workload "$WORKLOAD" --time-budget "$BUDGET" || true
      echo
    done
    kill "$BULLMQ_WORKER_PID" 2>/dev/null || true
    wait "$BULLMQ_WORKER_PID" 2>/dev/null || true
  else
    echo "✗ BullMQ deps not installed. Run: (cd echo_bullmq && npm install)"
    echo "  skipping BullMQ comparison."
  fi
fi

# ── Temporal (needs the temporal dev server) ──────────────────────────────
echo "▶ starting Temporal dev server…"
if command -v temporal >/dev/null 2>&1; then
  (temporal server start-dev --log-level error --db-filename /tmp/chopflow_bench_temporal.db \
      > /tmp/chopflow_bench_temporal.log 2>&1 &)
elif command -v temporal-server >/dev/null 2>&1 && temporal-server server start-dev --help >/dev/null 2>&1; then
  (temporal-server server start-dev --log-level error \
      > /tmp/chopflow_bench_temporal.log 2>&1 &)
else
  echo "✗ temporal CLI (dev server) not found. Install: brew install temporal"
  echo "  skipping Temporal comparison."
  SKIP_TEMPORAL=1
fi

if [ -z "${SKIP_TEMPORAL:-}" ]; then
  sleep 4
  for n in $TASKS; do
    echo "════ temporal: ${n} ${WORKLOAD} ════"
    "$PY" echo_temporal.py --tasks "$n" --concurrency "$CONC" \
        --sample-size "$SAMPLE" --time-budget "$BUDGET" || true
    echo
  done
fi

# ── Ray (Python distributed-compute runtime, local cluster) ───────────────
# Ray spins up its own local cluster (head + workers) inside the driver process
# via ray.init(num_cpus=4). No external dependency beyond the `ray` + `numpy`
# packages. Apples-to-pears with a task queue (see README.md) — included with
# an explicit category caveat, not as a direct verdict.
if "$PY" -c "import ray" 2>/dev/null; then
  for n in $TASKS; do
    echo "════ ray: ${n} ${WORKLOAD} ════"
    "$PY" echo_ray.py --tasks "$n" --concurrency "$CONC" \
        --sample-size "$SAMPLE" --workload "$WORKLOAD" --time-budget "$BUDGET" || true
    echo
  done
else
  echo "✗ ray not installed in the venv. Run: uv pip install ray numpy"
  echo "  skipping Ray comparison."
fi

echo "✓ comparison done"
