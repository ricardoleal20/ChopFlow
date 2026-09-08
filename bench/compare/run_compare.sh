#!/usr/bin/env bash
# Head-to-head comparison runner: Celery + Temporal vs ChopFlow.
#
# Starts the dependencies each system needs (Redis for Celery, Temporal dev
# server for Temporal), runs the same 1K/10K/100K/1M sweep through each driver,
# and prints RESULT lines in the shared machine-readable format. ChopFlow's own
# numbers come from bench/run.sh — run that first on the same machine.
#
# Same fairness contract for every system (see README.md):
#   - same machine, same no-op echo workload, same sweep
#   - one worker process, 4 execution slots
#   - end-to-end throughput (submit → last completion) + sampled p50/p95/p99
#   - 1M time budget honored honestly ("did not finish" if exceeded)
#
# Usage:
#   bash bench/compare/run_compare.sh                      # 1k/10k/100k
#   TASKS="1000 10000 100000 1000000" bash bench/compare/run_compare.sh  # +1M
#   BUDGET=1200 bash bench/compare/run_compare.sh          # 1M capped at 20min
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/bench/compare"

TASKS="${TASKS:-1000 10000 100000}"
CONC="${CONC:-32}"
SAMPLE="${SAMPLE:-500}"
BUDGET="${BUDGET:-0}"
PY="${PY:-python3}"

# Prefer the dedicated venv if present.
if [ -x .venv/bin/python ]; then PY=".venv/bin/python"; fi

cleanup() {
  echo "▶ tearing down…"
  pkill -f "redis-server .*6379" 2>/dev/null || true
  pkill -f "temporal.*start-dev" 2>/dev/null || true
  pkill -f "celery -A echo_celery worker" 2>/dev/null || true
}
trap cleanup EXIT

# ── Celery (needs Redis + a Celery worker) ────────────────────────────────
echo "▶ starting Redis for Celery…"
if command -v redis-server >/dev/null 2>&1; then
  redis-server --daemonize yes --port 6379 --save "" --appendonly no 2>/dev/null || true
else
  echo "✗ redis-server not found. Install: brew install redis"
  echo "  skipping Celery comparison."
  SKIP_CELERY=1
fi

if [ -z "${SKIP_CELERY:-}" ]; then
  sleep 1
  echo "▶ starting Celery worker (concurrency=4, prefork)…"
  "$PY" -m celery -A echo_celery worker --concurrency=4 --loglevel=warning --pool=prefork \
      > /tmp/chopflow_bench_celery_worker.log 2>&1 &
  CELERY_WORKER_PID=$!
  sleep 4
  for n in $TASKS; do
    echo "════ celery: ${n} tasks ════"
    "$PY" echo_celery.py --tasks "$n" --concurrency "$CONC" \
        --sample-size "$SAMPLE" --time-budget "$BUDGET" || true
    echo
  done
  kill "$CELERY_WORKER_PID" 2>/dev/null || true
  wait "$CELERY_WORKER_PID" 2>/dev/null || true
fi

# ── Temporal (needs the temporal dev server) ──────────────────────────────
echo "▶ starting Temporal dev server…"
if command -v temporal >/dev/null 2>&1; then
  (temporal server start-dev --log-level error --db-filename /tmp/chopflow_bench_temporal.db \
      > /tmp/chopflow_bench_temporal.log 2>&1 &)
  # fall back to the raw server binary name some installs ship
elif command -v temporal-server >/dev/null 2>&1 && temporal-server server start-dev --help >/dev/null 2>&1; then
  (temporal-server server start-dev --log-level error \
      > /tmp/chopflow_bench_temporal.log 2>&1 &)
else
  echo "✗ temporal CLI (dev server) not found. Install: brew install temporal"
  echo "  (needs the temporalio/cli 'temporal server start-dev' subcommand)"
  echo "  skipping Temporal comparison."
  SKIP_TEMPORAL=1
fi

if [ -z "${SKIP_TEMPORAL:-}" ]; then
  sleep 4
  for n in $TASKS; do
    echo "════ temporal: ${n} tasks ════"
    "$PY" echo_temporal.py --tasks "$n" --concurrency "$CONC" \
        --sample-size "$SAMPLE" --time-budget "$BUDGET" || true
    echo
  done
fi

echo "✓ comparison done"
