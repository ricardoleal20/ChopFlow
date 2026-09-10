#!/usr/bin/env bash
# Head-to-head comparison runner: Celery + Temporal + apalis + River vs ChopFlow.
#
# Starts the dependencies each system needs (Redis for Celery+apalis, Temporal
# dev server for Temporal, PostgreSQL for River), runs the same sweep through
# each driver, and prints RESULT lines in the shared machine-readable format.
# ChopFlow's own numbers come from bench/run.sh — run that first.
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

# Prefer the dedicated venv if present.
if [ -x .venv/bin/python ]; then PY=".venv/bin/python"; fi

cleanup() {
  echo "▶ tearing down…"
  pkill -f "redis-server .*6379" 2>/dev/null || true
  pkill -f "temporal.*start-dev" 2>/dev/null || true
  pkill -f "celery -A echo_celery worker" 2>/dev/null || true
}
trap cleanup EXIT

# Redis is shared by Celery and apalis. Start it once up front.
REDIS_UP=0
if command -v redis-server >/dev/null 2>&1; then
  echo "▶ starting Redis (Celery + apalis)…"
  redis-server --daemonize yes --port 6379 --save "" --appendonly no 2>/dev/null || true
  REDIS_UP=1
else
  echo "✗ redis-server not found. Install: brew install redis"
  echo "  skipping Celery + apalis comparisons."
  SKIP_CELERY=1; SKIP_APALIS=1
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

# ── apalis (Rust + Redis, in-process worker) ──────────────────────────────
if [ -z "${SKIP_APALIS:-}" ]; then
  APALIS_BIN="$ROOT/bench/compare/echo_apalis/target/release/echo_apalis"
  if [ ! -x "$APALIS_BIN" ]; then
    echo "▶ building apalis driver…"
    (cd "$ROOT/bench/compare/echo_apalis" && cargo build --release 2>&1 | tail -1)
  fi
  if [ -x "$APALIS_BIN" ]; then
    for n in $TASKS; do
      echo "════ apalis: ${n} ${WORKLOAD} ════"
      redis-cli flushdb >/dev/null 2>&1 || true
      "$APALIS_BIN" --tasks "$n" --concurrency "$CONC" \
          --sample-size "$SAMPLE" --workload "$WORKLOAD" --time-budget "$BUDGET" || true
      echo
    done
  else
    echo "✗ apalis driver did not build; skipping."
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

# ── River (Go + PostgreSQL, in-process worker) ────────────────────────────
# River needs PostgreSQL, which none of the other systems require. If it's not
# available, skip with a clear note rather than failing — the driver is ready,
# just waiting on the runtime (install: brew install postgresql@16).
if [ -z "${SKIP_RIVER:-}" ]; then
  if [ -z "${DATABASE_URL:-}" ] && ! command -v psql >/dev/null 2>&1; then
    echo "✗ PostgreSQL not found / DATABASE_URL unset. Install: brew install postgresql@16"
    echo "  then: createdb riverbench && export DATABASE_URL=postgres://localhost:5432/riverbench?sslmode=disable"
    echo "  skipping River comparison (driver ready, pending runtime)."
    SKIP_RIVER=1
  fi
fi
if [ -z "${SKIP_RIVER:-}" ]; then
  RIVER_DIR="$ROOT/bench/compare/echo_river"
  if [ ! -f "$RIVER_DIR/echo_river" ]; then
    echo "▶ building river driver…"
    (cd "$RIVER_DIR" && go build -o echo_river . 2>&1 | tail -3)
  fi
  if [ -f "$RIVER_DIR/echo_river" ]; then
    for n in $TASKS; do
      echo "════ river: ${n} ${WORKLOAD} ════"
      "$RIVER_DIR/echo_river" --tasks "$n" --concurrency "$CONC" \
          --sample-size "$SAMPLE" --workload "$WORKLOAD" --time-budget "$BUDGET" || true
      echo
    done
  else
    echo "✗ river driver did not build; skipping."
  fi
fi

echo "✓ comparison done"

