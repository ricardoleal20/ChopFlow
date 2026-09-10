#!/usr/bin/env bash
# Reproducible ChopFlow benchmark wrapper.
#
# Builds the workspace, starts a fresh in-memory broker and a worker, runs the
# benchmark harness at a few task counts, prints results, and tears down.
#
# Usage:
#   bash bench/run.sh                                # default sizes: 1k, 10k, 100k
#   TASKS="1000 10000 100000 1000000" bash bench/run.sh   # include the 1M run (long)
#   SAMPLE=500 BUDGET=1200 bash bench/run.sh
#
# Requires: cargo (builds the release binaries), uv (for the Python harness).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Silence per-task info! logs (Dispatched / completed). At 1M tasks these are
# millions of log lines of pure overhead that swamp the broker and distort the
# benchmark. warn keeps failures/retries visible.
export RUST_LOG="${RUST_LOG:-warn}"

BROKER_PORT="${BROKER_PORT:-8100}"
HTTP_PORT="${HTTP_PORT:-8101}"
BROKER_URL="http://localhost:${HTTP_PORT}"
TASKS="${TASKS:-1000 10000 100000}"
CONC="${CONC:-32}"
SAMPLE="${SAMPLE:-200}"
BUDGET="${BUDGET:-0}"

cleanup() {
  pkill -f "chopflow_broker start --port ${BROKER_PORT}" 2>/dev/null || true
  pkill -f "chopflow_worker start --broker http://localhost:${BROKER_PORT}" 2>/dev/null || true
}
trap cleanup EXIT
cleanup

echo "▶ building release binaries…"
cargo build --release 2>&1 | tail -1

echo "▶ starting in-memory broker on gRPC :${BROKER_PORT} / HTTP :${HTTP_PORT}…"
./target/release/chopflow_broker start \
    --port "${BROKER_PORT}" --http-port "${HTTP_PORT}" --storage memory \
    > /tmp/chopflow_bench_broker.log 2>&1 &
sleep 2

echo "▶ starting worker (cpu:4, heartbeat 5s)…"
./target/release/chopflow_worker start \
    --broker "http://localhost:${BROKER_PORT}" --tags bench --resources cpu:4 \
    --heartbeat-interval 5 \
    > /tmp/chopflow_bench_worker.log 2>&1 &
sleep 2

echo
for n in $TASKS; do
  echo "══════════════════════════════════════════════════"
  echo "benchmark: ${n} tasks (concurrency ${CONC})"
  echo "══════════════════════════════════════════════════"
  uv run --with httpx python3 bench/bench.py \
      --broker "${BROKER_URL}" --tasks "${n}" --concurrency "${CONC}" \
      --sample-size "${SAMPLE}" --time-budget "${BUDGET}" || true
  echo
  # fresh broker state between runs for clean numbers
  pkill -f "chopflow_broker start --port ${BROKER_PORT}" 2>/dev/null || true
  pkill -f "chopflow_worker start --broker http://localhost:${BROKER_PORT}" 2>/dev/null || true
  sleep 1
  ./target/release/chopflow_broker start \
      --port "${BROKER_PORT}" --http-port "${HTTP_PORT}" --storage memory \
      > /tmp/chopflow_bench_broker.log 2>&1 &
  ./target/release/chopflow_worker start \
      --broker "http://localhost:${BROKER_PORT}" --tags bench --resources cpu:4 \
      --heartbeat-interval 5 \
      > /tmp/chopflow_bench_worker.log 2>&1 &
  sleep 2
done

echo "✓ done"
