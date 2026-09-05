#!/usr/bin/env bash
# Multi-worker dispatch demo.
#
# Starts a broker and TWO demo workers with different tags and resources, then
# seeds a mix of tasks so you can watch work route to the matching worker:
#   - `gpu,ml` tasks → worker A (cpu:8 gpu:1)
#   - `cpu`     tasks → worker B (cpu:4)
# Open the dashboard at http://localhost:8080 and watch the Workers view.
# Ctrl+C stops everything.
set -euo pipefail

cargo build
./target/debug/chopflow_broker start --http-port 8080 --port 8000 --storage memory --open &
BROKER_PID=$!
sleep 1

# Worker A: GPU-capable
./target/debug/chopflow_demo_worker start \
    --broker http://localhost:8000 --tags gpu,ml --resources cpu:8,gpu:1 &
WORKER_A=$!
# Worker B: CPU-only
./target/debug/chopflow_demo_worker start \
    --broker http://localhost:8000 --tags cpu --resources cpu:4 &
WORKER_B=$!
sleep 1

echo "broker=$BROKER_PID workerA(gpu,ml)=$WORKER_A workerB(cpu)=$WORKER_B"
echo "seeding tasks routed by tag…"

# A few GPU/ML tasks → routed to worker A
for i in 1 2 3; do
  curl -s -X POST http://localhost:8080/api/tasks \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"batch_compute\",\"payload\":{\"n\":128},\"tags\":[\"ml\"]}" >/dev/null
done
# A few CPU tasks → routed to worker B
for i in 1 2 3; do
  curl -s -X POST http://localhost:8080/api/tasks \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"batch_compute\",\"payload\":{\"n\":96},\"tags\":[\"cpu\"]}" >/dev/null
done

echo "open http://localhost:8080 — check the Workers view to see tags + resources."
echo "Ctrl+C to stop."
trap 'kill $BROKER_PID $WORKER_A $WORKER_B 2>/dev/null || true' EXIT
wait
