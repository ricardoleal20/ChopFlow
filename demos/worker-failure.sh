#!/usr/bin/env bash
# Worker failure & retry demo.
#
# Shows at-least-once execution: a task is claimed by a worker, that worker is
# killed mid-flight, and the broker requeues the work so another worker picks
# it up and completes it.
#
# Flow:
#   1. start broker + worker A + worker B (both `cpu`-tagged)
#   2. enqueue a single long-running task (simulate_pipeline ~ staged sleeps)
#   3. once it's Running on one worker, kill that worker
#   4. the broker's reconcile/timeout path requeues the task; the surviving
#      worker claims and completes it
# Watch the task ledger at http://localhost:8080.
set -euo pipefail

cargo build
./target/debug/chopflow_broker start --http-port 8080 --port 8000 --storage memory --open &
BROKER_PID=$!
sleep 1

./target/debug/chopflow_demo_worker start \
    --broker http://localhost:8000 --tags cpu --resources cpu:2 &
WORKER_A=$!
./target/debug/chopflow_demo_worker start \
    --broker http://localhost:8000 --tags cpu --resources cpu:2 &
WORKER_B=$!
sleep 1

echo "broker=$BROKER_PID workerA=$WORKER_A workerB=$WORKER_B"
echo "enqueueing a long-running simulate_pipeline task…"
TASK_ID=$(curl -s -X POST http://localhost:8080/api/tasks \
    -H 'Content-Type: application/json' \
    -d '{"name":"simulate_pipeline","payload":{"stages":3,"secs":8},"tags":["cpu"]}' \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['task_id'])")
echo "task id: $TASK_ID"

echo "waiting for it to enter Running…"
for _ in $(seq 1 20); do
  STATUS=$(curl -s "http://localhost:8080/api/tasks/$TASK_ID" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
  if [ "$STATUS" = "Running" ]; then break; fi
  sleep 0.5
done
echo "task status: $STATUS"

echo "killing worker A ($WORKER_A) mid-flight — the task should be requeued…"
kill $WORKER_A 2>/dev/null || true

echo "watch the dashboard: worker B should pick up the requeued task and complete it."
echo "polling until terminal… (Ctrl+C to stop watching)"
for _ in $(seq 1 60); do
  STATUS=$(curl -s "http://localhost:8080/api/tasks/$TASK_ID" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
  echo "  status: $STATUS"
  case "$STATUS" in
    Completed|Failed|DeadLettered|Cancelled) break;;
  esac
  sleep 1
done

trap 'kill $BROKER_PID $WORKER_A $WORKER_B 2>/dev/null || true' EXIT
echo "done."
