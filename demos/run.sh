#!/usr/bin/env bash
# Demo run story: broker + demo worker + seed → live dashboard.
#
# Builds the workspace, starts an in-memory broker (with --open to launch the
# dashboard), starts a demo worker wired to the 4 demo handlers, then seeds
# one task of each handler type plus two schedules. Ctrl+C stops everything.
set -euo pipefail

cargo build
./target/debug/chopflow_broker start --http-port 8080 --port 8000 --storage memory --open &
BROKER_PID=$!
sleep 1
./target/debug/chopflow_demo_worker start --broker http://localhost:8000 --tags demo,ml --resources cpu:4 &
WORKER_PID=$!
sleep 1
./target/debug/chopflow_demo_seed http://localhost:8080
echo "Broker: $BROKER_PID, Worker: $WORKER_PID. Ctrl+C to stop."
trap 'kill $BROKER_PID $WORKER_PID 2>/dev/null || true' EXIT
wait
