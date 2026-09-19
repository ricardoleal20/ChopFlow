#!/usr/bin/env bash
# RAG demo run story: broker + demo worker (rate-limited resources) + seed.
#
# Builds the workspace, starts an in-memory broker, starts a demo worker that
# declares a static cpu plus a replenishing llm.rpm token bucket, then seeds
# the demo tasks — including the durable rag.ingest pipeline (chunk → embed →
# index), submitted twice with the same idempotency key. Ctrl+C stops
# everything.
set -euo pipefail

cargo build
./target/debug/chopflow_broker start --http-port 8080 --port 8000 --storage memory &
BROKER_PID=$!
sleep 1
./target/debug/chopflow_demo_worker start --broker http://localhost:8000 --tags demo --resources "cpu:4,llm.rpm:60@60/60" &
WORKER_PID=$!
sleep 1
./target/debug/chopflow_demo_seed http://localhost:8080
echo "Broker: $BROKER_PID, Worker: $WORKER_PID. Ctrl+C to stop."
echo "open http://localhost:8080 — watch the rag.ingest task's checkpoints grow chunk -> embed -> index"
trap 'kill $BROKER_PID $WORKER_PID 2>/dev/null || true' EXIT
wait
