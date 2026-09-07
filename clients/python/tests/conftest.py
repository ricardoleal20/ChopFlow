"""Pytest fixtures: build + run the real Rust broker for the Python tests.

The broker binary is built once per session (skipped if already present). Each
test session gets a fresh in-memory broker on ephemeral ports.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
BROKER_BIN = REPO_ROOT / "target" / "debug" / "chopflow_broker"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _build_broker() -> Path:
    if BROKER_BIN.exists():
        return BROKER_BIN
    print("\n[conftest] building chopflow_broker (one-time)…", flush=True)
    env = dict(os.environ)
    rc = subprocess.run(
        ["cargo", "build", "-p", "chopflow_broker"],
        cwd=str(REPO_ROOT),
        env=env,
    ).returncode
    if rc != 0 or not BROKER_BIN.exists():
        pytest.fail(
            f"failed to build chopflow_broker (rc={rc}). "
            "Run `cargo build -p chopflow_broker` manually to see the error.",
            pytrace=False,
        )
    return BROKER_BIN


@pytest.fixture(scope="session")
def broker() -> str:
    """Start a fresh in-memory broker; yield its gRPC ``host:port``."""
    binary = _build_broker()
    grpc_port = _free_port()
    http_port = _free_port()

    proc = subprocess.Popen(
        [
            str(binary),
            "start",
            "--host", "127.0.0.1",
            "--port", str(grpc_port),
            "--http-port", str(http_port),
            "--storage", "memory",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    target = f"127.0.0.1:{grpc_port}"
    try:
        # Wait for the gRPC port to accept connections.
        deadline = time.time() + 30
        while time.time() < deadline:
            if proc.poll() is not None:
                pytest.fail(
                    f"broker exited early (rc={proc.returncode})", pytrace=False
                )
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                if s.connect_ex(("127.0.0.1", grpc_port)) == 0:
                    break
            time.sleep(0.2)
        else:
            pytest.fail("broker did not become ready within 30s", pytrace=False)

        yield target
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture()
def client(broker):
    """A connected ChopFlowClient for the session broker."""
    from chopflow import ChopFlowClient

    c = ChopFlowClient.connect(broker, timeout=15)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture()
def worker(broker):
    """A background Python worker subscribed to the ``default`` tag, so client
    tests that enqueue echo/default tasks can reach a terminal state."""
    from chopflow import ChopFlowWorker

    w = (
        ChopFlowWorker.builder()
        .broker(broker)
        .tags("default")
        .resources("cpu", 2)
        .poll_interval(0.2)
        .heartbeat_interval(1)
        .build()
    )
    w.start()
    # Wait for registration so tests don't race the poll loop.
    import time
    from chopflow import ChopFlowClient

    probe = ChopFlowClient.connect(broker, timeout=15)
    try:
        deadline = time.time() + 15
        while time.time() < deadline:
            if probe.list_workers():
                break
            time.sleep(0.2)
    finally:
        probe.close()
    yield w
    w.stop()
