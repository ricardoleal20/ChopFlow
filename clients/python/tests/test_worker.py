"""Worker-SDK integration tests against the real broker."""

import time

from chopflow import ChopFlowWorker


def test_worker_executes_registered_handler(broker):
    worker = (
        ChopFlowWorker.builder()
        .broker(broker)
        .tags("py-test")
        .resources("cpu", 1)
        .poll_interval(0.2)
        .heartbeat_interval(1)
        .build()
    )
    worker.register("double", lambda p: {"doubled": p["n"] * 2})
    worker.start()
    try:
        # Wait for registration.
        _wait_for_worker_count(broker, 1)

        from chopflow import ChopFlowClient

        client = ChopFlowClient.connect(broker, timeout=15)
        try:
            result = (
                client.enqueue("double").payload({"n": 21}).tags("py-test").enqueue()
            )
            task = result.get(timeout=20)
            assert task.is_completed
            assert task.result == {"doubled": 42}
        finally:
            client.close()
    finally:
        worker.stop()


def test_worker_default_handler_handles_unknown_task(broker):
    worker = (
        ChopFlowWorker.builder()
        .broker(broker)
        .tags("py-default")
        .resources("cpu", 1)
        .poll_interval(0.2)
        .heartbeat_interval(1)
        .build()
    )
    # Override the default handler to something distinctive.
    worker.register_default(lambda p: {"via": "default", "got": p})
    worker.start()
    try:
        _wait_for_worker_count(broker, 1)
        from chopflow import ChopFlowClient

        client = ChopFlowClient.connect(broker, timeout=15)
        try:
            # No explicit handler for "mystery" → falls back to default.
            result = (
                client.enqueue("mystery").payload({"k": 1}).tags("py-default").enqueue()
            )
            task = result.get(timeout=20)
            assert task.is_completed
            assert task.result == {"via": "default", "got": {"k": 1}}
        finally:
            client.close()
    finally:
        worker.stop()


def _wait_for_worker_count(broker, count, timeout=15):
    from chopflow import ChopFlowClient

    client = ChopFlowClient.connect(broker, timeout=15)
    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if len(client.list_workers()) >= count:
                return
            time.sleep(0.2)
        raise AssertionError(f"worker did not register within {timeout}s")
    finally:
        client.close()
