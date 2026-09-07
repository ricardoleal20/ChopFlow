"""Producer-client integration tests against the real broker."""

from datetime import datetime, timedelta, timezone

import pytest

from chopflow import ChopFlowClient
from chopflow.errors import TaskFailedError, TaskTimeoutError
from chopflow.models import OverlapPolicy, TaskStatus


def _wait_worker(client, timeout=15):
    """Wait until at least one worker is registered."""
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        if client.list_workers():
            return
        time.sleep(0.2)
    raise AssertionError("no worker registered within 15s")


def test_enqueue_and_get_returns_completed(client, worker):
    _wait_worker(client)
    result = (
        client.enqueue("echo")
        .payload({"hello": "world"})
        .tags("default")
        .priority(7)
        .enqueue()
    )
    assert result.id
    task = result.get(timeout=30)
    assert task.is_completed
    assert task.priority == 7
    assert task.result["status"] == "ok"
    assert task.result["echo"] == {"hello": "world"}


def test_enqueue_raw_json_string_payload(client, worker):
    _wait_worker(client)
    result = client.enqueue("echo").payload('{"n": 42}').tags("default").enqueue()
    task = result.get(timeout=30)
    assert task.is_completed
    assert task.result["echo"]["n"] == 42


def test_get_task_returns_none_for_unknown_id(client):
    # The broker requires a UUID-format id; use one that doesn't exist.
    assert client.get_task("00000000-0000-0000-0000-000000000000") is None


def test_list_tasks_includes_enqueued(client, worker):
    _wait_worker(client)
    client.enqueue("echo").payload({"a": 1}).tags("default").enqueue().get(timeout=30)
    tasks = client.list_tasks(limit=50)
    assert any(t.name == "echo" for t in tasks)


def test_get_stats(client, worker):
    _wait_worker(client)
    stats = client.get_stats()
    # After enqueuing + completing at least one task, completed >= 1.
    client.enqueue("echo").payload({}).tags("default").enqueue().get(timeout=30)
    stats = client.get_stats()
    assert stats.tasks_completed >= 1
    assert stats.active_workers >= 1


def test_list_workers(client, worker):
    _wait_worker(client)
    workers = client.list_workers()
    # Stopped workers from earlier tests can linger in the registry for up to
    # the heartbeat-liveness window, so only assert our default-tagged worker
    # is present rather than an exact count.
    assert len(workers) >= 1
    assert any("default" in w.tags for w in workers)


def test_cancel_queued_task(client):
    # No worker for the "unhandled" tag, so the task stays queued and can be
    # cancelled. Use a tag no worker subscribes to.
    result = (
        client.enqueue("noop")
        .payload({})
        .tags("nobody-subscribed-to-this")
        .enqueue()
    )
    # Give the broker a moment to persist it as Queued.
    import time

    t = client.get_task(result.id)
    assert t is not None
    assert client.cancel(result.id) is True
    t = client.get_task(result.id)
    assert t.status == TaskStatus.CANCELLED


def test_async_result_raises_timeout(client):
    result = (
        client.enqueue("noop")
        .payload({})
        .tags("nobody-subscribed-to-this")
        .enqueue()
    )
    with pytest.raises(TaskTimeoutError):
        result.get(timeout=1, raise_on_failure=False)


def test_async_result_raises_on_failure(client, worker):
    _wait_worker(client)
    # The worker's default handler is echo, which succeeds. Force a failure by
    # registering a handler that raises, then enqueue that task name. Use
    # max_retries=1 so the broker dead-letters quickly (default 3 retries with
    # a 5s base backoff would take ~35s to terminal).
    worker.register("boom", lambda p: (_ for _ in ()).throw(RuntimeError("nope")))
    result = client.enqueue("boom").payload({}).tags("default").max_retries(1).enqueue()
    with pytest.raises(TaskFailedError):
        result.get(timeout=30)
    # With raise_on_failure=False we get the task back in a failed/dead-lettered state.
    result2 = client.enqueue("boom").payload({}).tags("default").max_retries(1).enqueue()
    task = result2.get(timeout=30, raise_on_failure=False)
    assert task.status in (TaskStatus.FAILED, TaskStatus.DEADLETTERED)


def test_schedule_crud(client):
    sid = client.create_cron_schedule(
        "nightly",
        "build",
        "0 9 * * *",
        tags=["ci"],
        resources={"cpu": 4},
        overlap=OverlapPolicy.OVERLAP_SKIP,
    )
    assert sid

    schedules = client.list_schedules()
    assert any(s.id == sid and s.name == "nightly" for s in schedules)
    sched = next(s for s in schedules if s.id == sid)
    assert sched.cron == "0 9 * * *"
    assert sched.task_name == "build"
    assert sched.overlap_policy_name == "OVERLAP_SKIP"
    assert sched.enabled is True

    assert client.delete_schedule(sid) is True
    assert all(s.id != sid for s in client.list_schedules())


def test_oneshot_schedule(client):
    eta = datetime.now(timezone.utc) + timedelta(days=1)
    sid = client.create_oneshot_schedule(
        "one-off", "report", eta, overlap=OverlapPolicy.OVERLAP_ALLOW
    )
    schedules = client.list_schedules()
    sched = next(s for s in schedules if s.id == sid)
    assert sched.eta is not None
    assert sched.overlap_policy_name == "OVERLAP_ALLOW"
    client.delete_schedule(sid)


def test_connect_context_manager(broker):
    with ChopFlowClient.connect(broker, timeout=15) as c:
        assert c.get_stats() is not None
