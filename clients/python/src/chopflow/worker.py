"""The ChopFlow worker SDK.

Mirrors the Java SDK: a :class:`ChopFlowWorker` builder, a :func:`task`
decorator for handler registration (Python's native equivalent of Java's
``@ChopTask`` reflection scan), and a heartbeat + poll loop that pulls work
from the broker, runs a handler, and acknowledges the result.

Handlers receive the parsed payload (a Python ``dict``/``list``/etc.) and
return any JSON-serializable object. A handler that raises causes a failure
ack; the broker owns retries.
"""

from __future__ import annotations

import json as _json
import logging
import signal
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import grpc

from ._generated import chopflow_pb2 as pb
from ._generated import chopflow_pb2_grpc as pb_grpc
from .client import _strip_scheme
from .models import TaskStatus

log = logging.getLogger("chopflow.worker")

#: A handler takes a parsed payload and returns a JSON-serializable result.
TaskHandler = Callable[[Any], Any]


def _echo_handler(payload: Any) -> Any:
    return {"status": "ok", "echo": payload}


def task(name: str) -> Callable[[TaskHandler], TaskHandler]:
    """Decorator registering a function as the handler for ``name``.

    The decorated function is returned unchanged so it can still be called
    directly; registration is a side effect captured by :class:`ChopFlowWorker`
    via the module-level registry it scans at build time. For simplicity and
    explicitness, prefer ``worker.register("name", fn)``; this decorator is
    provided for Java-parity ergonomics.
    """

    def deco(fn: TaskHandler) -> TaskHandler:
        _DECORATED.setdefault(name, []).append(fn)
        return fn

    return deco


# Module-level registry of @task-decorated handlers. A worker scans this at
# build() time and clears the entries it consumes, so decoration order across
# modules works regardless of import order.
_DECORATED: Dict[str, List[TaskHandler]] = {}


class ChopFlowWorker:
    """A pull-based ChopFlow worker.

    Build with :meth:`builder`::

        ChopFlowWorker.builder() \\
            .broker("localhost:8000") \\
            .tags("default") \\
            .resources("cpu", 1) \\
            .build()
    """

    def __init__(
        self,
        broker: str,
        tags: List[str],
        resources: Dict[str, int],
        *,
        heartbeat_interval: float = 30.0,
        poll_interval: float = 2.0,
        max_tasks_per_poll: int = 4,
    ):
        self._broker = _strip_scheme(broker)
        self._tags = list(tags)
        self._resources = dict(resources)
        self._heartbeat_interval = heartbeat_interval
        self._poll_interval = poll_interval
        self._max_tasks_per_poll = max_tasks_per_poll

        self._handlers: Dict[str, TaskHandler] = {
            "echo": _echo_handler,
            "default": _echo_handler,
        }

        self._worker_id: Optional[str] = None
        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[pb_grpc.ChopFlowBrokerStub] = None

        self._stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._poll_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # builder
    # ------------------------------------------------------------------
    @classmethod
    def builder(cls) -> "Builder":
        return Builder()

    # ------------------------------------------------------------------
    # handler registration
    # ------------------------------------------------------------------
    def register(self, name: str, handler: TaskHandler) -> "ChopFlowWorker":
        self._handlers[name] = handler
        return self

    def register_default(self, handler: TaskHandler) -> "ChopFlowWorker":
        self._handlers["default"] = handler
        return self

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._worker_id is not None:
            return
        self._connect_and_register()
        self._stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="chopflow-heartbeat"
        )
        self._poll_thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="chopflow-poll"
        )
        self._heartbeat_thread.start()
        self._poll_thread.start()

    def start_and_await(self) -> None:
        """Start, then block until SIGINT/SIGTERM (or :meth:`stop`)."""
        self.start()
        handler = lambda *_: self._stop.set()
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        try:
            while not self._stop.is_set():
                time.sleep(0.2)
        finally:
            self.stop()

    def stop(self) -> None:
        self._stop.set()
        if self._channel is not None:
            self._channel.close()
            self._channel = None
            self._stub = None
        self._worker_id = None

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    def _connect_and_register(self) -> None:
        backoff = 0.5
        while not self._stop.is_set():
            try:
                channel = grpc.insecure_channel(self._broker)
                stub = pb_grpc.ChopFlowBrokerStub(channel)
                resp = stub.RegisterWorker(
                    pb.RegisterWorkerRequest(
                        address="localhost",
                        tags=self._tags,
                        resources=self._resources,
                    )
                )
                self._channel = channel
                self._stub = stub
                self._worker_id = resp.worker_id
                log.info("worker registered: %s", self._worker_id)
                return
            except grpc.RpcError as e:
                log.warning("could not reach broker (%s); retrying in %ss", e.code(), backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 5.0)

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set() and self._stub is not None:
            try:
                self._stub.WorkerHeartbeat(
                    pb.WorkerHeartbeatRequest(
                        worker_id=self._worker_id,
                        resources=pb.ResourceAvailability(
                            available=self._resources,
                            total=self._resources,
                        ),
                    )
                )
            except grpc.RpcError as e:
                log.warning("heartbeat failed: %s", e.code())
            self._stop.wait(self._heartbeat_interval)

    def _poll_loop(self) -> None:
        while not self._stop.is_set() and self._stub is not None:
            try:
                resp = self._stub.FetchTasks(
                    pb.FetchTasksRequest(
                        worker_id=self._worker_id,
                        max_tasks=self._max_tasks_per_poll,
                    )
                )
                for task in resp.tasks:
                    self._execute(task)
            except grpc.RpcError as e:
                log.warning("fetch failed: %s", e.code())
                self._stop.wait(self._poll_interval)
                continue
            self._stop.wait(self._poll_interval)

    def _execute(self, task_msg) -> None:
        task_id = task_msg.id
        name = task_msg.name
        try:
            payload = _json.loads(task_msg.payload) if task_msg.payload else {}
        except ValueError as e:
            self._ack(task_id, False, {"status": "error", "message": f"bad payload: {e}"})
            return

        handler = self._handlers.get(name) or self._handlers.get("default")
        if handler is None:
            self._ack(
                task_id,
                False,
                {"status": "error", "message": f"Unknown task type: {name}"},
            )
            return

        try:
            result = handler(payload)
            self._ack(task_id, True, result)
        except Exception as e:  # noqa: BLE001
            self._ack(task_id, False, {"status": "error", "message": str(e) or repr(e)})

    def _ack(self, task_id: str, success: bool, result: Any) -> None:
        try:
            self._stub.AcknowledgeTask(
                pb.AcknowledgeTaskRequest(
                    worker_id=self._worker_id,
                    task_id=task_id,
                    success=success,
                    result=_json.dumps(result) if not isinstance(result, str) else result,
                )
            )
        except grpc.RpcError as e:
            log.warning("ack failed for %s: %s", task_id, e.code())


class Builder:
    """Fluent builder for :class:`ChopFlowWorker`."""

    def __init__(self) -> None:
        self._broker = "localhost:8000"
        self._tags: List[str] = ["default"]
        self._resources: Dict[str, int] = {}
        self._heartbeat_interval = 30.0
        self._poll_interval = 2.0
        self._max_tasks_per_poll = 4

    def broker(self, broker: str) -> "Builder":
        self._broker = broker
        return self

    def tags(self, *tags: str) -> "Builder":
        # Replaces, mirroring the Java builder.
        self._tags = list(tags)
        return self

    def resources(self, name: str, amount: int) -> "Builder":
        self._resources[name] = amount
        return self

    def heartbeat_interval(self, seconds: float) -> "Builder":
        self._heartbeat_interval = seconds
        return self

    def poll_interval(self, seconds: float) -> "Builder":
        self._poll_interval = seconds
        return self

    def max_tasks_per_poll(self, n: int) -> "Builder":
        self._max_tasks_per_poll = n
        return self

    def build(self) -> ChopFlowWorker:
        resources = self._resources or {"cpu": 1}
        worker = ChopFlowWorker(
            broker=self._broker,
            tags=self._tags,
            resources=resources,
            heartbeat_interval=self._heartbeat_interval,
            poll_interval=self._poll_interval,
            max_tasks_per_poll=self._max_tasks_per_poll,
        )
        # Consume any @task-decorated handlers registered since the last build.
        if _DECORATED:
            for name, fns in _DECORATED.items():
                worker.register(name, fns[-1])
            _DECORATED.clear()
        return worker
