"""The ChopFlow producer client.

Mirrors the Java SDK's surface: :meth:`ChopFlowClient.connect`, a fluent
:meth:`enqueue` builder returning an :class:`AsyncResult` that polls to a
terminal state, plus task/queue/worker/schedule management helpers.
"""

from __future__ import annotations

import json as _json
import time
from datetime import datetime
from typing import Dict, List, Optional

import grpc
from google.protobuf.empty_pb2 import Empty

from ._generated import chopflow_pb2 as pb
from ._generated import chopflow_pb2_grpc as pb_grpc
from .errors import ChopFlowError, TaskFailedError, TaskTimeoutError
from .models import (
    QueueStats,
    Schedule,
    Task,
    Worker,
    _dt_to_ts,
)

#: Poll interval for :meth:`AsyncResult.get`, in seconds.
POLL_INTERVAL_S = 0.5


def _strip_scheme(target: str) -> str:
    for scheme in ("https://", "http://"):
        if target.startswith(scheme):
            return target[len(scheme) :]
    return target


class ChopFlowClient:
    """Blocking gRPC client for the ChopFlow broker.

    Use :meth:`connect` (a context manager) to create one::

        with ChopFlowClient.connect("localhost:8000") as client:
            ...
    """

    def __init__(self, channel: grpc.Channel):
        self._channel = channel
        self._stub = pb_grpc.ChopFlowBrokerStub(channel)

    # ------------------------------------------------------------------
    # construction / lifecycle
    # ------------------------------------------------------------------
    @classmethod
    def connect(
        cls, target: str, *, timeout: Optional[float] = None
    ) -> "ChopFlowClient":
        """Open a plaintext gRPC channel to ``target`` (``host:port`` or
        ``http://host:port``)."""
        addr = _strip_scheme(target)
        channel = grpc.insecure_channel(addr)
        if timeout is not None:
            try:
                grpc.channel_ready_future(channel).result(timeout=timeout)
            except grpc.FutureTimeoutError as e:
                raise ChopFlowError(
                    f"broker at {addr} not reachable within {timeout}s"
                ) from e
        return cls(channel)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> "ChopFlowClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------
    # enqueue
    # ------------------------------------------------------------------
    def enqueue(self, name: str) -> "EnqueueBuilder":
        """Begin building a task to enqueue. Returns a fluent builder."""
        return EnqueueBuilder(self, name)

    def _enqueue(self, request: "pb.EnqueueTaskRequest") -> str:
        try:
            resp = self._stub.EnqueueTask(request)
        except grpc.RpcError as e:
            raise ChopFlowError(f"EnqueueTask failed: {e.details() or e.code()}") from e
        return resp.task_id

    # ------------------------------------------------------------------
    # task / queue inspection
    # ------------------------------------------------------------------
    def get_task(self, task_id: str) -> Optional[Task]:
        """Look up a task by id. Returns ``None`` if the task does not exist
        (the broker signals this with ``NOT_FOUND`` or an empty response)."""
        try:
            resp = self._stub.GetTaskStatus(pb.GetTaskStatusRequest(task_id=task_id))
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise ChopFlowError(
                f"GetTaskStatus failed: {e.details() or e.code()}"
            ) from e
        if not resp.HasField("task"):
            return None
        return Task.from_proto(resp.task)

    def list_tasks(
        self,
        limit: int = 50,
        offset: int = 0,
        statuses: Optional[List[int]] = None,
    ) -> List[Task]:
        req = pb.ListTasksRequest(
            limit=limit, offset=offset, filter_status=statuses or []
        )
        try:
            resp = self._stub.ListTasks(req)
        except grpc.RpcError as e:
            raise ChopFlowError(f"ListTasks failed: {e.details() or e.code()}") from e
        return [Task.from_proto(t) for t in resp.tasks]

    def cancel(self, task_id: str) -> bool:
        try:
            resp = self._stub.CancelTask(pb.CancelTaskRequest(task_id=task_id))
        except grpc.RpcError as e:
            raise ChopFlowError(f"CancelTask failed: {e.details() or e.code()}") from e
        return resp.success

    def get_stats(self) -> QueueStats:
        try:
            resp = self._stub.GetQueueStats(pb.GetQueueStatsRequest())
        except grpc.RpcError as e:
            raise ChopFlowError(
                f"GetQueueStats failed: {e.details() or e.code()}"
            ) from e
        return QueueStats.from_proto(resp)

    def list_workers(self) -> List[Worker]:
        try:
            resp = self._stub.ListWorkers(Empty())
        except grpc.RpcError as e:
            raise ChopFlowError(f"ListWorkers failed: {e.details() or e.code()}") from e
        return [Worker.from_proto(w) for w in resp.workers]

    # ------------------------------------------------------------------
    # schedules
    # ------------------------------------------------------------------
    def create_schedule(self, schedule: "pb.Schedule") -> str:
        try:
            resp = self._stub.CreateSchedule(
                pb.CreateScheduleRequest(schedule=schedule)
            )
        except grpc.RpcError as e:
            raise ChopFlowError(
                f"CreateSchedule failed: {e.details() or e.code()}"
            ) from e
        return resp.schedule_id

    def list_schedules(self) -> List[Schedule]:
        try:
            resp = self._stub.ListSchedules(pb.ListSchedulesRequest())
        except grpc.RpcError as e:
            raise ChopFlowError(
                f"ListSchedules failed: {e.details() or e.code()}"
            ) from e
        return [Schedule.from_proto(s) for s in resp.schedules]

    def delete_schedule(self, schedule_id: str) -> bool:
        try:
            resp = self._stub.DeleteSchedule(pb.DeleteScheduleRequest(id=schedule_id))
        except grpc.RpcError as e:
            raise ChopFlowError(
                f"DeleteSchedule failed: {e.details() or e.code()}"
            ) from e
        return resp.success

    def create_cron_schedule(
        self,
        name: str,
        task_name: str,
        cron: str,
        *,
        payload: object = None,
        tags: Optional[List[str]] = None,
        resources: Optional[Dict[str, int]] = None,
        max_retries: int = 3,
        overlap: int = pb.OverlapPolicy.OVERLAP_SKIP,
        priority: int = 0,
        enabled: bool = True,
    ) -> str:
        """Convenience builder for a cron schedule. Returns the new schedule id."""
        return self.create_schedule(
            _build_schedule(
                name=name,
                task_name=task_name,
                payload=payload,
                tags=tags or [],
                resources=resources or {},
                max_retries=max_retries,
                overlap=overlap,
                priority=priority,
                enabled=enabled,
                cron=cron,
                eta=None,
            )
        )

    def create_oneshot_schedule(
        self,
        name: str,
        task_name: str,
        eta: datetime,
        *,
        payload: object = None,
        tags: Optional[List[str]] = None,
        resources: Optional[Dict[str, int]] = None,
        max_retries: int = 3,
        overlap: int = pb.OverlapPolicy.OVERLAP_SKIP,
        priority: int = 0,
        enabled: bool = True,
    ) -> str:
        """Convenience builder for a one-shot schedule. Returns the new schedule id."""
        return self.create_schedule(
            _build_schedule(
                name=name,
                task_name=task_name,
                payload=payload,
                tags=tags or [],
                resources=resources or {},
                max_retries=max_retries,
                overlap=overlap,
                priority=priority,
                enabled=enabled,
                cron=None,
                eta=eta,
            )
        )


# ----------------------------------------------------------------------
# enqueue builder + async result
# ----------------------------------------------------------------------
class EnqueueBuilder:
    """Fluent builder for enqueueing a task."""

    def __init__(self, client: ChopFlowClient, name: str):
        self._client = client
        self._name = name
        self._payload: object = None
        self._tags: List[str] = []
        self._resources: Dict[str, int] = {}
        self._max_retries: int = 0
        self._priority: int = 0
        self._eta: Optional[datetime] = None

    def payload(self, value: object) -> "EnqueueBuilder":
        """Set the payload. A ``str`` is taken as raw JSON; anything else is
        JSON-serialized. Defaults to ``{}``."""
        self._payload = value
        return self

    def tags(self, *tags: str) -> "EnqueueBuilder":
        self._tags.extend(tags)
        return self

    def resources(self, name: str, amount: int) -> "EnqueueBuilder":
        self._resources[name] = amount
        return self

    def max_retries(self, n: int) -> "EnqueueBuilder":
        self._max_retries = n
        return self

    def priority(self, p: int) -> "EnqueueBuilder":
        """Dispatch priority (higher = claimed before lower). Defaults to ``0``,
        which preserves FIFO ordering within a priority tier."""
        self._priority = p
        return self

    def eta(self, when: datetime) -> "EnqueueBuilder":
        self._eta = when
        return self

    def enqueue(self) -> "AsyncResult":
        payload_str = _serialize_payload(self._payload)
        req = pb.EnqueueTaskRequest(
            name=self._name,
            payload=payload_str,
            tags=self._tags,
            resources=self._resources,
            max_retries=self._max_retries,
            priority=self._priority,
        )
        if self._eta is not None:
            req.eta.CopyFrom(_dt_to_ts(self._eta))
        task_id = self._client._enqueue(req)
        return AsyncResult(self._client, task_id)


class AsyncResult:
    """A polling handle on an enqueued task.

    :meth:`get` polls :meth:`ChopFlowClient.get_task` until the task reaches a
    terminal status (COMPLETED, FAILED, DEADLETTERED, CANCELLED), then returns
    the final :class:`Task`.
    """

    def __init__(self, client: ChopFlowClient, task_id: str):
        self._client = client
        self.id = task_id

    def get(
        self,
        timeout: Optional[float] = None,
        *,
        poll_interval: float = POLL_INTERVAL_S,
        raise_on_failure: bool = True,
    ) -> Task:
        """Block until the task reaches a terminal status.

        If ``timeout`` is given and elapses, raises :class:`TaskTimeoutError`.
        If ``raise_on_failure`` is true (default) and the task did not COMPLETE,
        raises :class:`TaskFailedError` with the final task attached.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            task = self._client.get_task(self.id)
            if task is None:
                raise ChopFlowError(f"task {self.id} not found")
            if task.is_terminal:
                if raise_on_failure and not task.is_completed:
                    err = TaskFailedError(
                        f"task {self.id} ended in status {task.status_name}"
                    )
                    err.task = task  # type: ignore[attr-defined]
                    raise err
                return task
            if deadline is not None and time.monotonic() >= deadline:
                raise TaskTimeoutError(
                    f"task {self.id} did not reach a terminal status within {timeout}s"
                )
            time.sleep(poll_interval)


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def _serialize_payload(value: object) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        # Validate it's JSON; if not, treat as a raw string payload.
        try:
            _json.loads(value)
            return value
        except ValueError:
            return _json.dumps(value)
    return _json.dumps(value)


def _build_schedule(
    *,
    name: str,
    task_name: str,
    payload: object,
    tags: List[str],
    resources: Dict[str, int],
    max_retries: int,
    overlap: int,
    priority: int,
    enabled: bool,
    cron: Optional[str],
    eta: Optional[datetime],
) -> "pb.Schedule":
    if (cron is None) == (eta is None):
        raise ValueError("exactly one of cron or eta must be set")
    kind = pb.ScheduleKind()
    if cron is not None:
        kind.cron = cron
    else:
        kind.eta.CopyFrom(_dt_to_ts(eta))  # type: ignore[arg-type]
    return pb.Schedule(
        id="",
        name=name,
        task_template=pb.TaskTemplate(
            name=task_name,
            payload=_serialize_payload(payload),
            tags=tags,
            resources=resources,
            max_retries=max_retries,
            priority=priority,
        ),
        kind=kind,
        overlap_policy=overlap,
        enabled=enabled,
    )
