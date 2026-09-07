"""Pythonic dataclasses wrapping the ChopFlow proto messages."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from . import _generated  # noqa: F401  (ensures package import works)
from ._generated import chopflow_pb2 as pb
from google.protobuf.timestamp_pb2 import Timestamp

# Re-export the proto enums for ergonomics.
TaskStatus = pb.TaskStatus
OverlapPolicy = pb.OverlapPolicy

#: The four terminal task statuses.
TERMINAL_STATUSES = frozenset(
    {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.DEADLETTERED,
        TaskStatus.CANCELLED,
    }
)


def _ts_to_dt(ts) -> Optional[datetime]:
    if ts is None or (ts.seconds == 0 and ts.nanos == 0):
        return None
    return datetime.fromtimestamp(ts.seconds + ts.nanos / 1e9, tz=timezone.utc)


def _dt_to_ts(dt: datetime) -> Timestamp:
    ts = Timestamp()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts.FromDatetime(dt)
    return ts


@dataclass(frozen=True)
class Task:
    """A ChopFlow task, in whatever status it currently holds."""

    id: str
    name: str
    status: int
    tags: List[str] = field(default_factory=list)
    payload: object = None
    retry_count: int = 0
    max_retries: int = 3
    result: object = None
    resources: Dict[str, int] = field(default_factory=dict)
    eta: Optional[datetime] = None
    enqueue_time: Optional[datetime] = None
    schedule_id: str = ""
    priority: int = 0

    @property
    def status_name(self) -> str:
        return pb.TaskStatus.Name(self.status)

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def is_completed(self) -> bool:
        return self.status == TaskStatus.COMPLETED

    @classmethod
    def from_proto(cls, msg) -> "Task":
        import json as _json

        payload = None
        if msg.payload:
            try:
                payload = _json.loads(msg.payload)
            except (ValueError, TypeError):
                payload = msg.payload
        result = None
        if msg.result:
            try:
                result = _json.loads(msg.result)
            except (ValueError, TypeError):
                result = msg.result
        return cls(
            id=msg.id,
            name=msg.name,
            status=msg.status,
            tags=list(msg.tags),
            payload=payload,
            retry_count=msg.retry_count,
            max_retries=msg.max_retries,
            result=result,
            resources=dict(msg.resources),
            eta=_ts_to_dt(msg.eta),
            enqueue_time=_ts_to_dt(msg.enqueue_time),
            schedule_id=msg.schedule_id,
            priority=msg.priority,
        )


@dataclass(frozen=True)
class QueueStats:
    queue_length: int
    tasks_processing: int
    tasks_completed: int
    tasks_failed: int
    active_workers: int

    @classmethod
    def from_proto(cls, msg) -> "QueueStats":
        return cls(
            queue_length=msg.queue_length,
            tasks_processing=msg.tasks_processing,
            tasks_completed=msg.tasks_completed,
            tasks_failed=msg.tasks_failed,
            active_workers=msg.active_workers,
        )


@dataclass(frozen=True)
class Worker:
    id: str
    address: str
    tags: List[str]
    available: Dict[str, int]
    total: Dict[str, int]
    assigned_tasks: List[str]
    last_heartbeat: Optional[datetime]

    @classmethod
    def from_proto(cls, msg) -> "Worker":
        res = msg.resources
        return cls(
            id=msg.id,
            address=msg.address,
            tags=list(msg.tags),
            available=dict(res.available) if res else {},
            total=dict(res.total) if res else {},
            assigned_tasks=list(msg.assigned_tasks),
            last_heartbeat=_ts_to_dt(msg.last_heartbeat),
        )


@dataclass(frozen=True)
class Schedule:
    id: str
    name: str
    task_name: str
    payload: object
    tags: List[str]
    resources: Dict[str, int]
    max_retries: int
    priority: int
    cron: Optional[str]
    eta: Optional[datetime]
    overlap_policy: int
    enabled: bool
    last_fired: Optional[datetime]
    next_fire: Optional[datetime]
    created_at: Optional[datetime]

    @property
    def overlap_policy_name(self) -> str:
        return pb.OverlapPolicy.Name(self.overlap_policy)

    @classmethod
    def from_proto(cls, msg) -> "Schedule":
        import json as _json

        tt = msg.task_template
        payload = None
        if tt and tt.payload:
            try:
                payload = _json.loads(tt.payload)
            except (ValueError, TypeError):
                payload = tt.payload
        cron = None
        eta = None
        if msg.kind and msg.kind.WhichOneof("kind") == "cron":
            cron = msg.kind.cron
        elif msg.kind and msg.kind.WhichOneof("kind") == "eta":
            eta = _ts_to_dt(msg.kind.eta)
        return cls(
            id=msg.id,
            name=msg.name,
            task_name=tt.name if tt else "",
            payload=payload,
            tags=list(tt.tags) if tt else [],
            resources=dict(tt.resources) if tt else {},
            max_retries=tt.max_retries if tt else 3,
            priority=tt.priority if tt else 0,
            cron=cron,
            eta=eta,
            overlap_policy=msg.overlap_policy,
            enabled=msg.enabled,
            last_fired=_ts_to_dt(msg.last_fired),
            next_fire=_ts_to_dt(msg.next_fire),
            created_at=_ts_to_dt(msg.created_at),
        )
