"""ChopFlow Python SDK.

A producer client and worker SDK for ChopFlow, a durable distributed task
queue with a Rust core. Speaks gRPC to the broker.

Quickstart::

    from chopflow import ChopFlowClient

    with ChopFlowClient.connect("localhost:8000") as client:
        result = client.enqueue("echo").payload({"hello": "world"}).tags("default").enqueue()
        task = result.get(timeout=30)
        print(task.status, task.result)
"""

from .client import AsyncResult, ChopFlowClient, QueueStats, Task
from .errors import ChopFlowError
from .worker import ChopFlowWorker, task

__all__ = [
    "AsyncResult",
    "ChopFlowClient",
    "ChopFlowError",
    "ChopFlowWorker",
    "QueueStats",
    "Task",
    "task",
]

__version__ = "0.1.0"
