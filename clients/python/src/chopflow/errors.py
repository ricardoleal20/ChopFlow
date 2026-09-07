"""Errors raised by the ChopFlow SDK."""


class ChopFlowError(Exception):
    """Base class for all SDK errors."""


class TaskTimeoutError(ChopFlowError):
    """Raised when :meth:`AsyncResult.get` does not reach a terminal status
    before its timeout."""


class TaskFailedError(ChopFlowError):
    """Raised when :meth:`AsyncResult.get` resolves a task that did not complete
    successfully (FAILED / DEADLETTERED / CANCELLED). The final :class:`Task`
    is available as ``error.task``."""
