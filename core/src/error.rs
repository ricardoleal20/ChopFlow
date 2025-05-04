/*!
# Error Module

This module defines the error handling infrastructure for ChopFlow.

A robust error system is essential for:
- Providing clear, actionable error messages
- Distinguishing between different error types
- Enabling proper error propagation
- Supporting recovery from failures
- Facilitating debugging

The module includes:
- The `ChopFlowError` enum that categorizes different error types
- A `Result` type alias for convenience
- Specialized error variants for each subsystem (queue, dispatcher, etc.)
- Integration with the standard error handling traits

This consistent error handling approach ensures that failures in ChopFlow
can be properly identified, reported, and potentially recovered from.
*/

use thiserror::Error;

/// Result type alias for ChopFlow operations
pub type Result<T> = std::result::Result<T, ChopFlowError>;

/// Errors that can occur during ChopFlow operations
#[derive(Debug, Error)]
pub enum ChopFlowError {
    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("Queue error: {0}")]
    QueueError(String),

    #[error("Dispatcher error: {0}")]
    DispatcherError(String),

    #[error("Worker error: {0}")]
    WorkerError(String),

    #[error("Resource error: {0}")]
    ResourceError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
