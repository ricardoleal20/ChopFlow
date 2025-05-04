/*!
# ChopFlow Core

Core library for ChopFlow, a distributed task queue system written in Rust.

This library provides the fundamental components for building distributed task queues:

- Task definitions with metadata like tags and ETA
- Queue implementations for managing task storage and retrieval
- Dispatcher for routing tasks to appropriate workers
- Resource tracking for CPU, GPU and memory allocation
- Retry policies with configurable backoff strategies
*/

pub mod dispatcher;
pub mod error;
pub mod queue;
pub mod resources;
pub mod retry;
pub mod task;

pub use error::Result;

pub use dispatcher::Dispatcher;
pub use queue::Queue;
pub use resources::ResourceRequirements;
pub use retry::RetryPolicy;
/// Re-export core types for convenience
pub use task::Task;
