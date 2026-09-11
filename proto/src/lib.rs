//! Generated gRPC protobuf types for ChopFlow.
//!
//! This crate owns `proto/chopflow.proto` and compiles it once via
//! `tonic-build`. The broker, worker, CLI, and llm-worker all re-export the
//! generated [`chopflow`] module from here instead of each invoking `protoc`
//! against a path outside their own crate (which `cargo publish` rejects,
//! since the tarball is verified in isolation and external files are absent).
//!
//! Keeping the proto in its own crate also means the generated code is
//! compiled exactly once across the workspace, not once per consumer.

/// Generated protobuf module: messages, enums, and the `ChopFlowBroker` gRPC
/// service trait + server/client types.
//
// The generated gRPC methods return `Result<_, tonic::Status>` and the oneof
// enums are large, which trips `result_large_err` / `large_enum_variant`. The
// types come from tonic/prost, so we allow these two lints on the module.
#[allow(clippy::result_large_err, clippy::large_enum_variant)]
pub mod chopflow {
    tonic::include_proto!("chopflow");
}

use chopflow:: {
    ResourceAvailability as ProtoResourceAvailability, Task as ProtoTask,
    TaskStatus as ProtoTaskStatus, Worker as ProtoWorker,
};
use chopflow_core::dispatcher::Worker;
use chopflow_core::task::{Task, TaskStatus};

// Conversions between core domain types and their proto representations.
//
// These `From` impls live here (not in the broker) because of Rust's orphan
// rule: now that the generated proto types are defined in this crate rather
// than re-included per consumer, only this crate can implement traits for them.
// Both sides were previously "local" to the broker via `include_proto!`; after
// the move to a shared crate the broker sees both `Task` (core, foreign) and
// `ProtoTask` (proto, foreign) as foreign, which forbids the impl there. Here
// the proto type is local, so the impl is legal.
impl From<Task> for ProtoTask {
    fn from(task: Task) -> Self {
        ProtoTask {
            id: task.id.to_string(),
            name: task.name,
            payload: serde_json::to_string(&task.payload).unwrap_or_default(),
            tags: task.tags,
            enqueue_time: Some(prost_types::Timestamp {
                seconds: task.enqueue_time.timestamp(),
                nanos: task.enqueue_time.timestamp_subsec_nanos() as i32,
            }),
            eta: task.eta.map(|eta| prost_types::Timestamp {
                seconds: eta.timestamp(),
                nanos: eta.timestamp_subsec_nanos() as i32,
            }),
            retry_count: task.retry_count,
            max_retries: task.max_retries,
            status: task.status as i32,
            resources: task.resources,
            result: task.result.unwrap_or_default(),
            schedule_id: task.schedule_id.map(|u| u.to_string()).unwrap_or_default(),
            priority: task.priority,
        }
    }
}

impl From<ProtoTaskStatus> for TaskStatus {
    fn from(status: ProtoTaskStatus) -> Self {
        match status {
            ProtoTaskStatus::Created => TaskStatus::Created,
            ProtoTaskStatus::Queued => TaskStatus::Queued,
            ProtoTaskStatus::Running => TaskStatus::Running,
            ProtoTaskStatus::Completed => TaskStatus::Completed,
            ProtoTaskStatus::Failed => TaskStatus::Failed,
            ProtoTaskStatus::Deadlettered => TaskStatus::DeadLettered,
            ProtoTaskStatus::Cancelled => TaskStatus::Cancelled,
        }
    }
}

impl From<Worker> for ProtoWorker {
    fn from(worker: Worker) -> Self {
        ProtoWorker {
            id: worker.id.to_string(),
            address: worker.address,
            tags: worker.tags,
            resources: Some(ProtoResourceAvailability {
                available: worker.resources.available.clone(),
                total: worker.resources.total.clone(),
            }),
            assigned_tasks: worker
                .assigned_tasks
                .iter()
                .map(|id| id.to_string())
                .collect(),
            last_heartbeat: Some(prost_types::Timestamp {
                seconds: worker.last_heartbeat.timestamp(),
                nanos: worker.last_heartbeat.timestamp_subsec_nanos() as i32,
            }),
        }
    }
}
