/*!
# Checkpoint Module

This module defines the checkpoint abstraction for ChopFlow pipelines.

A task may declare a list of pipeline stages (see `Task::stages`). As a
context-aware handler completes each stage it records a [`Checkpoint`] —
the stage's intermediate output persisted via `Storage::save_checkpoint`
(an upsert keyed by `(task_id, stage)`). On a retry the worker re-fetches
the task's checkpoints and the handler resumes from the last completed
stage instead of restarting, giving Temporal-style durable execution
without a workflow engine.
*/

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A recorded completion of one pipeline stage of a task.
///
/// Upserted per `(task_id, stage)`: saving the same stage again overwrites
/// `payload` and `recorded_at`. Listed per task ordered by `recorded_at`
/// ascending (see `Storage::checkpoints`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The task this checkpoint belongs to
    pub task_id: Uuid,

    /// Name of the completed pipeline stage (e.g. `"chunk"`, `"embed"`)
    pub stage: String,

    /// Serialized stage output (the state to resume from)
    pub payload: String,

    /// When this checkpoint was recorded
    pub recorded_at: DateTime<Utc>,
}
