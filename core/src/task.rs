/*!
# Task Module

This module defines the core task abstraction for ChopFlow.

Tasks are the fundamental work units in ChopFlow. Each task has:
- A unique identifier
- A name/type for routing
- Serialized payload data (task arguments)
- Optional tags for task routing and filtering
- Timing information (enqueue time, ETA)
- Retry tracking
- Resource requirements

This module also implements task status tracking and lifecycle management through
the `TaskStatus` enum.
*/

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Status of a task in the system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Task has been created but not yet enqueued
    Created,
    /// Task is waiting in the queue
    Queued,
    /// Task has been assigned to a worker and is being processed
    Running,
    /// Task completed successfully
    Completed,
    /// Task failed but may be retried
    Failed,
    /// Task failed permanently and was moved to the dead-letter queue
    DeadLettered,
}

/// A task to be executed by a worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique identifier for the task
    pub id: Uuid,

    /// Task name/type, used for routing
    pub name: String,

    /// Serialized payload containing the task arguments
    pub payload: serde_json::Value,

    /// Tags for task routing and filtering
    pub tags: Vec<String>,

    /// When the task was created/enqueued
    pub enqueue_time: DateTime<Utc>,

    /// Earliest time at which the task should be executed
    pub eta: Option<DateTime<Utc>>,

    /// Number of times this task has been retried
    pub retry_count: u32,

    /// Maximum number of retries before moving to dead-letter queue
    pub max_retries: u32,

    /// Current status of the task
    pub status: TaskStatus,

    /// Resources required by this task (CPU, GPU, etc.)
    pub resources: HashMap<String, u32>,
}

impl Task {
    /// Create a new task with default values
    pub fn new(name: String, payload: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            payload,
            tags: Vec::new(),
            enqueue_time: Utc::now(),
            eta: None,
            retry_count: 0,
            max_retries: 3, // Default to 3 retries
            status: TaskStatus::Created,
            resources: HashMap::new(),
        }
    }

    /// Add a tag to the task
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add multiple tags to the task
    pub fn with_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags.extend(tags.into_iter().map(Into::into));
        self
    }

    /// Set the ETA (earliest execution time)
    pub fn with_eta(mut self, eta: DateTime<Utc>) -> Self {
        self.eta = Some(eta);
        self
    }

    /// Set maximum number of retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Add a resource requirement
    pub fn with_resource(mut self, resource: impl Into<String>, amount: u32) -> Self {
        self.resources.insert(resource.into(), amount);
        self
    }

    /// Check if the task is ready to be executed (based on ETA)
    pub fn is_ready(&self) -> bool {
        match self.eta {
            Some(eta) => Utc::now() >= eta,
            None => true,
        }
    }

    /// Mark the task as running
    pub fn mark_running(&mut self) {
        self.status = TaskStatus::Running;
    }

    /// Mark the task as completed
    pub fn mark_completed(&mut self) {
        self.status = TaskStatus::Completed;
    }

    /// Mark the task as failed and increment retry count
    pub fn mark_failed(&mut self) {
        self.retry_count += 1; // Upgrade the count to 1

        // If the retry count exceeds the max retries, mark the task as dead-lettered
        if self.retry_count > self.max_retries {
            self.status = TaskStatus::DeadLettered;
        } else {
            self.status = TaskStatus::Failed;
        }
    }
}
