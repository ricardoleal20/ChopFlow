/*!
# Dispatcher Module

This module defines the task dispatcher for ChopFlow.

The dispatcher is responsible for:
- Managing worker registration and heartbeats
- Matching tasks from the queue to appropriate workers
- Tracking worker resources and capabilities
- Handling task acknowledgments and retries
- Monitoring worker health

Key components include:
- The `Worker` struct that represents a task execution node
- The `Dispatcher` trait defining the interface for all dispatcher implementations
- An in-memory implementation (`InMemoryDispatcher`) for local task routing
- Worker health monitoring through heartbeats
- Resource-aware task scheduling

The dispatcher acts as the central coordination point in ChopFlow,
ensuring tasks are routed to workers with the right capabilities and resources.
*/

use crate::error::{ChopFlowError, Result};
use crate::resources::{ResourceAvailability, ResourceRequirements};
use crate::task::Task;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};
use uuid::Uuid;

/// A worker that can execute tasks
#[derive(Debug, Clone)]
pub struct Worker {
    /// Unique identifier for the worker
    pub id: Uuid,

    /// Worker hostname or address
    pub address: String,

    /// Tags that this worker can process
    pub tags: Vec<String>,

    /// Available resources on this worker
    pub resources: ResourceAvailability,

    /// Current assigned tasks
    pub assigned_tasks: Vec<Uuid>,

    /// Last heartbeat time from this worker
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
}

impl Worker {
    /// Create a new worker
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            address: address.into(),
            tags: Vec::new(),
            resources: ResourceAvailability::new(),
            assigned_tasks: Vec::new(),
            last_heartbeat: chrono::Utc::now(),
        }
    }

    /// Add a tag that this worker can process
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add multiple tags that this worker can process
    pub fn with_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags.extend(tags.into_iter().map(Into::into));
        self
    }

    /// Add a resource that this worker has
    pub fn with_resource(mut self, resource: impl Into<String>, amount: u32) -> Self {
        self.resources.add_resource(resource, amount);
        self
    }

    /// Check if worker can handle a task based on tags and resources
    pub fn can_handle(&self, task: &Task) -> bool {
        // Check if worker has any of the required tags
        let has_matching_tag = if task.tags.is_empty() {
            true
        } else {
            task.tags.iter().any(|tag| self.tags.contains(tag))
        };

        if !has_matching_tag {
            return false;
        }

        // Check if worker has sufficient resources
        let requirements = ResourceRequirements {
            resources: task.resources.clone(),
        };

        requirements.can_be_satisfied_by(&self.resources)
    }

    /// Update worker heartbeat
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = chrono::Utc::now();
    }

    /// Check if worker is considered alive (recent heartbeat)
    pub fn is_alive(&self) -> bool {
        let now = chrono::Utc::now();
        let heartbeat_age = now - self.last_heartbeat;
        heartbeat_age < chrono::Duration::seconds(30)
    }
}

/// Trait for the worker registry and resource allocator.
///
/// Under ChopFlow's pull model the broker drives task delivery via the
/// `FetchTasks` RPC; the dispatcher no longer pulls from a queue itself. Its
/// only responsibilities are tracking registered workers (and their liveness
/// via heartbeats) and allocating/releasing task resources on those workers.
#[async_trait]
pub trait Dispatcher: Send + Sync + 'static {
    /// Register a new worker
    async fn register_worker(&mut self, worker: Worker) -> Result<()>;

    /// Update a worker's heartbeat
    async fn heartbeat(&mut self, worker_id: &Uuid) -> Result<()>;

    /// Get a list of all registered workers
    async fn list_workers(&self) -> Result<Vec<Worker>>;

    /// Look up a single worker by id.
    async fn get_worker(&self, worker_id: &Uuid) -> Result<Option<Worker>>;

    /// Assign a task to a worker: allocate its resources on the worker and
    /// record it in the worker's `assigned_tasks`. Returns
    /// [`ChopFlowError::DispatcherError`] if the worker is unknown or cannot
    /// satisfy the task's resource requirements. Does **not** mutate task
    /// state — the caller (broker) owns task lifecycle via its storage.
    async fn assign_task(&mut self, worker_id: &Uuid, task: &Task) -> Result<()>;

    /// Release a task's resources from a worker and drop it from the
    /// worker's `assigned_tasks`. Idempotent: a missing assignment is not an
    /// error (useful for cancellation / crash recovery).
    async fn release_task(&mut self, worker_id: &Uuid, task: &Task) -> Result<()>;
}

/// In-memory implementation of the Dispatcher trait
pub struct InMemoryDispatcher {
    /// Registered workers
    workers: Arc<Mutex<HashMap<Uuid, Worker>>>,
}

impl InMemoryDispatcher {
    /// Create a new in-memory dispatcher
    pub fn new() -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Dispatcher for InMemoryDispatcher {
    async fn register_worker(&mut self, worker: Worker) -> Result<()> {
        let mut workers = self.workers.lock().await;

        let worker_id = worker.id;
        workers.insert(worker_id, worker);

        info!("Registered worker {}", worker_id);
        Ok(())
    }

    async fn heartbeat(&mut self, worker_id: &Uuid) -> Result<()> {
        let mut workers = self.workers.lock().await;

        if let Some(worker) = workers.get_mut(worker_id) {
            worker.heartbeat();
            debug!("Received heartbeat from worker {}", worker_id);
            Ok(())
        } else {
            Err(ChopFlowError::DispatcherError(format!(
                "Worker not found: {}",
                worker_id
            )))
        }
    }

    async fn list_workers(&self) -> Result<Vec<Worker>> {
        let workers = self.workers.lock().await;
        Ok(workers.values().cloned().collect())
    }

    async fn get_worker(&self, worker_id: &Uuid) -> Result<Option<Worker>> {
        let workers = self.workers.lock().await;
        Ok(workers.get(worker_id).cloned())
    }

    async fn assign_task(&mut self, worker_id: &Uuid, task: &Task) -> Result<()> {
        let mut workers = self.workers.lock().await;

        let worker = workers.get_mut(worker_id).ok_or_else(|| {
            ChopFlowError::DispatcherError(format!("Worker not found: {}", worker_id))
        })?;

        // Allocate the task's resources on the worker. `allocate` returns
        // false if the requirements cannot be satisfied, so translate that
        // into a dispatcher error rather than silently dropping the task.
        let requirements = ResourceRequirements {
            resources: task.resources.clone(),
        };
        if !worker.resources.allocate(&requirements) {
            return Err(ChopFlowError::DispatcherError(format!(
                "Worker {} cannot satisfy resources {:?} for task {}",
                worker_id, task.resources, task.id
            )));
        }

        worker.assigned_tasks.push(task.id);
        debug!("Assigned task {} to worker {}", task.id, worker_id);
        Ok(())
    }

    async fn release_task(&mut self, worker_id: &Uuid, task: &Task) -> Result<()> {
        let mut workers = self.workers.lock().await;

        let Some(worker) = workers.get_mut(worker_id) else {
            // Worker gone (crashed / evicted) — nothing to release.
            return Ok(());
        };

        // Release the resources the task had reserved.
        let requirements = ResourceRequirements {
            resources: task.resources.clone(),
        };
        worker.resources.release(&requirements);

        // Drop the task from the worker's assigned list if present.
        if let Some(pos) = worker.assigned_tasks.iter().position(|id| id == &task.id) {
            worker.assigned_tasks.remove(pos);
        }

        debug!("Released task {} from worker {}", task.id, worker_id);
        Ok(())
    }
}
