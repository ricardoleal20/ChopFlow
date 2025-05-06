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
use crate::queue::Queue;
use crate::resources::{ResourceAvailability, ResourceRequirements};
use crate::task::{Task, TaskStatus};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{debug, info, warn};
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

/// Trait for dispatching tasks to workers
#[async_trait]
pub trait Dispatcher: Send + Sync + 'static {
    /// Register a new worker
    async fn register_worker(&mut self, worker: Worker) -> Result<()>;

    /// Update a worker's heartbeat
    async fn heartbeat(&mut self, worker_id: &Uuid) -> Result<()>;

    /// Dispatch any available tasks to workers
    async fn dispatch(&mut self) -> Result<usize>;

    /// Handle a task completion acknowledgment
    async fn ack_task(&mut self, worker_id: &Uuid, task_id: &Uuid, success: bool) -> Result<()>;

    /// Get a list of all registered workers
    async fn list_workers(&self) -> Result<Vec<Worker>>;

    /// Start the dispatcher background task
    async fn start(&mut self) -> Result<()>;
}

/// In-memory implementation of the Dispatcher trait
pub struct InMemoryDispatcher {
    /// The queue to pull tasks from
    queue: Arc<dyn Queue>,

    /// Registered workers
    workers: Arc<Mutex<HashMap<Uuid, Worker>>>,

    /// Dispatcher configuration
    config: DispatcherConfig,
}

/// Configuration for the dispatcher
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// How often to poll the queue for new tasks (in milliseconds)
    pub poll_interval_ms: u64,

    /// Maximum number of tasks to dispatch in a single poll
    pub max_batch_size: usize,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 100,
            max_batch_size: 10,
        }
    }
}

impl InMemoryDispatcher {
    /// Create a new in-memory dispatcher
    pub fn new(queue: Arc<dyn Queue>) -> Self {
        Self {
            queue,
            workers: Arc::new(Mutex::new(HashMap::new())),
            config: DispatcherConfig::default(),
        }
    }

    /// Create a new in-memory dispatcher with custom configuration
    pub fn with_config(queue: Arc<dyn Queue>, config: DispatcherConfig) -> Self {
        Self {
            queue,
            workers: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Handle task cancellation
    /// Unassigns the task from any worker and updates worker resources
    pub async fn handle_task_cancellation(&mut self, task_id: &Uuid) -> Result<()> {
        let mut workers = self.workers.lock().await;

        // Find any worker that has this task assigned
        for (worker_id, worker) in workers.iter_mut() {
            if let Some(pos) = worker.assigned_tasks.iter().position(|id| id == task_id) {
                // Remove the task from the worker's assigned tasks
                worker.assigned_tasks.remove(pos);

                // In a real implementation, we would also:
                // 1. Update the worker's available resources
                // 2. Send a cancellation notification to the worker
                // 3. Handle any cleanup required

                info!(
                    "Removed cancelled task {} from worker {}",
                    task_id, worker_id
                );
            }
        }

        Ok(())
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

    async fn dispatch(&mut self) -> Result<usize> {
        // Find available workers
        let mut workers = self.workers.lock().await;

        // Find workers that are alive
        let alive_workers: Vec<Uuid> = workers
            .iter()
            .filter(|(_, worker)| worker.is_alive())
            .map(|(id, _)| *id)
            .collect();

        if alive_workers.is_empty() {
            return Ok(0);
        }

        // Collect all tags that our workers support
        let all_worker_tags: Vec<String> = workers.values().flat_map(|w| w.tags.clone()).collect();

        // Dequeue tasks matching those tags
        let mut dispatched = 0;

        for _ in 0..self.config.max_batch_size {
            let task = match self.queue.dequeue_matching(&all_worker_tags).await? {
                Some(task) => task,
                None => break,
            };

            // Find a worker for this task
            let mut assigned = false;

            for worker_id in &alive_workers {
                let worker = workers.get_mut(worker_id).unwrap();

                if worker.can_handle(&task) {
                    // Allocate resources
                    let requirements = ResourceRequirements {
                        resources: task.resources.clone(),
                    };

                    if worker.resources.allocate(&requirements) {
                        // Assign task to worker
                        worker.assigned_tasks.push(task.id);

                        // Mark task as running
                        let mut running_task = task.clone();
                        running_task.mark_running();
                        self.queue.update(running_task).await?;

                        info!("Dispatched task {} to worker {}", task.id, worker_id);

                        assigned = true;
                        dispatched += 1;
                        break;
                    }
                }
            }

            if !assigned {
                // No suitable worker found, put task back in queue
                warn!("No suitable worker found for task {}, requeueing", task.id);
                self.queue.enqueue(task).await?;
            }
        }

        Ok(dispatched)
    }

    async fn ack_task(&mut self, worker_id: &Uuid, task_id: &Uuid, success: bool) -> Result<()> {
        let mut workers = self.workers.lock().await;

        let worker = workers.get_mut(worker_id).ok_or_else(|| {
            ChopFlowError::DispatcherError(format!("Worker not found: {}", worker_id))
        })?;

        // Find and remove the task from assigned tasks
        let task_idx = worker
            .assigned_tasks
            .iter()
            .position(|id| id == task_id)
            .ok_or_else(|| {
                ChopFlowError::DispatcherError(format!(
                    "Task {} not assigned to worker {}",
                    task_id, worker_id
                ))
            })?;

        worker.assigned_tasks.remove(task_idx);

        // Get the task from the queue
        let mut task = match self.queue.get(task_id).await? {
            Some(task) => task,
            None => return Err(ChopFlowError::TaskNotFound(task_id.to_string())),
        };

        // Release resources on the worker
        let requirements = ResourceRequirements {
            resources: task.resources.clone(),
        };
        worker.resources.release(&requirements);

        // Update task status
        if success {
            task.mark_completed();
            info!("Task {} completed successfully", task_id);
        } else {
            task.mark_failed();

            if task.status == TaskStatus::Failed {
                // Task should be retried, enqueue it again
                info!(
                    "Task {} failed, will retry (attempt {}/{})",
                    task_id, task.retry_count, task.max_retries
                );
                self.queue.enqueue(task.clone()).await?;
            } else {
                // Task moved to dead letter queue
                warn!(
                    "Task {} failed permanently after {} retries",
                    task_id,
                    task.retry_count - 1
                );
            }
        }

        // Update the task in the queue
        self.queue.update(task).await?;

        Ok(())
    }

    async fn list_workers(&self) -> Result<Vec<Worker>> {
        let workers = self.workers.lock().await;
        Ok(workers.values().cloned().collect())
    }

    async fn start(&mut self) -> Result<()> {
        let queue = self.queue.clone();
        let workers = self.workers.clone();
        let config = self.config.clone();

        // Start a background task for polling the queue and dispatching tasks
        tokio::spawn(async move {
            loop {
                // Remove dead workers
                {
                    let mut workers_lock = match workers.lock().await {
                        lock => lock,
                    };

                    let dead_workers: Vec<Uuid> = workers_lock
                        .iter()
                        .filter(|(_, worker)| !worker.is_alive())
                        .map(|(id, _)| *id)
                        .collect();

                    for worker_id in dead_workers {
                        workers_lock.remove(&worker_id);
                        info!("Removed dead worker {}", worker_id);
                    }
                }

                // Create a temporary dispatcher to use for this iteration
                let mut dispatcher = InMemoryDispatcher {
                    queue: queue.clone(),
                    workers: workers.clone(),
                    config: config.clone(),
                };

                if let Err(e) = dispatcher.dispatch().await {
                    warn!("Error dispatching tasks: {}", e);
                }

                // Sleep before next poll
                time::sleep(time::Duration::from_millis(config.poll_interval_ms)).await;
            }
        });

        Ok(())
    }
}
