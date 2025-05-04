/*!
# Queue Module

This module defines the queue abstraction for ChopFlow.

Queues are responsible for:
- Storing tasks waiting to be processed
- Providing efficient retrieval of tasks based on readiness and tags
- Managing task lifecycle states
- Supporting concurrency and thread-safety

The module includes:
- The `Queue` trait that defines the interface for all queue implementations
- An in-memory implementation (`InMemoryQueue`) using a `VecDeque` with `Mutex`
- Support for ETA-based scheduling of tasks
- Tag-based task routing capabilities

Future implementations could include persistent queues backed by databases
or specialized data structures for optimized scheduling.
*/

use crate::error::{ChopFlowError, Result};
use crate::task::{Task, TaskStatus};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use uuid::Uuid;

/// Queue trait defining the interface for task queues
#[async_trait]
pub trait Queue: Send + Sync + 'static {
    /// Enqueue a task
    async fn enqueue(&self, task: Task) -> Result<()>;

    /// Dequeue a task that's ready for execution
    async fn dequeue(&self) -> Result<Option<Task>>;

    /// Dequeue a task matching the given tags
    async fn dequeue_matching(&self, tags: &[String]) -> Result<Option<Task>>;

    /// Get a task by ID
    async fn get(&self, id: &Uuid) -> Result<Option<Task>>;

    /// Update task status
    async fn update(&self, task: Task) -> Result<()>;

    /// Get queue length
    async fn len(&self) -> Result<usize>;

    /// Check if queue is empty
    async fn is_empty(&self) -> Result<bool>;
}

/// In-memory implementation of the Queue trait using a VecDeque with Mutex
pub struct InMemoryQueue {
    queue: Arc<Mutex<VecDeque<Task>>>,
    // Separate map for faster lookups by ID
    tasks_by_id: Arc<Mutex<std::collections::HashMap<Uuid, usize>>>,
}

impl InMemoryQueue {
    /// Create a new in-memory queue
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            tasks_by_id: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait]
impl Queue for InMemoryQueue {
    async fn enqueue(&self, mut task: Task) -> Result<()> {
        task.status = TaskStatus::Queued;

        let mut queue = self.queue.lock().await;
        let mut tasks_by_id = self.tasks_by_id.lock().await;

        let position = queue.len();
        queue.push_back(task.clone());
        tasks_by_id.insert(task.id, position);

        Ok(())
    }

    async fn dequeue(&self) -> Result<Option<Task>> {
        // Poll in a loop with backoff until we find a ready task
        let mut backoff = Duration::from_millis(10);
        const MAX_BACKOFF: Duration = Duration::from_secs(1);

        loop {
            // We need to scope the locks to drop them before awaiting
            let ready_task = {
                let mut queue = self.queue.lock().await;

                if queue.is_empty() {
                    None
                } else {
                    // Find the first task that's ready to execute
                    let mut ready_index = None;
                    for (i, task) in queue.iter().enumerate() {
                        if task.is_ready() {
                            ready_index = Some(i);
                            break;
                        }
                    }

                    if let Some(i) = ready_index {
                        // Remove the task from the queue
                        let task = queue.remove(i).ok_or_else(|| {
                            ChopFlowError::QueueError("Failed to remove task from queue".into())
                        })?;

                        Some((task, i))
                    } else {
                        None
                    }
                }
            };

            if let Some((task, idx)) = ready_task {
                // Now update the tasks_by_id map
                let mut tasks_by_id = self.tasks_by_id.lock().await;
                tasks_by_id.remove(&task.id);

                // Update positions for all tasks after the removed one
                for (_, pos) in tasks_by_id.iter_mut() {
                    if *pos > idx {
                        *pos -= 1;
                    }
                }

                return Ok(Some(task));
            }

            // No ready tasks, sleep with backoff
            time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
        }
    }

    async fn dequeue_matching(&self, tags: &[String]) -> Result<Option<Task>> {
        if tags.is_empty() {
            return self.dequeue().await;
        }

        // Similar to dequeue but with tag matching
        let mut backoff = Duration::from_millis(10);
        const MAX_BACKOFF: Duration = Duration::from_secs(1);

        loop {
            // Scope the locks to drop them before awaiting
            let matching_task = {
                let mut queue = self.queue.lock().await;

                if queue.is_empty() {
                    None
                } else {
                    let mut ready_index = None;
                    'outer: for (i, task) in queue.iter().enumerate() {
                        if !task.is_ready() {
                            continue;
                        }

                        // Check if task has any of the requested tags
                        for tag in tags {
                            if task.tags.contains(tag) {
                                ready_index = Some(i);
                                break 'outer;
                            }
                        }
                    }

                    if let Some(i) = ready_index {
                        // Remove the task from the queue
                        let task = queue.remove(i).ok_or_else(|| {
                            ChopFlowError::QueueError("Failed to remove task from queue".into())
                        })?;

                        Some((task, i))
                    } else {
                        None
                    }
                }
            };

            if let Some((task, idx)) = matching_task {
                // Update tasks_by_id map
                let mut tasks_by_id = self.tasks_by_id.lock().await;
                tasks_by_id.remove(&task.id);

                // Update positions for all tasks after the removed one
                for (_, pos) in tasks_by_id.iter_mut() {
                    if *pos > idx {
                        *pos -= 1;
                    }
                }

                return Ok(Some(task));
            }

            time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
        }
    }

    async fn get(&self, id: &Uuid) -> Result<Option<Task>> {
        let tasks_by_id = self.tasks_by_id.lock().await;

        let position = match tasks_by_id.get(id) {
            Some(pos) => *pos,
            None => return Ok(None),
        };

        let queue = self.queue.lock().await;

        Ok(queue.get(position).cloned())
    }

    async fn update(&self, task: Task) -> Result<()> {
        let id = task.id;

        let tasks_by_id = self.tasks_by_id.lock().await;

        let position = match tasks_by_id.get(&id) {
            Some(pos) => *pos,
            None => return Err(ChopFlowError::TaskNotFound(id.to_string())),
        };

        let mut queue = self.queue.lock().await;

        if let Some(existing_task) = queue.get_mut(position) {
            *existing_task = task;
            Ok(())
        } else {
            Err(ChopFlowError::TaskNotFound(id.to_string()))
        }
    }

    async fn len(&self) -> Result<usize> {
        let queue = self.queue.lock().await;
        Ok(queue.len())
    }

    async fn is_empty(&self) -> Result<bool> {
        let queue = self.queue.lock().await;
        Ok(queue.is_empty())
    }
}
