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

    /// Non-blocking version of [`dequeue_matching`]: returns `None`
    /// immediately if no ready matching task is available instead of
    /// waiting. This is what dispatchers and brokers should use when
    /// polling, so they never tie up an executor with a sleep loop.
    async fn try_dequeue_matching(&self, tags: &[String]) -> Result<Option<Task>>;

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

impl Default for InMemoryQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryQueue {
    /// Create a new in-memory queue
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            tasks_by_id: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Remove the first ready task whose tags intersect `tags` (or any task
    /// when `tags` is empty). Non-blocking: returns `Ok(None)` when nothing
    /// matches. Holds both locks for the duration of the removal so the
    /// `tasks_by_id` index stays consistent.
    async fn pop_ready_matching(&self, tags: &[String]) -> Result<Option<Task>> {
        // Acquire both locks up front so the index and the deque cannot
        // drift apart between removal and reindexing.
        let mut queue = self.queue.lock().await;
        let mut tasks_by_id = self.tasks_by_id.lock().await;

        // Find the first ready task matching the requested tags.
        let ready_index = {
            let mut found = None;
            'outer: for (i, task) in queue.iter().enumerate() {
                if !task.is_ready() {
                    continue;
                }
                if tags.is_empty() {
                    found = Some(i);
                    break;
                }
                for tag in tags {
                    if task.tags.contains(tag) {
                        found = Some(i);
                        break 'outer;
                    }
                }
            }
            found
        };

        let Some(idx) = ready_index else {
            return Ok(None);
        };

        let task = queue
            .remove(idx)
            .ok_or_else(|| ChopFlowError::QueueError("Failed to remove task from queue".into()))?;

        // Keep the by-id index consistent: drop the removed task and shift
        // every position that came after it.
        tasks_by_id.remove(&task.id);
        for pos in tasks_by_id.values_mut() {
            if *pos > idx {
                *pos -= 1;
            }
        }

        Ok(Some(task))
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
            if let Some(task) = self.pop_ready_matching(&[]).await? {
                return Ok(Some(task));
            }

            // No ready tasks, sleep with backoff
            time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
        }
    }

    async fn dequeue_matching(&self, tags: &[String]) -> Result<Option<Task>> {
        // Blocking variant: poll with backoff until a matching ready task
        // appears. Kept for API compatibility; the broker/dispatcher use the
        // non-blocking `try_dequeue_matching` instead.
        let mut backoff = Duration::from_millis(10);
        const MAX_BACKOFF: Duration = Duration::from_secs(1);

        loop {
            if let Some(task) = self.pop_ready_matching(tags).await? {
                return Ok(Some(task));
            }

            time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
        }
    }

    async fn try_dequeue_matching(&self, tags: &[String]) -> Result<Option<Task>> {
        // Non-blocking: return immediately with whatever is available.
        self.pop_ready_matching(tags).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn task(name: &str) -> Task {
        Task::new(name.into(), serde_json::json!({}))
    }

    #[tokio::test]
    async fn enqueue_then_try_dequeue_matching_returns_fifo() {
        let q = InMemoryQueue::new();
        let t1 = task("a");
        let t2 = task("b");
        q.enqueue(t1.clone()).await.unwrap();
        q.enqueue(t2.clone()).await.unwrap();

        let got = q.try_dequeue_matching(&[]).await.unwrap().unwrap();
        assert_eq!(got.id, t1.id); // FIFO order
        assert_eq!(q.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn try_dequeue_returns_none_when_empty() {
        let q = InMemoryQueue::new();
        assert!(q.try_dequeue_matching(&[]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tag_matching_filters_tasks() {
        let q = InMemoryQueue::new();
        let gpu = Task::new("gpu".into(), serde_json::json!({})).with_tag("gpu");
        let cpu = Task::new("cpu".into(), serde_json::json!({})).with_tag("cpu");
        q.enqueue(gpu).await.unwrap();
        q.enqueue(cpu).await.unwrap();

        let got = q
            .try_dequeue_matching(&["cpu".into()])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.name, "cpu");
    }

    #[tokio::test]
    async fn eta_gating_blocks_ready_task() {
        let q = InMemoryQueue::new();
        let future = Utc::now() + chrono::Duration::hours(1);
        let delayed = Task::new("later".into(), serde_json::json!({})).with_eta(future);
        q.enqueue(delayed).await.unwrap();

        // Not ready yet -> nothing claimed.
        assert!(q.try_dequeue_matching(&[]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn index_stays_consistent_after_removals() {
        // Removing a task from the middle must not corrupt the by-id lookup
        // for tasks that came after it.
        let q = InMemoryQueue::new();
        let t1 = task("a");
        let t2 = task("b");
        let t3 = task("c");
        q.enqueue(t1.clone()).await.unwrap();
        q.enqueue(t2.clone()).await.unwrap();
        q.enqueue(t3.clone()).await.unwrap();

        // Dequeue t1 (index 0), then t3 must still be retrievable via get.
        q.try_dequeue_matching(&[]).await.unwrap();
        let got3 = q.get(&t3.id).await.unwrap().unwrap();
        assert_eq!(got3.id, t3.id);
        let got2 = q.get(&t2.id).await.unwrap().unwrap();
        assert_eq!(got2.id, t2.id);
    }
}
