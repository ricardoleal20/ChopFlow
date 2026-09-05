//! Integration tests for the InMemoryDispatcher worker registry and resource
//! accounting. Uses no gRPC / no storage — exercises the dispatcher directly.

use chopflow_core::dispatcher::{Dispatcher, InMemoryDispatcher, Worker};
use chopflow_core::resources::ResourceAvailability;
use chopflow_core::task::Task;
use chrono::Utc;
use uuid::Uuid;

fn worker_with(cpu: u32) -> Worker {
    let mut w = Worker::new("127.0.0.1:0");
    w.resources = ResourceAvailability::new();
    w.resources.add_resource("cpu", cpu);
    w
}

fn task_needing(cpu: u32) -> Task {
    let mut t = Task::new("job".into(), serde_json::json!({}));
    t.resources.insert("cpu".into(), cpu);
    t
}

#[tokio::test]
async fn register_and_list_workers() {
    let mut d = InMemoryDispatcher::new();
    let w = worker_with(4);
    let id = w.id;
    d.register_worker(w).await.unwrap();

    let workers = d.list_workers().await.unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].id, id);
    assert_eq!(d.get_worker(&id).await.unwrap().unwrap().id, id);
    assert!(d.get_worker(&Uuid::new_v4()).await.unwrap().is_none());
}

#[tokio::test]
async fn heartbeat_updates_liveness() {
    let mut d = InMemoryDispatcher::new();
    let w = worker_with(4);
    let id = w.id;
    d.register_worker(w).await.unwrap();

    // Fresh worker is alive.
    assert!(d.get_worker(&id).await.unwrap().unwrap().is_alive());

    // Simulate a stale heartbeat (older than the 30s liveness window).
    {
        // We can't reach the internal map directly; instead verify the public
        // contract: a worker whose heartbeat is current stays alive. The
        // liveness boundary is exercised by the evict test below via time.
    }
    // A real heartbeat keeps it alive.
    d.heartbeat(&id).await.unwrap();
    assert!(d.get_worker(&id).await.unwrap().unwrap().is_alive());

    // Unknown worker -> error.
    assert!(d.heartbeat(&Uuid::new_v4()).await.is_err());
}

#[tokio::test]
async fn assign_and_release_account_for_resources() {
    let mut d = InMemoryDispatcher::new();
    let w = worker_with(4);
    let id = w.id;
    d.register_worker(w).await.unwrap();

    let t1 = task_needing(3);
    d.assign_task(&id, &t1).await.unwrap();

    // 4 - 3 = 1 cpu available now.
    let w = d.get_worker(&id).await.unwrap().unwrap();
    assert_eq!(w.resources.available.get("cpu"), Some(&1));
    assert_eq!(w.assigned_tasks.len(), 1);

    // A task needing 2 cpu should fail (only 1 left).
    let t2 = task_needing(2);
    assert!(d.assign_task(&id, &t2).await.is_err());

    // Releasing t1 restores 3 cpu (back to 4 total).
    d.release_task(&id, &t1).await.unwrap();
    let w = d.get_worker(&id).await.unwrap().unwrap();
    assert_eq!(w.resources.available.get("cpu"), Some(&4));
    assert!(w.assigned_tasks.is_empty());
}

#[tokio::test]
async fn release_is_idempotent_for_unknown_worker() {
    let mut d = InMemoryDispatcher::new();
    let t = task_needing(1);
    // Unknown worker -> Ok (no-op), not an error.
    d.release_task(&Uuid::new_v4(), &t).await.unwrap();
}

#[tokio::test]
async fn assign_unknown_worker_errors() {
    let mut d = InMemoryDispatcher::new();
    let t = task_needing(1);
    assert!(d.assign_task(&Uuid::new_v4(), &t).await.is_err());
}

#[tokio::test]
async fn can_handle_respects_tags_and_resources() {
    let mut w = Worker::new("h");
    w.resources.add_resource("cpu", 4);
    w.tags = vec!["gpu".into()];

    // Tag matches and resources satisfied.
    let mut t = Task::new("j".into(), serde_json::json!({}));
    t.tags = vec!["gpu".into()];
    t.resources.insert("cpu".into(), 2);
    assert!(w.can_handle(&t));

    // Tag does not match.
    t.tags = vec!["cpu".into()];
    assert!(!w.can_handle(&t));

    // No tags required -> tag check passes, but resources must still fit.
    t.tags = vec![];
    t.resources.insert("cpu".into(), 99);
    assert!(!w.can_handle(&t));
    t.resources.insert("cpu".into(), 2);
    assert!(w.can_handle(&t));
}

#[tokio::test]
async fn worker_liveness_boundary() {
    // is_alive uses a 30s threshold; a heartbeat 60s in the past is dead.
    let mut w = worker_with(4);
    w.last_heartbeat = Utc::now() - chrono::Duration::seconds(60);
    assert!(!w.is_alive());

    w.heartbeat();
    assert!(w.is_alive());
}
