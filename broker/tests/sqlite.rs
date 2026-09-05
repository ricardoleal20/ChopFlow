//! Persistence tests for `SqliteStorage`: round-trip CRUD, atomic claiming,
//! and the durability/reconciliation guarantees that make broker restarts safe.
//!
//! These use `tempfile` so each test gets an isolated db file on disk.

use chopflow_core::storage::{Storage, TaskFilter};
use chopflow_core::task::{Task, TaskStatus};
use chopflow_core::SqliteStorage;

fn make_task(name: &str) -> Task {
    let mut t = Task::new(name.into(), serde_json::json!({"n": 1}));
    t.tags = vec!["default".into()];
    t
}

#[tokio::test]
async fn round_trip_insert_get_list() {
    let store = SqliteStorage::open_in_memory().unwrap();

    let t1 = make_task("a");
    let t2 = make_task("b");
    store.insert(t1.clone()).await.unwrap();
    store.insert(t2.clone()).await.unwrap();

    // get by id
    let got = store.get(&t1.id).await.unwrap().unwrap();
    assert_eq!(got.name, "a");
    assert_eq!(got.status, TaskStatus::Created);

    // list all
    let all = store.list(&TaskFilter::default()).await.unwrap();
    assert_eq!(all.len(), 2);

    // count_pending is 0 (nothing Queued yet)
    assert_eq!(store.count_pending().await.unwrap(), 0);
}

#[tokio::test]
async fn claim_ready_atomic_and_status_driven() {
    let store = SqliteStorage::open_in_memory().unwrap();

    // Three queued tasks, two matching the "gpu" tag, one matching "cpu".
    let mut a = make_task("gpu-a");
    a.tags = vec!["gpu".into()];
    a.status = TaskStatus::Queued;

    let mut b = make_task("gpu-b");
    b.tags = vec!["gpu".into()];
    b.status = TaskStatus::Queued;

    let mut c = make_task("cpu-c");
    c.tags = vec!["cpu".into()];
    c.status = TaskStatus::Queued;

    store.insert(a.clone()).await.unwrap();
    store.insert(b.clone()).await.unwrap();
    store.insert(c.clone()).await.unwrap();

    assert_eq!(store.count_pending().await.unwrap(), 3);

    // A gpu worker claims up to 4 -> gets exactly the two gpu tasks, now Running.
    let claimed = store.claim_ready(&["gpu".into()], 4).await.unwrap();
    assert_eq!(claimed.len(), 2);
    assert!(claimed.iter().all(|t| t.status == TaskStatus::Running));

    // The cpu task is still pending and unclaimed.
    assert_eq!(store.count_pending().await.unwrap(), 1);

    // A second claim by the gpu worker finds nothing left.
    let again = store.claim_ready(&["gpu".into()], 4).await.unwrap();
    assert!(again.is_empty());

    // The cpu worker claims its task.
    let cpu_claim = store.claim_ready(&["cpu".into()], 4).await.unwrap();
    assert_eq!(cpu_claim.len(), 1);
    assert_eq!(store.count_pending().await.unwrap(), 0);
}

#[tokio::test]
async fn claim_ready_honors_eta() {
    let store = SqliteStorage::open_in_memory().unwrap();

    let mut future = make_task("later");
    future.status = TaskStatus::Queued;
    future.eta = Some(chrono::Utc::now() + chrono::Duration::hours(1));
    store.insert(future).await.unwrap();

    // Not ready yet -> nothing claimed, even though it's Queued.
    let claimed = store.claim_ready(&[], 4).await.unwrap();
    assert!(claimed.is_empty());
    assert_eq!(store.count_pending().await.unwrap(), 1);
}

#[tokio::test]
async fn tasks_persist_across_reopen() {
    // Durability: a task written to disk survives dropping the storage handle
    // and reopening the same file — i.e. a broker restart.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("chopflow.db");
    let path_str = db_path.to_str().unwrap().to_string();

    let task_id = {
        let store = SqliteStorage::open(&path_str).unwrap();
        let t = make_task("persistent");
        let id = t.id;
        store.insert(t).await.unwrap();
        id
    };

    // Reopen the same file — the task must still be there.
    let store = SqliteStorage::open(&path_str).unwrap();
    let got = store.get(&task_id).await.unwrap().unwrap();
    assert_eq!(got.name, "persistent");
    let all = store.list(&TaskFilter::default()).await.unwrap();
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn reconcile_resets_running_to_queued() {
    // Reconciliation: a task left Running (worker crashed mid-execution) must
    // be returned to Queued when the broker restarts, so it can be reclaimed.
    let dir = tempfile::tempdir().unwrap();
    let path_str = dir.path().join("chopflow.db").to_str().unwrap().to_string();

    let task_id = {
        let store = SqliteStorage::open(&path_str).unwrap();
        let mut t = make_task("stuck");
        t.status = TaskStatus::Running;
        let id = t.id;
        store.insert(t).await.unwrap();
        id
    };

    // Simulate a restart: reopen and reconcile.
    let store = SqliteStorage::open(&path_str).unwrap();
    let reset = store.reconcile().await.unwrap();
    assert_eq!(reset, 1);

    let got = store.get(&task_id).await.unwrap().unwrap();
    assert_eq!(got.status, TaskStatus::Queued);

    // Now it's claimable again.
    let claimed = store.claim_ready(&[], 4).await.unwrap();
    assert_eq!(claimed.len(), 1);
}
