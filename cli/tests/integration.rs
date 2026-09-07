//! End-to-end integration tests for the CLI against a real in-process broker.
//!
//! These spin up the real `ChopFlowBrokerService` (in-memory storage) on an
//! ephemeral port, then drive the CLI's own handler functions (`enqueue_task`,
//! `get_status`, `schedule_create/list/delete`) against it. Side effects are
//! verified by querying the broker directly with a gRPC client, rather than
//! parsing CLI stdout — more robust and avoids stdout-capture races.

use chopflow_broker::chopflow::{
    self, chop_flow_broker_client::ChopFlowBrokerClient, GetTaskStatusRequest, ListSchedulesRequest,
    ListTasksRequest,
};
use chopflow_broker::{build_storage, ChopFlowBrokerService, StorageBackend};
use chopflow_core::resources::ResourceAvailability;
use chopflow_worker::{connect_and_register, WorkerState};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::Request;

/// Start a broker on an ephemeral port; return its gRPC URL.
async fn broker_url() -> String {
    let storage = build_storage(&StorageBackend::Memory).unwrap();
    let service = ChopFlowBrokerService::new(storage);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = chopflow_broker::serve_with_listener(service, listener).await;
    });

    format!("http://{}", addr)
}

/// Start a background worker (echo handler) against the broker so enqueued
/// tasks can complete.
async fn start_worker(url: String) {
    let tags = vec!["default".to_string()];
    let resources: HashMap<String, u32> = [("cpu".to_string(), 4)].into_iter().collect();
    let worker_id = connect_and_register(&url, &tags, &resources).await.unwrap();
    let availability = ResourceAvailability {
        available: resources.clone(),
        total: resources.clone(),
    };
    let worker_state = Arc::new(Mutex::new(WorkerState::new(
        worker_id,
        url,
        availability,
        tags,
        4,
    )));
    tokio::spawn(async move {
        let _ = chopflow_worker::start_task_processing(&worker_state).await;
    });
}

async fn list_tasks(url: &str) -> Vec<chopflow::Task> {
    let mut client = ChopFlowBrokerClient::connect(url.to_string()).await.unwrap();
    client
        .list_tasks(Request::new(ListTasksRequest {
            limit: 50,
            offset: 0,
            filter_status: vec![],
        }))
        .await
        .unwrap()
        .into_inner()
        .tasks
}

async fn status_of(url: &str, task_id: &str) -> chopflow::Task {
    let mut client = ChopFlowBrokerClient::connect(url.to_string()).await.unwrap();
    client
        .get_task_status(Request::new(GetTaskStatusRequest {
            task_id: task_id.to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .task
        .unwrap()
}

async fn wait_until_terminal(url: &str, task_id: &str) -> chopflow::Task {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let task = status_of(url, task_id).await;
        let terminal = matches!(task.status, 3 | 4 | 5 | 6);
        if terminal {
            return task;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("task {} never reached terminal status within 15s", task_id);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn list_schedules(url: &str) -> Vec<chopflow::Schedule> {
    let mut client = ChopFlowBrokerClient::connect(url.to_string()).await.unwrap();
    client
        .list_schedules(Request::new(ListSchedulesRequest {}))
        .await
        .unwrap()
        .into_inner()
        .schedules
}

#[tokio::test]
async fn cli_enqueues_task_that_completes() {
    let url = broker_url().await;
    start_worker(url.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempfile::tempdir().unwrap();
    let task_path = dir.path().join("task.json");
    std::fs::write(&task_path, r#"{"hello":"world"}"#).unwrap();

    chopflow_cli::enqueue_task(
        url.clone(),
        task_path,
        Some("echo".into()),
        Some("default".into()),
        None,
        0,
    )
    .await
    .unwrap();

    // The CLI doesn't return the task id; find the task via the broker. The
    // broker is fresh per test, so there's exactly one.
    let tasks = list_tasks(&url).await;
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].id.clone();
    assert_eq!(tasks[0].name, "echo");

    let task = wait_until_terminal(&url, &task_id).await;
    assert_eq!(task.status, 3); // COMPLETED
    let result: serde_json::Value = serde_json::from_str(&task.result).unwrap();
    assert_eq!(result["status"], "ok");
    assert_eq!(result["echo"]["hello"], "world");
}

#[tokio::test]
async fn cli_get_status_all_runs_without_error() {
    let url = broker_url().await;
    start_worker(url.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempfile::tempdir().unwrap();
    let task_path = dir.path().join("task.json");
    std::fs::write(&task_path, r#"{"x":1}"#).unwrap();
    chopflow_cli::enqueue_task(
        url.clone(),
        task_path,
        Some("echo".into()),
        Some("default".into()),
        None,
        0,
    )
    .await
    .unwrap();

    // `status --all` should succeed and list the task.
    chopflow_cli::get_status(url.clone(), None, true)
        .await
        .unwrap();
}

#[tokio::test]
async fn cli_get_status_queue_stats_runs_without_error() {
    let url = broker_url().await;
    // `status` with no flags prints queue stats.
    chopflow_cli::get_status(url.clone(), None, false)
        .await
        .unwrap();
}

#[tokio::test]
async fn cli_schedule_create_list_delete_roundtrip() {
    let url = broker_url().await;

    chopflow_cli::schedule_create(
        url.clone(),
        "nightly-build".into(),
        "build".into(),
        Some("0 9 * * *".into()),
        None,
        "{}".into(),
        "ci".into(),
        "cpu:4".into(),
        3,
        "skip".into(),
        0,
    )
    .await
    .unwrap();

    let schedules = list_schedules(&url).await;
    assert_eq!(schedules.len(), 1);
    let sched = &schedules[0];
    assert_eq!(sched.name, "nightly-build");
    let template = sched.task_template.clone().unwrap();
    assert_eq!(template.name, "build");
    assert_eq!(template.tags, vec!["ci"]);
    assert_eq!(template.resources.get("cpu"), Some(&4));
    assert_eq!(sched.overlap_policy, 0); // OVERLAP_SKIP
    let kind = sched.kind.clone().unwrap().kind.unwrap();
    assert!(matches!(kind, chopflow::schedule_kind::Kind::Cron(_)));
    let schedule_id = sched.id.clone();

    // `schedule_list` (CLI) should also run clean.
    chopflow_cli::schedule_list(url.clone()).await.unwrap();

    // Delete via the CLI.
    chopflow_cli::schedule_delete(url.clone(), schedule_id.clone())
        .await
        .unwrap();

    let after = list_schedules(&url).await;
    assert!(after.is_empty(), "schedule should be deleted");
}

#[tokio::test]
async fn cli_schedule_create_oneshot() {
    let url = broker_url().await;

    chopflow_cli::schedule_create(
        url.clone(),
        "one-off".into(),
        "report".into(),
        None,
        Some("2026-12-31T23:59:59Z".into()),
        "{}".into(),
        "".into(),
        "".into(),
        3,
        "allow".into(),
        0,
    )
    .await
    .unwrap();

    let schedules = list_schedules(&url).await;
    assert_eq!(schedules.len(), 1);
    let sched = &schedules[0];
    assert_eq!(sched.name, "one-off");
    assert_eq!(sched.overlap_policy, 2); // OVERLAP_ALLOW
    let kind = sched.kind.clone().unwrap().kind.unwrap();
    assert!(matches!(kind, chopflow::schedule_kind::Kind::Eta(_)));
}
