//! End-to-end integration tests for the worker against a real in-process broker.
//!
//! These spin up the real `ChopFlowBrokerService` (in-memory storage) on an
//! ephemeral port, then drive the worker's own loop functions (`connect_and_register`,
//! `fetch_tasks`, `execute_task`) against it — proving the worker and broker agree
//! on the gRPC contract end to end.

use chopflow_broker::chopflow::{
    self, chop_flow_broker_client::ChopFlowBrokerClient, EnqueueTaskRequest, GetTaskStatusRequest,
};
use chopflow_broker::{build_storage, ChopFlowBrokerService, StorageBackend};
use chopflow_core::resources::ResourceAvailability;
use chopflow_worker::{connect_and_register, execute_task, fetch_tasks, WorkerState};
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

async fn status_of(
    client: &mut ChopFlowBrokerClient<tonic::transport::Channel>,
    task_id: &str,
) -> chopflow::Task {
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

/// Register a worker via the worker's own registration path, enqueue an `echo`
/// task, let the worker fetch + execute + ack it, and assert the stored result.
async fn run_echo_end_to_end() -> chopflow::Task {
    let url = broker_url().await;

    // Register through the worker SDK (exercises connect_and_register).
    let tags = vec!["default".to_string()];
    let resources: HashMap<String, u32> = [("cpu".to_string(), 4)].into_iter().collect();
    let worker_id = connect_and_register(&url, &tags, &resources).await.unwrap();

    // Build the worker state the way start_worker does.
    let availability = ResourceAvailability {
        available: resources.clone(),
        total: resources.clone(),
    };
    let worker_state = Arc::new(Mutex::new(WorkerState::new(
        worker_id.clone(),
        url.clone(),
        availability,
        tags,
    )));

    // Enqueue an echo task as a producer would.
    let mut client = ChopFlowBrokerClient::connect(url.clone()).await.unwrap();
    let resp = client
        .enqueue_task(Request::new(EnqueueTaskRequest {
            name: "echo".into(),
            payload: r#"{"hello":"world"}"#.into(),
            tags: vec!["default".into()],
            eta: None,
            max_retries: 3,
            resources: Default::default(),
        }))
        .await
        .unwrap();
    let task_id = resp.into_inner().task_id;

    // Worker fetches the task.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let tasks = loop {
        let fetched = fetch_tasks(&worker_state).await.unwrap();
        if !fetched.is_empty() {
            break fetched;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("worker never fetched the task within 10s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Execute + ack each fetched task.
    for task in tasks {
        execute_task(&worker_state, task).await.unwrap();
    }

    status_of(&mut client, &task_id).await
}

#[tokio::test]
async fn worker_completes_echo_task_and_stores_result() {
    let task = run_echo_end_to_end().await;

    assert_eq!(task.status, chopflow::TaskStatus::Completed as i32);
    let result: serde_json::Value = serde_json::from_str(&task.result).unwrap();
    assert_eq!(result["status"], "ok");
    assert_eq!(result["echo"]["hello"], "world");
}

#[tokio::test]
async fn worker_acks_unknown_task_as_failure_via_default_handler() {
    // The built-in `default` handler is echo, so an unknown task name falls
    // back to it and still succeeds. Verify that fallback path completes.
    let url = broker_url().await;

    let tags = vec!["default".to_string()];
    let resources: HashMap<String, u32> = [("cpu".to_string(), 1)].into_iter().collect();
    let worker_id = connect_and_register(&url, &tags, &resources).await.unwrap();

    let availability = ResourceAvailability {
        available: resources.clone(),
        total: resources.clone(),
    };
    // Remove the `default` handler so unknown tasks are acked as failure.
    let worker_state = Arc::new(Mutex::new(WorkerState::new(
        worker_id.clone(),
        url.clone(),
        availability,
        tags,
    )));
    {
        let mut state = worker_state.lock().await;
        state.task_registry = chopflow_worker::TaskRegistry::new();
    }

    let mut client = ChopFlowBrokerClient::connect(url.clone()).await.unwrap();
    let resp = client
        .enqueue_task(Request::new(EnqueueTaskRequest {
            name: "totally_unknown".into(),
            payload: "{}".into(),
            tags: vec!["default".into()],
            eta: None,
            max_retries: 1,
            resources: Default::default(),
        }))
        .await
        .unwrap();
    let task_id = resp.into_inner().task_id;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let tasks = loop {
        let fetched = fetch_tasks(&worker_state).await.unwrap();
        if !fetched.is_empty() {
            break fetched;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("worker never fetched the task within 10s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    for task in tasks {
        execute_task(&worker_state, task).await.unwrap();
    }

    let task = status_of(&mut client, &task_id).await;
    // No handler → ack failure. With max_retries=1 the broker re-queues once
    // (retry_count goes 1→2 > max_retries=1) then dead-letters. Either FAILED
    // (mid-retry) or DEADLETTERED is an acceptable terminal observation; both
    // prove the failure ack propagated. We accept either non-COMPLETED state.
    assert_ne!(
        task.status,
        chopflow::TaskStatus::Completed as i32,
        "unknown task should not have completed"
    );
    assert!(task.result.contains("Unknown task type"), "got: {}", task.result);
}
