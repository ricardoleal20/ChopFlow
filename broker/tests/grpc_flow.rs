//! End-to-end gRPC integration tests for the broker.
//!
//! These spin up the real `ChopFlowBrokerService` (backed by in-memory
//! storage) on an ephemeral port and drive it with a real gRPC client. This is
//! the test that would have caught the original task-flow bugs (tasks marked
//! complete before execution, queued tasks never re-dispatched, results not
//! stored, cancellation dead branches).

use chopflow_broker::chopflow::{
    self, chop_flow_broker_client::ChopFlowBrokerClient, AcknowledgeTaskRequest,
    CancelTaskRequest, EnqueueTaskRequest, FetchTasksRequest, GetTaskStatusRequest,
    RegisterWorkerRequest,
};
use chopflow_broker::{build_storage, ChopFlowBrokerService, StorageBackend};
use std::time::Duration;
use tonic::Request;

/// Start a broker backed by in-memory storage on an ephemeral port and return
/// a connected client.
async fn setup() -> ChopFlowBrokerClient<tonic::transport::Channel> {
    let storage = build_storage(&StorageBackend::Memory).unwrap();
    let service = ChopFlowBrokerService::new(storage);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = chopflow_broker::serve_with_listener(service, listener).await;
    });

    let url = format!("http://{}", addr);
    ChopFlowBrokerClient::connect(url).await.unwrap()
}

async fn register_worker(client: &mut ChopFlowBrokerClient<tonic::transport::Channel>) -> String {
    let resp = client
        .register_worker(Request::new(RegisterWorkerRequest {
            address: "127.0.0.1".into(),
            tags: vec!["default".into()],
            resources: [("cpu".into(), 4)].into(),
        }))
        .await
        .unwrap();
    resp.into_inner().worker_id
}

async fn enqueue(
    client: &mut ChopFlowBrokerClient<tonic::transport::Channel>,
    name: &str,
    max_retries: u32,
) -> String {
    let resp = client
        .enqueue_task(Request::new(EnqueueTaskRequest {
            name: name.into(),
            payload: r#"{"x":1}"#.into(),
            tags: vec!["default".into()],
            eta: None,
            max_retries,
            resources: Default::default(),
            priority: 0,
        }))
        .await
        .unwrap();
    resp.into_inner().task_id
}

/// Pull a specific task from the broker, waiting through any retry backoff
/// ETA until it becomes claimable again. Bounded so a logic regression fails
/// fast instead of hanging.
async fn fetch_until(
    client: &mut ChopFlowBrokerClient<tonic::transport::Channel>,
    worker_id: &str,
    task_id: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        let f = client
            .fetch_tasks(Request::new(FetchTasksRequest {
                worker_id: worker_id.to_string(),
                max_tasks: 4,
            }))
            .await
            .unwrap()
            .into_inner();
        if f.tasks.iter().any(|t| t.id == task_id) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("task {} never became claimable within 45s", task_id);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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

#[tokio::test]
async fn task_flows_to_completion_with_stored_result() {
    let mut client = setup().await;
    let worker_id = register_worker(&mut client).await;
    let task_id = enqueue(&mut client, "echo", 3).await;

    // Worker pulls the task.
    let fetched = client
        .fetch_tasks(Request::new(FetchTasksRequest {
            worker_id: worker_id.clone(),
            max_tasks: 4,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.tasks.len(), 1);
    assert_eq!(fetched.tasks[0].id, task_id);
    assert_eq!(fetched.tasks[0].status, chopflow::TaskStatus::Running as i32);

    // Worker acknowledges success with a result.
    client
        .acknowledge_task(Request::new(AcknowledgeTaskRequest {
            worker_id,
            task_id: task_id.clone(),
            success: true,
            result: r#"{"ok":true}"#.into(),
        }))
        .await
        .unwrap();

    // Status reflects completion + stored result.
    let status = status_of(&mut client, &task_id).await;
    assert_eq!(status.status, chopflow::TaskStatus::Completed as i32);
    assert_eq!(status.result, r#"{"ok":true}"#);
}

#[tokio::test]
async fn failure_retries_then_deadletters() {
    let mut client = setup().await;
    let worker_id = register_worker(&mut client).await;
    let task_id = enqueue(&mut client, "flaky", 2).await; // max_retries = 2

    // Fail it 3 times: attempts 1 and 2 retry (re-queued with backoff ETA),
    // the 3rd exceeds max_retries(2) -> DeadLettered.
    for _ in 0..3 {
        fetch_until(&mut client, &worker_id, &task_id).await;
        client
            .acknowledge_task(Request::new(AcknowledgeTaskRequest {
                worker_id: worker_id.clone(),
                task_id: task_id.clone(),
                success: false,
                result: r#"{"err":"boom"}"#.into(),
            }))
            .await
            .unwrap();
    }

    let status = status_of(&mut client, &task_id).await;
    assert_eq!(status.status, chopflow::TaskStatus::Deadlettered as i32);
    assert_eq!(status.retry_count, 3);
}

#[tokio::test]
async fn cancel_queued_task() {
    let mut client = setup().await;
    // No worker registered -> task stays Queued.
    let task_id = enqueue(&mut client, "pending", 3).await;

    let cancelled = client
        .cancel_task(Request::new(CancelTaskRequest {
            task_id: task_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(cancelled.success);

    let status = status_of(&mut client, &task_id).await;
    assert_eq!(status.status, chopflow::TaskStatus::Cancelled as i32);
}

#[tokio::test]
async fn queued_task_dispatches_when_worker_joins_later() {
    let mut client = setup().await;
    // Enqueue with no worker available.
    let task_id = enqueue(&mut client, "delayed", 3).await;

    // Now register a worker and pull.
    let worker_id = register_worker(&mut client).await;
    let fetched = client
        .fetch_tasks(Request::new(FetchTasksRequest {
            worker_id,
            max_tasks: 4,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.tasks.len(), 1);
    assert_eq!(fetched.tasks[0].id, task_id);
}
