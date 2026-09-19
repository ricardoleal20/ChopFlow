//! End-to-end gRPC integration tests for the broker.
//!
//! These spin up the real `ChopFlowBrokerService` (backed by in-memory
//! storage) on an ephemeral port and drive it with a real gRPC client. This is
//! the test that would have caught the original task-flow bugs (tasks marked
//! complete before execution, queued tasks never re-dispatched, results not
//! stored, cancellation dead branches).

use chopflow_broker::chopflow::{
    self, chop_flow_broker_client::ChopFlowBrokerClient, AcknowledgeTaskRequest, CancelTaskRequest,
    EnqueueTaskRequest, FetchTasksRequest, GetCheckpointsRequest, GetTaskStatusRequest,
    RegisterWorkerRequest, ResourceSpec, SaveCheckpointRequest, WorkerHeartbeatRequest,
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
            resources: [(
                "cpu".into(),
                ResourceSpec {
                    capacity: 4,
                    refill_amount: 0,
                    refill_period_secs: 0,
                },
            )]
            .into(),
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
            stages: Vec::new(),
            idempotency_key: String::new(),
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
        // Heartbeat like a real worker: under a loaded parallel test run the
        // 3-round backoff wait can outlive the 30s liveness window and the
        // broker would otherwise reject our fetch with "stale heartbeat".
        let _ = client
            .worker_heartbeat(Request::new(WorkerHeartbeatRequest {
                worker_id: worker_id.to_string(),
                resources: None,
            }))
            .await;
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
    assert_eq!(
        fetched.tasks[0].status,
        chopflow::TaskStatus::Running as i32
    );

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

#[tokio::test]
async fn checkpoint_save_and_fetch_roundtrip_in_order() {
    let mut client = setup().await;
    let task_id = enqueue(&mut client, "pipeline", 0).await;

    // No checkpoints yet.
    let cps = client
        .get_checkpoints(Request::new(GetCheckpointsRequest {
            task_id: task_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(cps.checkpoints.is_empty());

    // Save two stages in order. recorded_at has millisecond resolution and
    // the store orders by recorded_at, so stage the saves apart.
    for (stage, payload) in [("chunk", r#"{"chunks":3}"#), ("embed", r#"{"next":3}"#)] {
        client
            .save_checkpoint(Request::new(SaveCheckpointRequest {
                task_id: task_id.clone(),
                stage: stage.into(),
                payload: payload.into(),
            }))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Upserting the first stage overwrites its payload and refreshes its
    // recorded_at, so it reorders to the end (listing is recorded_at order).
    client
        .save_checkpoint(Request::new(SaveCheckpointRequest {
            task_id: task_id.clone(),
            stage: "chunk".into(),
            payload: r#"{"chunks":4}"#.into(),
        }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let cps = client
        .get_checkpoints(Request::new(GetCheckpointsRequest {
            task_id: task_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(cps.checkpoints.len(), 2);
    assert_eq!(cps.checkpoints[0].task_id, task_id);
    assert_eq!(cps.checkpoints[0].stage, "embed");
    assert_eq!(cps.checkpoints[1].stage, "chunk");
    assert_eq!(cps.checkpoints[1].payload, r#"{"chunks":4}"#);
    // recorded_at is an RFC 3339 timestamp string.
    assert!(!cps.checkpoints[0].recorded_at.is_empty());
    assert!(cps.checkpoints[0].recorded_at <= cps.checkpoints[1].recorded_at);
}

#[tokio::test]
async fn checkpoint_for_unknown_task_errors() {
    let mut client = setup().await;

    let err = client
        .save_checkpoint(Request::new(SaveCheckpointRequest {
            task_id: "00000000-0000-0000-0000-000000000000".into(),
            stage: "chunk".into(),
            payload: "{}".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    let err = client
        .get_checkpoints(Request::new(GetCheckpointsRequest {
            task_id: "00000000-0000-0000-0000-000000000000".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // Malformed task id -> invalid argument, not a lookup.
    let err = client
        .get_checkpoints(Request::new(GetCheckpointsRequest {
            task_id: "not-a-uuid".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn worker_registers_with_replenishing_resource_spec() {
    let mut client = setup().await;

    // `llm.rpm: 10@10/60` (capacity 10, refills 10 per 60s) plus a static cpu.
    let resp = client
        .register_worker(Request::new(RegisterWorkerRequest {
            address: "127.0.0.1".into(),
            tags: vec!["default".into()],
            resources: [
                (
                    "cpu".into(),
                    ResourceSpec {
                        capacity: 4,
                        refill_amount: 0,
                        refill_period_secs: 0,
                    },
                ),
                (
                    "llm.rpm".into(),
                    ResourceSpec {
                        capacity: 10,
                        refill_amount: 10,
                        refill_period_secs: 60,
                    },
                ),
            ]
            .into(),
        }))
        .await
        .unwrap();
    assert!(!resp.into_inner().worker_id.is_empty());

    // ListWorkers shows the declared availability (bucket starts full).
    let workers = client
        .list_workers(Request::new(()))
        .await
        .unwrap()
        .into_inner()
        .workers;
    assert_eq!(workers.len(), 1);
    let resources = workers[0].resources.as_ref().unwrap();
    assert_eq!(resources.available.get("cpu"), Some(&4));
    assert_eq!(resources.available.get("llm.rpm"), Some(&10));
    assert_eq!(resources.total.get("llm.rpm"), Some(&10));
}
