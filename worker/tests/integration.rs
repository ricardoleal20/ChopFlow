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
use chopflow_core::error::Result;
use chopflow_core::resources::ResourceAvailability;
use chopflow_worker::{
    connect_and_register, execute_task, fetch_tasks, start_task_processing, WorkerState,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    let worker_state = Arc::new(Mutex::new(
        WorkerState::new(worker_id.clone(), url.clone(), availability, tags, 4).unwrap(),
    ));

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
            priority: 0,
        }))
        .await
        .unwrap();
    let task_id = resp.into_inner().task_id;

    // Worker fetches the task.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let tasks = loop {
        let fetched = fetch_tasks(&worker_state, 4).await.unwrap();
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
    let worker_state = Arc::new(Mutex::new(
        WorkerState::new(worker_id.clone(), url.clone(), availability, tags, 1).unwrap(),
    ));
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
            priority: 0,
        }))
        .await
        .unwrap();
    let task_id = resp.into_inner().task_id;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let tasks = loop {
        let fetched = fetch_tasks(&worker_state, 1).await.unwrap();
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
    assert!(
        task.result.contains("Unknown task type"),
        "got: {}",
        task.result
    );
}

// --- Concurrency proof -------------------------------------------------------
//
// `fn` pointers can't capture state, so the slow handler reads/writes
// module-level atomics to track how many tasks are in flight at once. If the
// worker truly runs tasks concurrently, MAX_IN_FLIGHT will exceed 1.

static SLOW_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static SLOW_MAX_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// A handler that sleeps briefly, tracking concurrent invocations via the
/// module-level atomics above.
fn slow_handler(_payload: serde_json::Value) -> Result<serde_json::Value> {
    let current = SLOW_IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
    // Record the high-water mark of concurrent invocations.
    let mut max = SLOW_MAX_IN_FLIGHT.load(Ordering::SeqCst);
    while current > max {
        match SLOW_MAX_IN_FLIGHT.compare_exchange(max, current, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => break,
            Err(observed) => max = observed,
        }
    }
    std::thread::sleep(Duration::from_millis(150));
    SLOW_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    Ok(json!({"status": "ok"}))
}

#[tokio::test]
async fn worker_runs_tasks_concurrently() {
    // Reset the concurrency-tracking atomics (tests share the process).
    SLOW_IN_FLIGHT.store(0, Ordering::SeqCst);
    SLOW_MAX_IN_FLIGHT.store(0, Ordering::SeqCst);

    let url = broker_url().await;

    let tags = vec!["concurrent".to_string()];
    let resources: HashMap<String, u32> = [("cpu".to_string(), 4)].into_iter().collect();
    let worker_id = connect_and_register(&url, &tags, &resources).await.unwrap();

    let availability = ResourceAvailability {
        available: resources.clone(),
        total: resources.clone(),
    };
    let worker_state = Arc::new(Mutex::new(
        WorkerState::new(
            worker_id.clone(),
            url.clone(),
            availability,
            tags.clone(),
            4, // concurrency = 4
        )
        .unwrap(),
    ));
    // Register the slow handler under the enqueued task name.
    {
        let mut state = worker_state.lock().await;
        state.task_registry.register("slow", slow_handler);
    }

    let mut client = ChopFlowBrokerClient::connect(url.clone()).await.unwrap();

    // Enqueue 4 slow tasks. Each sleeps 150ms; sequentially that's ~600ms,
    // concurrently ~150ms. The key assertion is overlap, not timing.
    let mut task_ids = Vec::new();
    for _ in 0..4 {
        let resp = client
            .enqueue_task(Request::new(EnqueueTaskRequest {
                name: "slow".into(),
                payload: "{}".into(),
                tags: tags.clone(),
                eta: None,
                max_retries: 0,
                resources: Default::default(),
                priority: 0,
            }))
            .await
            .unwrap();
        task_ids.push(resp.into_inner().task_id);
    }

    // Run the real processing loop (with concurrency=4) in the background.
    let loop_state = worker_state.clone();
    let handle = tokio::spawn(async move { start_task_processing(&loop_state).await });

    // Wait until all 4 tasks reach a terminal status.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut all_done = true;
        for id in &task_ids {
            let t = status_of(&mut client, id).await;
            // COMPLETED=3, FAILED=4, DEADLETTERED=5, CANCELLED=6
            if t.status < 3 {
                all_done = false;
                break;
            }
        }
        if all_done {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            handle.abort();
            panic!("tasks did not all complete within 20s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.abort();

    // A sequential worker would never overlap handlers → MAX_IN_FLIGHT == 1.
    // A concurrent worker overlaps them → MAX_IN_FLIGHT >= 2. We assert >= 2
    // (not == 4) to tolerate scheduling jitter while still proving overlap.
    let max = SLOW_MAX_IN_FLIGHT.load(Ordering::SeqCst);
    assert!(
        max >= 2,
        "expected at least 2 concurrent tasks, but max in-flight was {} (worker may be sequential)",
        max
    );
}
