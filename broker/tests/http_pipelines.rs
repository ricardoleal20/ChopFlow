//! HTTP tests for durable pipelines: staged tasks, checkpoints on the
//! single-task view, and idempotent submits.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chopflow_broker::http::router;
use chopflow_broker::BrokerState;
use chopflow_core::storage::Storage;
use tower::ServiceExt;

async fn app() -> (axum::Router, std::sync::Arc<dyn Storage>) {
    let storage: std::sync::Arc<dyn Storage> =
        std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::new(storage.clone());
    (router(state), storage)
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn post_task(app: &axum::Router, body: &str) -> (StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tasks")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    (status, body_json(resp).await)
}

async fn get_task(app: &axum::Router, id: &str) -> (StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/tasks/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    (status, body_json(resp).await)
}

#[tokio::test]
async fn stages_round_trip_through_http() {
    let (app, _storage) = app().await;

    let (status, v) = post_task(
        &app,
        r#"{"name":"rag.ingest","payload":{"document":"..."},"stages":["chunk","embed","index"]}"#,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let task_id = v["task_id"].as_str().unwrap().to_string();

    // Single-task view carries the declared stages.
    let (status, v) = get_task(&app, &task_id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["stages"], serde_json::json!(["chunk", "embed", "index"]));
    // A task without checkpoints omits the field entirely.
    assert!(v.get("checkpoints").is_none());
    assert!(v.get("idempotency_key").is_none());

    // List view carries them too.
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/tasks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let v = body_json(resp).await;
    let tasks = v["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        tasks[0]["stages"],
        serde_json::json!(["chunk", "embed", "index"])
    );
}

#[tokio::test]
async fn get_task_includes_checkpoints_in_recorded_order() {
    let (app, storage) = app().await;

    let (_, v) = post_task(&app, r#"{"name":"rag.ingest","payload":{}}"#).await;
    let task_id = uuid::Uuid::parse_str(v["task_id"].as_str().unwrap()).unwrap();

    // Save checkpoints the way a worker would (via the storage the broker
    // shares with the gRPC SaveCheckpoint path). recorded_at has millisecond
    // resolution, so stage the saves apart to get a deterministic order.
    for stage in ["chunk", "embed"] {
        storage
            .save_checkpoint(&task_id, stage, &format!(r#"{{"{stage}":true}}"#))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let (status, v) = get_task(&app, &task_id.to_string()).await;
    assert_eq!(status, StatusCode::OK);
    let cps = v["checkpoints"].as_array().unwrap();
    assert_eq!(cps.len(), 2);
    assert_eq!(cps[0]["stage"], "chunk");
    assert_eq!(cps[0]["payload"], r#"{"chunk":true}"#);
    assert_eq!(cps[0]["task_id"], task_id.to_string());
    assert!(cps[0]["recorded_at"].as_i64().unwrap() > 0);
    assert_eq!(cps[1]["stage"], "embed");
    assert!(cps[0]["recorded_at"].as_i64().unwrap() <= cps[1]["recorded_at"].as_i64().unwrap());
}

#[tokio::test]
async fn idempotent_submit_returns_existing_task() {
    let (app, _storage) = app().await;
    let body = r#"{"name":"ingest","payload":{"doc":1},"idempotency_key":"doc-1"}"#;

    // First submit creates (201, no deduplicated flag).
    let (status, v) = post_task(&app, body).await;
    assert_eq!(status, StatusCode::CREATED);
    let first_id = v["task_id"].as_str().unwrap().to_string();
    assert!(v.get("deduplicated").is_none());

    // Second submit with the same key returns the SAME task id with
    // deduplicated: true and 200 OK.
    let (status, v) = post_task(&app, body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["task_id"].as_str().unwrap(), first_id);
    assert_eq!(v["deduplicated"], true);

    // The stored task carries the key, and exactly one task exists.
    let (status, v) = get_task(&app, &first_id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["idempotency_key"], "doc-1");

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/tasks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let v = body_json(resp).await;
    assert_eq!(v["tasks"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn concurrent_same_key_submits_create_exactly_one_task() {
    let (app, _storage) = app().await;
    let body = r#"{"name":"ingest","payload":{"doc":1},"idempotency_key":"race"}"#;

    // Two in-flight submits with the same key: the lookup+insert pair runs
    // under the broker's submit lock, so only one task is created and both
    // responses resolve to the same id.
    let mk = || {
        Request::builder()
            .method("POST")
            .uri("/api/tasks")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let (a, b) = tokio::join!(app.clone().oneshot(mk()), app.clone().oneshot(mk()),);
    let (va, vb) = (body_json(a.unwrap()).await, body_json(b.unwrap()).await);
    assert_eq!(
        va["task_id"].as_str().unwrap(),
        vb["task_id"].as_str().unwrap()
    );

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/tasks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let v = body_json(resp).await;
    assert_eq!(v["tasks"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn token_broker_still_guards_the_task_routes() {
    let storage: std::sync::Arc<dyn Storage> =
        std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::new(storage).with_api_tokens(vec!["sekret".to_string()]);
    let app = router(state);

    // No token -> 401 on both the submit and the single-task view.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tasks")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"x","payload":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/tasks/00000000-0000-0000-0000-000000000000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Correct bearer still works (auth is inherited from the existing
    // routes, so the new fields flow through unchanged).
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tasks")
                .header("authorization", "Bearer sekret")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"x","payload":{},"idempotency_key":"k"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}
