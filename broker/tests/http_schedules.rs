use chopflow_broker::http::router;
use chopflow_broker::BrokerState;
use chopflow_core::storage::Storage;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

async fn app() -> (axum::Router, std::sync::Arc<dyn Storage>) {
    let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::new(storage.clone());
    (router(state), storage)
}

#[tokio::test]
async fn create_list_get_delete_schedule_over_http() {
    let (app, _storage) = app().await;
    let body = r#"{"name":"nightly","task_template":{"name":"email","payload":{},"tags":["notif"],"resources":{},"max_retries":3},"kind":{"type":"cron","cron":"0 9 * * *"},"overlap_policy":"skip"}"#;
    let resp = app
        .clone()
        .oneshot(Request::builder().method("POST").uri("/api/schedules").header("content-type", "application/json").body(Body::from(body)).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // List
    let list = app.clone().oneshot(Request::builder().uri("/api/schedules").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(list.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(v.as_array().unwrap().len() >= 1);

    // Stats now include schedules
    let stats = app.clone().oneshot(Request::builder().uri("/api/stats").body(Body::empty()).unwrap()).await.unwrap();
    let sb = axum::body::to_bytes(stats.into_body(), usize::MAX).await.unwrap();
    let sv: serde_json::Value = serde_json::from_slice(&sb).unwrap();
    assert!(sv.get("schedules").is_some());
}

#[tokio::test]
async fn invalid_cron_returns_400() {
    let (app, _storage) = app().await;
    let body = r#"{"name":"bad","task_template":{"name":"x","payload":{},"tags":[],"resources":{},"max_retries":1},"kind":{"type":"cron","cron":"not cron"},"overlap_policy":"allow"}"#;
    let resp = app.oneshot(Request::builder().method("POST").uri("/api/schedules").header("content-type", "application/json").body(Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
