use axum::body::Body;
use axum::http::{Request, StatusCode};
use chopflow_broker::http::router;
use chopflow_broker::BrokerState;
use chopflow_core::config::Environment;
use chopflow_core::storage::Storage;
use tower::ServiceExt;

fn catalog() -> Vec<Environment> {
    vec![
        Environment {
            name: "local".into(),
            region: "default".into(),
            grpc_url: "http://localhost:8000".into(),
            http_url: "http://localhost:8080".into(),
        },
        Environment {
            name: "prod".into(),
            region: "us-east-1".into(),
            grpc_url: "http://broker.prod:8000".into(),
            http_url: "http://broker.prod:8080".into(),
        },
    ]
}

fn app_with(env: &str, region: &str, catalog: Vec<Environment>) -> axum::Router {
    app_with_token(env, region, catalog, None)
}

fn app_with_token(
    env: &str,
    region: &str,
    catalog: Vec<Environment>,
    token: Option<&str>,
) -> axum::Router {
    let storage: std::sync::Arc<dyn Storage> =
        std::sync::Arc::new(chopflow_core::InMemoryStorage::new());
    let state = BrokerState::with_identity(storage, env.into(), region.into(), catalog)
        .with_api_tokens(token.into_iter().map(str::to_string).collect());
    router(state)
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn environments_serves_catalog_and_marks_current() {
    let app = app_with("local", "default", catalog());

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/environments")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = body_json(resp).await;

    // current matches the broker's --env. http_url is always cleared on the
    // current entry: the serving broker is same-origin to its dashboard, so the
    // dashboard must not retarget when "current" is selected.
    assert_eq!(v["current"]["name"], "local");
    assert_eq!(v["current"]["region"], "default");
    assert_eq!(v["current"]["http_url"], "");

    // full catalog is served.
    let envs = v["environments"].as_array().unwrap();
    assert_eq!(envs.len(), 2);
    assert_eq!(envs[0]["name"], "local");
    assert_eq!(envs[1]["name"], "prod");
    assert_eq!(envs[1]["region"], "us-east-1");
}

#[tokio::test]
async fn environments_synthesizes_current_when_not_in_catalog() {
    // A standalone broker whose --env isn't listed still gets a current entry
    // (same-origin, empty urls) so the dashboard has something to display.
    let app = app_with("ci", "us-west-2", Vec::new());

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/environments")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = body_json(resp).await;
    assert_eq!(v["current"]["name"], "ci");
    assert_eq!(v["current"]["region"], "us-west-2");
    assert_eq!(v["current"]["http_url"], "");
    assert!(v["environments"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn stats_exposes_env_and_region() {
    let app = app_with("prod", "us-east-1", catalog());

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = body_json(resp).await;
    assert_eq!(v["env"], "prod");
    assert_eq!(v["region"], "us-east-1");
}

#[tokio::test]
async fn cors_allows_cross_origin_preflight() {
    // The dashboard may be served by broker A and fetch broker B's API — a
    // cross-origin request. The broker must answer preflight OPTIONS with
    // permissive CORS headers. `CorsLayer::very_permissive` mirrors the
    // request Origin (broader-compatible than a literal `*`), so we assert the
    // echoed origin + that the requested method is allowed.
    let app = app_with("local", "default", catalog());
    let resp = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/stats")
                .header("origin", "http://localhost:8080")
                .header("access-control-request-method", "GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "http://localhost:8080"
    );
    assert!(resp.headers().get("access-control-allow-methods").is_some());
}

// ---- API token auth --------------------------------------------------------

#[tokio::test]
async fn no_token_means_open_api_and_auth_required_false() {
    let app = app_with("local", "default", catalog());

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let v = body_json(resp).await;
    assert_eq!(v["auth_required"], false);
}

#[tokio::test]
async fn token_broker_rejects_missing_and_wrong_tokens() {
    let app = app_with_token("local", "default", catalog(), Some("sekret"));

    // No header -> 401.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Wrong token -> 401.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/stats")
                .header("authorization", "Bearer wrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn token_broker_accepts_correct_bearer() {
    let app = app_with_token("local", "default", catalog(), Some("sekret"));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/stats")
                .header("authorization", "Bearer sekret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = body_json(resp).await;
    assert_eq!(v["auth_required"], true);
}

#[tokio::test]
async fn healthz_stays_open_with_token() {
    // Adopt-probes and load balancers must work without a token.
    let app = app_with_token("local", "default", catalog(), Some("sekret"));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
