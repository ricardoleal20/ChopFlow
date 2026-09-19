//! Integration tests for the streamable-HTTP MCP transport (`--http`).
//!
//! Drives the axum router directly (no process spawn): the MCP initialize
//! handshake and a tools/list call over HTTP POST /mcp, exactly what a remote
//! MCP client (Claude Desktop) does against the gateway URL.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

fn router() -> axum::Router {
    router_with_access(None)
}

fn router_with_access(access: Option<&str>) -> axum::Router {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    chopflow_mcp::http_router(
        chopflow_mcp::ChopFlowMcp::new(client, "http://127.0.0.1:1") // unreachable on purpose
            .with_access_token(access.map(str::to_string)),
    )
}

async fn post_mcp(body: String) -> (StatusCode, String) {
    let (status, _session, body) = post_on(router(), body, None).await;
    (status, body)
}

/// POST /mcp on `router`, optionally carrying an `mcp-session-id`. Returns
/// the status, the session id the server assigned (if any), and the body.
/// The router is taken by value (it is cheaply cloneable and shares session
/// state), so a multi-request session flow reuses one router.
async fn post_on(
    router: axum::Router,
    body: String,
    session_id: Option<&str>,
) -> (StatusCode, Option<String>, String) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(sid) = session_id {
        builder = builder.header("mcp-session-id", sid);
    }
    let resp = router
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let session_id = resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        session_id,
        String::from_utf8(bytes.to_vec()).unwrap(),
    )
}

fn jsonrpc(method: &str, params: &str, id: i64) -> String {
    format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"{method}","params":{params}}}"#)
}

const INIT_PARAMS: &str = r#"{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}"#;

/// The response body may be JSON or an SSE stream (both are valid streamable
/// HTTP); either way it must contain the JSON-RPC result we look for.
#[tokio::test]
async fn initialize_handshake_over_http() {
    // The session id header is required for subsequent requests, but the
    // handshake itself must succeed standalone.
    let (status, body) = post_mcp(jsonrpc("initialize", INIT_PARAMS, 1)).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert!(
        body.contains("serverInfo") || body.contains("result"),
        "handshake result missing: {body}"
    );
    assert!(!body.contains("error"), "handshake errored: {body}");
}

#[tokio::test]
async fn tools_list_without_initialize_is_rejected() {
    // Streamable HTTP enforces the initialize-first rule per session.
    let (status, body) = post_mcp(jsonrpc("tools/list", "{}", 2)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    assert!(
        body.contains("initialize"),
        "should point at the missing handshake: {body}"
    );
}

#[tokio::test]
async fn initialize_then_tools_list_over_http_session() {
    // Full session flow, as a real remote MCP client (Claude Desktop) does:
    // initialize → capture the assigned session id → tools/list with it.
    // One shared router = one session manager, exactly like one live server.
    let router = router();
    let (status, session_id, body) =
        post_on(router.clone(), jsonrpc("initialize", INIT_PARAMS, 1), None).await;
    assert_eq!(status, StatusCode::OK, "initialize body: {body}");
    assert!(!body.contains("error"), "initialize errored: {body}");
    let session_id = session_id.expect("server must assign an mcp-session-id");

    let (status, _sid, body) =
        post_on(router, jsonrpc("tools/list", "{}", 2), Some(&session_id)).await;
    assert_eq!(status, StatusCode::OK, "tools/list body: {body}");
    assert!(
        body.contains("get_stats") && body.contains("list_tasks"),
        "ChopFlow tool listing missing: {body}"
    );
}

#[tokio::test]
async fn unknown_path_is_not_mcp() {
    let resp = router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/not-mcp")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---- Gateway access token (--access-token) -------------------------------

fn init_payload() -> String {
    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\",\"capabilities\":{},\"clientInfo\":{\"name\":\"test\",\"version\":\"0\"}}}"
        .to_string()
}

#[tokio::test]
async fn access_token_401_without_and_200_with() {
    // No auth configured -> open.
    let open = router();
    let (status, _, _) = post_on(open, init_payload(), None).await;
    assert_eq!(status, StatusCode::OK, "no token configured => open");

    // Token configured -> 401 without it.
    let guarded = router_with_access(Some("mcp-secret"));
    let (status, _, body) = post_on(guarded.clone(), init_payload(), None).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "missing token is rejected: {body}"
    );

    // Authorization: Bearer header -> 200.
    let ok = post_authed(guarded.clone(), init_payload(), "Bearer mcp-secret").await;
    assert_eq!(ok, StatusCode::OK, "Bearer header accepted");

    // ?access_token= query param -> 200.
    let via_query = post_authed(guarded, init_payload(), "Query mcp-secret").await;
    assert_eq!(via_query, StatusCode::OK, "query access_token accepted");
}

async fn post_authed(router: axum::Router, body: String, auth: &str) -> StatusCode {
    let mut builder = Request::builder()
        .method("POST")
        .uri(match auth.split_once(' ').unwrap() {
            ("Query", t) => format!("/mcp?access_token={t}"),
            _ => "/mcp".to_string(),
        })
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some((kind, t)) = auth.split_once(' ') {
        if kind == "Bearer" {
            builder = builder.header("authorization", format!("Bearer {t}"));
        }
    }
    let resp = router
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    resp.status()
}

// ---- Tool plumbing: stages / idempotency_key / checkpoints -----------------

use std::sync::{Arc, Mutex};

/// Spin up a tiny capture broker on an ephemeral port: `POST /api/tasks`
/// records the request body and answers with a canned enqueue response;
/// `GET /api/tasks/:id` serves a canned staged-pipeline task JSON (with
/// `stages`, `idempotency_key`, and `checkpoints`). Returns the broker's
/// base URL and the captured request bodies, so the MCP tool layer can be
/// tested end-to-end without the real broker.
async fn capture_broker() -> (String, Arc<Mutex<Vec<serde_json::Value>>>) {
    let captured: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let seen = captured.clone();

    let app = axum::Router::new()
        .route(
            "/api/tasks",
            axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(body);
                    axum::Json(serde_json::json!({"task_id": "t-1"}))
                }
            }),
        )
        .route(
            "/api/tasks/:id",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({
                    "id": "t-1",
                    "name": "rag.ingest",
                    "status": "running",
                    "stages": ["chunk", "embed", "index"],
                    "idempotency_key": "k-1",
                    "checkpoints": [
                        {
                            "task_id": "t-1",
                            "stage": "chunk",
                            "payload": "{\"chunks\":3}",
                            "recorded_at": 1758163200000i64
                        }
                    ]
                }))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), captured)
}

/// MCP router pointing at the capture broker, sharing one session manager.
fn broker_router(base: String) -> axum::Router {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    chopflow_mcp::http_router(chopflow_mcp::ChopFlowMcp::new(client, base))
}

/// Initialize an MCP session on `router` and return the assigned session id.
async fn initialized_session(router: axum::Router) -> String {
    let (status, session, body) =
        post_on(router, jsonrpc("initialize", INIT_PARAMS, 1), None).await;
    assert_eq!(status, StatusCode::OK, "initialize body: {body}");
    session.expect("server must assign an mcp-session-id")
}

#[tokio::test]
async fn enqueue_task_forwards_stages_and_idempotency_key() {
    let (base, captured) = capture_broker().await;
    let router = broker_router(base);
    let session = initialized_session(router.clone()).await;

    let call = jsonrpc(
        "tools/call",
        r#"{"name":"enqueue_task","arguments":{"name":"rag.ingest","payload":{"document":"..."},"tags":["rag"],"stages":["chunk","embed","index"],"idempotency_key":"doc-42"}}"#,
        2,
    );
    let (status, _, body) = post_on(router, call, Some(&session)).await;
    assert_eq!(status, StatusCode::OK, "tools/call body: {body}");

    let bodies = captured.lock().unwrap();
    assert_eq!(bodies.len(), 1, "exactly one POST /api/tasks expected");
    assert_eq!(bodies[0]["name"], "rag.ingest");
    assert_eq!(
        bodies[0]["stages"],
        serde_json::json!(["chunk", "embed", "index"]),
        "stages must be forwarded in the HTTP body"
    );
    assert_eq!(
        bodies[0]["idempotency_key"], "doc-42",
        "idempotency_key must be forwarded in the HTTP body"
    );
}

#[tokio::test]
async fn enqueue_task_omits_stages_and_key_when_not_given() {
    let (base, captured) = capture_broker().await;
    let router = broker_router(base);
    let session = initialized_session(router.clone()).await;

    let call = jsonrpc(
        "tools/call",
        r#"{"name":"enqueue_task","arguments":{"name":"echo","payload":{"x":1}}}"#,
        2,
    );
    let (status, _, body) = post_on(router, call, Some(&session)).await;
    assert_eq!(status, StatusCode::OK, "tools/call body: {body}");

    let bodies = captured.lock().unwrap();
    assert_eq!(bodies.len(), 1);
    assert!(
        bodies[0].get("stages").is_none(),
        "plain submits must not declare stages: {}",
        bodies[0]
    );
    assert!(
        bodies[0].get("idempotency_key").is_none(),
        "plain submits must not send an idempotency key: {}",
        bodies[0]
    );
}

#[tokio::test]
async fn get_task_returns_stages_and_checkpoints_verbatim() {
    let (base, _captured) = capture_broker().await;
    let router = broker_router(base);
    let session = initialized_session(router.clone()).await;

    let call = jsonrpc(
        "tools/call",
        r#"{"name":"get_task","arguments":{"id":"t-1"}}"#,
        2,
    );
    let (status, _, body) = post_on(router, call, Some(&session)).await;
    assert_eq!(status, StatusCode::OK, "tools/call body: {body}");

    // The tool proxies the broker's task JSON untouched: stages,
    // idempotency_key, and checkpoints must all survive the round trip. (The
    // response may be SSE-wrapped with the JSON escaped, so assert on plain
    // substrings rather than quoted keys.)
    for expected in ["stages", "idempotency_key", "checkpoints", "chunk", "embed"] {
        assert!(
            body.contains(expected),
            "task JSON missing {expected}: {body}"
        );
    }
}
