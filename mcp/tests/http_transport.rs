//! Integration tests for the streamable-HTTP MCP transport (`--http`).
//!
//! Drives the axum router directly (no process spawn): the MCP initialize
//! handshake and a tools/list call over HTTP POST /mcp, exactly what a remote
//! MCP client (Claude Desktop) does against the gateway URL.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

fn router() -> axum::Router {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    chopflow_mcp::http_router(chopflow_mcp::ChopFlowMcp::new(
        client,
        "http://127.0.0.1:1", // unreachable on purpose: handshake/list never call the broker
    ))
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
