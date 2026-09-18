//! Tauri commands — the app's surface for the frontend (broker/ui).
//!
//! The frontend detects it is running inside Tauri (`window.__TAURI__`) and
//! uses these instead of assuming a local broker: it asks the app to manage
//! the local broker, list/add/remove remotes, switch connections, and toggle
//! the MCP gateway. The web dashboard (browser) never calls these.

use serde::Serialize;
use tauri::{AppHandle, Emitter, State};

use crate::connections::{ConnectionStore, Remote};
use crate::supervisor::{Supervisor, LOCAL_HTTP_PORT};

/// Shared app state managed by Tauri.
pub struct SharedState {
    pub store: std::sync::Mutex<ConnectionStore>,
    pub supervisor: std::sync::Arc<Supervisor>,
    pub data_dir: std::path::PathBuf,
}

impl SharedState {
    pub(crate) fn with_store<R>(&self, f: impl FnOnce(&mut ConnectionStore) -> R) -> R {
        let mut store = self.store.lock().unwrap();
        f(&mut store)
    }

    pub(crate) fn persist(&self) -> Result<(), String> {
        let snapshot = self.store.lock().unwrap().clone();
        snapshot.save(&self.data_dir)
    }

    /// HTTP base of the active connection ("local" or a remote name).
    async fn active_base(&self, supervisor: &Supervisor) -> String {
        let last = self.store.lock().unwrap().effective_last_used();
        if last == "local" {
            supervisor.local_http_base().await
        } else {
            self.store
                .lock()
                .unwrap()
                .remote(&last)
                .map(|r| r.http_url.trim_end_matches('/').to_string())
                .unwrap_or_else(|| format!("http://127.0.0.1:{LOCAL_HTTP_PORT}"))
        }
    }

    /// Bearer token of the active connection, if the remote requires one.
    async fn active_token(&self) -> Option<String> {
        let last = self.store.lock().unwrap().effective_last_used();
        if last == "local" {
            return None; // managed local broker is never API-token protected
        }
        self.store
            .lock()
            .unwrap()
            .remote(&last)
            .and_then(|r| r.token.clone())
    }
}

/// Everything the frontend needs in one call.
#[derive(Serialize)]
pub struct AppStateDto {
    pub first_run_done: bool,
    pub remotes: Vec<Remote>,
    pub last_used: String,
    pub local: crate::supervisor::LocalBrokerStatus,
    pub local_http_base: String,
    pub mcp_enabled: bool,
    pub mcp_url: Option<String>,
    pub chopflow_binary: Option<String>,
    pub chopflow_version: Option<String>,
    /// Where the app keeps its data (connections.json, chopflow.db, …).
    pub data_dir: String,
    /// Full path of the managed local broker's SQLite database.
    pub db_path: String,
    /// Port the managed local broker listens on for gRPC (workers/CLI).
    pub local_grpc_port: u16,
}

#[tauri::command]
pub async fn app_get_state(
    app: AppHandle,
    state: State<'_, SharedState>,
) -> Result<AppStateDto, String> {
    let supervisor = state.supervisor.clone();
    let (remotes, last_used, first_run_done, mcp_enabled) = state.with_store(|s| {
        (
            s.remotes.clone(),
            s.effective_last_used(),
            s.first_run_done,
            s.mcp_enabled,
        )
    });
    let local = supervisor.status().await;
    let local_http_base = supervisor.local_http_base().await;
    let (_http_port, grpc_port) = supervisor.ports().await;
    let mcp_url = if supervisor.mcp_running() {
        Some(format!(
            "http://127.0.0.1:{}/mcp",
            crate::supervisor::MCP_HTTP_PORT
        ))
    } else {
        None
    };
    let chopflow_binary = Supervisor::resolve_binary()
        .ok()
        .map(|p| p.display().to_string());
    let chopflow_version = match &chopflow_binary {
        Some(bin) => tokio::process::Command::new(bin)
            .arg("--version")
            .output()
            .await
            .ok()
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string()),
        None => None,
    };
    let _ = app; // reserved for future event emission
    Ok(AppStateDto {
        first_run_done,
        remotes,
        last_used,
        local,
        local_http_base,
        mcp_enabled,
        mcp_url,
        chopflow_binary,
        chopflow_version,
        data_dir: state.data_dir.display().to_string(),
        db_path: state.data_dir.join("chopflow.db").display().to_string(),
        local_grpc_port: grpc_port,
    })
}

#[tauri::command]
pub fn app_add_remote(
    state: State<'_, SharedState>,
    name: String,
    http_url: String,
    token: Option<String>,
) -> Result<(), String> {
    let name = name.trim().to_string();
    let http_url = http_url.trim().trim_end_matches('/').to_string();
    if name.is_empty() || http_url.is_empty() {
        return Err("name and http_url are required".into());
    }
    let token = token
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    state.with_store(|s| {
        s.upsert_remote(Remote {
            name,
            http_url,
            token,
        })
    });
    state.persist()
}

#[tauri::command]
pub fn app_remove_remote(state: State<'_, SharedState>, name: String) -> Result<bool, String> {
    let removed = state.with_store(|s| s.remove_remote(&name));
    state.persist()?;
    Ok(removed)
}

#[tauri::command]
pub async fn app_set_last_used(
    state: State<'_, SharedState>,
    connection: String,
) -> Result<String, String> {
    if connection != "local" && !state.with_store(|s| s.remote(&connection).is_some()) {
        return Err(format!("unknown connection: {connection}"));
    }
    state.with_store(|s| s.last_used = Some(connection.clone()));
    state.persist()?;

    // Retarget the MCP gateway at the new active broker if it is running.
    let supervisor = state.supervisor.clone();
    if state.with_store(|s| s.mcp_enabled) && supervisor.mcp_running() {
        let base = state.active_base(&supervisor).await;
        let token = state.active_token().await;
        supervisor.start_mcp(&base, token.as_deref()).await?;
    }
    Ok(connection)
}

#[tauri::command]
pub async fn app_start_local(
    app: AppHandle,
    state: State<'_, SharedState>,
) -> Result<crate::supervisor::LocalBrokerStatus, String> {
    let data_dir = state.data_dir.clone();
    let supervisor = state.supervisor.clone();
    let status = supervisor
        .ensure_started(data_dir, std::process::id())
        .await?;
    // Tell the frontend the broker state changed (e.g. from the tray).
    let _ = app.emit("local-broker-status", &status);
    Ok(status)
}

#[tauri::command]
pub async fn app_stop_local(
    state: State<'_, SharedState>,
) -> Result<crate::supervisor::LocalBrokerStatus, String> {
    let supervisor = state.supervisor.clone();
    Ok(supervisor.stop_broker().await)
}

#[tauri::command]
pub async fn app_set_mcp(
    app: AppHandle,
    state: State<'_, SharedState>,
    enabled: bool,
) -> Result<Option<String>, String> {
    let supervisor = state.supervisor.clone();
    let url = if enabled {
        let base = state.active_base(&supervisor).await;
        // The MCP child points at the active connection; a local base needs
        // the local broker up first.
        let token = state.active_token().await;
        Some(supervisor.start_mcp(&base, token.as_deref()).await?)
    } else {
        supervisor.stop_mcp().await;
        None
    };
    state.with_store(|s| s.mcp_enabled = enabled);
    state.persist()?;
    let _ = app.emit("mcp-status", &url);
    Ok(url)
}

#[tauri::command]
pub fn app_complete_first_run(state: State<'_, SharedState>) -> Result<(), String> {
    state.with_store(|s| s.first_run_done = true);
    state.persist()
}

#[tauri::command]
pub async fn app_get_logs(state: State<'_, SharedState>) -> Result<Vec<String>, String> {
    Ok(state.supervisor.logs().await)
}

/// Destructive: stop managed children, wipe every local data file
/// (connections.json + chopflow.db + prisma-wal), and return to a pristine
/// first-run state. The frontend reloads after invoking.
#[tauri::command]
pub fn app_reset(state: State<'_, SharedState>) -> Result<(), String> {
    state.supervisor.shutdown_blocking();
    for f in [
        "connections.json",
        "connections.json.tmp",
        "chopflow.db",
        "chopflow.db-shm",
        "chopflow.db-wal",
    ] {
        let _ = std::fs::remove_file(state.data_dir.join(f));
    }
    Ok(())
}
