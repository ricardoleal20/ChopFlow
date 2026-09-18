//! Tauri commands — the app's surface for the frontend (broker/ui).
//!
//! The frontend detects it is running inside Tauri (`window.__TAURI__`) and
//! uses these instead of assuming a local broker: it asks the app to manage
//! the local broker, list/add/remove remotes, switch connections, and toggle
//! the MCP gateway. The web dashboard (browser) never calls these.

use serde::Serialize;
use tauri::{AppHandle, Emitter, State};

use crate::connections::{ConnectionStore, LocalToken, Remote};
use crate::supervisor::{Supervisor, LOCAL_HTTP_PORT};

/// Shared app state managed by Tauri.
pub struct SharedState {
    pub store: std::sync::Mutex<ConnectionStore>,
    pub supervisor: std::sync::Arc<Supervisor>,
    /// The Tauri-resolved app-data dir (always the OS default). The data-dir
    /// override pointer lives here so it survives moves of the data itself.
    pub default_data_dir: std::path::PathBuf,
    /// Where the app actually keeps its data today (default or user-chosen).
    pub data_dir: std::sync::Mutex<std::path::PathBuf>,
}

/// File (in the DEFAULT app-data dir) holding the user-chosen data dir.
pub const DATA_DIR_OVERRIDE: &str = "data-dir.txt";

impl SharedState {
    pub(crate) fn with_store<R>(&self, f: impl FnOnce(&mut ConnectionStore) -> R) -> R {
        let mut store = self.store.lock().unwrap();
        f(&mut store)
    }

    /// Resolve the data dir at startup: the OS default, or the user-chosen
    /// dir when a valid override pointer exists.
    pub fn resolve_data_dir(default: std::path::PathBuf) -> std::path::PathBuf {
        let raw = std::fs::read_to_string(default.join(DATA_DIR_OVERRIDE))
            .unwrap_or_default();
        let candidate = std::path::PathBuf::from(raw.trim());
        if candidate.is_absolute() && candidate.is_dir() {
            candidate
        } else {
            default
        }
    }

    /// Current data dir (default or overridden).
    pub fn data_dir(&self) -> std::path::PathBuf {
        self.data_dir.lock().unwrap().clone()
    }

    pub(crate) fn persist(&self) -> Result<(), String> {
        let snapshot = self.store.lock().unwrap().clone();
        snapshot.save(&self.data_dir())
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

    /// Bearer token of the active connection: the first local token (any is
    /// valid) when local is active, or the remote's token.
    async fn active_token(&self) -> Option<String> {
        let last = self.store.lock().unwrap().effective_last_used();
        if last == "local" {
            return self
                .store
                .lock()
                .unwrap()
                .local_tokens
                .first()
                .map(|t| t.token.clone());
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
    /// Labels of the local broker's accepted tokens (values are shown once,
    /// never here).
    pub local_tokens: Vec<String>,
    /// A token value the app itself uses to talk to its local broker (the
    /// first configured one) when the broker is protected.
    pub local_api_token: Option<String>,
    /// Optional token the MCP gateway itself requires (--access-token).
    pub mcp_access_token: Option<String>,
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
    let (local_tokens, local_api_token, mcp_access_token) = {
        let store = state.store.lock().unwrap();
        let ids = store.local_tokens.iter().map(|t| t.id.clone()).collect();
        let first = store.local_tokens.first().map(|t| t.token.clone());
        (ids, first, store.mcp_access_token.clone())
    };
    let data_dir = state.data_dir();
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
        data_dir: data_dir.display().to_string(),
        db_path: data_dir.join("chopflow.db").display().to_string(),
        local_grpc_port: grpc_port,
        local_tokens,
        local_api_token,
        mcp_access_token,
    })
}

/// Move the app's data (connections store + broker database) to a new folder.
/// Stops the managed children, moves the files, records the override pointer
/// (in the OS-default app-data dir, so it survives the move), and points the
/// app at the new dir. The frontend reloads afterwards, which restarts the
/// local broker against the new database.
#[tauri::command]
pub fn app_set_data_dir(state: State<'_, SharedState>, path: String) -> Result<String, String> {
    let new_dir = std::path::PathBuf::from(path.trim());
    if !new_dir.is_absolute() {
        return Err("the data folder must be an absolute path".into());
    }
    std::fs::create_dir_all(&new_dir).map_err(|e| format!("create {new_dir:?}: {e}"))?;

    let old_dir = state.data_dir();
    if old_dir == new_dir {
        return Ok(old_dir.display().to_string());
    }

    // Stop the broker before touching its database.
    state.supervisor.shutdown_blocking();

    for f in [
        "connections.json",
        "connections.json.tmp",
        "chopflow.db",
        "chopflow.db-shm",
        "chopflow.db-wal",
    ] {
        let from = old_dir.join(f);
        let to = new_dir.join(f);
        if from.exists() {
            move_file(&from, &to).map_err(|e| format!("move {f}: {e}"))?;
        }
    }

    // Record the override in the OS-default dir and switch over.
    std::fs::write(
        state.default_data_dir.join(DATA_DIR_OVERRIDE),
        new_dir.display().to_string(),
    )
    .map_err(|e| format!("write data-dir override: {e}"))?;
    *state.data_dir.lock().unwrap() = new_dir.clone();
    Ok(new_dir.display().to_string())
}

/// rename with a copy fallback (the two dirs may live on different volumes).
fn move_file(from: &std::path::Path, to: &std::path::Path) -> Result<(), String> {
    if std::fs::rename(from, to).is_err() {
        std::fs::copy(from, to).map_err(|e| e.to_string())?;
        std::fs::remove_file(from).map_err(|e| e.to_string())?;
    }
    Ok(())
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
        let access = state.with_store(|s| s.mcp_access_token.clone());
        supervisor
            .start_mcp(&base, token.as_deref(), access.as_deref())
            .await?;
    }
    Ok(connection)
}

#[tauri::command]
pub async fn app_start_local(
    app: AppHandle,
    state: State<'_, SharedState>,
) -> Result<crate::supervisor::LocalBrokerStatus, String> {
    let data_dir = state.data_dir();
    let tokens = state.with_store(|s| s.local_tokens.iter().map(|t| t.token.clone()).collect::<Vec<String>>());
    let supervisor = state.supervisor.clone();
    let status = supervisor
        .ensure_started(data_dir, std::process::id(), &tokens)
        .await?;
    // Tell the frontend the broker state changed (e.g. from the tray).
    let _ = app.emit("local-broker-status", &status);
    Ok(status)
}

/// Stop the managed local broker (adopted brokers are left alone).
#[tauri::command]
pub async fn app_stop_local(
    state: State<'_, SharedState>,
) -> Result<crate::supervisor::LocalBrokerStatus, String> {
    let supervisor = state.supervisor.clone();
    Ok(supervisor.stop_broker().await)
}

/// Toggle the persistent MCP over-HTTP gateway, pointed at the active
/// connection. Returns the endpoint URL (or None when disabled).
#[tauri::command]
pub async fn app_set_mcp(
    app: AppHandle,
    state: State<'_, SharedState>,
    enabled: bool,
) -> Result<Option<String>, String> {
    let supervisor = state.supervisor.clone();
    let url = if enabled {
        let base = state.active_base(&supervisor).await;
        let token = state.active_token().await;
        let access = state.with_store(|s| s.mcp_access_token.clone());
        Some(
            supervisor
                .start_mcp(&base, token.as_deref(), access.as_deref())
                .await?,
        )
    } else {
        supervisor.stop_mcp().await;
        None
    };
    state.with_store(|s| s.mcp_enabled = enabled);
    state.persist()?;
    let _ = app.emit("mcp-status", &url);
    Ok(url)
}

/// Mark the first-run gate as done.
#[tauri::command]
pub fn app_complete_first_run(state: State<'_, SharedState>) -> Result<(), String> {
    state.with_store(|s| s.first_run_done = true);
    state.persist()
}

/// Ring-buffer log lines from the supervisor + managed children.
#[tauri::command]
pub async fn app_get_logs(state: State<'_, SharedState>) -> Result<Vec<String>, String> {
    Ok(state.supervisor.logs().await)
}

/// Write the current log ring to a user-chosen path (the frontend picks the
/// path via the native save dialog, then calls this command).
#[tauri::command]
pub async fn app_export_logs(
    state: State<'_, SharedState>,
    path: String,
) -> Result<(), String> {
    let logs = state.supervisor.logs().await;
    std::fs::write(&path, logs.join("\n"))
        .map_err(|e| format!("write {path}: {e}"))
}

/// Add a labelled token the app's local broker accepts. `id` is the
/// operator-chosen identifier (who / what holds this token); `token` is the
/// Bearer value, shown to the user exactly once by the caller. Applies
/// immediately: the managed broker restarts with the new token set.
#[tauri::command]
pub async fn app_add_local_token(
    state: State<'_, SharedState>,
    id: String,
    token: String,
) -> Result<LocalToken, String> {
    let id = id.trim().to_string();
    let token = token.trim().to_string();
    if id.is_empty() {
        return Err("identifier is required (who or what uses this token)".into());
    }
    if token.is_empty() {
        return Err("token value is required".into());
    }
    let dup = state.with_store(|s| s.local_tokens.iter().any(|t| t.id == id));
    if dup {
        return Err(format!("a token labelled \"{id}\" already exists"));
    }
    let entry = LocalToken { id: id.clone(), token };
    state.with_store(|s| s.local_tokens.push(entry.clone()));
    state.persist()?;
    // Restart the managed broker so the new token applies now (adopted
    // brokers are external and intentionally left alone).
    restart_local_broker(&state).await?;
    Ok(entry)
}

/// Revoke one labelled local token by its identifier. Other tokens keep
/// working; the managed broker restarts without it.
#[tauri::command]
pub async fn app_remove_local_token(
    state: State<'_, SharedState>,
    id: String,
) -> Result<(), String> {
    if !state.with_store(|s| {
        let before = s.local_tokens.len();
        s.local_tokens.retain(|t| t.id != id);
        s.local_tokens.len() != before
    }) {
        return Err(format!("no token labelled \"{id}\""));
    }
    state.persist()?;
    restart_local_broker(&state).await?;
    Ok(())
}

/// Restart the managed broker and retarget the MCP gateway after a token
/// change, so the new credential set is live right now.
async fn restart_local_broker(state: &State<'_, SharedState>) -> Result<(), String> {
    let supervisor = state.supervisor.clone();
    if !supervisor.broker_alive() {
        return Ok(());
    }
    let dir = state.data_dir();
    let tokens = state.with_store(|s| s.local_tokens.iter().map(|t| t.token.clone()).collect::<Vec<String>>());
    supervisor.stop_broker().await;
    supervisor.ensure_started(dir, std::process::id(), &tokens).await?;
    if state.with_store(|s| s.mcp_enabled) && supervisor.mcp_running() {
        let base = state.active_base(&supervisor).await;
        let tok = state.active_token().await;
        let access = state.with_store(|s| s.mcp_access_token.clone());
        supervisor
            .start_mcp(&base, tok.as_deref(), access.as_deref())
            .await?;
    }
    Ok(())
}

/// Set (or clear) the token the MCP gateway itself requires. When set, every
/// MCP client must present `Authorization: Bearer <token>`. Applies
/// immediately when the gateway is running (it restarts with the new token);
/// the broker's own auth is unaffected.
#[tauri::command]
pub async fn app_set_mcp_access_token(
    state: State<'_, SharedState>,
    token: Option<String>,
) -> Result<Option<String>, String> {
    let token = token
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    let changed = state.with_store(|s| {
        if s.mcp_access_token == token {
            false
        } else {
            s.mcp_access_token = token.clone();
            true
        }
    });
    if !changed {
        return Ok(token);
    }
    state.persist()?;
    let supervisor = state.supervisor.clone();
    if supervisor.mcp_running() {
        let base = state.active_base(&supervisor).await;
        let api_tok = state.active_token().await;
        supervisor
            .start_mcp(&base, api_tok.as_deref(), token.as_deref())
            .await?;
    }
    Ok(token)
}

/// Non-destructive preview of the first-run welcome: clears only the
/// first_run_done flag so the frontend reloads into the same wizard a fresh
/// install (or Delete-all) shows. Nothing is wiped; "Continue" in the wizard
/// re-persists the flag.
#[tauri::command]
pub fn app_preview_welcome(state: State<'_, SharedState>) -> Result<(), String> {
    state.with_store(|s| s.first_run_done = false);
    state.persist()
}

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
        let _ = std::fs::remove_file(state.data_dir().join(f));
    }
    // Back to the OS-default data dir for the fresh first run.
    let _ = std::fs::remove_file(state.default_data_dir.join(DATA_DIR_OVERRIDE));
    *state.data_dir.lock().unwrap() = state.default_data_dir.clone();
    Ok(())
}
