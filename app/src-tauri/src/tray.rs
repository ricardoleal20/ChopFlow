//! macOS menu bar presence (next to Wi-Fi / battery / clock).
//!
//! A ChopFlow status item with quick actions: open the window, start/stop
//! the local broker, toggle the MCP gateway, quit (which tears down managed
//! children). The window close button hides the window instead of quitting —
//! the tray is the app's home once the window is closed (macOS convention).

use tauri::menu::{Menu, MenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{AppHandle, Emitter, Manager};

/// Build the menu bar item. Called once from setup.
pub fn init(app: &AppHandle) -> tauri::Result<()> {
    let open = MenuItem::with_id(app, "open", "Open ChopFlow", true, None::<&str>)?;
    let start = MenuItem::with_id(app, "start-local", "Start local broker", true, None::<&str>)?;
    let stop = MenuItem::with_id(app, "stop-local", "Stop local broker", true, None::<&str>)?;
    let mcp = MenuItem::with_id(app, "toggle-mcp", "Toggle MCP gateway", true, None::<&str>)?;
    let quit = MenuItem::with_id(app, "quit", "Quit ChopFlow", true, None::<&str>)?;

    let menu = Menu::with_items(app, &[&open, &start, &stop, &mcp, &quit])?;

    let _tray = TrayIconBuilder::with_id("chopflow-tray")
        .menu(&menu)
        .show_menu_on_left_click(true)
        .tooltip("ChopFlow")
        .icon(app.default_window_icon().expect("app icon").clone())
        .on_menu_event(|app, event| match event.id().as_ref() {
            "open" => show_main_window(app),
            "start-local" => spawn_supervisor_action(app, |supervisor, data_dir| async move {
                supervisor
                    .ensure_started(data_dir, std::process::id())
                    .await
            }),
            "stop-local" => spawn_supervisor_action(app, |supervisor, _dir| async move {
                Ok(supervisor.stop_broker().await)
            }),
            "toggle-mcp" => toggle_mcp(app),
            "quit" => app.exit(0), // RunEvent::Exit tears down children
            _ => {}
        })
        .build(app)?;
    Ok(())
}

fn show_main_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.set_focus();
    }
}

/// Run a supervisor action off the menu-event thread, then notify the
/// frontend so the UI reflects tray-initiated changes.
fn spawn_supervisor_action<T, F, Fut>(app: &AppHandle, action: F)
where
    T: serde::Serialize + Clone + 'static,
    F: FnOnce(std::sync::Arc<crate::supervisor::Supervisor>, std::path::PathBuf) -> Fut
        + Send
        + 'static,
    Fut: std::future::Future<Output = Result<T, String>> + Send,
{
    let state = app.state::<crate::commands::SharedState>();
    let supervisor = state.supervisor.clone();
    let data_dir = state.data_dir.clone();
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        match action(supervisor, data_dir).await {
            Ok(result) => {
                let _ = app.emit("local-broker-status", &result);
            }
            Err(e) => {
                let _ = app.emit("supervisor-error", &e);
            }
        }
    });
}

/// Toggle the MCP gateway from the tray, respecting the store's enabled flag.
fn toggle_mcp(app: &AppHandle) {
    let currently_enabled = app
        .state::<crate::commands::SharedState>()
        .supervisor
        .mcp_running();
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        let state = app.state::<crate::commands::SharedState>();
        let supervisor = state.supervisor.clone();
        let result = if currently_enabled {
            supervisor.stop_mcp().await;
            Ok(None::<String>)
        } else {
            // Point the gateway at the active connection. Extract the remote
            // URL before awaiting — the MutexGuard must not live across it.
            let last = state.store.lock().unwrap().effective_last_used();
            let remote_url = state
                .store
                .lock()
                .unwrap()
                .remote(&last)
                .map(|r| r.http_url.trim_end_matches('/').to_string());
            let base = match remote_url {
                Some(url) => url,
                None => supervisor.local_http_base().await, // "local" or dangling remote
            };
            let token = state
                .store
                .lock()
                .unwrap()
                .remote(&last)
                .and_then(|r| r.token.clone());
            supervisor
                .start_mcp(&base, token.as_deref())
                .await
                .map(Some)
        };
        let new_enabled = matches!(result, Ok(Some(_)));
        {
            let mut s = state.store.lock().unwrap();
            s.mcp_enabled = new_enabled;
            let _ = s.save(&state.data_dir);
        }
        match result {
            Ok(url) => {
                let _ = app.emit("mcp-status", &url);
            }
            Err(e) => {
                let _ = app.emit("supervisor-error", &e);
            }
        }
    });
}
