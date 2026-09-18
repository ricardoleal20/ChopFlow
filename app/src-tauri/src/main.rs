// ChopFlow desktop app — a Tauri 2 shell around the React operations console
// that additionally SUPERVISES the local ChopFlow stack:
//
//   - spawns/stops the local `chopflow broker start` child (with a
//     `--parent-pid` watchdog so a force-quit never orphans it),
//   - optionally runs the `chopflow mcp --http` gateway,
//   - owns the connection store (local + remotes) and restores the last one,
//   - lives in the macOS menu bar (tray) with quick actions.
//
// The window loads the Vite-built frontend (frontendDist) in release, or the
// dev server (devUrl) in `tauri dev`. All task data still flows through the
// broker's HTTP API, so the desktop app is a first-class client of the same
// broker a web browser would talk to — nothing is forked or re-implemented.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;
mod connections;
mod supervisor;
mod tray;

use tauri::Manager;

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            commands::app_get_state,
            commands::app_add_remote,
            commands::app_remove_remote,
            commands::app_set_last_used,
            commands::app_start_local,
            commands::app_stop_local,
            commands::app_set_mcp,
            commands::app_complete_first_run,
            commands::app_get_logs,
            commands::app_set_data_dir,
            commands::app_add_local_token,
            commands::app_remove_local_token,
            commands::app_reset,
        ])
        .setup(|app| {
            // Regular activation policy: the app is a normal windowed app
            // with a menu-bar presence. Without this the macOS menu bar may
            // never show the ChopFlow menus even when the window is focused.
            #[cfg(target_os = "macos")]
            let _ = app.set_activation_policy(tauri::ActivationPolicy::Regular);

            // App-data dir (~/Library/Application Support/io.chopflow.ops),
            // or the user-chosen dir when a valid override pointer exists.
            let default_data_dir = app
                .path()
                .app_data_dir()
                .expect("app data dir must resolve");
            std::fs::create_dir_all(&default_data_dir).ok();
            let data_dir = commands::SharedState::resolve_data_dir(default_data_dir.clone());
            std::fs::create_dir_all(&data_dir).ok();

            let store = connections::ConnectionStore::load(&data_dir);
            app.manage(commands::SharedState {
                store: std::sync::Mutex::new(store),
                supervisor: std::sync::Arc::new(supervisor::Supervisor::new()),
                default_data_dir,
                data_dir: std::sync::Mutex::new(data_dir),
            });

            // Menu bar presence. Failure is non-fatal: the app still works
            // as a plain window, we just log it.
            if let Err(e) = tray::init(app.handle()) {
                eprintln!("tray init failed (app continues without it): {e}");
            }
            Ok(())
        })
        .on_window_event(|window, event| {
            // macOS convention: the close button hides the window; the app
            // keeps running in the menu bar. Quit via the tray or ⌘Q.
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                let _ = window.hide();
                api.prevent_close();
            }
        })
        .build(tauri::generate_context!())
        .expect("error while building ChopFlow app")
        .run(|app, event| {
            if let tauri::RunEvent::Exit = event {
                // Tear down managed children. Adopted (external) brokers are
                // left alone by stop_broker.
                if let Some(state) = app.try_state::<commands::SharedState>() {
                    let supervisor = state.supervisor.clone();
                    tauri::async_runtime::block_on(async move {
                        supervisor.shutdown().await;
                    });
                }
            }
        });
}
