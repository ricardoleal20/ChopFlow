//! macOS menu bar presence + native app menu.
//!
//! The menu bar item (next to Wi-Fi / battery / clock) is a grouped control
//! center: Open · Local broker (Start/Stop) · MCP (Start/Stop + copy endpoint)
//! · Remote brokers (live connected/offline summary, click to switch) · Quit.
//! The macOS app menu (the "ChopFlow" menu when the app is focused) gains a
//! "Settings…" (⌘,) item that opens the in-app Settings drawer.

use tauri::image::Image;
use tauri::menu::{IconMenuItem, Menu, MenuItem, PredefinedMenuItem, Submenu};
use tauri::tray::TrayIconBuilder;
use tauri::{AppHandle, Emitter, Manager};

use crate::commands::SharedState;
use crate::supervisor::{Supervisor, MCP_HTTP_PORT};

const TRAY_ID: &str = "chopflow-tray";

fn icon(data: &'static [u8]) -> Image<'static> {
    Image::from_bytes(data).expect("embedded menu icon is a valid PNG")
}

fn play_icon() -> Image<'static> {
    icon(include_bytes!("../icons/menu-play.png"))
}
fn stop_icon() -> Image<'static> {
    icon(include_bytes!("../icons/menu-stop.png"))
}
fn power_icon() -> Image<'static> {
    icon(include_bytes!("../icons/menu-power.png"))
}

/// Probe a broker's /healthz (blocking, short timeout) for the tray summary.
fn probe_health(base: &str) -> bool {
    reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_millis(400))
        .build()
        .ok()
        .and_then(|c| {
            c.get(format!("{}/healthz", base.trim_end_matches('/')))
                .send()
                .ok()
        })
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

/// Active connection HTTP base + optional token (async, from the store).
async fn active_endpoint(state: &SharedState, sup: &Supervisor) -> (String, Option<String>) {
    let last = state.store.lock().unwrap().effective_last_used();
    let remote = state.store.lock().unwrap().remote(&last).cloned();
    if last == "local" || remote.is_none() {
        return (sup.local_http_base().await, None);
    }
    let r = remote.expect("checked above");
    (r.http_url.trim_end_matches('/').to_string(), r.token)
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

pub fn init(app: &AppHandle) -> tauri::Result<()> {
    setup_app_menu(app)?;
    build_tray(app)?;
    Ok(())
}

/// The native macOS menu bar shown when the app is focused. Built from
/// `Menu::default` (the full standard set: ChopFlow · File · Edit · View ·
/// Window · Help with the predefined items) with our "Settings…" (⌘,)
/// injected right under About in the app submenu. Falls back to a minimal
/// hand-built app menu if the default menu is unavailable.
fn setup_app_menu(app: &AppHandle) -> tauri::Result<()> {
    let settings =
        MenuItem::with_id(app, "menu-settings", "Settings…", true, Some("CmdOrCtrl+,"))?;

    let menu = match Menu::default(app) {
        Ok(m) => {
            // The first submenu is the app menu (titled "ChopFlow").
            if let Some(tauri::menu::MenuItemKind::Submenu(app_sub)) = m.items()?.first() {
                app_sub.insert(&settings, 1)?;
            }
            m
        }
        Err(_) => Menu::with_items(
            app,
            &[
                &PredefinedMenuItem::about(
                    app,
                    None::<&str>,
                    None::<tauri::menu::AboutMetadata>,
                )?,
                &PredefinedMenuItem::separator(app)?,
                &settings,
                &PredefinedMenuItem::separator(app)?,
                &PredefinedMenuItem::hide(app, None::<&str>)?,
                &PredefinedMenuItem::hide_others(app, None::<&str>)?,
                &PredefinedMenuItem::show_all(app, None::<&str>)?,
                &PredefinedMenuItem::separator(app)?,
                &PredefinedMenuItem::quit(app, None::<&str>)?,
            ],
        )?,
    };
    app.set_menu(menu)?;

    // App-menu events have their own handler (the tray menu uses
    // `on_menu_event` on the tray builder).
    app.on_menu_event(|app, event| {
        if event.id().as_ref() == "menu-settings" {
            open_settings(app);
        }
    });
    Ok(())
}

fn build_tray(app: &AppHandle) -> tauri::Result<()> {
    let menu = render_menu(app)?;
    let chop = icon(include_bytes!("../icons/tray-template.png"));
    TrayIconBuilder::with_id(TRAY_ID)
        .icon(chop)
        .icon_as_template(true)
        .menu(&menu)
        .show_menu_on_left_click(true)
        .tooltip("ChopFlow")
        .on_menu_event(handle_tray_event)
        .build(app)?;
    Ok(())
}

/// Rebuild the tray menu from current state (call after any mutation).
pub fn rebuild(app: &AppHandle) {
    let menu = match render_menu(app) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("tray rebuild failed: {e}");
            return;
        }
    };
    if let Some(tray) = app.tray_by_id(TRAY_ID) {
        let _ = tray.set_menu(Some(menu));
    }
}

pub fn show_main_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.set_focus();
    }
}

pub fn open_settings(app: &AppHandle) {
    show_main_window(app);
    let _ = app.emit("open-settings", ());
}

// ---------------------------------------------------------------------------
// Menu rendering
// ---------------------------------------------------------------------------

fn render_menu(app: &AppHandle) -> tauri::Result<Menu<tauri::Wry>> {
    let state = app.state::<SharedState>();
    let sup = state.supervisor.clone();
    let (remotes, active) = {
        let s = state.store.lock().unwrap();
        (s.remotes.clone(), s.effective_last_used())
    };
    let mcp_on = sup.mcp_running();
    let broker_alive = sup.broker_alive();

    let open = MenuItem::with_id(app, "open", "Open", true, None::<&str>)?;

    let start = IconMenuItem::with_id(
        app,
        "local-start",
        "Start broker",
        true,
        Some(play_icon()),
        None::<&str>,
    )?;
    let stop = IconMenuItem::with_id(
        app,
        "local-stop",
        "Stop broker",
        true,
        Some(stop_icon()),
        None::<&str>,
    )?;
    let local = Submenu::with_items(
        app,
        format!("Local broker · {}", if broker_alive { "running" } else { "stopped" }),
        true,
        &[&start, &stop],
    )?;

    let (toggle_id, toggle_label, toggle_icon) = if mcp_on {
        ("mcp-stop", "Stop gateway", stop_icon())
    } else {
        ("mcp-start", "Start gateway", play_icon())
    };
    let mcp_toggle =
        IconMenuItem::with_id(app, toggle_id, toggle_label, true, Some(toggle_icon), None::<&str>)?;
    let copy = MenuItem::with_id(app, "mcp-copy", "Copy endpoint URL", true, None::<&str>)?;
    let mcp = Submenu::with_items(
        app,
        if mcp_on { "MCP · running" } else { "MCP" },
        true,
        &[&mcp_toggle, &copy],
    )?;

    // Remote brokers: live summary; clicking a remote switches to it.
    let remote = if remotes.is_empty() {
        let empty = MenuItem::with_id(
            app,
            "remote-empty",
            "No remote brokers — add in Settings",
            false,
            None::<&str>,
        )?;
        Submenu::with_items(app, "Remote brokers", true, &[&empty])?
    } else {
        let mut items: Vec<Box<dyn tauri::menu::IsMenuItem<tauri::Wry>>> = Vec::new();
        for r in &remotes {
            let health = probe_health(&r.http_url);
            let marker = if health { "●" } else { "○" };
            let status = if health { "connected" } else { "offline" };
            let tick = if active == r.name { " ✓" } else { "" };
            let label = format!("{marker} {} · {}{tick}", r.name, status);
            let item = MenuItem::with_id(app, format!("remote-{}", r.name), label, true, None::<&str>)?;
            items.push(Box::new(item));
        }
        let refs: Vec<&dyn tauri::menu::IsMenuItem<tauri::Wry>> =
            items.iter().map(|b| b.as_ref()).collect();
        Submenu::with_items(app, "Remote brokers", true, &refs)?
    };

    let quit = IconMenuItem::with_id(
        app,
        "quit",
        "Quit ChopFlow",
        true,
        Some(power_icon()),
        None::<&str>,
    )?;

    Menu::with_items(
        app,
        &[
            &open,
            &PredefinedMenuItem::separator(app)?,
            &local,
            &mcp,
            &PredefinedMenuItem::separator(app)?,
            &remote,
            &PredefinedMenuItem::separator(app)?,
            &quit,
        ],
    )
}

// ---------------------------------------------------------------------------
// Menu events
// ---------------------------------------------------------------------------

fn handle_tray_event(app: &AppHandle, event: tauri::menu::MenuEvent) {
    match event.id().as_ref() {
        "open" => show_main_window(app),
        "menu-settings" => open_settings(app),
        "local-start" => {
            let app2 = app.clone();
            let state = app.state::<SharedState>();
            let dir = state.data_dir.clone();
            let sup = state.supervisor.clone();
            tauri::async_runtime::spawn(async move {
                let _ = sup.ensure_started(dir, std::process::id()).await;
                rebuild(&app2);
            });
        }
        "local-stop" => {
            let app2 = app.clone();
            let state = app.state::<SharedState>();
            let sup = state.supervisor.clone();
            tauri::async_runtime::spawn(async move {
                let _ = sup.stop_broker().await;
                rebuild(&app2);
            });
        }
        "mcp-start" => {
            let app2 = app.clone();
            let state = app.state::<SharedState>();
            let sup = state.supervisor.clone();
            tauri::async_runtime::spawn(async move {
                let st = app2.state::<SharedState>();
                let (base, token) = active_endpoint(&st, &sup).await;
                let url = sup.start_mcp(&base, token.as_deref()).await.ok();
                st.with_store(|s| s.mcp_enabled = true);
                let _ = st.persist();
                let _ = app2.emit("mcp-status", &url);
                rebuild(&app2);
            });
        }
        "mcp-stop" => {
            let app2 = app.clone();
            let state = app.state::<SharedState>();
            let sup = state.supervisor.clone();
            tauri::async_runtime::spawn(async move {
                let st = app2.state::<SharedState>();
                sup.stop_mcp().await;
                st.with_store(|s| s.mcp_enabled = false);
                let _ = st.persist();
                rebuild(&app2);
            });
        }
        "mcp-copy" => {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.eval(&format!(
                    "navigator.clipboard.writeText('http://127.0.0.1:{MCP_HTTP_PORT}/mcp')"
                ));
            }
        }
        "remote-empty" => {}
        id if id.starts_with("remote-") => {
            let name = id.trim_start_matches("remote-").to_string();
            let state = app.state::<SharedState>();
            let exists = state.with_store(|s| s.remote(&name).is_some());
            if exists {
                state.with_store(|s| s.last_used = Some(name.clone()));
                let _ = state.persist();
                let _ = app.emit("connection-switched", &name);
                rebuild(app);
            }
        }
        "quit" => app.exit(0),
        _ => {}
    }
}