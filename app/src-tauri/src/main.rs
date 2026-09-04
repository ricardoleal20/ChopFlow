// ChopFlow desktop shell — a thin Tauri 2 wrapper around the React operations
// console. The window loads the Vite-built frontend (frontendDist) in release,
// or the dev server (devUrl) in `tauri dev`. All data still flows through the
// broker's HTTP API at http://127.0.0.1:8080 (set via VITE_API_BASE in
// .env.tauri), so the desktop app is a first-class client of the same broker a
// web browser would talk to — nothing is forked or re-implemented here.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    tauri::Builder::default()
        .run(tauri::generate_context!())
        .expect("error while running ChopFlow desktop app");
}
