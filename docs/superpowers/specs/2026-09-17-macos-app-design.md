# ChopFlow macOS App — Design Spec

Date: 2026-09-17
Status: Approved (design approved in conversation; user directed to proceed to build)

## Goal

A native macOS app (`app/src-tauri`, Tauri 2) that is the ChopFlow operations
console: it **starts and supervises a local broker**, optionally runs a
**persistent MCP gateway (HTTP)**, and connects to **remote brokers** — with
live switching between them. It reuses the existing React dashboard
(`broker/ui`) as its UI and reuses the shipped `chopflow` binaries as its
engines. The app crate stays out of the core Rust workspace (no gRPC
toolchain; the app talks HTTP only).

## Non-goals (v1)

- No embedded/in-process broker (spawn binary instead — crash isolation,
  version decoupling, respects the app crate's no-toolchain boundary).
- No auto-restart policies beyond notify + manual restart.
- No spawning workers from the app.
- No code signing / notarization / autoupdate (unsigned DMG first).
- No broker federation or cross-broker state sharing (environments v0.1.4
  already covers identity + catalogs).

## Architecture

The app grows from a passive shell into a **process supervisor + connection
manager**. Three repo-level changes:

| Piece | Change |
|---|---|
| `app/src-tauri` | Passive shell → supervisor: Tauri commands + tokio child-process management + tray icon + connection store |
| `chopflow broker start` | New `--parent-pid <pid>` watchdog flag: the broker self-terminates if the parent process dies (covers app force-quit) |
| `chopflow mcp` | New HTTP transport alongside stdio: `chopflow mcp --http <bind>` |

## 1 · Welcome / first-run page

The first thing the app shows — before the dashboard — while the interfaces
load, and the place for initial configuration.

**Sequence (progressive loading states, honest and live):**

1. App boot — brand mark + "ChopFlow" wordmark.
2. "Starting local broker…" (spawn → `/healthz` 200 gate; shows the actual
   step: spawning, waiting for health).
3. "Checking connections…" (probing saved remotes' `/healthz` in parallel).
4. "Loading dashboard…" then transition in.

**First-run configuration (only when no saved connections exist):**

- Choice: **"Start local"** (default — local broker only, nothing else needed)
  or **"Connect to a remote"** (add name + HTTP URL; optional, repeatable).
- Skippable: "Add later" → local-only. Remotes can always be added later from
  the switcher/settings.

**Subsequent runs:** the loading sequence still shows (broker start + remote
probe), but configuration is skipped — the app restores the last-used
connection.

Design (layout, motion, copy) is produced via OpenDesign and integrated as a
React view in `broker/ui` gated on `isTauri` + first-run state.

## 2 · Connection model & switching

- **App-owned connection store**, persisted (JSON in Tauri app-data dir):
  `{ remotes: [{name, http_url}], lastUsed: "local" | <remote name>, mcpEnabled, firstRunDone }`.
- **"Local"** is a built-in managed connection (not user-editable like remotes).
- **Switching** reuses the v0.1.4 mechanism: `setApiBase(url)` + invalidate all
  queries. The local broker **keeps running** while viewing a remote; switching
  back is instant.
- **Launch behavior**: restore `lastUsed`. If local → ensure the broker is
  running first (ready gate), then show the dashboard.
- **Port conflict (adopt-vs-bind)**: if something already listens on the local
  HTTP port, probe `/healthz` + `/api/stats` — if it is a ChopFlow broker,
  **adopt** it (connect, badge "external", do not kill on quit); otherwise pick
  the next free port and spawn there.

## 3 · Local broker lifecycle (supervisor)

- **Start**: spawn the `chopflow` binary with fixed args:
  `broker start --host 127.0.0.1 --port 8000 --http-port 8080 --storage sqlite --db-path <app-data>/chopflow.db --env local --region local --parent-pid <app pid>`.
- **Binary resolution**: prefer `chopflow` on PATH (respects `brew upgrade`);
  fall back to the sidecar bundled in the .app. Settings shows which
  binary + version is in use.
- **Ready gate**: poll `GET /healthz` until 200 (bounded timeout → error with
  retry).
- **Monitor**: child exit → notification + restart button (no auto-restart v1).
- **Quit**: SIGTERM the child on app exit; `--parent-pid` watchdog covers
  force-quit. SQLite is durable → no data loss; `Running` tasks reconcile to
  `Queued` on next start.
- **Logs**: in-memory ring buffer (last ~2k lines) exposed via command; simple
  viewer in settings.
- Adopted (external) brokers are never killed by the app.

## 4 · MCP gateway (optional, HTTP)

- `chopflow mcp` gains a streamable-HTTP transport: `chopflow mcp --http 127.0.0.1:8810 --broker <url>` serves MCP at `/mcp` (same tool set as stdio; stdio stays the default when `--http` is absent).
- App toggle in settings. ON → spawn as a supervised child pointing at the
  **active** connection's HTTP URL (local or remote); the UI shows the URL +
  copy-to-clipboard config snippet for Claude Desktop (remote MCP server).
  OFF → kill the child.
- Switching connections while the gateway is on → restart the child against
  the new broker URL.

## 5 · Menu bar presence (tray)

A ChopFlow status item in the macOS menu bar (next to Wi-Fi/battery/clock)
via Tauri 2 `tray-icon`:

- Icon: the ChopFlow mark (template image, adapts to light/dark menu bar).
- Tooltip / first line: current state — `ChopFlow · local (running)`,
  `ChopFlow · prod (remote)`, `ChopFlow · local (stopped)`.
- Menu: Open ChopFlow · Start/Stop local broker · MCP gateway toggle ·
  Quit (stops managed children).
- The app keeps running when the window is closed (macOS convention);
  Quit from the tray or ⌘Q exits and tears down children.

## 6 · Sidecar & distribution

- Bundle `chopflow` as a Tauri `externalBin` sidecar (aarch64 + x86_64) in the
  .app; PATH resolution takes precedence (see §3).
- v1: unsigned `.app` + DMG (`targets: "app"` today; add `dmg`).
- `beforeBuildCommand` builds the UI in tauri mode; release cadence manual
  (`pnpm --dir broker/ui tauri build`).

## 7 · UI integration (frontend)

All additive, gated on `isTauri` (`window.__TAURI__`); the web dashboard is
unchanged and keeps using `/api/environments`.

- **TopBar switcher**: lists `Local` (status dot: running / stopped / external)
  + app-store remotes; selection drives the same `setApiBase` flow.
- **Broker status pill**: connection name, env · region (from `/api/stats`),
  binary version.
- **Settings**: connections manager (add/edit/remove remotes), MCP gateway
  panel, local broker controls (db path, ports, binary info, logs viewer).
- **Welcome page** (§1) as the pre-dashboard gate.

## 8 · Testing

- **broker**: `--parent-pid` integration test (spawn child pointed at a
  short-lived parent; assert the child exits when the parent dies).
- **mcp**: HTTP transport test (initialize handshake + one tool call over
  HTTP against a test broker).
- **app crate**: unit tests for the connection store (save/load/migrate), the
  spawn-args builder, and the adopt-vs-bind probe decision (mocked listeners).
- **E2E manual checklist** (`tauri dev`): fresh first-run → welcome → local
  up; add remote → switch → switch back; kill broker (child exit →
  notification); MCP toggle on → Claude Desktop config copy; tray menu
  actions; quit tears down children; force-quit app → watchdog kills broker.

## 9 · Roadmap (post-v1)

Auto-restart with backoff · spawning local workers from the app ·
notarization + autoupdate · Sparkle-style updates · metrics panel.
