// appBridge.ts — the desktop (Tauri) half of the app's control plane.
//
// The same React dashboard is served to a browser (where it talks to whatever
// broker the page is hosted by) and inside the Tauri shell. In the shell, the
// Rust side supervises the local broker, owns the connection store, and runs
// the optional MCP gateway — this module bridges to it via `invoke`, guiding
// the welcome/loading gate and connection switching.
//
// In a browser these calls never happen: `isTauri()` gates them, and the
// helpers return sensible browser-mode values.

import { invoke } from "@tauri-apps/api/core";

export const isTauri = (): boolean =>
  typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

export type Remote = { name: string; http_url: string; token?: string | null };

export type LocalTokenCreated = { id: string; token: string };

export type LocalStatus =
  | { kind: "running"; http_port: number; grpc_port: number; pid?: number }
  | { kind: "adopted"; http_port: number; grpc_port: number }
  | { kind: "stopped" }
  | { kind: "failed"; reason: string };

export type AppState = {
  first_run_done: boolean;
  remotes: Remote[];
  last_used: string;
  local: LocalStatus;
  local_http_base: string;
  mcp_enabled: boolean;
  mcp_url: string | null;
  chopflow_binary: string | null;
  chopflow_version: string | null;
  data_dir: string;
  db_path: string;
  local_grpc_port: number;
  /** Labels of the local broker's accepted tokens (values show once only). */
  local_tokens: string[];
  /** Token value the app itself uses against its local broker. */
  local_api_token: string | null;
};

export async function appGetState(): Promise<AppState> {
  return invoke("app_get_state");
}

export async function appStartLocal(): Promise<LocalStatus> {
  return invoke("app_start_local");
}

export async function appStopLocal(): Promise<LocalStatus> {
  return invoke("app_stop_local");
}

export async function appAddRemote(
  name: string,
  httpUrl: string,
  token?: string | null,
): Promise<void> {
  return invoke("app_add_remote", { name, httpUrl, token: token || null });
}

export async function appRemoveRemote(name: string): Promise<boolean> {
  return invoke("app_remove_remote", { name });
}

export async function appSetLastUsed(connection: string): Promise<string> {
  return invoke("app_set_last_used", { connection });
}

export async function appSetMcp(enabled: boolean): Promise<string | null> {
  return invoke("app_set_mcp", { enabled });
}

export async function appCompleteFirstRun(): Promise<void> {
  return invoke("app_complete_first_run");
}

export async function appGetLogs(): Promise<string[]> {
  return invoke("app_get_logs");
}

/// Move the app's data (connections + broker db) to a new folder. The app
/// reloads afterwards; the broker restarts against the new database.
export async function appSetDataDir(path: string): Promise<string> {
  return invoke("app_set_data_dir", { path });
}

/// Add a labelled token the local broker accepts. Returns {id, token} for
/// the one-time display; the value is never shown again afterwards. The
/// managed broker restarts with the new credential set.
export async function appAddLocalToken(id: string, token: string): Promise<LocalTokenCreated> {
  return invoke("app_add_local_token", { id, token });
}

/// Revoke a labelled local token by its identifier; other tokens keep
/// working. The managed broker restarts without it.
export async function appRemoveLocalToken(id: string): Promise<void> {
  return invoke("app_remove_local_token", { id });
}

/// Destructive reset: wipe local data (connections + db) and restart first-run.
export async function appReset(): Promise<void> {
  return invoke("app_reset");
}

/// Local broker HTTP base from a status, or null when not serving.
export function localHttpBase(status: LocalStatus): string | null {
  switch (status.kind) {
    case "running":
    case "adopted":
      return `http://127.0.0.1:${status.http_port}`;
    default:
      return null;
  }
}
