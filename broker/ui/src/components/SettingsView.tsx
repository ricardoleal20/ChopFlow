// SettingsView.tsx — the app's full-screen settings surface.
//
// Replaces the old right-hand drawer: a separate screen with a "Go back to
// the dashboard" header and its own tabs (Storage · Remote servers · MCP ·
// Logs · Danger zone). Reached from the sidebar (desktop app), the native
// macOS "Settings…" (⌘,) menu item, or the #settings hash in the browser
// (where app-managed sections show a desktop-only notice).

import { useCallback, useEffect, useState } from "react";
import { appGetLogs, appReset, appSetMcp, type AppState } from "../lib/appBridge";
import type { useAppTauri } from "../hooks/useAppTauri";

type Shell = ReturnType<typeof useAppTauri>;

export type SettingsTab = "storage" | "remotes" | "mcp" | "logs" | "danger";

const TABS: { key: SettingsTab; label: string; danger?: boolean }[] = [
  { key: "storage", label: "Storage" },
  { key: "remotes", label: "Remote servers" },
  { key: "mcp", label: "MCP" },
  { key: "logs", label: "Logs" },
  { key: "danger", label: "Danger zone", danger: true },
];

type Props = {
  shell: Shell;
  tab: SettingsTab;
  onTab: (t: SettingsTab) => void;
  onBack: () => void;
};

const Btn = ({
  children,
  onClick,
  disabled,
  variant,
}: {
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  variant?: "primary" | "ghost";
}) => (
  <button
    type="button"
    className={`s-btn${variant === "primary" ? " s-btn-primary" : variant === "ghost" ? " s-btn-ghost" : ""}`}
    onClick={onClick}
    disabled={disabled}
  >
    {children}
  </button>
);

const Row = ({ label, value }: { label: string; value: string }) => (
  <div className="s-row">
    <span className="s-row-label">{label}</span>
    <code className="s-row-value" title={value}>
      {value}
    </code>
  </div>
);

// Sections that talk to the app's control plane (Tauri commands) need the
// desktop shell; in a plain browser tab we say so instead of failing.
const NeedsApp = ({ shell, children }: { shell: Shell; children: React.ReactNode }) =>
  shell.tauri ? (
    <>{children}</>
  ) : (
    <p className="s-hint">
      Only available in the ChopFlow desktop app — this is the web console served by the broker.
    </p>
  );

export default function SettingsView({ shell, tab, onTab, onBack }: Props) {
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [token, setToken] = useState("");
  const [adding, setAdding] = useState(false);
  const [formOpen, setFormOpen] = useState(false);
  const [mcpBusy, setMcpBusy] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [logsTouched, setLogsTouched] = useState(false);

  const loadLogs = useCallback(async () => {
    setLogs(await appGetLogs());
    setLogsTouched(true);
  }, []);

  useEffect(() => {
    if (shell.tauri && tab === "logs") void loadLogs();
  }, [shell.tauri, tab, loadLogs]);

  const state: AppState | null = shell.state;
  const localLabel =
    shell.local?.kind === "running" || shell.local?.kind === "adopted"
      ? `running · ${shell.local.http_port}`
      : shell.local?.kind === "failed"
        ? `failed — ${shell.local.reason}`
        : "stopped";

  const submitRemote = (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim() || !url.trim()) return;
    setAdding(true);
    void shell.addRemote(name.trim(), url.trim(), token.trim() || null).finally(() => {
      setName("");
      setUrl("");
      setToken("");
      setAdding(false);
    });
  };

  return (
    <div className="sv-page">
      <div className="sv-top">
        <div className="sv-topbar">
          <button type="button" className="sv-back" onClick={onBack}>
            <svg
              viewBox="0 0 24 24"
              width={14}
              height={14}
              fill="none"
              stroke="currentColor"
              strokeWidth={2.4}
              strokeLinecap="round"
              strokeLinejoin="round"
              aria-hidden="true"
            >
              <path d="M19 12H5M11 18l-6-6 6-6" />
            </svg>
            Go back to the dashboard
          </button>
          <h1 className="sv-title">Settings</h1>
        </div>
      </div>

      <div className="sv-inner">
        <nav className="sv-tabs" aria-label="Settings sections">
          {TABS.map(({ key, label, danger }) => (
            <button
              key={key}
              type="button"
              className={`sv-tab${tab === key ? " active" : ""}${danger ? " sv-tab-danger" : ""}`}
              onClick={() => onTab(key)}
            >
              {label}
            </button>
          ))}
        </nav>

        {tab === "storage" ? (
          <section className="s-sec">
            <h3>Storage &amp; local broker</h3>
            <NeedsApp shell={shell}>
              <Row label="Data folder" value={state?.data_dir ?? "…"} />
              <Row label="Database" value={state?.db_path ?? "…"} />
              <Row
                label="Local ports"
                value={`gRPC ${state?.local_grpc_port ?? 8000} · HTTP ${shell.local?.kind === "running" || shell.local?.kind === "adopted" ? shell.local.http_port : 8080}`}
              />
              <Row label="Local broker" value={localLabel} />
              <Row label="Binary" value={state?.chopflow_binary ?? "(not found)"} />
              <Row label="Version" value={state?.chopflow_version ?? "…"} />
            </NeedsApp>
          </section>
        ) : tab === "remotes" ? (
          <section className="s-sec">
            <h3>Remote servers</h3>
            <NeedsApp shell={shell}>
              <ul className="s-remotes">
                {(state?.remotes ?? []).map((r) => (
                  <li key={r.name} className="s-remote">
                    <span className="s-rdot" />
                    <span className="s-rname">{r.name}</span>
                    <code className="s-rurl">{r.http_url}</code>
                    <code className="s-rtok">{r.token ? "·".repeat(8) : "no token"}</code>
                    <Btn variant="ghost" onClick={() => void shell.removeRemote(r.name)}>
                      ×
                    </Btn>
                  </li>
                ))}
              </ul>
              {!state?.remotes.length ? (
                <p className="s-hint">No remote servers yet — add one below.</p>
              ) : null}
              <hr className="s-hbar" />
              {formOpen ? (
                <form className="s-add" onSubmit={submitRemote}>
                  <input
                    className="s-in"
                    placeholder="Name"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    aria-label="Remote name"
                  />
                  <input
                    className="s-in s-monow"
                    placeholder="http://host:8080"
                    value={url}
                    onChange={(e) => setUrl(e.target.value)}
                    aria-label="Remote URL"
                  />
                  <input
                    className="s-in s-monow"
                    type="password"
                    placeholder="Token (optional)"
                    value={token}
                    onChange={(e) => setToken(e.target.value)}
                    aria-label="API token"
                  />
                  <Btn variant="primary" disabled={!name.trim() || !url.trim() || adding}>
                    Add
                  </Btn>
                </form>
              ) : (
                <button
                  type="button"
                  className="sv-add-link"
                  onClick={() => setFormOpen(true)}
                  disabled={!shell.tauri}
                >
                  <svg
                    viewBox="0 0 24 24"
                    width={13}
                    height={13}
                    fill="none"
                    stroke="currentColor"
                    strokeWidth={2.5}
                    strokeLinecap="round"
                    aria-hidden="true"
                  >
                    <path d="M12 5v14M5 12h14" />
                  </svg>
                  Add new remote server
                </button>
              )}
            </NeedsApp>
          </section>
        ) : tab === "mcp" ? (
          <section className="s-sec">
            <h3>MCP gateway</h3>
            <NeedsApp shell={shell}>
              <div className="s-mcp">
                <label className="s-toggle">
                  <input
                    type="checkbox"
                    checked={Boolean(state?.mcp_enabled)}
                    disabled={mcpBusy}
                    onChange={async (e) => {
                      setMcpBusy(true);
                      try {
                        await appSetMcp(e.target.checked);
                        window.location.reload();
                      } finally {
                        setMcpBusy(false);
                      }
                    }}
                  />
                  <span />
                </label>
                <span>Persistent MCP over HTTP</span>
              </div>
              {state?.mcp_url ? (
                <p className="s-hint">
                  Endpoint <code>{state.mcp_url}</code> — point Claude Desktop / other MCP clients
                  here.
                </p>
              ) : null}
            </NeedsApp>
          </section>
        ) : tab === "logs" ? (
          <section className="s-sec">
            <h3>Broker logs</h3>
            <NeedsApp shell={shell}>
              <div className="s-loghead">
                <span className="s-hint">
                  {logsTouched ? `${logs.length} lines (ring buffer)` : "…"}
                </span>
                <Btn variant="ghost" onClick={() => void loadLogs()}>
                  Refresh
                </Btn>
              </div>
              <pre className="s-logs">{(logs.length ? logs : ["(no logs yet)"]).join("\n")}</pre>
            </NeedsApp>
          </section>
        ) : (
          <section className="s-sec s-danger">
            <h3>Danger zone</h3>
            <NeedsApp shell={shell}>
              <button
                type="button"
                className="s-btn s-btn-danger"
                onClick={async () => {
                  if (
                    window.confirm(
                      "Delete ALL ChopFlow local information?\n\nThis removes your connections and the local database, then restarts the app fresh.",
                    )
                  ) {
                    await appReset();
                    window.location.reload();
                  }
                }}
              >
                Delete all local information
              </button>
              <p className="s-hint">
                Wipes connections, tokens, and chopflow.db — back to first run.
              </p>
            </NeedsApp>
          </section>
        )}
      </div>
    </div>
  );
}
