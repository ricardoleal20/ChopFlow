// SettingsDrawer.tsx — desktop-only app settings (Tauri shell).
//
// Answers "where does my stuff live?" and gives the connection/MCP/log
// controls the spec promised: data dir + db path + local ports + binary info,
// a connection manager (add/remove remote with optional Bearer token), the MCP
// gateway toggle, and a ring-buffer log viewer. Web dashboard never renders it.

import { useCallback, useEffect, useState } from "react";
import { appGetLogs, appSetMcp, type AppState } from "../lib/appBridge";
import type { useAppTauri } from "../hooks/useAppTauri";

type Shell = ReturnType<typeof useAppTauri>;

type Props = {
  shell: Shell;
  onClose: () => void;
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

export default function SettingsDrawer({ shell, onClose }: Props) {
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [token, setToken] = useState("");
  const [adding, setAdding] = useState(false);
  const [mcpBusy, setMcpBusy] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [logsTouched, setLogsTouched] = useState(false);

  const loadLogs = useCallback(async () => {
    setLogs(await appGetLogs());
    setLogsTouched(true);
  }, []);

  useEffect(() => {
    void loadLogs();
  }, [loadLogs]);

  const state: AppState | null = shell.state;
  const localLabel =
    shell.local?.kind === "running" || shell.local?.kind === "adopted"
      ? `running · ${shell.local.http_port}`
      : shell.local?.kind === "failed"
        ? `failed — ${shell.local.reason}`
        : "stopped";

  return (
    <div className="s-backdrop" onClick={onClose}>
      <aside className="s-panel" onClick={(e) => e.stopPropagation()}>
        <header className="s-head">
          <h2>ChopFlow · Settings</h2>
          <button type="button" className="s-x" onClick={onClose} aria-label="Close settings">
            <svg
              viewBox="0 0 24 24"
              width={12}
              height={12}
              fill="none"
              stroke="currentColor"
              strokeWidth="3"
              strokeLinecap="round"
              aria-hidden="true"
            >
              <path d="M18 6 6 18M6 6l12 12" />
            </svg>
          </button>
        </header>

        <div className="s-body">
          <section className="s-sec">
            <h3>Storage &amp; local broker</h3>
            <Row label="Data folder" value={state?.data_dir ?? "…"} />
            <Row label="Database" value={state?.db_path ?? "…"} />
            <Row
              label="Local ports"
              value={`gRPC ${state?.local_grpc_port ?? 8000} · HTTP ${shell.local?.kind === "running" || shell.local?.kind === "adopted" ? shell.local.http_port : 8080}`}
            />
            <Row label="Local broker" value={localLabel} />
            <Row label="Binary" value={state?.chopflow_binary ?? "(not found)"} />
            <Row label="Version" value={state?.chopflow_version ?? "…"} />
          </section>

          <section className="s-sec">
            <h3>Connections</h3>
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
              <p className="s-hint">No remotes yet — add one from the Welcome or here.</p>
            ) : null}
            <form
              className="s-add"
              onSubmit={(e) => {
                e.preventDefault();
                if (!name.trim() || !url.trim()) return;
                setAdding(true);
                void shell.addRemote(name.trim(), url.trim(), token.trim() || null).finally(() => {
                  setName("");
                  setUrl("");
                  setToken("");
                  setAdding(false);
                });
              }}
            >
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
          </section>

          <section className="s-sec">
            <h3>MCP gateway</h3>
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
          </section>

          <section className="s-sec">
            <h3>Broker logs</h3>
            <div className="s-loghead">
              <span className="s-hint">
                {logsTouched ? `${logs.length} lines (ring buffer)` : "…"}
              </span>
              <Btn variant="ghost" onClick={() => void loadLogs()}>
                Refresh
              </Btn>
            </div>
            <pre className="s-logs">{(logs.length ? logs : ["(no logs yet)"]).join("\n")}</pre>
          </section>
        </div>
      </aside>
    </div>
  );
}
