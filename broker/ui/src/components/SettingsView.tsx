// SettingsView.tsx — the app's full-screen settings surface.
//
// Replaces the old right-hand drawer: a separate screen with a "Go back to
// the dashboard" header and its own tabs (Storage · Remote servers · MCP ·
// Security · Logs · Danger zone). Reached from the sidebar (desktop app), the native
// macOS "Settings…" (⌘,) menu item, or the #settings hash in the browser
// (where app-managed sections show a desktop-only notice).

import { useCallback, useEffect, useState } from "react";
import {
  appAddLocalToken,
  appExportLogs,
  appGetLogs,
  appPreviewWelcome,
  appRemoveLocalToken,
  appReset,
  appSetAuthEnabled,
  appSetDataDir,
  appSetMcp,
  appSetMcpAccessToken,
  appSetMcpAuthEnabled,
  type AppState,
  type LocalTokenCreated,
} from "../lib/appBridge";
import type { useAppTauri } from "../hooks/useAppTauri";

type Shell = ReturnType<typeof useAppTauri>;

export type SettingsTab = "storage" | "remotes" | "mcp" | "security" | "logs" | "danger";

const TABS: { key: SettingsTab; label: string; danger?: boolean }[] = [
  { key: "storage", label: "Storage" },
  { key: "remotes", label: "Remote servers" },
  { key: "mcp", label: "MCP" },
  { key: "security", label: "Security" },
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
  type = "button",
  className,
}: {
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  variant?: "primary" | "ghost" | "danger";
  type?: "button" | "submit";
  className?: string;
}) => (
  <button
    type={type}
    className={`s-btn${variant === "primary" ? " s-btn-primary" : variant === "ghost" ? " s-btn-ghost" : variant === "danger" ? " s-btn-danger" : ""}${className ? ` ${className}` : ""}`}
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
  const [logsCopied, setLogsCopied] = useState(false);
  const [logsSaved, setLogsSaved] = useState(false);
  const [dirEdit, setDirEdit] = useState(false);
  const [dirPath, setDirPath] = useState("");
  const [dirBusy, setDirBusy] = useState(false);
  const [tokId, setTokId] = useState("");
  const [tokValue, setTokValue] = useState("");
  const [tokBusy, setTokBusy] = useState(false);
  const [justCreated, setJustCreated] = useState<LocalTokenCreated | null>(null);
  const [copied, setCopied] = useState(false);
  // Two-step confirmations. window.confirm is unsupported in the Tauri
  // webview (it silently returns false), so destructive actions confirm
  // inline instead.
  const [confirming, setConfirming] = useState<{ kind: "revoke"; id: string } | null>(null);
  const [dirConfirm, setDirConfirm] = useState(false);
  const [dangerConfirm, setDangerConfirm] = useState(false);
  const [previewConfirm, setPreviewConfirm] = useState(false);
  const [secBusy, setSecBusy] = useState(false);
  const [mcpTokVal, setMcpTokVal] = useState("");
  const [mcpTokBusy, setMcpTokBusy] = useState(false);
  const [mcpTokShow, setMcpTokShow] = useState(false);
  const [mcpJustSet, setMcpJustSet] = useState<string | null>(null);
  const [mcpJustCopied, setMcpJustCopied] = useState(false);

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

  // Security switch: flips whether the broker REQUIRES tokens. It never
  // creates or deletes tokens — they are only enforced (or not).
  const toggleAuth = async () => {
    setSecBusy(true);
    try {
      await appSetAuthEnabled(!state?.auth_enabled);
      await shell.reloadState();
    } finally {
      setSecBusy(false);
    }
  };

  // MCP access switch: flips whether the gateway enforces its token. Never
  // creates or deletes the token — it is only enforced (or not).
  const toggleMcpAccess = async () => {
    setMcpTokBusy(true);
    try {
      await appSetMcpAuthEnabled(!state?.mcp_access_enabled);
      await shell.reloadState();
    } finally {
      setMcpTokBusy(false);
    }
  };

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
              {dirEdit ? (
                <form
                  className="s-add"
                  onSubmit={(e) => {
                    e.preventDefault();
                    if (!dirPath.trim() || dirBusy) return;
                    if (!dirConfirm) {
                      setDirConfirm(true);
                      return;
                    }
                    setDirConfirm(false);
                    setDirBusy(true);
                    void appSetDataDir(dirPath.trim())
                      .then(() => window.location.reload())
                      .catch((err: unknown) => {
                        window.alert(String(err));
                        setDirBusy(false);
                      });
                  }}
                >
                  <input
                    className="s-in s-monow"
                    placeholder="/absolute/path/to/folder"
                    value={dirPath}
                    onChange={(e) => setDirPath(e.target.value)}
                    aria-label="New data folder"
                    autoFocus
                  />
                  <Btn
                    variant="ghost"
                    disabled={dirBusy}
                    onClick={() => {
                      if (!shell.tauri) return;
                      void (async () => {
                        const { open } = await import("@tauri-apps/plugin-dialog");
                        const sel = await open({ directory: true, multiple: false });
                        if (typeof sel === "string") setDirPath(sel);
                      })();
                    }}
                  >
                    Browse…
                  </Btn>
                  <Btn type="submit" variant="primary" disabled={!dirPath.trim() || dirBusy}>
                    {dirConfirm ? "Confirm move" : "Move data"}
                  </Btn>
                  {dirConfirm ? (
                    <Btn variant="ghost" disabled={dirBusy} onClick={() => setDirConfirm(false)}>
                      Cancel
                    </Btn>
                  ) : null}
                  <Btn
                    variant="ghost"
                    onClick={() => {
                      setDirEdit(false);
                      setDirPath("");
                    }}
                  >
                    Cancel
                  </Btn>
                </form>
              ) : (
                <button
                  type="button"
                  className="sv-add-link"
                  onClick={() => {
                    setDirPath(state?.data_dir ?? "");
                    setDirEdit(true);
                  }}
                >
                  <svg
                    viewBox="0 0 24 24"
                    width={13}
                    height={13}
                    fill="none"
                    stroke="currentColor"
                    strokeWidth={2}
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    aria-hidden="true"
                  >
                    <path d="M12 20h9M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4Z" />
                  </svg>
                  Change data folder
                </button>
              )}
              <p className="s-hint">
                Connections, tokens, and the local broker database live here. Changing it moves
                everything and restarts the broker.
              </p>
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
                      Remove
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
                  <Btn
                    type="submit"
                    variant="primary"
                    disabled={!name.trim() || !url.trim() || adding}
                  >
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
                        await shell.reloadState();
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

              <h3 style={{ marginTop: 18 }}>Access token</h3>
              <div className="s-mcp">
                <label className="s-toggle">
                  <input
                    type="checkbox"
                    checked={Boolean(state?.mcp_access_enabled)}
                    disabled={mcpTokBusy}
                    onChange={() => void toggleMcpAccess()}
                  />
                  <span />
                </label>
                <span>
                  {state?.mcp_access_enabled
                    ? "Requires token to connect"
                    : "Open — anyone that connects can use it"}
                </span>
              </div>
              <p className="s-hint" style={{ marginBottom: 10 }}>
                {state?.mcp_access_enabled
                  ? state.mcp_access_token
                    ? "The gateway is protected: every MCP client must send the token as Authorization: Bearer."
                    : "Security is on, but there is no access token yet — set one below. Until then the endpoint stays open."
                  : state?.mcp_access_token
                    ? "Security is off for the gateway: the stored token is kept but not required. Flip the switch on to enforce it."
                    : "The gateway is open. Set an access token below (independent from the broker's tokens), then flip the switch."}
              </p>
              {mcpJustSet ? (
                <div className="sv-token-once" role="status">
                  <div className="sv-token-once-title">MCP access token — shown once</div>
                  <p className="sv-token-once-legend">
                    This token will only show once, please store it somewhere safe. Every MCP client
                    must present it (Authorization: Bearer).
                  </p>
                  <code className="sv-token-once-value">{mcpJustSet}</code>
                  <div className="sv-token-once-actions">
                    <Btn
                      variant="primary"
                      onClick={() => {
                        void navigator.clipboard?.writeText(mcpJustSet).then(() => {
                          setMcpJustCopied(true);
                          window.setTimeout(() => setMcpJustCopied(false), 1400);
                        });
                      }}
                    >
                      {mcpJustCopied ? "Copied!" : "Copy token"}
                    </Btn>
                    <Btn variant="ghost" onClick={() => setMcpJustSet(null)}>
                      Done
                    </Btn>
                  </div>
                </div>
              ) : null}

              <Row
                label="Status"
                value={
                  state?.mcp_access_enabled
                    ? state.mcp_access_token
                      ? "protected — token required"
                      : "security on (no token yet)"
                    : "open — security off"
                }
              />
              {state?.mcp_access_token ? (
                <div className="s-row">
                  <span className="s-row-label">Token</span>
                  <code className="s-row-value">
                    {mcpTokShow ? state.mcp_access_token : "·".repeat(24)}
                  </code>
                  <Btn variant="ghost" onClick={() => setMcpTokShow((v) => !v)}>
                    {mcpTokShow ? "Hide" : "Show"}
                  </Btn>
                  <Btn
                    variant="ghost"
                    disabled={mcpTokBusy}
                    onClick={() => {
                      setMcpTokBusy(true);
                      const fresh = `chopflow-mcp-${crypto.randomUUID?.() ?? Math.random().toString(36).slice(2)}-${Date.now().toString(36)}`;
                      void appSetMcpAccessToken(fresh)
                        .then((kept) => {
                          setMcpJustSet(kept);
                          return shell.reloadState();
                        })
                        .catch((err: unknown) => window.alert(String(err)))
                        .finally(() => setMcpTokBusy(false));
                    }}
                  >
                    Regenerate
                  </Btn>
                  <Btn
                    variant="ghost"
                    disabled={mcpTokBusy}
                    onClick={() => {
                      setMcpTokBusy(true);
                      void appSetMcpAccessToken(null)
                        .then(() => shell.reloadState())
                        .catch((err: unknown) => window.alert(String(err)))
                        .finally(() => setMcpTokBusy(false));
                    }}
                  >
                    Remove
                  </Btn>
                </div>
              ) : null}

              <form
                className="s-add"
                onSubmit={(e) => {
                  e.preventDefault();
                  const value = mcpTokVal.trim();
                  if (mcpTokBusy || !value) return;
                  setMcpTokBusy(true);
                  void appSetMcpAccessToken(value)
                    .then((kept) => {
                      setMcpJustSet(kept);
                      setMcpTokVal("");
                      return shell.reloadState();
                    })
                    .catch((err: unknown) => window.alert(String(err)))
                    .finally(() => setMcpTokBusy(false));
                }}
              >
                <input
                  className="s-in s-monow"
                  placeholder="Paste a token, or generate one"
                  value={mcpTokVal}
                  onChange={(e) => setMcpTokVal(e.target.value)}
                  aria-label="MCP access token"
                />
                <Btn
                  variant="ghost"
                  disabled={mcpTokBusy}
                  onClick={() =>
                    setMcpTokVal(
                      `chopflow-mcp-${crypto.randomUUID?.() ?? Math.random().toString(36).slice(2)}-${Date.now().toString(36)}`,
                    )
                  }
                >
                  Generate
                </Btn>
                <Btn type="submit" variant="primary" disabled={!mcpTokVal.trim() || mcpTokBusy}>
                  Set token
                </Btn>
              </form>
              <p className="s-hint">
                When set, every MCP client (Claude Desktop, Cursor…) must send this as
                Authorization: Bearer. Independent from the broker's own tokens.
              </p>
            </NeedsApp>
          </section>
        ) : tab === "security" ? (
          <section className="s-sec">
            <h3>Local broker tokens</h3>
            <NeedsApp shell={shell}>
              <div className="s-mcp">
                <label className="s-toggle">
                  <input
                    type="checkbox"
                    checked={Boolean(state?.auth_enabled)}
                    disabled={secBusy}
                    onChange={() => void toggleAuth()}
                  />
                  <span />
                </label>
                <span>
                  {state?.auth_enabled
                    ? "Secured — a token is required to connect"
                    : "Open — anyone can connect"}
                </span>
              </div>
              <p className="s-hint" style={{ marginBottom: 10 }}>
                {state?.auth_enabled
                  ? state.local_tokens.length
                    ? "The broker is protected: every request needs a labelled token. Add more or revoke them below."
                    : "Security is on, but there are no tokens yet — add one below. Until then the broker stays open."
                  : "Security is off. Tokens (if any) are kept but not required — you can flip the switch on any time."}
              </p>
              {justCreated ? (
                <div className="sv-token-once" role="status">
                  <div className="sv-token-once-title">
                    Token created for “{justCreated.id}” — shown once
                  </div>
                  <p className="sv-token-once-legend">
                    This token will only show once, please store it somewhere safe. You will not see
                    it again.
                  </p>
                  <code className="sv-token-once-value">{justCreated.token}</code>
                  <div className="sv-token-once-actions">
                    <Btn
                      variant="primary"
                      className={copied ? "sv-copied" : ""}
                      onClick={() => {
                        void navigator.clipboard?.writeText(justCreated.token).then(() => {
                          setCopied(true);
                          window.setTimeout(() => setCopied(false), 1400);
                        });
                      }}
                    >
                      {copied ? (
                        <>
                          <svg
                            viewBox="0 0 24 24"
                            width={13}
                            height={13}
                            fill="none"
                            stroke="currentColor"
                            strokeWidth={3}
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            aria-hidden="true"
                          >
                            <path d="M20 6 9 17l-5-5" />
                          </svg>
                          Copied!
                        </>
                      ) : (
                        "Copy token"
                      )}
                    </Btn>
                    <Btn
                      variant="ghost"
                      onClick={() => {
                        setJustCreated(null);
                        setTokId("");
                        setTokValue("");
                      }}
                    >
                      Done
                    </Btn>
                  </div>
                </div>
              ) : null}

              <Row
                label="Status"
                value={
                  state?.auth_enabled
                    ? state.local_tokens.length
                      ? `secured — ${state.local_tokens.length} token${state.local_tokens.length > 1 ? "s" : ""} enforced`
                      : "security on (no tokens yet)"
                    : "open — security off"
                }
              />

              {state && state.local_tokens.length ? (
                <ul className="s-remotes">
                  {state.local_tokens.map((id) => (
                    <li key={id} className="s-remote">
                      <span className="s-rdot" />
                      <span className="s-rname">{id}</span>
                      <code className="s-rurl">••••••••••••••••••••••••</code>
                      <Btn
                        variant="ghost"
                        disabled={tokBusy}
                        onClick={() => setConfirming({ kind: "revoke", id })}
                      >
                        Revoke
                      </Btn>
                    </li>
                  ))}
                </ul>
              ) : null}

              {confirming ? (
                <div className="sv-confirm">
                  <span className="sv-confirm-msg">
                    Revoke token “{confirming.id}”? Clients using it will stop working; the local
                    broker restarts without it.
                  </span>
                  <Btn
                    variant="danger"
                    disabled={tokBusy}
                    onClick={() => {
                      setTokBusy(true);
                      void appRemoveLocalToken(confirming.id)
                        .then(() => shell.reloadState())
                        .catch((err: unknown) => window.alert(String(err)))
                        .finally(() => {
                          setTokBusy(false);
                          setConfirming(null);
                        });
                    }}
                  >
                    Confirm revoke
                  </Btn>
                  <Btn variant="ghost" disabled={tokBusy} onClick={() => setConfirming(null)}>
                    Cancel
                  </Btn>
                </div>
              ) : null}

              <p className="s-hint">
                Give each client its own labelled token so you know who holds what: one for this
                machine's CLI, one for Claude Desktop, one for a CI pipeline. Revoking one does not
                affect the others. The app and its MCP gateway authenticate automatically.
              </p>

              <form
                className="s-add s-add-stack"
                onSubmit={(e) => {
                  e.preventDefault();
                  const id = tokId.trim();
                  const value = tokValue.trim();
                  if (tokBusy || !id || !value) return;
                  if (state?.local_tokens.includes(id)) {
                    window.alert(
                      `A token labelled "${id}" already exists — pick another identifier.`,
                    );
                    return;
                  }
                  setTokBusy(true);
                  void appAddLocalToken(id, value)
                    .then((created) => {
                      setJustCreated(created);
                      // The form resets: the token is shown only in the
                      // one-time panel above, never left visible below.
                      setTokId("");
                      setTokValue("");
                      return shell.reloadState();
                    })
                    .catch((err: unknown) => window.alert(String(err)))
                    .finally(() => setTokBusy(false));
                }}
              >
                <input
                  className="s-in"
                  placeholder="Identifier (who uses it?)"
                  value={tokId}
                  onChange={(e) => setTokId(e.target.value)}
                  aria-label="Token identifier"
                />
                <input
                  className="s-in s-monow"
                  placeholder="Token value (or generate one)"
                  value={tokValue}
                  onChange={(e) => setTokValue(e.target.value)}
                  aria-label="Token value"
                />
                <Btn
                  variant="ghost"
                  disabled={tokBusy}
                  onClick={() =>
                    setTokValue(
                      `chopflow-${crypto.randomUUID?.() ?? Math.random().toString(36).slice(2)}-${Date.now().toString(36)}`,
                    )
                  }
                >
                  Generate
                </Btn>
                <Btn
                  type="submit"
                  variant="primary"
                  disabled={!tokId.trim() || !tokValue.trim() || tokBusy}
                >
                  Add token
                </Btn>
              </form>
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
                <div className="s-logactions">
                  <Btn
                    variant="ghost"
                    disabled={!logs.length}
                    className={logsCopied ? "s-btn-soft-success" : ""}
                    onClick={() => {
                      void navigator.clipboard?.writeText(logs.join("\n")).then(() => {
                        setLogsCopied(true);
                        window.setTimeout(() => setLogsCopied(false), 1400);
                      });
                    }}
                  >
                    {logsCopied ? "Copied!" : "Copy"}
                  </Btn>
                  <Btn
                    variant="ghost"
                    disabled={!logs.length || logsSaved || !shell.tauri}
                    className={logsSaved ? "s-btn-soft-success" : ""}
                    onClick={() => {
                      void (async () => {
                        const { save } = await import("@tauri-apps/plugin-dialog");
                        const target = await save({
                          defaultPath: "chopflow.log",
                          filters: [{ name: "Log", extensions: ["log"] }],
                        });
                        if (!target) return;
                        try {
                          await appExportLogs(target);
                          setLogsSaved(true);
                          window.setTimeout(() => setLogsSaved(false), 1400);
                        } catch (err) {
                          window.alert(String(err));
                        }
                      })();
                    }}
                  >
                    {logsSaved ? "Saved!" : "Download .log"}
                  </Btn>
                  <Btn variant="ghost" onClick={() => void loadLogs()}>
                    Refresh
                  </Btn>
                </div>
              </div>
              <pre className="s-logs">{(logs.length ? logs : ["(no logs yet)"]).join("\n")}</pre>
            </NeedsApp>
          </section>
        ) : (
          <section className="s-sec">
            <h3>Danger zone</h3>
            <NeedsApp shell={shell}>
              <div className="sv-danger-card">
                <svg
                  className="sv-danger-icon"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth={1.8}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  aria-hidden="true"
                >
                  <path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z" />
                  <path d="M12 9v4M12 17h.01" />
                </svg>
                <div className="sv-danger-body">
                  <div className="sv-danger-title">Delete all local information</div>
                  <p className="sv-danger-desc">
                    Permanently wipes every connection and token, the local broker database, and the
                    data-folder override — then restarts the app as a fresh first run. This cannot
                    be undone.
                  </p>
                  {dangerConfirm ? (
                    <p className="sv-danger-warn">
                      Are you absolutely sure? This deletes everything and cannot be undone.
                    </p>
                  ) : null}
                  <div className="sv-danger-actions">
                    <button
                      type="button"
                      className={`s-btn s-btn-danger${dangerConfirm ? " sv-danger-final" : ""}`}
                      onClick={async () => {
                        if (!dangerConfirm) {
                          setDangerConfirm(true);
                          return;
                        }
                        await appReset();
                        window.location.reload();
                      }}
                    >
                      {dangerConfirm ? "Yes — delete everything" : "Delete everything"}
                    </button>
                    {dangerConfirm ? (
                      <Btn variant="ghost" onClick={() => setDangerConfirm(false)}>
                        Cancel
                      </Btn>
                    ) : null}
                  </div>
                  {previewConfirm ? (
                    <div className="sv-confirm">
                      <span className="sv-confirm-msg">
                        Show the first-run welcome? Nothing is deleted — “Continue” in the wizard
                        brings you back here.
                      </span>
                      <Btn
                        variant="primary"
                        onClick={() => {
                          void appPreviewWelcome().then(() => window.location.reload());
                        }}
                      >
                        Show welcome
                      </Btn>
                      <Btn variant="ghost" onClick={() => setPreviewConfirm(false)}>
                        Cancel
                      </Btn>
                    </div>
                  ) : (
                    <button
                      type="button"
                      className="sv-add-link sv-preview-link"
                      onClick={() => setPreviewConfirm(true)}
                    >
                      Preview first-run welcome
                    </button>
                  )}
                </div>
              </div>
            </NeedsApp>
          </section>
        )}
      </div>
    </div>
  );
}
