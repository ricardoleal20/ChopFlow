// Welcome.tsx — the app's boot + first-run gate (Tauri only).
//
// Ported from the OpenDesign artifact `welcome.html` (project
// chopflow-design-4a5f, GLM-5.3): a dark-first loader with the app's real
// icon and a 3-step rail (starting local broker → checking connections →
// loading dashboard). On first run the configuration card is a 4-step
// wizard taken before "Finish setup":
//   1. Environment — start the local broker or add a remote
//   2. MCP — optional persistent MCP-over-HTTP gateway
//   3. Security — optional first labeled API token (shown once)
//   4. Learn — quickstart commands (worker, SDK, MCP endpoint)
// Finishing shows the loading rail, then the dashboard loads.

import { useState } from "react";
import {
  appAddLocalToken,
  appSetMcp,
  type AppState,
  type LocalStatus,
  type LocalTokenCreated,
} from "../lib/appBridge";
import appIcon from "../assets/app-icon.png";
import "../welcome.css";

export type BootStep = "starting-broker" | "checking-connections" | "loading-dashboard";

type Props = {
  state: AppState | null; // null while the Rust side is still responding
  bootStep: BootStep | null;
  local: LocalStatus | null;
  error: string | null;
  onStartLocal: () => void;
  onRemoveRemote?: (name: string) => Promise<void>;
  onAddRemote: (name: string, url: string, token?: string | null) => Promise<void>;
  onProceed: () => void; // finish first run / dismiss
};

const BOOT_ORDER: BootStep[] = ["starting-broker", "checking-connections", "loading-dashboard"];
const LOADING = "Loading dashboard…";

const SETUP_LABELS = ["Environment", "MCP", "Security", "Learn"] as const;

const Check = () => (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="3"
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    <path d="M20 6 9 17l-5-5" />
  </svg>
);

// A monospace quickstart block with a copy button (used by the Learn step).
function Quickstart({ code, label }: { code: string; label: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="w-qs">
      <div className="w-qs-head">
        <span className="w-qs-label">{label}</span>
        <button
          type="button"
          className={`w-qs-copy${copied ? " done" : ""}`}
          onClick={() => {
            void navigator.clipboard?.writeText(code).then(() => {
              setCopied(true);
              window.setTimeout(() => setCopied(false), 1400);
            });
          }}
        >
          {copied ? "Copied!" : "Copy"}
        </button>
      </div>
      <code className="w-qs-code">{code}</code>
    </div>
  );
}

export default function Welcome({
  state,
  bootStep,
  local,
  error,
  onStartLocal,
  onRemoveRemote,
  onAddRemote,
  onProceed,
}: Props) {
  const firstRun = state ? !state.first_run_done : true;
  const [starting, setStarting] = useState(false);
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [token, setToken] = useState("");
  const [adding, setAdding] = useState(false);
  const [hint, setHint] = useState("");

  // Wizard state.
  const [step, setStep] = useState(0);
  const [finishing, setFinishing] = useState(false);
  const [mcpOn, setMcpOn] = useState(state?.mcp_enabled ?? false);
  const [mcpUrl, setMcpUrl] = useState<string | null>(state?.mcp_url ?? null);
  const [mcpBusy, setMcpBusy] = useState(false);
  const [tokId, setTokId] = useState("");
  const [tokValue, setTokValue] = useState("");
  const [tokBusy, setTokBusy] = useState(false);
  const [tokCreated, setTokCreated] = useState<LocalTokenCreated | null>(null);
  const [tokCopied, setTokCopied] = useState(false);

  // Boot vs config: during the boot sequence show the rail; while finishing
  // first run we show the rail too ("Finishing setup…").
  const showBoot = bootStep !== null || finishing;
  const showConfig = !showBoot && firstRun;

  const activeIndex = BOOT_ORDER.indexOf(bootStep ?? BOOT_ORDER[0]);
  const stepState = (i: number): "done" | "active" | "pending" =>
    i < activeIndex ? "done" : i === activeIndex ? "active" : "pending";

  const localKind = local?.kind ?? null;
  const localDeploy = localKind === "running" || localKind === "adopted";
  const slotState = starting ? "starting" : localKind ? localKind : "idle";
  const localUrl =
    local && (local.kind === "running" || local.kind === "adopted")
      ? `http://127.0.0.1:${local.http_port}`
      : null;

  const startLocal = () => {
    if (starting || localDeploy) return;
    setStarting(true);
    onStartLocal();
    window.setTimeout(() => setStarting(false), 1500);
  };

  const submitRemote = async (e: React.FormEvent) => {
    e.preventDefault();
    setHint("");
    const n = name.trim();
    const u = url.trim();
    if (!n) return setHint("Enter a name for this remote.");
    if (!/^https?:\/\/\S+$/.test(u)) return setHint("URL must start with http:// or https://.");
    if (state?.remotes.some((r) => r.name === n))
      return setHint("A remote with this name already exists.");
    setAdding(true);
    try {
      await onAddRemote(n, u, token.trim() || null);
      setName("");
      setUrl("");
      setToken("");
    } finally {
      setAdding(false);
    }
  };

  const toggleMcp = async () => {
    if (mcpBusy) return;
    if (!localDeploy) {
      startLocal();
      window.setTimeout(() => void toggleMcp(), 900);
      return;
    }
    setMcpBusy(true);
    const next = !mcpOn;
    try {
      const url = await appSetMcp(next);
      setMcpOn(next);
      setMcpUrl(url);
    } catch (err) {
      window.alert(String(err));
    } finally {
      setMcpBusy(false);
    }
  };

  const createToken = async (e: React.FormEvent) => {
    e.preventDefault();
    const id = tokId.trim();
    const value = tokValue.trim();
    if (tokBusy || !id || !value) return;
    if (state?.local_tokens.includes(id)) {
      window.alert(`A token labelled "${id}" already exists — pick another identifier.`);
      return;
    }
    setTokBusy(true);
    try {
      const created = await appAddLocalToken(id, value);
      setTokCreated(created);
      setTokId("");
      setTokValue("");
    } catch (err) {
      window.alert(String(err));
    } finally {
      setTokBusy(false);
    }
  };

  const remotes = state?.remotes ?? [];
  const canFinish = Boolean(localDeploy) || remotes.length > 0;

  const finish = () => {
    if (!canFinish || finishing) return;
    setFinishing(true);
    void onProceed();
  };

  const next = () => {
    if (step === SETUP_LABELS.length - 1) return finish();
    setStep((s) => Math.min(s + 1, SETUP_LABELS.length - 1));
  };

  return (
    <div className="welcome-shell w-tile">
      {!error ? null : (
        <p style={{ position: "absolute", left: 22, bottom: 40, color: "var(--danger-fg)" }}>
          {error}
        </p>
      )}

      <div className="w-titlebar" aria-hidden="true">
        <div className="w-lights">
          <i className="w-l-close" />
          <i className="w-l-min" />
          <i className="w-l-zoom" />
        </div>
      </div>

      <main className="w-stage">
        <div className="w-phases">
          {/* Boot / loading rail */}
          <section
            className={`w-phase w-boot${showBoot ? " w-active" : ""}`}
            aria-label="Starting ChopFlow"
            aria-hidden={!showBoot}
          >
            <div className="w-lockup">
              <div className="w-mark-wrap">
                <div className="w-mark-glow" aria-hidden="true" />
                <div className="w-brand-tile w-boot-tile">
                  <img src={appIcon} alt="ChopFlow" />
                </div>
              </div>
              <div className="w-wordmark">
                <b>Chop</b>
                <span>Flow</span>
              </div>
            </div>
            <div className="w-rail-wrap">
              <ol className="w-boot-rail">
                {BOOT_ORDER.map((label, i) => (
                  <li key={label} className="w-step" data-state={finishing ? "done" : stepState(i)}>
                    <span className="w-dot">
                      <Check />
                    </span>
                    <span className="w-lbl">
                      {label === "loading-dashboard"
                        ? finishing
                          ? "Finishing setup…"
                          : LOADING
                        : label === "starting-broker"
                          ? "Starting local broker…"
                          : "Checking connections…"}
                    </span>
                  </li>
                ))}
              </ol>
            </div>
          </section>

          {/* First-run wizard */}
          {showConfig ? (
            <section
              className="w-phase w-config w-active"
              aria-label="First-run setup"
              aria-hidden={false}
            >
              <div className="w-lockup-sm">
                <div className="w-brand-tile w-sm-tile">
                  <img src={appIcon} alt="ChopFlow" />
                </div>
                <div className="w-wordmark w-sm">
                  <b>Chop</b>
                  <span>Flow</span>
                </div>
              </div>

              <div className="w-card">
                {/* Step progress */}
                <ol className="w-steps" aria-label="Setup progress">
                  {SETUP_LABELS.map((label, i) => (
                    <li
                      key={label}
                      className={`w-step-chip${i === step ? " active" : ""}${i < step ? " done" : ""}`}
                    >
                      <span className="w-step-num">{i < step ? <Check /> : i + 1}</span>
                      <span>{label}</span>
                    </li>
                  ))}
                </ol>

                {step === 0 ? (
                  <div className="w-step-body">
                    <h2 className="w-card-title">Get your first environment running</h2>
                    <p className="w-card-sub">
                      Start the bundled broker on this machine, or connect to one you already run.
                    </p>

                    <div className="w-broker-slot" data-state={slotState}>
                      <button
                        type="button"
                        className="w-btn w-btn-primary"
                        disabled={starting || Boolean(localDeploy)}
                        onClick={startLocal}
                      >
                        <span className="w-spinner" aria-hidden="true" />
                        <span>
                          {localDeploy
                            ? localKind === "adopted"
                              ? "External broker connected"
                              : "Local broker running"
                            : starting
                              ? "Starting…"
                              : "Start local broker"}
                        </span>
                      </button>
                      <p className="w-broker-status" aria-live="polite">
                        <span className="w-dot-live" aria-hidden="true" />
                        <span className="w-ok">
                          {localKind === "adopted" ? "external broker" : "local running"}
                        </span>
                        <span className="w-sep">·</span>
                        <code>{localUrl}</code>
                      </p>
                    </div>

                    <div className="w-sect-remote">
                      <form className="w-rf-row" onSubmit={submitRemote}>
                        <input
                          className="w-in w-in-name"
                          placeholder="Remote name"
                          value={name}
                          onChange={(e) => setName(e.target.value)}
                          aria-label="Remote name"
                        />
                        <input
                          className="w-in w-in-url"
                          placeholder="http://10.0.0.4:8080"
                          value={url}
                          onChange={(e) => setUrl(e.target.value)}
                          aria-label="Remote URL"
                        />
                        <input
                          className="w-in w-in-token"
                          type="password"
                          placeholder="Token (optional)"
                          value={token}
                          onChange={(e) => setToken(e.target.value)}
                          aria-label="API token (optional)"
                        />
                        <button
                          type="submit"
                          className="w-btn w-btn-ghost w-btn-sm"
                          disabled={adding || !name.trim() || !url.trim()}
                        >
                          Add remote
                        </button>
                      </form>
                      {hint ? (
                        <p className="w-rf-hint" role="status">
                          {hint}
                        </p>
                      ) : null}
                      {remotes.length ? (
                        <ul className="w-chips">
                          {remotes.map((r) => (
                            <li key={r.name} className="w-chip">
                              <span className="w-chip-dot" aria-hidden="true" />
                              <span className="w-chip-name">{r.name}</span>
                              <span className="w-chip-url">{r.http_url}</span>
                              <button
                                type="button"
                                className="w-chip-x"
                                aria-label={`Remove ${r.name}`}
                                onClick={() => {
                                  if (onRemoveRemote) void onRemoveRemote(r.name);
                                }}
                              >
                                <svg
                                  viewBox="0 0 24 24"
                                  width={10}
                                  height={10}
                                  fill="none"
                                  stroke="currentColor"
                                  strokeWidth="3"
                                  strokeLinecap="round"
                                  aria-hidden="true"
                                >
                                  <path d="M18 6 6 18M6 6l12 12" />
                                </svg>
                              </button>
                            </li>
                          ))}
                        </ul>
                      ) : null}
                    </div>
                  </div>
                ) : step === 1 ? (
                  <div className="w-step-body">
                    <h2 className="w-card-title">MCP gateway</h2>
                    <p className="w-card-sub">
                      Expose this broker to AI assistants (Claude Desktop, Cursor…) over streamable
                      HTTP. Optional — you can toggle it later in Settings.
                    </p>
                    <label className="w-mcp">
                      <input
                        type="checkbox"
                        checked={mcpOn}
                        disabled={mcpBusy}
                        onChange={toggleMcp}
                      />
                      <span className="w-mcp-track">
                        <span className="w-mcp-knob" />
                      </span>
                      <span className="w-mcp-label">
                        {mcpBusy ? "Starting…" : mcpOn ? "MCP gateway on" : "MCP gateway off"}
                      </span>
                    </label>
                    {mcpUrl ? (
                      <p className="w-mcp-url">
                        Endpoint <code>{mcpUrl}</code>
                      </p>
                    ) : null}
                  </div>
                ) : step === 2 ? (
                  <div className="w-step-body">
                    <h2 className="w-card-title">Protect your broker</h2>
                    <p className="w-card-sub">
                      Give each client its own labelled token — optional, you can add more later in
                      Settings → Security.
                    </p>
                    {tokCreated ? (
                      <div className="sv-token-once" role="status">
                        <div className="sv-token-once-title">
                          Token created for “{tokCreated.id}” — shown once
                        </div>
                        <p className="sv-token-once-legend">
                          This token will only show once, please store it somewhere safe.
                        </p>
                        <code className="sv-token-once-value">{tokCreated.token}</code>
                        <div className="sv-token-once-actions">
                          <button
                            type="button"
                            className="w-btn w-btn-primary w-btn-sm"
                            onClick={() => {
                              void navigator.clipboard?.writeText(tokCreated.token).then(() => {
                                setTokCopied(true);
                                window.setTimeout(() => setTokCopied(false), 1400);
                              });
                            }}
                          >
                            {tokCopied ? "Copied!" : "Copy token"}
                          </button>
                        </div>
                      </div>
                    ) : (
                      <form className="s-add s-add-stack" onSubmit={createToken}>
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
                        <button
                          type="button"
                          className="w-btn w-btn-ghost w-btn-sm"
                          disabled={tokBusy}
                          onClick={() =>
                            setTokValue(
                              `chopflow-${crypto.randomUUID?.() ?? Math.random().toString(36).slice(2)}-${Date.now().toString(36)}`,
                            )
                          }
                        >
                          Generate
                        </button>
                        <button
                          type="submit"
                          className="w-btn w-btn-primary w-btn-sm"
                          disabled={tokBusy || !tokId.trim() || !tokValue.trim()}
                        >
                          Create token
                        </button>
                      </form>
                    )}
                    <button type="button" className="w-skip" onClick={() => setStep((s) => s + 1)}>
                      Skip — set up later
                    </button>
                  </div>
                ) : (
                  <div className="w-step-body">
                    <h2 className="w-card-title">Ready to run work</h2>
                    <p className="w-card-sub">
                      Your local broker is up. Here are the quick ways to start pushing tasks.
                    </p>
                    <Quickstart
                      label="Run a worker (Rust)"
                      code="chopflow worker start --broker http://127.0.0.1:8080"
                    />
                    <Quickstart
                      label="Python client"
                      code={
                        'pip install chopflow\n\nfrom chopflow import Client\nc = Client("http://127.0.0.1:8080")\nc.enqueue("echo", {"message": "hello"})'
                      }
                    />
                    <Quickstart
                      label="MCP endpoint (if enabled)"
                      code="http://127.0.0.1:8810/mcp"
                    />
                  </div>
                )}
              </div>

              <div className="w-under">
                <div className="w-nav">
                  <button
                    type="button"
                    className="w-btn w-btn-ghost w-btn-sm"
                    disabled={step === 0}
                    onClick={() => setStep((s) => Math.max(s - 1, 0))}
                  >
                    ← Back
                  </button>
                  <button
                    type="button"
                    className="w-continue"
                    disabled={step === SETUP_LABELS.length - 1 ? !canFinish : false}
                    onClick={next}
                  >
                    {step === SETUP_LABELS.length - 1 ? (
                      <>
                        Finish setup <span className="w-arr">→</span>
                      </>
                    ) : (
                      <>
                        Continue <span className="w-arr">→</span>
                      </>
                    )}
                  </button>
                </div>
                {step === SETUP_LABELS.length - 1 && !canFinish ? (
                  <p className="w-continue-hint">
                    Start the local broker or add a remote to finish setup.
                  </p>
                ) : null}
              </div>
            </section>
          ) : null}
        </div>
      </main>

      <p className="w-build-line">
        {state?.chopflow_version ? `chopflow ${state.chopflow_version}` : "chopflow"} ·{" "}
        {state?.chopflow_binary ?? "local broker"}
      </p>
    </div>
  );
}
