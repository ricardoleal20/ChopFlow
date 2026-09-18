// Welcome.tsx — the app's boot + first-run gate (Tauri only).
//
// Flow on a fresh install:
//   1. Animated hero — "Welcome to ChopFlow" + Start. (The broker boots in
//      the background; the loading rail is NOT shown first.)
//   2. Setup wizard, 4 screens (no visible step chips — just a linear flow):
//      Environment (local/remote) · MCP (gateway + own access token) ·
//      Security (protect toggle + first labelled token) · Learn (Rust /
//      Python / Java guides linking to the docs site).
//   3. "Finish setup" → the loading rail ("Loading broker…" etc.) → dashboard.
// Non-first-run launches keep the plain boot gate.

import { useState } from "react";
import {
  appAddLocalToken,
  appSetAuthEnabled,
  appSetMcp,
  appSetMcpAccessToken,
  type AppState,
  type LocalStatus,
  type LocalTokenCreated,
} from "../lib/appBridge";
import appIcon from "../assets/app-icon.png";
import langRust from "../assets/langs/rust.svg";
import langPython from "../assets/langs/python.svg";
import langJava from "../assets/langs/java.svg";
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
const DOCS_URL = "https://chopflow.ricardoleal20.dev";

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

// Generic open of the docs site for a language guide.
function openDocs() {
  void (async () => {
    try {
      const { openUrl } = await import("@tauri-apps/plugin-opener");
      await openUrl(DOCS_URL);
    } catch {
      window.open(DOCS_URL, "_blank");
    }
  })();
}

// ---- Language guide cards (Learn screen) --------------------------------

const LANGS = [
  {
    key: "rust",
    label: "Rust",
    img: langRust,
    blurb: "chopflow-worker, typed handlers, cargo",
  },
  {
    key: "python",
    label: "Python",
    img: langPython,
    blurb: "pip client SDK, decorators, async",
  },
  {
    key: "java",
    label: "Java",
    img: langJava,
    blurb: "Maven SDK, @Task handlers",
  },
] as const;

function LearnCard({ lang }: { lang: (typeof LANGS)[number] }) {
  return (
    <button type="button" className="w-lang" onClick={openDocs}>
      <span className="w-lang-icon">
        <img src={lang.img} alt={`${lang.label} icon`} />
      </span>
      <span className="w-lang-body">
        <span className="w-lang-title">{lang.label}</span>
        <span className="w-lang-blurb">{lang.blurb}</span>
      </span>
      <span className="w-lang-link">
        Docs <span aria-hidden>↗</span>
      </span>
    </button>
  );
}

// ---- Component -----------------------------------------------------------

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

  // Hero gate (first-run only).
  const [heroDone, setHeroDone] = useState(false);

  // Wizard state.
  const [step, setStep] = useState(0);
  const [finishing, setFinishing] = useState(false);

  // Environment (step 0).
  const [starting, setStarting] = useState(false);
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [token, setToken] = useState("");
  const [adding, setAdding] = useState(false);
  const [hint, setHint] = useState("");

  // MCP (step 1).
  const [mcpOn, setMcpOn] = useState(state?.mcp_enabled ?? false);
  const [mcpUrl, setMcpUrl] = useState<string | null>(state?.mcp_url ?? null);
  const [mcpBusy, setMcpBusy] = useState(false);
  const [mcpToken, setMcpToken] = useState<string | null>(state?.mcp_access_token ?? null);
  const [mcpTokenOnce, setMcpTokenOnce] = useState<string | null>(null);
  const [mcpCopied, setMcpCopied] = useState(false);

  // Security (step 2).
  const [protect, setProtect] = useState(
    state?.auth_enabled ?? (state?.local_tokens.length ?? 0) > 0,
  );
  const [tokId, setTokId] = useState("");
  const [tokValue, setTokValue] = useState("");
  const [tokBusy, setTokBusy] = useState(false);
  const [justCreated, setJustCreated] = useState<LocalTokenCreated | null>(null);
  const [tokCopied, setTokCopied] = useState(false);
  const [addAnother, setAddAnother] = useState(false);

  // Phases: hero mirrors first-run gating; boot rail only during real boot
  // (non-first-run) or while finishing setup.
  const showHero = firstRun && !heroDone;
  const showWizard = firstRun && heroDone && !finishing;
  const showBoot = finishing || (bootStep !== null && !firstRun);

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

  // MCP: toggle the gateway, and optionally protect it with its own token.
  const ensureLocalFirst = async () => {
    if (!localDeploy) {
      startLocal();
      await new Promise((r) => window.setTimeout(r, 900));
    }
  };

  const toggleMcp = async (want: boolean) => {
    if (mcpBusy) return;
    await ensureLocalFirst();
    setMcpBusy(true);
    try {
      const endpoint = await appSetMcp(want);
      setMcpOn(want);
      setMcpUrl(endpoint);
    } catch (err) {
      window.alert(String(err));
    } finally {
      setMcpBusy(false);
    }
  };

  const toggleMcpToken = async (want: boolean) => {
    if (want) {
      const value = `chopflow-mcp-${crypto.randomUUID?.() ?? Math.random().toString(36).slice(2)}-${Date.now().toString(36)}`;
      try {
        const kept = await appSetMcpAccessToken(value);
        setMcpToken(kept);
        setMcpTokenOnce(value);
      } catch (err) {
        window.alert(String(err));
      }
    } else {
      try {
        await appSetMcpAccessToken(null);
        setMcpToken(null);
        setMcpTokenOnce(null);
      } catch (err) {
        window.alert(String(err));
      }
    }
  };

  // Security: protect toggle + create the first labelled token.
  const tokenCount = (state?.local_tokens.length ?? 0) + (justCreated ? 1 : 0);
  const securityOk = !protect || tokenCount > 0;

  const toggleProtect = async (want: boolean) => {
    setProtect(want);
    try {
      // Just flip whether tokens are required — never delete the ones kept.
      await appSetAuthEnabled(want);
    } catch {
      /* best effort */
    }
  };

  const createToken = async (e: React.FormEvent) => {
    e.preventDefault();
    const id = tokId.trim();
    const value = tokValue.trim();
    if (tokBusy || !id || !value) return;
    if (state?.local_tokens.includes(id) || justCreated?.id === id) {
      window.alert(`A token labelled "${id}" already exists — pick another identifier.`);
      return;
    }
    setTokBusy(true);
    try {
      const created = await appAddLocalToken(id, value);
      setJustCreated(created);
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

  const next = () => {
    if (step === 2 && !securityOk) return;
    if (step === 3) return finish();
    setStep((s) => Math.min(s + 1, 3));
  };

  const finish = () => {
    if (!canFinish || finishing) return;
    setFinishing(true);
    // Hold the loading rail long enough to be legible (3.5s) — it shows
    // "Starting local broker… → Checking connections… → Loading dashboard…"
    // before the dashboard appears.
    window.setTimeout(() => void onProceed(), 3500);
  };

  const MCP_TOKEN_REVEALED = mcpTokenOnce !== null && mcpToken !== null;
  const WIZARD_LAST = 3;

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
          {/* Hero — the very first thing a fresh install sees */}
          {showHero ? (
            <section className="w-phase w-hero w-active" aria-label="Welcome to ChopFlow">
              <div className="w-hero-inner">
                <div className="w-hero-mark">
                  <div className="w-hero-glow" aria-hidden="true" />
                  <div className="w-brand-tile w-hero-tile">
                    <img src={appIcon} alt="ChopFlow" />
                  </div>
                </div>
                <h1 className="w-hero-title">
                  Welcome to <b>ChopFlow</b>
                </h1>
                <p className="w-hero-sub">
                  Your local durable task queue. Let's get you started working in a minute.
                </p>
                <button
                  type="button"
                  className="w-btn w-btn-primary w-hero-cta"
                  onClick={() => setHeroDone(true)}
                >
                  Start <span className="w-arr">→</span>
                </button>
              </div>
            </section>
          ) : null}

          {/* Boot / loading rail (real boot or "Finish setup") */}
          {showBoot ? (
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
                    <li
                      key={label}
                      className="w-step"
                      data-state={
                        showBoot && finishing ? (i < 2 ? "done" : "active") : stepState(i)
                      }
                    >
                      <span className="w-dot">
                        <Check />
                      </span>
                      <span className="w-lbl">
                        {label === "loading-dashboard"
                          ? LOADING
                          : label === "starting-broker"
                            ? "Starting local broker…"
                            : "Checking connections…"}
                      </span>
                    </li>
                  ))}
                </ol>
              </div>
            </section>
          ) : null}

          {/* First-run wizard */}
          {showWizard ? (
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
                  <div className="w-step-body w-centered">
                    <h2 className="w-card-title">MCP gateway</h2>
                    <p className="w-card-sub">
                      Expose this broker to AI assistants (Claude Desktop, Cursor…) over streamable
                      HTTP. Optional — toggle later in Settings.
                    </p>
                    <div className="w-mcp-wrap">
                      <label className="w-mcp">
                        <input
                          type="checkbox"
                          checked={mcpOn}
                          disabled={mcpBusy}
                          onChange={() => void toggleMcp(!mcpOn)}
                        />
                        <span className="w-mcp-track">
                          <span className="w-mcp-knob" />
                        </span>
                        <span className="w-mcp-label">
                          {mcpBusy ? "Starting…" : mcpOn ? "MCP gateway on" : "MCP gateway off"}
                        </span>
                      </label>
                      {/* Protecting the gateway is a Security decision too —
                          the question is always visible, even before enabling it. */}
                      <label className="w-mcp">
                        <input
                          type="checkbox"
                          checked={Boolean(mcpToken)}
                          disabled={mcpBusy}
                          onChange={() => void toggleMcpToken(!mcpToken)}
                        />
                        <span className="w-mcp-track">
                          <span className="w-mcp-knob" />
                        </span>
                        <span className="w-mcp-label">Requires token to connect</span>
                      </label>
                    </div>
                    <div className="w-mcp-legend-wrap">
                      <p className="w-mcp-legend">
                        {mcpToken
                          ? "Needs to generate tokens to connect."
                          : "Everyone that connects can use it without problem."}
                      </p>
                      {MCP_TOKEN_REVEALED ? (
                        <div className="sv-token-once" role="status">
                          <div className="sv-token-once-title">MCP access token — shown once</div>
                          <p className="sv-token-once-legend">
                            This token will only show once, please store it somewhere safe. Each MCP
                            client must present it (Authorization: Bearer).
                          </p>
                          <code className="sv-token-once-value">{mcpTokenOnce}</code>
                          <div className="sv-token-once-actions">
                            <button
                              type="button"
                              className="w-btn w-btn-primary w-btn-sm"
                              onClick={() => {
                                void navigator.clipboard?.writeText(mcpTokenOnce ?? "").then(() => {
                                  setMcpCopied(true);
                                  window.setTimeout(() => setMcpCopied(false), 1400);
                                });
                              }}
                            >
                              {mcpCopied ? "Copied!" : "Copy token"}
                            </button>
                            <button
                              type="button"
                              className="w-btn w-btn-ghost w-btn-sm"
                              onClick={() => setMcpTokenOnce(null)}
                            >
                              Done
                            </button>
                          </div>
                        </div>
                      ) : null}
                      {mcpToken && !MCP_TOKEN_REVEALED ? (
                        <button
                          type="button"
                          className="w-skip w-add-another"
                          onClick={() =>
                            void toggleMcpToken(false).then(() => void toggleMcpToken(true))
                          }
                        >
                          Generate a new access token
                        </button>
                      ) : null}
                      {mcpOn ? (
                        <p className="w-mcp-url">
                          Endpoint{" "}
                          <code>{mcpUrl ?? state?.mcp_url ?? "http://127.0.0.1:8810/mcp"}</code>
                        </p>
                      ) : null}
                    </div>
                  </div>
                ) : step === 2 ? (
                  <div className="w-step-body">
                    <h2 className="w-card-title">Protect your broker</h2>
                    <p className="w-card-sub">
                      Choose how clients connect: open, or secured with labelled API tokens (one per
                      client). You can manage both later in Settings → Security.
                    </p>
                    <label className="w-mcp">
                      <input
                        type="checkbox"
                        checked={protect}
                        onChange={() => void toggleProtect(!protect)}
                      />
                      <span className="w-mcp-track">
                        <span className="w-mcp-knob" />
                      </span>
                      <span className="w-mcp-label">
                        {protect
                          ? "Secured — a token is required to connect"
                          : "Open — anyone can connect"}
                      </span>
                    </label>

                    {protect && tokenCount === 0 ? (
                      <>
                        <p className="w-rf-hint" role="status">
                          Create your first token to finish securing the broker.
                        </p>
                        {justCreated ? (
                          <div className="sv-token-once" role="status">
                            <div className="sv-token-once-title">
                              Token created for “{justCreated.id}” — shown once
                            </div>
                            <p className="sv-token-once-legend">
                              This token will only show once, please store it somewhere safe.
                            </p>
                            <code className="sv-token-once-value">{justCreated.token}</code>
                            <div className="sv-token-once-actions">
                              <button
                                type="button"
                                className="w-btn w-btn-primary w-btn-sm"
                                onClick={() => {
                                  void navigator.clipboard
                                    ?.writeText(justCreated.token)
                                    .then(() => {
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
                          <>
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
                            <button
                              type="button"
                              className="w-skip"
                              onClick={() => {
                                void toggleProtect(false);
                                setStep((st) => st + 1);
                              }}
                            >
                              Skip it — set up later
                            </button>
                          </>
                        )}
                      </>
                    ) : null}
                    {protect && tokenCount > 0 ? (
                      <p className="w-mcp-url">
                        ✓ Secured with {tokenCount} labelled token{tokenCount > 1 ? "s" : ""}.
                      </p>
                    ) : null}
                    {protect && tokenCount > 0 && !addAnother ? (
                      <button
                        type="button"
                        className="w-skip w-add-another"
                        onClick={() => setAddAnother(true)}
                      >
                        + Add another token
                      </button>
                    ) : null}
                    {protect && tokenCount > 0 && addAnother ? (
                      <div className="w-mcp-url">
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
                          <button
                            type="button"
                            className="w-skip"
                            onClick={() => setAddAnother(false)}
                          >
                            Cancel
                          </button>
                        </form>
                      </div>
                    ) : null}
                    {!protect ? (
                      <p className="w-mcp-url">
                        Everything that can reach this address can use the broker. You can enable
                        tokens later.
                      </p>
                    ) : null}
                  </div>
                ) : (
                  <div className="w-step-body">
                    <h2 className="w-card-title">
                      Learn and see examples of how to implement workers
                    </h2>
                    <p className="w-card-sub">
                      Pick your language — every guide links to the full ChopFlow documentation.
                    </p>
                    {LANGS.map((lang) => (
                      <LearnCard key={lang.key} lang={lang} />
                    ))}
                    <a
                      className="w-lang w-lang-all"
                      href={DOCS_URL}
                      target="_blank"
                      rel="noreferrer"
                    >
                      <span className="w-lang-body">
                        <span className="w-lang-title">Full documentation</span>
                        <span className="w-lang-blurb">chopflow.ricardoleal20.dev</span>
                      </span>
                      <span className="w-lang-link">
                        Open <span aria-hidden>↗</span>
                      </span>
                    </a>
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
                    disabled={step === WIZARD_LAST ? !canFinish : step === 2 ? !securityOk : false}
                    onClick={next}
                  >
                    {step === WIZARD_LAST ? (
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
                {step === WIZARD_LAST && !canFinish ? (
                  <p className="w-continue-hint">
                    Start the local broker or add a remote to finish setup.
                  </p>
                ) : null}
                {step === 2 && !securityOk ? (
                  <p className="w-continue-hint">
                    Create a token (or turn protection off) to continue.
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
