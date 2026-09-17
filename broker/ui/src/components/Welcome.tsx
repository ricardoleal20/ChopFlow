// Welcome.tsx — the app's boot + first-run gate (Tauri only).
//
// Ported from the OpenDesign artifact `welcome.html` (project
// chopflow-design-4a5f, GLM-5.3): a dark-first loader with a brand mark and a
// 3-step rail (starting local broker → checking connections → loading
// dashboard), then, on first run, a configuration card (start local broker,
// add remote chips, continue). All state is live-wired to the app bridge —
// this is the real control plane, not a static mockup.

import { useState } from "react";
import type { AppState, LocalStatus } from "../lib/appBridge";
import "../welcome.css";

export type BootStep = "starting-broker" | "checking-connections" | "loading-dashboard";

type Props = {
  state: AppState | null; // null while the Rust side is still responding
  bootStep: BootStep | null;
  local: LocalStatus | null;
  error: string | null;
  onStartLocal: () => void;
  onStopLocal?: () => void; // reserved (tray/settings manage it); unused in v1
  onRemoveRemote?: (name: string) => Promise<void>;
  onAddRemote: (name: string, url: string) => Promise<void>;
  onProceed: () => void; // finish first run / dismiss
};

const BOOT_ORDER: BootStep[] = ["starting-broker", "checking-connections", "loading-dashboard"];
const LOADING = "Loading dashboard…";

const BrandMark = ({ size }: { size: number }) => (
  <svg viewBox="0 0 48 48" fill="none" width={size} height={size} aria-hidden="true">
    <path
      d="M10.8 17.4 13.4 5.8 18.9 14.8 C20.5 13.5 27.5 13.5 29.1 14.8 L34.6 5.8 37.2 17.4 C38 21.6 37.3 25.9 35.7 29.7 C33.9 34 30.4 37.7 26.4 39.7 C25.6 40.2 24.8 40.5 24 40.5 C23.2 40.5 22.4 40.2 21.6 39.7 C17.6 37.7 14.1 34 12.3 29.7 C10.7 25.9 10 21.6 10.8 17.4 Z"
      fill="#fff"
      stroke="#fff"
      strokeWidth="2"
      strokeLinejoin="round"
    />
    <circle cx="19.7" cy="25.4" r="1.9" fill="#101623" />
    <circle cx="28.3" cy="25.4" r="1.9" fill="#101623" />
    <path
      d="M21.5 30.6h5L24 34.4z"
      fill="#101623"
      stroke="#101623"
      strokeWidth="2"
      strokeLinejoin="round"
    />
  </svg>
);

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
  const [adding, setAdding] = useState(false);
  const [hint, setHint] = useState("");

  // Boot vs config: during the boot sequence show the rail; on first run,
  // after boot completes, reveal the configuration card.
  const showBoot = bootStep !== null;
  const showConfig = !showBoot && firstRun;

  const activeIndex = BOOT_ORDER.indexOf(bootStep ?? BOOT_ORDER[0]);
  const stepState = (i: number): "done" | "active" | "pending" =>
    i < activeIndex ? "done" : i === activeIndex ? "active" : "pending";

  const localDeploy = local && (local.kind === "running" || local.kind === "adopted");
  const slotState = starting ? "starting" : localDeploy ? local.kind : "idle";
  const localUrl = localDeploy ? `http://127.0.0.1:${local.http_port}` : null;

  const startLocal = () => {
    if (starting || localDeploy) return;
    setStarting(true);
    onStartLocal();
    // The bridge completes asynchronously; stop the spinner once state lands.
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
      await onAddRemote(n, u);
      setName("");
      setUrl("");
    } finally {
      setAdding(false);
    }
  };

  const remotes = state?.remotes ?? [];

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
                  <BrandMark size={40} />
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
                  <li key={label} className="w-step" data-state={stepState(i)}>
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

          {/* First-run configuration */}
          {showConfig ? (
            <section
              className="w-phase w-config w-active"
              aria-label="First-run setup"
              aria-hidden={false}
            >
              <div className="w-lockup-sm">
                <div className="w-brand-tile w-sm-tile">
                  <BrandMark size={18} />
                </div>
                <div className="w-wordmark w-sm">
                  <b>Chop</b>
                  <span>Flow</span>
                </div>
              </div>

              <div className="w-card">
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
                        ? local!.kind === "adopted"
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
                      {local!.kind === "adopted" ? "external broker" : "local running"}
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

              <div className="w-under">
                <button type="button" className="w-continue" onClick={() => void onProceed()}>
                  Continue <span className="w-arr">→</span>
                </button>
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
