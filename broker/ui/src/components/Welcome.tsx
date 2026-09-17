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

// The canonical ChopFlow mark (assets/icons/chopflow.svg) — a geometric
// German Shepherd in ink / white / flow-blue. Rendered white-only so it reads
// on the indigo brand tiles and always matches the app icon.
const ShepherdPaths = () => (
  <g strokeLinejoin="miter" strokeLinecap="square">
    <path
      fill="#F8FAFC"
      d="M617 234 L513 390 L461 506 L329 685 L310 762 L355 686 L491 542 L517 577 L548 522 L537 472 L609 285 L641 434 L612 492 L674 443 Z"
    />
    <path fill="#F8FAFC" d="M780 278 L696 402 L723 420 L768 334 L768 386 L751 436 L780 459 Z" />
    <path
      fill="#F8FAFC"
      d="M695 430 L623 579 L517 655 L493 763 L614 893 L648 980 L670 825 L603 743 L767 617 L757 595 L680 568 L749 539 L739 509 L781 535 L799 599 L923 669 L907 690 L943 732 L976 685 L834 584 L816 521 Z"
    />
    <path
      fill="#F8FAFC"
      d="M918 757 L882 782 L854 788 L838 788 L740 760 L715 771 L745 776 L842 805 L856 805 L888 797 Z"
    />
  </g>
);

const BrandMark = ({ size }: { size: number }) => (
  <svg viewBox="260 190 760 880" width={size} height={size} fill="none" aria-hidden="true">
    <ShepherdPaths />
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

  // Continue needs at least ONE live path forward: the local broker running
  // (or adopted) OR at least one remote — not both, not neither.
  const canProceed = Boolean(localDeploy) || remotes.length > 0;

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
                <button
                  type="button"
                  className="w-continue"
                  disabled={!canProceed}
                  onClick={() => void onProceed()}
                >
                  Continue <span className="w-arr">→</span>
                </button>
                {!canProceed ? (
                  <p className="w-continue-hint">
                    Start the local broker or add a remote to continue.
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
