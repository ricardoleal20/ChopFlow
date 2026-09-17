// Welcome.tsx — the app's boot + first-run gate (Tauri only).
//
// While the interfaces load it shows the live sequence (starting local
// broker, checking connections, loading dashboard). On a first run it is the
// place to add the first remote(s) or just proceed local-only. Functional
// v0 baseline: drives the real bridge, and is the surface OpenDesign will
// restyle. In the browser this component is never rendered.

import { useState } from "react";
import type { AppState, LocalStatus } from "../lib/appBridge";

export type BootStep = "starting-broker" | "checking-connections" | "loading-dashboard";

type Props = {
  state: AppState | null; // null while the Rust side is still responding
  bootStep: BootStep | null;
  local: LocalStatus | null;
  error: string | null;
  onStartLocal: () => void;
  onStopLocal: () => void;
  onAddRemote: (name: string, url: string) => Promise<void>;
  onProceed: () => void; // finish first run / dismiss (non-recurring boot)
};

function statusLabel(status: LocalStatus | null): string {
  if (!status) return "unknown";
  switch (status.kind) {
    case "running":
      return `local running (http://127.0.0.1:${status.http_port})`;
    case "adopted":
      return `adopted external broker (http://127.0.0.1:${status.http_port})`;
    case "failed":
      return `failed — ${status.reason}`;
    default:
      return "stopped";
  }
}

function statusKind(status: LocalStatus | null): string {
  if (!status) return "stopped";
  if (status.kind === "running" || status.kind === "adopted") return "ok";
  if (status.kind === "failed") return "error";
  return "stopped";
}

export default function Welcome({
  state,
  bootStep,
  error,
  onStartLocal,
  onAddRemote,
  onProceed,
}: Props) {
  const firstRun = state ? !state.first_run_done : true;
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [adding, setAdding] = useState(false);

  const steps: { id: BootStep; label: string }[] = [
    { id: "starting-broker", label: "Starting local broker…" },
    { id: "checking-connections", label: "Checking connections…" },
    { id: "loading-dashboard", label: "Loading dashboard…" },
  ];
  const stepIndex = steps.findIndex((s) => s.id === bootStep);

  return (
    <div className="welcome">
      <div className="welcome__mark" aria-hidden="true">
        <svg viewBox="0 0 24 24" width="44" height="44" fill="none">
          <path
            d="M12 2C7 2 3 4.5 3 10v4c0 5.5 4 8 9 8s9-2.5 9-8v-4c0-5.5-4-8-9-8Z"
            stroke="currentColor"
            strokeWidth="1.6"
          />
          <circle cx="9" cy="10" r="1.4" fill="currentColor" />
          <circle cx="15" cy="10" r="1.4" fill="currentColor" />
          <path
            d="M8.5 14.5c.8 1 1.9 1.4 3.5 1.4s2.7-.4 3.5-1.4"
            stroke="currentColor"
            strokeWidth="1.3"
            strokeLinecap="round"
          />
        </svg>
      </div>

      <h1 className="welcome__title">ChopFlow</h1>
      <p className="welcome__lede">Durable distributed task queue — operations console.</p>

      {firstRun ? (
        <div className="welcome__card">
          <h2>Get your first environment running</h2>
          <p className="welcome__hint">
            Start a local broker (nothing else needed) or connect a remote you already run. Add more
            any time from the switcher.
          </p>

          {state?.local ? (
            <div className="welcome__status-row">
              <span className={`dot dot--${statusKind(state.local)}`} />
              <span>{statusLabel(state.local)}</span>
            </div>
          ) : null}

          <div className="welcome__actions">
            <button
              className="btn btn--primary"
              disabled={bootStep === "starting-broker"}
              onClick={() => void onStartLocal()}
            >
              {state?.local && state.local.kind === "running"
                ? "Local broker running"
                : "Start local broker"}
            </button>
            <button className="btn" onClick={() => void onProceed()}>
              Connect to remote instead
            </button>
          </div>

          <div className="welcome__add-remote">
            <input
              className="input"
              placeholder="Remote name (e.g. prod)"
              value={name}
              onChange={(e) => setName(e.target.value)}
              aria-label="Remote name"
            />
            <input
              className="input"
              placeholder="http://broker.prod:8080"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              aria-label="Remote HTTP URL"
            />
            <button
              className="btn btn--secondary"
              disabled={adding || !name.trim() || !url.trim()}
              onClick={async () => {
                setAdding(true);
                try {
                  await onAddRemote(name.trim(), url.trim());
                  setName("");
                  setUrl("");
                } finally {
                  setAdding(false);
                }
              }}
            >
              Add remote
            </button>
          </div>

          {state?.remotes?.length ? (
            <ul className="welcome__remotes">
              {state.remotes.map((r) => (
                <li key={r.name}>
                  <code>{r.name}</code>
                  <span>{r.http_url}</span>
                </li>
              ))}
            </ul>
          ) : null}

          <button className="btn btn--primary btn--block" onClick={() => void onProceed()}>
            Continue
          </button>
        </div>
      ) : (
        <div className="welcome__loading">
          <ol className="welcome__steps">
            {steps.map((s, i) => (
              <li
                key={s.id}
                className={i < stepIndex ? "done" : i === stepIndex ? "active" : "pending"}
              >
                <span className="welcome__step-dot" />
                {s.label}
              </li>
            ))}
          </ol>
          {error ? <p className="welcome__error">{error}</p> : null}
        </div>
      )}

      {state?.chopflow_version ? (
        <p className="welcome__footer">
          chopflow {state.chopflow_version}
          {state.chopflow_binary ? ` · ${state.chopflow_binary}` : ""}
        </p>
      ) : null}
    </div>
  );
}
