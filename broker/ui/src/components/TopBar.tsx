import { useEffect, useRef, useState } from "react";
import { PlusIcon, SearchIcon, ChevronDownIcon } from "./Icons";
import type { Stats } from "../lib/api";
import type { View } from "./Sidebar";
import { useEnvironments, useSwitchEnvironment } from "../hooks/useChopFlow";

interface Props {
  view: View;
  query: string;
  onQuery: (q: string) => void;
  onEnqueue: () => void;
  stats?: Stats;
  /** When non-null (desktop shell), show a small pill with the active
   * connection name next to the search. */
  shellActive?: string;
  /** Desktop shell: open the app Settings drawer. */
  onOpenSettings?: () => void;
}

// 54px command bar: environment switcher (with dropdown) on the left, breadcrumb,
// contextual search with ⌘K hint, a segmented live count pill, and the single
// primary CTA ("New Task").
export default function TopBar({
  view,
  query,
  onQuery,
  onEnqueue,
  stats,
  shellActive,
  onOpenSettings,
}: Props) {
  const [clusterOpen, setClusterOpen] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);
  const envsQ = useEnvironments();
  const switchEnv = useSwitchEnvironment();

  // Close the cluster dropdown on outside click.
  useEffect(() => {
    if (!clusterOpen) return;
    const onDoc = () => setClusterOpen(false);
    window.addEventListener("click", onDoc);
    return () => window.removeEventListener("click", onDoc);
  }, [clusterOpen]);

  // ⌘K / Ctrl-K focuses the search input.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        searchRef.current?.focus();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const label = view === "tasks" ? "Tasks" : view === "schedules" ? "Schedules" : "Workers";

  // Current identity: prefer the live stats poll (reflects the broker we're
  // actually connected to), fall back to the catalog's `current` entry, then to
  // a local/default placeholder so the chip always renders.
  const current = envsQ.data?.current;
  const envName = stats?.env ?? current?.name ?? "local";
  const regionName = stats?.region ?? current?.region ?? "default";
  const environments = envsQ.data?.environments ?? [];
  // The current entry is "live"; everything else is a switch target we don't
  // poll, so we honestly show "—" for their worker counts.
  const isCurrent = (name: string) => name === envName;

  return (
    <header className="topbar">
      {/* Active connection pill (desktop shell only) */}
      {shellActive ? <span className="shell-pill">{shellActive}</span> : null}
      {/* App settings gear (desktop shell only) */}
      {onOpenSettings ? (
        <button
          type="button"
          className="shell-gear"
          onClick={onOpenSettings}
          aria-label="App settings"
          title="App settings"
        >
          <svg
            viewBox="0 0 24 24"
            width="15"
            height="15"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
          >
            <circle cx="12" cy="12" r="3.1" />
            <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09a1.65 1.65 0 0 0 1.51-1 1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33h.01a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51h.01a1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82v.01a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1Z" />
          </svg>
        </button>
      ) : null}
      {/* Environment switcher */}
      <div className={`cluster-sel${clusterOpen ? " open" : ""}`}>
        <button
          className="cs-btn"
          onClick={(e) => {
            e.stopPropagation();
            setClusterOpen((o) => !o);
          }}
        >
          <span className="dot" />
          <span>
            {envName} · {regionName}
          </span>
          <ChevronDownIcon className="chev" />
        </button>
        <div className="cs-menu">
          {environments.length === 0 ? (
            <div className="cs-opt">
              <span className="dot live" />
              <b>
                {envName} · {regionName}
              </b>
              <small>{stats?.active_workers ?? 0} workers</small>
            </div>
          ) : (
            environments.map((e) => (
              <div
                key={e.name}
                className="cs-opt"
                style={{ cursor: "pointer" }}
                onClick={(ev) => {
                  ev.stopPropagation();
                  setClusterOpen(false);
                  switchEnv(e);
                }}
              >
                <span className={`dot ${isCurrent(e.name) ? "live" : "idle"}`} />
                <b>
                  {e.name} · {e.region}
                </b>
                <small>{isCurrent(e.name) ? `${stats?.active_workers ?? 0} workers` : "—"}</small>
              </div>
            ))
          )}
          <div className="cs-sep" />
          <div className="cs-opt" style={{ color: "var(--muted-2)", fontWeight: 400 }}>
            <PlusIcon style={{ width: 14, height: 14 }} />
            Configure in <span className="mono">config/environments.yml</span>
          </div>
        </div>
      </div>

      {/* Breadcrumb */}
      <div className="crumb">
        Operations <span className="sep">/</span> <b>{label}</b>
      </div>

      {/* Search */}
      <div className="search">
        <SearchIcon />
        <input
          ref={searchRef}
          type="text"
          value={query}
          onChange={(e) => onQuery(e.target.value)}
          placeholder={`Search ${view} by id, name, tag…`}
        />
        <kbd>⌘K</kbd>
      </div>

      {/* Live count pill */}
      {stats && (
        <div className="count-pill">
          <div className="cp-seg q">
            <span className="d" />
            <small>queued</small> <b>{stats.queue_length}</b>
          </div>
          <div className="cp-seg r">
            <span className="d" />
            <small>running</small> <b>{stats.tasks_processing}</b>
          </div>
          <div className="cp-seg t">
            <span className="d" />
            <small>total</small> <b>{stats.total_tasks}</b>
          </div>
        </div>
      )}

      {/* Primary CTA */}
      <button className="btn btn-primary" onClick={onEnqueue}>
        <PlusIcon />
        New Task
      </button>
    </header>
  );
}
