import { useEffect, useRef, useState } from "react";
import { PlusIcon, SearchIcon, ChevronDownIcon } from "./Icons";
import type { Stats } from "../lib/api";
import type { View } from "./Sidebar";

interface Props {
  view: View;
  query: string;
  onQuery: (q: string) => void;
  onEnqueue: () => void;
  stats?: Stats;
}

// 54px command bar: cluster selector (with dropdown) on the left, breadcrumb,
// contextual search with ⌘K hint, a segmented live count pill, and the single
// primary CTA ("New Task").
export default function TopBar({ view, query, onQuery, onEnqueue, stats }: Props) {
  const [clusterOpen, setClusterOpen] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);

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

  return (
    <header className="topbar">
      {/* Cluster selector */}
      <div className={`cluster-sel${clusterOpen ? " open" : ""}`}>
        <button
          className="cs-btn"
          onClick={(e) => {
            e.stopPropagation();
            setClusterOpen((o) => !o);
          }}
        >
          <span className="dot" />
          <span>local · default</span>
          <ChevronDownIcon className="chev" />
        </button>
        <div className="cs-menu">
          <div className="cs-opt">
            <span className="dot live" />
            <b>local · default</b>
            <small>{stats?.active_workers ?? 0} workers</small>
          </div>
          <div className="cs-opt">
            <span className="dot idle" />
            <b>prod · us-east</b>
            <small>—</small>
          </div>
          <div className="cs-sep" />
          <div className="cs-opt" style={{ color: "var(--accent)", fontWeight: 550 }}>
            <PlusIcon style={{ width: 14, height: 14 }} />
            Add cluster
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
