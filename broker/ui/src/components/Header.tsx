import { useStats } from "../hooks/useChopFlow";

// Floating glass command bar. Split nav: identity left, live cluster pulse
// right. The pulse dot reflects whether at least one worker is alive.
export function Header({ onEnqueue }: { onEnqueue: () => void }) {
  const { data: stats } = useStats();
  const online = (stats?.active_workers ?? 0) > 0;

  return (
    <header className="sticky top-0 z-40">
      <div className="mx-auto max-w-7xl px-4 sm:px-6">
        <div className="mt-4 flex items-center justify-between gap-4 rounded-2xl border border-border bg-surface/70 px-4 py-3 backdrop-blur-xl sm:px-5">
          {/* Identity */}
          <div className="flex items-center gap-3">
            <Logo />
            <div className="leading-tight">
              <div className="font-semibold tracking-tight">ChopFlow</div>
              <div className="text-xs text-muted">Operations</div>
            </div>
          </div>

          {/* Cluster status + primary action */}
          <div className="flex items-center gap-3">
            <span className="hidden items-center gap-2 rounded-full border border-border bg-canvas/60 px-3 py-1.5 text-xs sm:inline-flex">
              <span
                className={`h-2 w-2 rounded-full ${online ? "bg-accent animate-pulseDot" : "bg-subtle"}`}
                aria-hidden
              />
              <span className="text-muted">
                {online ? `${stats?.active_workers ?? 0} worker${(stats?.active_workers ?? 0) === 1 ? "" : "s"} online` : "idle"}
              </span>
            </span>
            <button
              onClick={onEnqueue}
              className="active:scale-[0.98] transition-transform duration-150 ease-out rounded-xl bg-accent px-3.5 py-2 text-sm font-semibold text-canvas shadow-glow hover:bg-accentDim focus-visible:ring-2 focus-visible:ring-accent/60"
            >
              Enqueue task
            </button>
          </div>
        </div>
      </div>
    </header>
  );
}

function Logo() {
  // A small mark: two stacked "chops" — a nod to the name without a stock icon.
  return (
    <span className="grid h-9 w-9 place-items-center rounded-xl bg-elevated ring-1 ring-borderStrong">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden>
        <path d="M3 7l6 5-6 5" stroke="#34d399" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M12 7l6 5-6 5" stroke="#38bdf8" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    </span>
  );
}
