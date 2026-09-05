# ChopFlow Design System

> **ChopFlow** — Durable task queue for distributed systems.
> A Rust-based distributed task queue with an embedded operations dashboard.
> This system is the canonical visual contract for every ChopFlow artifact. Tokens here
> supersede any inline values in shipping files. When this document and an artifact
> disagree, **this document wins for token values**; craft rules apply to anything it
> does not override.

Voice: precise, engineering-focused, calm. The reference is Temporal's operations
console — dense, trustworthy, never decorative. Every screen should feel like an
instrument a duty engineer can read at 3am without misreading a state.

---

## 1. Brand & Voice

| | |
|---|---|
| **Name** | ChopFlow |
| **Tagline** | Durable task queue for distributed systems. |
| **Tone** | Precise · engineering-focused · calm |
| **Reference** | Temporal operations console — density and professionalism, not marketing polish |
| **Mark** | ChopFlow wordmark. "Chop" set strong, "Flow" set muted, in the sidebar wordmark. Logo glyph: a compact forward-chevron / flow mark on a primary-to-violet gradient tile. |
| **Do** | Lead with state and data. Let mono type carry identifiers. One accent, used sparingly. |
| **Don't** | Decorative gradients on content surfaces. Emoji as icons. Marketing hero language in the console. Inventing metrics. |

---

## 2. Typography

**Families**

- **Sans (UI / body):** `Inter` — weights 400 / 500 / 600 / 700
- **Mono (data / code / IDs):** `JetBrains Mono` — weights 400 / 500 / 600

```css
--font-sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
--font-mono: 'JetBrains Mono', ui-monospace, 'SF Mono', Menlo, monospace;
```

Always declare the fallback chain. Never set a heading to `system-ui` alone.

**Type scale** (multiplicative ~1.2, capped at 6–8 sizes per screen)

| Role | Size | Weight | Line-height | Letter-spacing |
|---|---|---|---|---|
| Display | 28–32 px | 600 | 1.2 | −0.02em |
| H1 (view title) | 20 px | 600 | 1.25 | −0.015em |
| H2 | 16 px | 600 | 1.3 | −0.01em |
| H3 | 14 px | 600 | 1.4 | −0.005em |
| Body | 13–14 px | 400 | 1.5 | 0 |
| Small / table | 12–13 px | 400–500 | 1.5 | 0 |
| Caption / mono data | 11–12 px | 400–600 | 1.5 | 0.01–0.02em |

**Three-weight discipline:** 400 (read) · 500/550 (UI text, labels, nav) · 600 (titles,
badges, buttons). Reserve 700 for the wordmark and rare emphasis only.

**Letter-spacing rules (non-negotiable):**

- ALL CAPS labels (kickers, table headers, field keys): **+0.06em to +0.1em**.
- Display / H1 (≥20 px): −0.01em to −0.02em (Latin).
- Body & small: 0. Small (≤13 px) may take +0.01em.
- UI labels and button text: +0.02em.

**Mono usage:** task IDs, worker IDs, addresses, timestamps, JSON payloads, resource
values, counts. Disable ligatures on mono blocks: `font-feature-settings: "calt" 0`.

**Line length:** body copy capped at 50–75 ch (`max-width: 65ch`). Never `text-align:
justify` on the web.

**CJK note:** ChopFlow copy is English-only; if localized to CJK, raise display
line-height to 1.3–1.4 and drop negative tracking on CJK blocks.

---

## 3. Color Tokens

Dual theme via CSS variables. **Light is default** (`:root`); **dark** under `.dark`
(or `[data-theme="dark"]`). The **navy sidebar is constant** in both themes.

### 3.1 Light theme — `:root`

```css
:root {
  /* surfaces */
  --canvas:        #f7f8fa;   /* app background */
  --surface:       #ffffff;   /* cards, table, drawer */
  --surface-2:     #f2f4f7;   /* inset fills, table header, inputs resting */
  --surface-3:     #e8ebf1;   /* hover wash, meter track, kbd */

  /* lines */
  --border:        #e2e6ed;
  --border-strong: #cdd4df;

  /* text */
  --text:          #171b26;   /* primary */
  --text-2:        #3a4150;   /* secondary (derived) */
  --muted:         #586274;   /* captions, meta */
  --subtle:        #848ea0;   /* placeholders, disabled-adjacent */

  /* derived interaction (generate, do not hardcode beyond spec) */
  --hover:         #f2f5fc;   /* row + chip hover */
  --row-hover:     #f4f6fb;
  --row-active:    #eef2ff;   /* open drawer row */
  --focus-ring:    rgba(64, 96, 255, 0.35);
  --focus-ring-soft: rgba(64, 96, 255, 0.14);
}
```

### 3.2 Dark theme — `.dark`

```css
.dark {
  --canvas:        #0a0c10;
  --surface:       #11141b;
  --surface-2:     #171b24;
  --elevated:      #1c212f;   /* drawer, popovers float above surface */
  --surface-3:     #232938;   /* hover wash, meter track */

  --border:        #232938;
  --border-strong: #2f3648;

  --text:          #e7ebf3;
  --text-2:        #b9c1d1;   /* derived */
  --muted:         #8a93a6;
  --subtle:        #5c6578;

  --hover:         #1a2030;
  --row-hover:     #161c2b;
  --row-active:    #1b2342;
  --focus-ring:    rgba(64, 96, 255, 0.45);
  --focus-ring-soft: rgba(64, 96, 255, 0.22);
}
```

### 3.3 Navy sidebar — constant in both themes

```css
:root,
.dark {
  --sidebar-bg:        #141a27;
  --sidebar-surface:   #1e2637;   /* cluster card, hover wash */
  --sidebar-surface-2: #262f42;   /* active count pill, toggle track */
  --sidebar-border:    #262f42;
  --sidebar-text:      #ced6e6;   /* nav item default */
  --sidebar-text-strong: #f4f6fb; /* brand, active item */
  --sidebar-muted:     #7c879e;   /* labels, meta */
  --sidebar-active:    #4060ff;   /* active indicator bar */
}
```

### 3.4 Accents — constant in both themes

```css
:root,
.dark {
  --primary: #4060ff;   /* brand, primary CTA, active chips, focus */
  --info:    #3b82f6;   /* running */
  --success: #22c55a;   /* completed, live, healthy */
  --danger:  #ef2f2f;   /* failed, dead-lettered, destructive */
  --warn:    #f59e0b;   /* queued, retry warning */
  --purple:  #a855f7;   /* cancelled */
  --slate:   #64748b;   /* created, neutral */
}
```

**Accent discipline:** one accent role per surface. `--primary` appears at most twice
per viewport (typically: the active nav indicator + the single primary CTA). Status
colors carry semantic meaning only — never use them as decoration.

**Derived tint recipe (badges, chips, callouts):** tint = accent at 12–18% alpha over
the current `--surface`; text = a darker (light) / lighter (dark) shade of the same
hue. Generate with `color-mix(in oklch, <accent> 14%, <surface>)` and a matched
foreground. Never hand-pick unrelated hexes.

---

## 4. Status Semantics

Colorblind-safe: **a status is never communicated by color alone.** Every status
renders as a dot + label badge.

| Status | Token | Dot color | Dot motion | Label |
|---|---|---|---|---|
| created | `--slate` | `#64748b` | static | Created |
| queued | `--warn` | `#f59e0b` | static | Queued |
| running | `--info` | `#3b82f6` | **pulsing** (1.6s ease-out infinite) | Running |
| completed | `--success` | `#22c55a` | static | Completed |
| failed | `--danger` | `#ef2f2f` | static | Failed |
| dead-lettered | `--danger` | `#ef2f2f` | static | Dead-lettered |
| cancelled | `--purple` | `#a855f7` | static | Cancelled |

`StatusBadge` is the **only** sanctioned status representation. Reuse it in tables,
drawers, timelines, and chips. Never render a bare colored dot without its label.

---

## 5. Radius & Elevation

```css
--radius-sm: 6px;
--radius:    8px;     /* inputs, chips, buttons, small cards */
--radius-lg: 0.5rem;  /* 8px — table wrap, worker cards (use 12px where 8 feels tight) */
--radius-xl: 0.625rem;/* 10px — drawer header, large cards */
--radius-2xl: 0.875rem;/* 14px — modal surfaces, hero panels */
--radius-pill: 9999px;/* status badges, count pills, dots */
```

> Rounding is specified in rem per the brief; in practice the dashboard ships pixel
> equivalents (8/12/16/20px). Treat `--radius-lg` as 12px for visual consistency with
> the shipping console. Do not mix rem and px radii on the same surface.

**Elevation**

```css
--shadow-sm:    0 1px 2px rgba(15,23,42,.06), 0 1px 1px rgba(15,23,42,.04);
--shadow-card:  0 4px 14px -4px rgba(15,23,42,.10), 0 2px 4px rgba(15,23,42,.05);
--shadow-popover: 0 8px 28px -8px rgba(15,23,42,.16), 0 3px 8px rgba(15,23,42,.08);
--shadow-drawer: 0 18px 50px -12px rgba(15,23,42,.22), 0 6px 14px rgba(15,23,42,.10);
```

Dark theme deepens shadow alphas ~1.3× and uses `rgba(0,0,0,…)`.

---

## 6. Motion — 12 principles, applied

Calm and instrumental. Motion confirms state and guides the eye; it never performs.

| Principle | ChopFlow application |
|---|---|
| **Slow in / slow out** | Entrances use `cubic-bezier(.22,.61,.36,1)` (ease-out) ≤ 300 ms. Exits use ease-in ~180 ms. |
| **Squash & stretch** | Suppressed. Only `active: scale(0.98)` on buttons/rows — a press confirmation, not a bounce. |
| **Anticipation** | Drawer close chevron / X micro-rotates before the panel slides. |
| **Staging** | One focal change per moment: drawer opening dims the table; the table does not animate simultaneously. |
| **Follow-through** | Drawer panel settles, then backdrop fades — overlapping, not sequential. |
| **Slow out (overlapping)** | Row stagger ≤ 50 ms (40 ms in practice); cards stagger 60 ms. |
| **Arcs** | n/a — UI is planar; motion is linear-axis slides and fades. |
| **Secondary action** | Pulsing liveness dots and running-status dots accompany, never compete. |
| **Timing** | 140 ms (micro) · 220 ms (standard) · 300 ms (entrance cap). |
| **Exaggeration** | None. Restraint is the brand. |
| **Solid drawing** | Shadows + borders give surfaces weight; no flat floating panels. |
| **Appeal** | Consistent radius, mono/sans pairing, single accent — clarity over flair. |

**Pulsing dot** (liveness / running):

```css
@keyframes pulse-ring {
  0%   { transform: scale(.6); opacity: .7; }
  100% { transform: scale(1.8); opacity: 0; }
}
/* dot::after { border: 2px solid <accent>; animation: pulse-ring 2s ease-out infinite; } */
```

**Running badge dot** (tighter, attention-class):

```css
@keyframes dot-pulse {
  0%, 100% { box-shadow: 0 0 0 0 rgba(59,130,246,.5); }
  50%      { box-shadow: 0 0 0 4px rgba(59,130,246,0); }
} /* 1.6s ease-out infinite */
```

**Rules:**

- Entrance: opacity 0→1 + translateY(4–8px)→0, ease-out, ≤ 300 ms.
- Exit: opacity 1→0 + translateX(8px)→, ease-in, ~180 ms.
- Drawer: `translateX(100%)→0` ease-out open; reverse ease-in close. Backdrop opacity only.
- **Live data updates must be in-place.** Never re-render an entire list to update one
  cell — it restarts entrance animations and breaks hover/focus. Patch the changed node.
- Respect `prefers-reduced-motion`: disable pulses, staggers, and slides; keep opacity-only transitions.

---

## 7. Core Components

All components bind the tokens above. No raw hex inside a component.

### 7.1 StatusBadge

Dot + label pill. The sole status representation.

- Container: `--radius-pill`, `padding: 3px 9px 3px 8px`, `font-size: 11.5px`,
  `font-weight: 600`, `letter-spacing: 0.01em`.
- Background = accent tint (14% over surface); foreground = matched dark/light shade.
- Dot: 7 px, `--radius-pill`, status color. Running dot pulses (`dot-pulse`).
- **Forbidden:** color-only dots, color-only text, status icons without a label.

```html
<span class="status-badge running"><span class="dot"></span>Running</span>
```

### 7.2 Sidebar

Navy, always-dark, fixed left, 248 px (collapses to 64 px < 880 px).

- **Brand mark:** gradient tile (primary→violet) + wordmark ("Chop" strong, "Flow" muted).
- **Nav:** `Tasks`, `Workers`. Each item: icon (17 px) + label + right-aligned mono
  count pill. Active item: `--sidebar-text-strong` on `--sidebar-surface`, with a 3 px
  `--sidebar-active` indicator bar inset at the left edge (animated height-in on activation).
- **Cluster pulse card:** liveness dot (pulsing `--success`) + "Cluster live" + mono
  meta ("N workers · 1 region").
- **Footer:** theme toggle (track + knob, slides on dark) + mono version tag.
- Hover: item bg → `--sidebar-surface`, text → strong. Never change text toward `--sidebar-muted`.

### 7.3 TopBar

54 px command bar, `--surface` (light) / `--surface` (dark), bottom border.

- **Cluster selector:** button with live green dot + "local · default" + chevron.
  Dropdown lists clusters (live dot = `--success`, idle = `--subtle`), each with a mono
  worker count; "Add cluster" action at the bottom separated by a divider.
- **Breadcrumb:** "Operations / **Tasks**" — current segment bolded to `--text`.
- **Search:** leading magnifier icon, placeholder "Search tasks by id, name, tag…",
  trailing `⌘K` kbd hint. Resting on `--surface-2`; focus → `--surface`, `--primary`
  border, soft focus ring.
- **Live count pill:** segmented mono pill — queued (amber dot) · running (blue dot) ·
  total (subtle dot). Each segment updates in place.
- **Primary CTA:** "New Task" — the **only** solid primary button in the bar. `active:
  scale(0.98)`. Other entries are ghost/text.

### 7.4 TaskTable

Dense, calm, scannable. ~40 px rows.

- Wrap: `--surface`, `--border`, `--radius-lg`, single shadow on hover only via row bg.
- Header: 11 px, 600, ALL CAPS, `+0.06em`, `--muted`, on `--surface-2`.
- Columns: **ID** (mono, truncated ~8 chars) · **Name** (sans 550) · **Status**
  (`StatusBadge`) · **Tags** (mono pills) · **Retries** (mono `n/max`, right-aligned,
  turns `--warn` when ≥ 60% of max) · **Enqueued** (mono relative, updates in place) ·
  **Result** (mono, truncated; italic "running…" / "—" when empty).
- Row: pointer cursor; hover → `--row-hover`; open-drawer row → `--row-active`.
- Stagger entrance ≤ 40 ms per row, capped at ~200 ms.
- Clicking a row opens `TaskDrawer`. Empty state: centered search icon + "No tasks match this filter."
- Horizontal scroll only when viewport < table min-width (~780 px); never clip cells.

### 7.5 WorkerCard

Grid (auto-fill, min 320 px). Card: `--surface`, `--border`, `--radius-lg`, `--shadow-card` on hover, `translateY(-1px)` lift.

- **Head:** pulsing `--success` liveness dot + mono worker ID + mono address (right).
- **Tags:** mono pills.
- **Meters:** CPU and GPU. Each: uppercase label (10.5 px, `+0.05em`) + 7 px track on
  `--surface-3` + animated fill (`width 0→target` over 800 ms ease-out) + mono `n/max`.
  CPU fill = primary gradient; GPU fill = warn gradient; **full** (≥ 100%) fill = danger gradient.
- **Footer:** "N assigned" + "heartbeat Ym ago", mono, `--muted` with `--text-2` values.

### 7.6 TaskDrawer

Right slide-over, `max-width: 512 px`, full height, `--elevated` (dark) / `--surface`
(light), `--shadow-drawer`, `--border-strong` left edge. Dimmed backdrop (`rgba(10,16,30,.45)` + 2 px blur) behind.

- **Header:** task name + inline `StatusBadge` · full mono ID + "copy" link (turns
  `--success` "copied" for 1.4 s) · `Cancel` button (danger ghost, only for
  queued/running) · close X.
- **Tabs:** Summary · Lifecycle (with mono node count). Active tab: `--primary` text +
  2 px `--primary` underline.
- **Summary pane:** 2-col field grid (Status · Name · Tags · Retries · Enqueued ·
  Resources) on a 1 px `--border` grid; then **Payload** JSON block and **Result** JSON
  block — mono, `--surface-2`, syntax-highlighted (keys `--primary`, strings
  `--success`, numbers `--warn`, booleans `--purple`). Empty result → muted "— no result yet —".
  Retry warning callout (warn tint + triangle icon) when retries > 0.
- **Lifecycle pane:** vertical timeline. Nodes: Created → Queued → Running → terminal
  (Completed / Failed / Cancelled). Each node: dot + connecting rail colored by state
  (done = `--success`, queued-done = `--warn`, current = `--info` with pulsing ring,
  failed = `--danger`, cancelled = `--purple`, pending = dashed `--border-strong`).
  Retry callout at top when retries > 0 (backoff schedule 1s → 4s → 9s).
- Motion: `translateX(100%)→0` ease-out 220 ms open; reverse ease-in 180 ms close.
  Backdrop fades in parallel. Esc + backdrop click close.

### 7.7 Inputs

- **Text input:** 34 px, `--radius`, `--surface-2` resting, `--border-strong`. Focus →
  `--surface`, `--primary` border, `--focus-ring-soft` ring. Placeholder `--subtle`.
- **Buttons:** primary (solid `--primary`, white text, inset highlight), ghost
  (transparent, `--border-strong`, `--text-2`), danger (danger tint, danger text/border),
  icon (32 px square, `--muted`→`--text` on hover). All: `active: scale(0.98)`,
  `:focus-visible` ring.
- **Chips:** 30 px, `--surface`, `--border-strong`, status dot + label + mono count.
  Active → `--primary` border/text on primary tint.
- **Focus:** every focusable element has a `:focus-visible` ring
  (`2 px surface + 4 px --primary`), never just an outline:none black ring.

**Action economy:** one primary CTA per action per viewport. "New Task" is the sole
solid button in the TopBar; nav and drawer entries are ghost/text. A long page may
repeat the primary once at the end — never twice in one viewport.

---

## 8. Reference Content

Canonical copy and structure for the four reference surfaces. Use verbatim; do not
paraphrase the CLI flags or endpoints.

### 8.1 Architecture

ChopFlow is a Rust workspace with five crates and a single unified `Storage` trait.

| Crate | Responsibility |
|---|---|
| `core` | Domain types, task state machine, retry/backoff logic |
| `broker` | gRPC server (tonic), task scheduling, `Storage` orchestration |
| `worker` | Polling runtime, task execution, claim/ack lifecycle |
| `cli` | `chopflow_broker` / `chopflow_worker` binaries |
| `dashboard` | axum HTTP/JSON API + embedded SPA (React + Vite + Tailwind) |

**Transports & storage:**

- **gRPC:** tonic — `FetchTasks`, `claim_ready`, `AcknowledgeTask`.
- **Storage:** unified `Storage` trait; default backend **SQLite in WAL mode**.
- **HTTP/JSON:** axum serves `/api/*` and embeds the SPA bundle.
- **Frontend:** React + Vite + Tailwind, served from the broker's HTTP port.

**Pull model:** workers fetch work; the broker never pushes. This keeps workers
stateless and horizontally scalable, and makes backpressure implicit — a saturated
worker simply stops fetching.

### 8.2 Task Flow

```
enqueue ─▶ Queued ──FetchTasks──▶ [worker] claim_ready ──▶ Running
                                   │
            ┌──────────────────────┼─────────────────────┐
            ▼                      ▼                     ▼
       AcknowledgeTask        AcknowledgeTask        cancel
       (success)              (failure)              ──▶ Cancelled
            │                      │
            ▼                      ▼
       Completed              retry: exponential backoff + ETA
       (+ result)                 │
                              ┌───┴───┐
                              ▼       ▼
                          re-Queued   dead-letter
                          (retry)     (budget exhausted)
```

- **Enqueue** → task admitted as `Queued`.
- Worker calls `FetchTasks`; broker **claims a ready task** (`Queued → Running`).
- Worker calls `AcknowledgeTask`:
  - **success** → `Completed`, result persisted.
  - **failure** → retry with **exponential backoff** and a computed **ETA**; re-queued
    until the retry budget is exhausted, then **dead-lettered**.
- **cancel** (operator or API) at any pre-terminal state → `Cancelled`.

### 8.3 Dashboard Metrics

The dashboard surfaces cluster health, not marketing vanity numbers.

**Gauges:** `queue_length` · `tasks_processing` · `tasks_completed` · `tasks_failed` ·
`active_workers` · `total_tasks`.

**Status breakdown:** counts per status (created / queued / running / completed /
failed / dead-lettered / cancelled), each as a `StatusBadge` + mono count.

**Resource meters:** per-worker CPU and GPU utilization as animated bars (see
WorkerCard); cluster aggregate available in a metrics view.

All counts are **live** (poll interval ≤ 2 s) and **update in place** — never
re-render the list to change a number.

### 8.4 Quickstart

**Start the broker:**

```bash
chopflow_broker start \
  --host 127.0.0.1 \
  --port 8000 \
  --http-port 8080 \
  --open
```

`--open` launches the embedded dashboard at `http://127.0.0.1:8080`.

**Start a worker:**

```bash
chopflow_worker start \
  -b http://localhost:8000 \
  -t default \
  -r cpu:1
```

`-t` sets the task tags the worker claims; `-r` declares resources (e.g. `cpu:1`, `gpu:1`).

**HTTP API:**

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/api/stats` | Cluster gauges + status breakdown |
| `GET`  | `/api/tasks` | Task list (filterable by status) |
| `GET`  | `/api/workers` | Registered workers + liveness |
| `POST` | `/api/tasks` | Enqueue a task |
| `POST` | `/api/tasks/:id/cancel` | Cancel a queued/running task |

---

## 9. Craft Rules & Anti-Patterns

**Do:**

- Bind every color to a token. One accent. Mono for all identifiers and data.
- `StatusBadge` everywhere a status appears. Dot + label, always.
- In-place updates for live values; patch the node, don't rebuild the list.
- `:focus-visible` ring on every focusable element. Touch targets ≥ 44 px on mobile.
- Honest placeholders when real data is absent — never fabricate metrics.

**Don't (AI-slop tells):**

- Purple gradient washes or gradients on every surface layer.
- Emoji as functional icons. Hand-drawn SVG people.
- The "colored vertical bar + rounded card" callout as a default.
- Hover states that move text toward `--muted` or lower contrast.
- Inter/Roboto as a display face; `system-ui` alone on a heading.
- Two solid primary buttons for the same action in one viewport.
- Warm beige/cream backgrounds (ChopFlow is cool-neutral navy/slate).
- `white-space: nowrap` to force oversize type into adjacent elements; `overflow: hidden`
  to hide orphan characters. Fix the container instead.

**Contrast (hard requirements):** normal text ≥ 4.5:1; large text & icons ≥ 3:1. State
changes never reduce contrast. Hover moves the background ±0.06–0.12 on the OKLch L
channel, or adjusts border/shadow/position — never greys the foreground. Disabled is
the only state allowed to drop contrast.

**Layout integrity:** no accidental overlaps; no clipped/overflowing cells; no orphan
words on the final line; charts use filled encoding, never empty outlines.

---

## 10. Evolution Notes (from shipping dashboard)

The existing `chopflow-ops-dashboard.html` ships a near-compatible system with minor
token drift. This document is canonical; align the file as follows when next edited:

| Token (this system) | Shipping value (drift) | Action |
|---|---|---|
| `--canvas` (light) `#f7f8fa` | `#f5f7fb` | align to `#f7f8fa` |
| `--text` / `--muted` / `--subtle` | `--fg` / `--muted` / `--muted-2` | rename to semantic `text/muted/subtle` |
| `--surface-2` / `--surface-3` | `--surface-2` / `--surface-3` (fuzzy roles) | `surface-2` = inset fill; `surface-3` = hover/meter track |
| `--elevated` (dark) | absent | add for drawer/popover float |
| Sidebar tokens | `--sidebar` / `--sidebar-2` / `--sidebar-3` | rename to `--sidebar-bg` / `--sidebar-surface` / `--sidebar-surface-2` |
| Live ticker | full-table re-render | **must** patch the enqueued cell in place (rule §6) |
| `:focus-visible` | partial (buttons only) | add global ring for chips/nav/tabs/cluster selector |

All accent hexes (`--primary`, `--info`, `--success`, `--danger`, `--warn`, `--purple`,
`--slate`) and the navy sidebar base `#141a27` already match and remain locked.
