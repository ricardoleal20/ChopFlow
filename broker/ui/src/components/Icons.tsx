// Inline icon set (stroke-based, currentColor) matching the OpenDesign
// reference. Stroke width and sizing inherit from the surrounding styles.
import type { SVGProps } from "react";

type IconProps = SVGProps<SVGSVGElement>;

const stroke = (sw = 2) => ({
  fill: "none",
  stroke: "currentColor",
  strokeWidth: sw,
  strokeLinecap: "round" as const,
  strokeLinejoin: "round" as const,
  viewBox: "0 0 24 24",
});

// Brand flow mark — two forward chevrons, on the gradient tile.
export const BrandMark = (p: IconProps) => (
  <svg viewBox="0 0 24 24" fill="none" {...p}>
    <path d="M4 7h6l3 5-3 5H4l3-5z" fill="#fff" />
    <path d="M13 7h7v3h-4l1.5 2L16 14h4v3h-7l-2.5-4z" fill="#fff" opacity=".8" />
  </svg>
);

export const TasksIcon = (p: IconProps) => (
  <svg {...stroke(2)} {...p}>
    <path d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2" />
    <rect x="9" y="3" width="6" height="4" rx="1" />
    <path d="M9 12h6M9 16h4" />
  </svg>
);

export const WorkersIcon = (p: IconProps) => (
  <svg {...stroke(2)} {...p}>
    <rect x="3" y="4" width="18" height="12" rx="2" />
    <path d="M6 20h12M9 16v4M15 16v4" />
    <circle cx="8" cy="10" r="1" fill="currentColor" stroke="none" />
    <circle cx="12" cy="10" r="1" fill="currentColor" stroke="none" />
    <circle cx="16" cy="10" r="1" fill="currentColor" stroke="none" />
  </svg>
);

export const SunIcon = (p: IconProps) => (
  <svg {...stroke(2)} {...p}>
    <circle cx="12" cy="12" r="4" />
    <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
  </svg>
);

export const MoonIcon = (p: IconProps) => (
  <svg {...stroke(2)} {...p}>
    <path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z" />
  </svg>
);

export const SearchIcon = (p: IconProps) => (
  <svg {...stroke(2)} {...p}>
    <circle cx="11" cy="11" r="7" />
    <path d="m21 21-4.3-4.3" />
  </svg>
);

export const PlusIcon = (p: IconProps) => (
  <svg {...stroke(2.5)} {...p}>
    <path d="M12 5v14M5 12h14" />
  </svg>
);

export const CloseIcon = (p: IconProps) => (
  <svg {...stroke(2)} {...p}>
    <path d="M18 6 6 18M6 6l12 12" />
  </svg>
);

export const ChevronDownIcon = (p: IconProps) => (
  <svg {...stroke(2.5)} {...p}>
    <path d="M6 9l6 6 6-6" />
  </svg>
);

export const CopyIcon = (p: IconProps) => (
  <svg {...stroke(2)} {...p}>
    <rect x="9" y="9" width="11" height="11" rx="2" />
    <path d="M5 15V5a2 2 0 0 1 2-2h10" />
  </svg>
);

export const CheckIcon = (p: IconProps) => (
  <svg {...stroke(2.5)} {...p}>
    <path d="M20 6 9 17l-5-5" />
  </svg>
);

export const AlertIcon = (p: IconProps) => (
  <svg {...stroke(2)} {...p}>
    <path d="M12 9v4M12 17h.01" />
    <path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z" />
  </svg>
);

// Clock + calendar hybrid — the schedule glyph. Matches the icon set's
// stroke-based currentColor style (strokeWidth 1.5).
export const ScheduleIcon = (p: IconProps) => (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth={1.5}
    strokeLinecap="round"
    strokeLinejoin="round"
    {...p}
  >
    <rect x="3" y="4.5" width="18" height="16" rx="2" />
    <path d="M3 9h18M8 3v3M16 3v3" />
    <circle cx="12" cy="14" r="3.2" />
    <path d="M12 12.4v1.7l1.1 1.1" />
  </svg>
);
