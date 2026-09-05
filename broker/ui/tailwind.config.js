/** @type {import('tailwindcss').Config} */
// Dual-theme system: semantic colors are CSS variables (rgb channels) defined
// in index.css for :root (light) and .dark (dark). This avoids littering
// `dark:` across every component — toggling the `dark` class on <html> flips
// the whole UI. The navy sidebar keeps constant tokens in both themes, mirroring
// Temporal's always-dark rail.
export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        // Inter matches Temporal's UI type voice.
        sans: ["Inter", "system-ui", "sans-serif"],
        mono: ["JetBrains Mono", "ui-monospace", "monospace"],
      },
      colors: {
        canvas: "rgb(var(--canvas) / <alpha-value>)",
        surface: "rgb(var(--surface) / <alpha-value>)",
        surface2: "rgb(var(--surface2) / <alpha-value>)",
        elevated: "rgb(var(--elevated) / <alpha-value>)",
        border: "rgb(var(--border) / <alpha-value>)",
        borderStrong: "rgb(var(--border-strong) / <alpha-value>)",
        text: "rgb(var(--text) / <alpha-value>)",
        muted: "rgb(var(--muted) / <alpha-value>)",
        subtle: "rgb(var(--subtle) / <alpha-value>)",
        // Sidebar: constant navy in both themes (Temporal-style).
        sidebar: "rgb(var(--sidebar-bg) / <alpha-value>)",
        sidebarSurface: "rgb(var(--sidebar-surface) / <alpha-value>)",
        sidebarBorder: "rgb(var(--sidebar-border) / <alpha-value>)",
        sidebarText: "rgb(var(--sidebar-text) / <alpha-value>)",
        sidebarMuted: "rgb(var(--sidebar-muted) / <alpha-value>)",
        // Accents — constant across themes.
        primary: "rgb(var(--primary) / <alpha-value>)",
        info: "rgb(var(--info) / <alpha-value>)",
        success: "rgb(var(--success) / <alpha-value>)",
        danger: "rgb(var(--danger) / <alpha-value>)",
        warn: "rgb(var(--warn) / <alpha-value>)",
        purple: "rgb(var(--purple) / <alpha-value>)",
        slate2: "rgb(var(--slate) / <alpha-value>)",
      },
      borderRadius: {
        xl: "0.625rem",
        "2xl": "0.875rem",
      },
      boxShadow: {
        card: "0 1px 2px 0 rgba(0,0,0,0.06), 0 1px 0 0 rgba(255,255,255,0.04) inset",
        drawer: "-12px 0 40px -12px rgba(0,0,0,0.25)",
        popover: "0 8px 30px -6px rgba(0,0,0,0.25)",
      },
      keyframes: {
        pulseDot: {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0.35" },
        },
        dash: {
          to: { "stroke-dashoffset": "-16" },
        },
      },
      animation: {
        pulseDot: "pulseDot 1.8s ease-in-out infinite",
        dash: "dash 1s linear infinite",
      },
    },
  },
  plugins: [],
};
