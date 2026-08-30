/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["Geist", "Inter", "system-ui", "sans-serif"],
        mono: ["JetBrains Mono", "ui-monospace", "monospace"],
      },
      colors: {
        // Semantic surface tokens for a dark developer-tool canvas.
        canvas: "#0a0c10",
        surface: "#11141b",
        surface2: "#161a23",
        elevated: "#1c2130",
        border: "#232938",
        borderStrong: "#2e3548",
        muted: "#8a93a6",
        subtle: "#5b6478",
        text: "#e7ebf3",
        // Accents — chosen against the AI-default purple/indigo. Emerald for
        // success/active, amber for running/attention, rose for failure,
        // sky for info/links.
        accent: "#34d399",
        accentDim: "#10b981",
        warn: "#f59e0b",
        danger: "#f43f5e",
        info: "#38bdf8",
      },
      borderRadius: {
        xl: "0.75rem",
        "2xl": "1rem",
      },
      boxShadow: {
        card: "0 1px 0 0 rgba(255,255,255,0.03) inset, 0 1px 2px 0 rgba(0,0,0,0.4)",
        glow: "0 0 0 1px rgba(52,211,153,0.25), 0 8px 30px -8px rgba(52,211,153,0.15)",
      },
      keyframes: {
        pulseDot: {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0.35" },
        },
      },
      animation: {
        pulseDot: "pulseDot 1.8s ease-in-out infinite",
      },
    },
  },
  plugins: [],
};
