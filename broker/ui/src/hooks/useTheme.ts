import { useCallback, useEffect, useState } from "react";

// Dual-theme controller. The actual values live as CSS variables in index.css
// (light in :root, dark under `.dark`); this hook only flips the `dark` class
// on <html> and remembers the choice. `index.html` applies the saved theme
// before first paint to avoid a flash.
const STORAGE_KEY = "chopflow-theme";

type Theme = "light" | "dark";

function getInitial(): Theme {
  if (typeof document !== "undefined" && document.documentElement.classList.contains("dark")) {
    return "dark";
  }
  return "light";
}

export function useTheme() {
  const [theme, setTheme] = useState<Theme>(getInitial);

  // Keep the <html> class + localStorage in sync with state.
  useEffect(() => {
    const root = document.documentElement;
    root.classList.toggle("dark", theme === "dark");
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch {
      /* ignore storage failures (private mode, etc.) */
    }
  }, [theme]);

  const toggle = useCallback(() => {
    setTheme((t) => (t === "dark" ? "light" : "dark"));
  }, []);

  return { theme, toggle };
}
