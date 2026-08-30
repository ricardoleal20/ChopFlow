// Small formatting helpers shared across views.

const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });

// Compact relative time from an epoch-ms timestamp: "5s ago", "3m ago", etc.
export function timeAgo(ms: number): string {
  const diff = ms - Date.now();
  const sec = Math.round(diff / 1000);
  const abs = Math.abs(sec);
  if (abs < 60) return rtf.format(sec, "second");
  const min = Math.round(sec / 60);
  if (Math.abs(min) < 60) return rtf.format(min, "minute");
  const hr = Math.round(sec / 3600);
  if (Math.abs(hr) < 24) return rtf.format(hr, "hour");
  return rtf.format(Math.round(sec / 86400), "day");
}

// Short absolute clock time: "14:09:32".
export function clockTime(ms: number): string {
  return new Date(ms).toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

// Short ID for dense tables: first 8 chars of a UUID.
export function shortId(id: string): string {
  return id.slice(0, 8);
}

// Truncate a string to n chars with an ellipsis.
export function truncate(s: string, n: number): string {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

// Pretty-print a JSON payload/result for the detail drawer.
export function pretty(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}
