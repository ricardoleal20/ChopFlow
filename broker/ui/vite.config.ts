import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev server proxies /api to the broker's HTTP port (default 8080) so the
// frontend talks to the real backend with no CORS ceremony in dev.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://localhost:8080",
      "/healthz": "http://localhost:8080",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
