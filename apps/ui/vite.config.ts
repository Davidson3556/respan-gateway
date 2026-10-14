import path from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

const gateway = process.env.GATEWAY_URL ?? "http://127.0.0.1:8080";
const proxy = {
  "/ui/api": gateway,
  "/v1": { target: gateway, ws: true },
  "/v1beta": gateway,
  // Gemini Live's sessions, at the path Google's own clients dial.
  "/ws": { target: gateway, ws: true },
  "/healthz": gateway,
};

export default defineConfig({
  base: "/ui/",
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { "@": path.resolve(import.meta.dirname, "src") },
  },
  build: { outDir: "dist" },
  server: { proxy },
  preview: { proxy },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["src/test/setup.ts"],
  },
});
