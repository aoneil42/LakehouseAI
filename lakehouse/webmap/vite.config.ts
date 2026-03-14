import { defineConfig } from "vite";
import wasm from "vite-plugin-wasm";

export default defineConfig({
  plugins: [wasm()],
  optimizeDeps: {
    exclude: ["@geoarrow/geoparquet-wasm"],
  },
  server: {
    port: 5173,
    proxy: {
      "/api/agent": "http://localhost:8090",
      "/api": "http://localhost:8000",
      "/ws/agent": { target: "http://localhost:8090", ws: true },
      "/ws": { target: "http://localhost:8000", ws: true },
    },
  },
  build: {
    target: "es2022",
  },
});
