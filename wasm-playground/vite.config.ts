import { defineConfig } from "vite";
import UnoCSS from "unocss/vite";
import react from "@vitejs/plugin-react";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import jotaiDebugLabel from "jotai/babel/plugin-debug-label";
import jotaiReactRefresh from "jotai/babel/plugin-react-refresh";

// https://vitejs.dev/config/
const repoBase = process.env.GITHUB_REPOSITORY?.split("/")[1] ?? "FastFiRSt";

export default defineConfig({
  base: `/${repoBase}/`,
  plugins: [
    wasm(),
    topLevelAwait(),
    react({ babel: { plugins: [jotaiDebugLabel, jotaiReactRefresh] } }),
    UnoCSS(),
  ],
});
