import { defineConfig } from "vitest/config";

import { sharedVitestConfig } from "../../vitest.shared.js";

export default defineConfig(
  sharedVitestConfig({
    thresholds: { statements: 20, branches: 10, functions: 12, lines: 20 },
    integration: true,
    setupFile: "./src/vitest.setup.ts",
  }),
);
