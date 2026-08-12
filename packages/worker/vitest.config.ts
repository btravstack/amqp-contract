import { defineConfig } from "vitest/config";

import { sharedVitestConfig } from "../../vitest.shared.js";

export default defineConfig(
  sharedVitestConfig({
    thresholds: { statements: 30, branches: 33, functions: 31, lines: 30 },
    typecheck: true,
    integration: true,
    setupFile: "./src/vitest.setup.ts",
  }),
);
