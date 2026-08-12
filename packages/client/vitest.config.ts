import { defineConfig } from "vitest/config";

import { sharedVitestConfig } from "../../vitest.shared.js";

export default defineConfig(
  sharedVitestConfig({
    thresholds: { statements: 14, branches: 15, functions: 18, lines: 14 },
    typecheck: true,
    integration: true,
    setupFile: "./src/vitest.setup.ts",
  }),
);
