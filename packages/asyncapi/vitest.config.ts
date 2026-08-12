import { defineConfig } from "vitest/config";

import { sharedVitestConfig } from "../../vitest.shared.js";

export default defineConfig(
  sharedVitestConfig({
    thresholds: { statements: 95, branches: 78, functions: 95, lines: 95 },
  }),
);
