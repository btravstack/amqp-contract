import { defineConfig } from "vitest/config";

import { sharedVitestConfig } from "../../vitest.shared.js";

export default defineConfig(
  sharedVitestConfig({
    thresholds: { statements: 90, branches: 88, functions: 90, lines: 90 },
    typecheck: true,
  }),
);
