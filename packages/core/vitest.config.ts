import { defineConfig } from "vitest/config";

import { sharedVitestConfig } from "../../vitest.shared.js";

// The alias half of the arrangement `tsconfig.json` documents: with no
// devDependency to resolve through, the specs' `@amqp-contract/testing/*`
// imports are pointed at the source.
const testingSource = (entry: string) =>
  new URL(`../testing/src/${entry}.ts`, import.meta.url).pathname;

export default defineConfig(
  sharedVitestConfig({
    thresholds: { statements: 20, branches: 10, functions: 12, lines: 20 },
    integration: true,
    setupFile: "./src/vitest.setup.ts",
    alias: { "@amqp-contract/testing/extension": testingSource("extension") },
    globalSetup: testingSource("global-setup"),
  }),
);
