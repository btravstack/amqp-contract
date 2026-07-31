import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    reporters: ["default"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "json-summary", "html"],
      // `*.test-d.ts` files are typechecked, never executed — v8 counts every
      // line as uncovered and skews the denominator. Exclude them alongside
      // the integration suites.
      include: ["src/**", "!src/**/__tests__/**", "!src/**/*.test-d.ts"],
      // Ratchet floors (unit project only — integration coverage runs
      // separately in CI with Docker). Raise as coverage grows; never lower.
      thresholds: {
        statements: 90,
        branches: 88,
        functions: 90,
        lines: 90,
      },
    },
    typecheck: {
      enabled: true,
      include: ["src/**/*.test-d.ts"],
    },
  },
});
