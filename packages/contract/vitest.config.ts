import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    reporters: ["default"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "json-summary", "html"],
      include: ["src/**", "!src/**/__tests__/**"],
      // Ratchet floors (unit project only — integration coverage runs
      // separately in CI with Docker). Raise as coverage grows; never lower.
      thresholds: {
        statements: 50,
        branches: 85,
        functions: 40,
        lines: 50,
      },
    },
    typecheck: {
      enabled: true,
      include: ["src/**/*.test-d.ts"],
    },
  },
});
