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
        statements: 95,
        branches: 78,
        functions: 95,
        lines: 95,
      },
    },
  },
});
