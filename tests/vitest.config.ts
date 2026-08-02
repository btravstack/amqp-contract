import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    reporters: ["default"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "json-summary", "html"],
      include: ["src/**", "!src/**/__tests__/**"],
    },
    projects: [
      {
        test: {
          // Runs in the main gate. No broker: nothing here may need one.
          name: "unit",
          environment: "node",
          include: ["src/**/*.spec.ts"],
          exclude: ["src/**/__tests__/*.spec.ts"],
        },
      },
      {
        test: {
          name: "integration",
          environment: "node",
          globalSetup: "@amqp-contract/testing/global-setup",
          include: ["src/**/__tests__/*.spec.ts"],
          testTimeout: 10_000,
          hookTimeout: 10_000,
        },
      },
    ],
  },
});
