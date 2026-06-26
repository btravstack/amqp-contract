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
          name: "unit",
          setupFiles: ["./src/vitest.setup.ts"],
          include: ["src/**/*.spec.ts"],
          exclude: ["src/**/__tests__/*.spec.ts"],
        },
      },
      {
        test: {
          name: "integration",
          setupFiles: ["./src/vitest.setup.ts"],
          globalSetup: "@amqp-contract/testing/global-setup",
          include: ["src/**/__tests__/*.spec.ts"],
          testTimeout: 10_000,
          hookTimeout: 10_000,
        },
      },
    ],
  },
});
