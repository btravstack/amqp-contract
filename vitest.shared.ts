/**
 * Shared vitest configuration for every workspace.
 *
 * The six configs were ~85% identical boilerplate — same environment, same
 * reporters, same coverage provider/reporter/include, same unit/integration
 * project split, same timeouts — differing only in their coverage floors and
 * whether they declare type tests or need a broker. Those four differences are
 * the parameters below; everything else lives here once.
 *
 * Deliberately import-free so it resolves from any workspace without the root
 * needing a `vitest` dependency of its own. Each config wraps the result in
 * its own `defineConfig`.
 */

/** Coverage ratchet floors. Raise as coverage grows; never lower. */
export type CoverageThresholds = {
  statements: number;
  branches: number;
  functions: number;
  lines: number;
};

export type SharedConfigOptions = {
  /**
   * Coverage floors for the unit run. Omit for a workspace that carries none
   * (integration coverage runs separately in CI with Docker).
   */
  thresholds?: CoverageThresholds;
  /** Declare the `*.test-d.ts` type tests. */
  typecheck?: boolean;
  /**
   * Split into `unit` and `integration` projects. The integration project
   * boots a broker via testcontainers; the unit project must never need one.
   */
  integration?: boolean;
  /** Setup file applied to every project, relative to the package root. */
  setupFile?: string;
};

/** Type tests are typechecked, never executed — see the `include` note below. */
const TYPECHECK = { enabled: true, include: ["src/**/*.test-d.ts"] } as const;

export function sharedVitestConfig({
  thresholds,
  typecheck = false,
  integration = false,
  setupFile,
}: SharedConfigOptions = {}) {
  const setupFiles = setupFile ? { setupFiles: [setupFile] } : {};

  return {
    test: {
      environment: "node",
      reporters: ["default"],
      coverage: {
        provider: "v8",
        reporter: ["text", "json", "json-summary", "html"],
        // `*.test-d.ts` files are typechecked, never executed — v8 counts
        // every line as uncovered and skews the denominator. Excluded
        // alongside the integration suites.
        include: ["src/**", "!src/**/__tests__/**", "!src/**/*.test-d.ts"],
        ...(thresholds ? { thresholds } : {}),
      },
      // Without an integration split there is only one run, so type tests are
      // declared at the top level; with one they belong to the unit project.
      ...(typecheck && !integration ? { typecheck: TYPECHECK } : {}),
      ...(integration
        ? {
            projects: [
              {
                test: {
                  // Runs in the main gate. No broker: nothing here may need one.
                  name: "unit",
                  environment: "node",
                  ...setupFiles,
                  include: ["src/**/*.spec.ts"],
                  exclude: ["src/**/__tests__/*.spec.ts"],
                  ...(typecheck ? { typecheck: TYPECHECK } : {}),
                },
              },
              {
                test: {
                  name: "integration",
                  environment: "node",
                  ...setupFiles,
                  globalSetup: "@amqp-contract/testing/global-setup",
                  include: ["src/**/__tests__/*.spec.ts"],
                  testTimeout: 10_000,
                  hookTimeout: 10_000,
                },
              },
            ],
          }
        : {}),
    },
  };
}
