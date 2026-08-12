import { describe, expect, it } from "vitest";

import { sharedVitestConfig } from "../../vitest.shared.js";

/**
 * The six workspace configs exercise most of this factory, but not every
 * combination — `setupFile` without `integration` had no config using it, and
 * silently dropped the setup file. A dropped setup file does not fail a run,
 * it just quietly stops running the setup, so it needs an assertion rather
 * than a caller.
 */
describe("sharedVitestConfig", () => {
  it("applies setupFile at the top level when there is no integration split", () => {
    const config = sharedVitestConfig({ setupFile: "./src/vitest.setup.ts" });

    expect(config.test).toMatchObject({ setupFiles: ["./src/vitest.setup.ts"] });
    expect(config.test).not.toHaveProperty("projects");
  });

  it("applies setupFile to every project when split", () => {
    const config = sharedVitestConfig({ setupFile: "./src/vitest.setup.ts", integration: true });
    const projects = (config.test as { projects: { test: Record<string, unknown> }[] }).projects;

    expect(projects.map((p) => p.test["name"])).toEqual(["unit", "integration"]);
    for (const project of projects) {
      expect(project.test["setupFiles"]).toEqual(["./src/vitest.setup.ts"]);
    }
    // Projects do not inherit root `test` options, so the root must not also
    // declare it — that would be a second, unused source of truth.
    expect(config.test).not.toHaveProperty("setupFiles");
  });

  it("omits setupFiles entirely when no setup file is given", () => {
    expect(sharedVitestConfig().test).not.toHaveProperty("setupFiles");
    const split = sharedVitestConfig({ integration: true });
    const projects = (split.test as { projects: { test: Record<string, unknown> }[] }).projects;
    for (const project of projects) {
      expect(project.test).not.toHaveProperty("setupFiles");
    }
  });

  it("puts type tests at the top level unsplit, and on the unit project when split", () => {
    expect(sharedVitestConfig({ typecheck: true }).test).toHaveProperty("typecheck");

    const split = sharedVitestConfig({ typecheck: true, integration: true });
    const projects = (split.test as { projects: { test: Record<string, unknown> }[] }).projects;
    expect(split.test).not.toHaveProperty("typecheck");
    expect(projects[0]!.test).toHaveProperty("typecheck");
    expect(projects[1]!.test).not.toHaveProperty("typecheck");
  });
});
