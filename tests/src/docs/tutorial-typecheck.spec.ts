import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

import { afterAll, describe, expect, it } from "vitest";

/**
 * The getting-started tutorial tells the reader to run `npx tsc --noEmit`, so
 * its files and tsconfig must typecheck exactly as printed.
 *
 * They did not: under TypeScript 6, `types` defaults to none, so a tsconfig
 * without `"types": ["node"]` saw ~40 errors from Node globals used in our
 * (and amqplib's) declaration files. The snippet suite could not catch it — it
 * executes contracts, it never typechecks a reader's project.
 *
 * The project is materialised under `tests/` so its imports resolve through
 * this workspace's dependencies — the packages a reader would install.
 */

// TypeScript 7 ships no JS compiler API, so this runs the `tsc` binary — the
// command the tutorial itself prints. Its JS entry is resolved via
// `package.json` (the only subpath its `exports` map allows) and run under
// `process.execPath`, not the `.bin` shim (`tsc.cmd` on Windows).
const TSC = join(
  dirname(createRequire(import.meta.url).resolve("typescript/package.json")),
  "bin",
  "tsc",
);

const repoRoot = join(import.meta.dirname, "..", "..", "..");
const tutorialPath = join(repoRoot, "docs", "tutorial", "getting-started.md");
const projectDir = join(repoRoot, "tests", ".tutorial");

const markdown = readFileSync(tutorialPath, "utf8");
const blocks = [...markdown.matchAll(/^```(\w+)\n([\s\S]*?)^```$/gm)].map((match) => ({
  lang: match[1] ?? "",
  code: match[2] ?? "",
}));

/** Blocks whose first line names the file the reader creates: `// contract.ts`. */
const files = blocks.flatMap(({ code }) => {
  const name = /^\/\/ (\S+\.ts)\n/.exec(code)?.[1];
  return name ? [{ name, code }] : [];
});
const tsconfigBlock = blocks.find(
  ({ lang, code }) => lang === "json" && code.includes('"compilerOptions"'),
);
const installDevLine = markdown.split("\n").find((line) => line.startsWith("npm install -D "));

afterAll(() => {
  rmSync(projectDir, { recursive: true, force: true });
});

describe("getting-started tutorial", () => {
  it("prints the files, tsconfig and install line this check reads", () => {
    expect(files.map(({ name }) => name).sort()).toEqual([
      "consumer.ts",
      "contract.ts",
      "publisher.ts",
    ]);
    expect(tsconfigBlock).toBeDefined();
    expect(installDevLine).toBeDefined();
  });

  it("installs every @types package its tsconfig lists", () => {
    const { compilerOptions } = JSON.parse(tsconfigBlock?.code ?? "{}") as {
      compilerOptions?: { types?: string[] };
    };
    for (const type of compilerOptions?.types ?? []) {
      expect(installDevLine?.split(/\s+/)).toContain(`@types/${type}`);
    }
  });

  it("typechecks with the tsconfig it prints", { timeout: 60_000 }, () => {
    rmSync(projectDir, { recursive: true, force: true });
    mkdirSync(projectDir, { recursive: true });
    // Step 2's `npm pkg set type=module`.
    writeFileSync(join(projectDir, "package.json"), '{ "type": "module" }\n');
    for (const { name, code } of files) writeFileSync(join(projectDir, name), code);

    writeFileSync(join(projectDir, "tsconfig.json"), tsconfigBlock?.code ?? "{}");

    let output = "";
    try {
      execFileSync(process.execPath, [TSC, "--noEmit", "--pretty", "false"], {
        cwd: projectDir,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
      });
    } catch (error) {
      // stderr too: a `tsc` that crashes prints nothing to stdout.
      const { stdout, stderr, message } = error as {
        stdout?: string;
        stderr?: string;
        message: string;
      };
      output = `${stdout ?? ""}${stderr ?? ""}` || message;
    }

    expect(output).toBe("");
  });
});
