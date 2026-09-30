import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { beforeAll, describe, expect, it } from "vitest";

/**
 * What a user reads when a handler is wrong. The handler type is a small
 * named alias over the resolved message (`ConsumerHandler<{ … }>`), so a
 * mistake prints a short error instead of the whole contract type
 * (`WorkerInferConsumerHandlerEntry<ContractOutput<{ publishers: … }>>`).
 */

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");

// TypeScript 7 ships no JS compiler API, so this runs the `tsc` binary. Its JS
// entry is resolved via `package.json` — the only subpath its `exports` map
// allows — and run under `process.execPath`, not the `.bin` shim (`tsc.cmd` on
// Windows).
const TSC = join(
  dirname(createRequire(import.meta.url).resolve("typescript/package.json")),
  "bin",
  "tsc",
);

const CASES = [
  ["an async handler", "sendEmail: async () => OkAsync(undefined)"],
  ["a handler returning void", "sendEmail: () => {}"],
  ["a handler reading a missing field", "sendEmail: ({ input }) => OkAsync(input.payload.from)"],
] as const;

const fixtureSource = (handlers: string): string => `
import { defineConsumer, defineContract, defineMessage, defineQueue } from "@amqp-contract/contract";
import { TypedAmqpWorker } from "../src/index.js";
import { OkAsync } from "unthrown";
import { z } from "zod";

const contract = defineContract({
  consumers: {
    sendEmail: defineConsumer(
      defineQueue("emails", { onPoison: "drop" }),
      defineMessage(z.object({ to: z.string(), subject: z.string() })),
    ),
  },
});

TypedAmqpWorker.create({ contract, urls: [], handlers: { ${handlers} } });
void OkAsync;
`;

/**
 * Every case compiled as its own file in ONE `tsc` run, so the TypeScript lib
 * and dependency types load once instead of once per case. The files sit inside
 * the package, so they resolve its dependencies and compile with its tsconfig.
 */
function diagnoseAll(): Map<string, string[]> {
  const probeDir = mkdtempSync(join(packageDir, ".tsc-probe-"));
  let output = "";
  try {
    CASES.forEach(([, handlers], i) => {
      writeFileSync(join(probeDir, `fixture-${i}.ts`), fixtureSource(handlers));
    });
    writeFileSync(
      join(probeDir, "tsconfig.json"),
      JSON.stringify({
        extends: "../tsconfig.json",
        compilerOptions: { noEmit: true, rootDir: ".." },
        include: ["fixture-*.ts"],
      }),
    );
    execFileSync(process.execPath, [TSC, "-p", "tsconfig.json", "--pretty", "false"], {
      cwd: probeDir,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (error) {
    output = String((error as { stdout?: string }).stdout ?? "");
  } finally {
    rmSync(probeDir, { recursive: true, force: true });
  }

  // One block per diagnostic, `fixture-<i>.ts(<line>,<col>): error TS…: ` then
  // the message and its chained lines.
  const messages = new Map<number, string[]>();
  for (const block of output.split(/^(?=\S+\(\d+,\d+\): error TS)/m)) {
    const header = /^fixture-(\d+)\.ts\(\d+,\d+\): error TS\d+: /.exec(block);
    if (!header) continue;
    const i = Number(header[1]);
    messages.set(i, [...(messages.get(i) ?? []), block.slice(header[0].length).trimEnd()]);
  }
  return new Map(CASES.map(([name], i) => [name, messages.get(i) ?? []]));
}

describe("handler type errors are readable", () => {
  let diagnostics: Map<string, string[]>;

  beforeAll(() => {
    diagnostics = diagnoseAll();
  }, 60_000);

  it.for(CASES)("%s: the error names the resolved message, not the contract", ([name]) => {
    const errors = diagnostics.get(name) ?? [];

    expect(errors.length).toBeGreaterThan(0);
    for (const error of errors) {
      expect(error).not.toContain("ContractOutput<");
      expect(error.length).toBeLessThan(700);
    }
    expect(errors.join("\n")).toMatch(
      /ConsumerHandler(Entry)?<\{ to: string; subject: string; \}|'\{ to: string; subject: string; \}'/,
    );
  });
});
