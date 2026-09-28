import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import ts from "typescript";
import { beforeAll, describe, expect, it } from "vitest";

/**
 * What a user reads when a handler is wrong. The handler type is a small
 * named alias over the resolved message (`ConsumerHandler<{ … }>`), so a
 * mistake prints a short error instead of the whole contract type
 * (`WorkerInferConsumerHandlerEntry<ContractOutput<{ publishers: … }>>`).
 */

const here = dirname(fileURLToPath(import.meta.url));

const CASES = [
  ["an async handler", "sendEmail: async () => OkAsync(undefined)"],
  ["a handler returning void", "sendEmail: () => {}"],
  ["a handler reading a missing field", "sendEmail: ({ input }) => OkAsync(input.payload.from)"],
] as const;

const fixtureSource = (handlers: string): string => `
import { defineConsumer, defineContract, defineMessage, defineQueue } from "@amqp-contract/contract";
import { TypedAmqpWorker } from "./index.js";
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
 * Every case compiled as its own virtual file in ONE program, so the
 * TypeScript lib and dependency types load once (seconds on a cold CI runner)
 * instead of once per case.
 */
function diagnoseAll(): Map<string, string[]> {
  const fixtures = new Map(
    CASES.map(([name, handlers], i) => [
      join(here, `__handler-diagnostics-fixture-${i}__.ts`),
      { name, source: fixtureSource(handlers) },
    ]),
  );
  const config = ts.getParsedCommandLineOfConfigFile(
    join(here, "..", "tsconfig.json"),
    {},
    { ...ts.sys, onUnRecoverableConfigFileDiagnostic: () => {} },
  )!;
  const host = ts.createCompilerHost(config.options);
  const getSourceFile = host.getSourceFile.bind(host);
  host.getSourceFile = (fileName, language) => {
    const fixture = fixtures.get(fileName);
    return fixture
      ? ts.createSourceFile(fileName, fixture.source, language)
      : getSourceFile(fileName, language);
  };
  const program = ts.createProgram([...fixtures.keys()], { ...config.options, noEmit: true }, host);
  return new Map(
    [...fixtures].map(([fileName, { name }]) => [
      name,
      ts
        .getPreEmitDiagnostics(program, program.getSourceFile(fileName))
        .map((d) => ts.flattenDiagnosticMessageText(d.messageText, "\n")),
    ]),
  );
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
