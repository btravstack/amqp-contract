import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import ts from "typescript";
import { describe, expect, it } from "vitest";

/**
 * What a user reads when a handler is wrong. The handler type is a small
 * named alias over the resolved message (`ConsumerHandler<{ … }>`), so a
 * mistake prints a short error instead of the whole contract type
 * (`WorkerInferConsumerHandlerEntry<ContractOutput<{ publishers: … }>>`).
 */

const here = dirname(fileURLToPath(import.meta.url));
const fixture = join(here, "__handler-diagnostics-fixture__.ts");

function diagnose(handlers: string): string[] {
  const source = `
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
  const config = ts.getParsedCommandLineOfConfigFile(
    join(here, "..", "tsconfig.json"),
    {},
    { ...ts.sys, onUnRecoverableConfigFileDiagnostic: () => {} },
  )!;
  const host = ts.createCompilerHost(config.options);
  const getSourceFile = host.getSourceFile.bind(host);
  host.getSourceFile = (fileName, language) =>
    fileName === fixture
      ? ts.createSourceFile(fileName, source, language)
      : getSourceFile(fileName, language);
  const program = ts.createProgram([fixture], { ...config.options, noEmit: true }, host);
  return ts
    .getPreEmitDiagnostics(program, program.getSourceFile(fixture))
    .map((d) => ts.flattenDiagnosticMessageText(d.messageText, "\n"));
}

describe("handler type errors are readable", () => {
  it.for([
    ["an async handler", "sendEmail: async () => OkAsync(undefined)"],
    ["a handler returning void", "sendEmail: () => {}"],
    ["a handler reading a missing field", "sendEmail: ({ input }) => OkAsync(input.payload.from)"],
  ] as const)("%s: the error names the resolved message, not the contract", ([, handlers]) => {
    const errors = diagnose(handlers);

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
