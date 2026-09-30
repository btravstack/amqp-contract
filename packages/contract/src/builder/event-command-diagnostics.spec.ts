import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { stripVTControlCharacters } from "node:util";

import { beforeAll, describe, expect, it } from "vitest";

/**
 * The compiler's actual wording for the common event/command builder mistakes.
 *
 * `@ts-expect-error` (see `event-command-signatures.test-d.ts`) only proves a
 * mistake is rejected, not that the error says why. These builders used to be
 * overload sets, and TypeScript reports an overload failure against whichever
 * overload matched the argument count — so a forgotten routing key on a direct
 * exchange read "DirectExchangeDefinition is not assignable to
 * FanoutExchangeDefinition | HeadersExchangeDefinition". This compiles each
 * mistake with the package's own tsconfig and pins the message it produces.
 */

const packageDir = resolve(dirname(fileURLToPath(import.meta.url)), "../..");

// TypeScript 7 ships no JS compiler API, so this runs the `tsc` binary. Its JS
// entry is resolved via `package.json` — the only subpath its `exports` map
// allows — and run under `process.execPath`, not the `.bin` shim (`tsc.cmd` on
// Windows).
const TSC = join(
  dirname(createRequire(import.meta.url).resolve("typescript/package.json")),
  "bin",
  "tsc",
);

const cases = {
  directPublisherWithoutOptions: "defineEventPublisher(direct, message);",
  topicPublisherWithoutRoutingKey: "defineEventPublisher(topic, message, {});",
  fanoutPublisherWithRoutingKey: 'defineEventPublisher(fanout, message, { routingKey: "x" });',
  directCommandConsumerWithoutOptions: "defineCommandConsumer(queue, direct, message);",
  fanoutConsumerWithRoutingKey: 'defineEventConsumer(fanoutEvent, queue, { routingKey: "x" });',
  fanoutConsumerWithTopicBridge:
    "defineEventConsumer(fanoutEvent, queue, { bridgeExchange: topic });",
  directCommandPublisherWithRoutingKey:
    'defineCommandPublisher(directCommand, { routingKey: "t.x" });',
} as const;
type Case = keyof typeof cases;

const preamble = [
  'import { z } from "zod";',
  "import {",
  "  defineCommandConsumer,",
  "  defineCommandPublisher,",
  "  defineEventConsumer,",
  "  defineEventPublisher,",
  "  defineExchange,",
  "  defineMessage,",
  "  defineQueue,",
  '} from "../src/index.js";',
  'const direct = defineExchange("tasks", { type: "direct" });',
  'const topic = defineExchange("orders");',
  'const fanout = defineExchange("logs", { type: "fanout" });',
  "const message = defineMessage(z.object({ id: z.string() }));",
  'const queue = defineQueue("q");',
  "const fanoutEvent = defineEventPublisher(fanout, message);",
  'const directCommand = defineCommandConsumer(queue, direct, message, { routingKey: "t.run" });',
];

function compileCases(): Record<Case, string> {
  const names = Object.keys(cases) as Case[];
  const source = [...preamble, ...names.map((name) => cases[name])].join("\n");

  // Inside the package, so the probe resolves `zod` and compiles with the
  // package's own tsconfig.
  const probeDir = mkdtempSync(join(packageDir, ".tsc-probe-"));
  let output = "";
  try {
    writeFileSync(join(probeDir, "probe.ts"), source);
    writeFileSync(
      join(probeDir, "tsconfig.json"),
      JSON.stringify({
        extends: "../tsconfig.json",
        compilerOptions: { noEmit: true, rootDir: ".." },
        include: ["probe.ts"],
      }),
    );
    // `--pretty`: the plain format drops related information ("Arguments for
    // the rest parameter 'options' were not provided").
    execFileSync(process.execPath, [TSC, "-p", "tsconfig.json", "--pretty", "true"], {
      cwd: probeDir,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (error) {
    output = stripVTControlCharacters(String((error as { stdout?: string }).stdout ?? ""));
  } finally {
    rmSync(probeDir, { recursive: true, force: true });
  }

  // One block per diagnostic, headed `probe.ts:<line>:<col> - error TS…`. Its
  // message, chained lines and related information are kept; the code frames
  // (`<n> source…` and `~~~` underlines) and the `Found N errors` trailer are not.
  const byLine = new Map<number, string[]>();
  for (const block of output.split(/^(?=\S+:\d+:\d+ - error TS)/m)) {
    const line = /^probe\.ts:(\d+):\d+ - /.exec(block)?.[1];
    if (line === undefined) continue;
    const text = block
      .split("\n")
      .filter((row) => row.trim() !== "" && !/^\s*(\d+ |~+$)|^Found \d+ error/.test(row));
    byLine.set(Number(line), [...(byLine.get(Number(line)) ?? []), ...text]);
  }

  return Object.fromEntries(
    names.map((name, index) => [name, (byLine.get(preamble.length + index + 1) ?? []).join("\n")]),
  ) as Record<Case, string>;
}

describe("event/command builder diagnostics", () => {
  let messages: Record<Case, string>;

  beforeAll(() => {
    messages = compileCases();
  }, 60_000);

  it("compiles the preamble cleanly, so every message below belongs to its own case", () => {
    const all = Object.values(messages).join("\n");
    // Each case produced a diagnostic…
    for (const [name, text] of Object.entries(messages)) {
      expect(text, name).not.toBe("");
    }
    // …and none is an overload-resolution failure naming the wrong exchange type.
    expect(all).not.toMatch(/No overload matches this call/);
    expect(all).not.toMatch(/not assignable to parameter of type 'FanoutExchangeDefinition/);
  });

  it("reports a forgotten options argument on a direct publisher as a missing argument", () => {
    expect(messages.directPublisherWithoutOptions).toMatch(/Expected 3 arguments, but got 2/);
    expect(messages.directPublisherWithoutOptions).toMatch(/rest parameter 'options'/);
  });

  it("names routingKey when the options object omits it", () => {
    expect(messages.topicPublisherWithoutRoutingKey).toMatch(
      /Property 'routingKey' is missing in type '\{\}'/,
    );
  });

  it("rejects a routing key on a keyless exchange as an unknown option", () => {
    expect(messages.fanoutPublisherWithRoutingKey).toMatch(/'routingKey' does not exist in type/);
    expect(messages.fanoutConsumerWithRoutingKey).toMatch(/'routingKey' does not exist in type/);
  });

  it("reports a forgotten options argument on a direct command consumer as a missing argument", () => {
    expect(messages.directCommandConsumerWithoutOptions).toMatch(/Expected 4 arguments, but got 3/);
  });

  it("points a mismatched bridge at the bridgeExchange option", () => {
    expect(messages.fanoutConsumerWithTopicBridge).toMatch(
      /TopicExchangeDefinition<"orders">' is not assignable to type 'FanoutExchangeDefinition \| undefined'/,
    );
  });

  it("rejects a routing-key override on a direct command as an unknown option", () => {
    expect(messages.directCommandPublisherWithRoutingKey).toMatch(
      /'routingKey' does not exist in type/,
    );
  });
});
