# Documentation Snippet-Execution Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make it impossible to ship a documentation snippet that does not construct, by executing every `defineContract` example in CI with only the imports a reader would copy.

**Architecture:** A pure parser turns markdown into located snippets and is unit-tested on its own. A spec in the `tests` package's new unit project writes each snippet verbatim to a gitignored file inside the repo and dynamic-imports it; the import completing without throwing is the assertion. No preamble, no injected imports, no skip mechanism.

**Tech Stack:** TypeScript ESM, vitest.

**Source spec:** `docs/superpowers/specs/2026-08-02-snippet-execution-guard-design.md`

## Global Constraints

- No `any` — use `unknown` and narrow. Enforced by oxlint.
- Type aliases over interfaces (`type Foo = {}`, never `interface`).
- `.js` extensions required in all relative imports (ESM).
- Catalog dependencies only — never hardcode a version in a `package.json`.
- Conventional commits. Enforced by commitlint.
- Run `pnpm typecheck` before declaring a task done — it is not in the pre-commit hook.
- **Verbatim execution is the point.** No shared preamble, no injected imports, no scaffold prepended to a snippet. An earlier throwaway harness that injected a superset import preamble found **zero** defects; the version using each snippet's own imports immediately found three. A snippet that does not carry its own imports must fail.
- **No skip mechanism.** Every in-scope snippet is made to construct. Nothing is annotated as exempt.

---

## File Structure

| File                                                    | Responsibility                                              |
| ------------------------------------------------------- | ----------------------------------------------------------- |
| `tests/src/snippets/extract.ts` (create)                | Pure markdown → located snippets. No I/O.                   |
| `tests/src/snippets/extract.spec.ts` (create)           | Unit tests for the parser.                                  |
| `tests/src/snippets/discover.ts` (create)               | Which markdown files are in scope. Filesystem walk.         |
| `tests/src/snippets/snippet-execution.spec.ts` (create) | Writes each snippet out, imports it, asserts it constructs. |
| `tests/vitest.config.ts` (modify)                       | Split into `unit` and `integration` projects.               |
| `tests/package.json` (modify)                           | Add a `test` script.                                        |
| `tests/src/__tests__/` (create, by move)                | The nine existing broker-dependent specs.                   |
| `.gitignore` (modify)                                   | Ignore the generated snippet directory.                     |

---

### Task 1: Split the `tests` package into unit and integration projects

**Files:**

- Modify: `tests/vitest.config.ts`
- Modify: `tests/package.json`
- Move: all nine `tests/src/*.spec.ts` → `tests/src/__tests__/`

**Interfaces:**

- Produces: a `unit` vitest project in `tests` that runs **without** a broker, reachable from the repo root via `pnpm test`

**Why:** `tests/vitest.config.ts` sets `globalSetup` at the **top level**, so every spec in the package starts a RabbitMQ container. And `tests/package.json` has only `test:integration` — no `test` script — so root `pnpm test` (turbo `test`) skips the package entirely. Without this split, the snippet guard would demand a broker it does not use and would be absent from the gate most changes go through.

**The sorting trap:** all nine current specs move. Eight depend on the broker through the `@amqp-contract/testing/extension` fixture. The ninth, `rabbitmq-config.spec.ts`, imports no fixture and reads as a unit test at a glance — but it calls `inject("__TESTCONTAINERS_RABBITMQ_IP__")`, which `globalSetup` provides, so it belongs with the rest. "No fixture import" is the wrong heuristic here.

- [ ] **Step 1: Move the nine specs**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
mkdir -p tests/src/__tests__
git mv tests/src/client-worker.spec.ts \
       tests/src/compression.spec.ts \
       tests/src/decompression-cap.spec.ts \
       tests/src/dlx-routability.spec.ts \
       tests/src/middleware.spec.ts \
       tests/src/rabbitmq-config.spec.ts \
       tests/src/rpc.spec.ts \
       tests/src/safe-defaults.spec.ts \
       tests/src/unroutable-publish.spec.ts \
       tests/src/__tests__/
```

Their imports are all package-scoped (`@amqp-contract/...`), not relative, so the move needs no import edits. Verify with `grep -rn 'from "\.\.' tests/src/__tests__/` — expect no output.

- [ ] **Step 2: Rewrite the vitest config with projects**

Replace `tests/vitest.config.ts` entirely:

```ts
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
```

`globalSetup` moves from the top level onto the `integration` project only — that is the change that lets the unit project run without Docker.

- [ ] **Step 3: Add the `test` script**

In `tests/package.json`, alongside the existing `test:integration`:

```json
    "test": "vitest run --project unit",
```

and change `test:integration` to name its project explicitly, matching `client` and `worker`:

```json
    "test:integration": "vitest run --project integration",
```

- [ ] **Step 4: Verify the split**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm --filter @amqp-contract/tests test
```

Expected: PASS with **no test files** — the unit project is empty until Task 3, and no Docker container starts. If a container starts, `globalSetup` is still bound too broadly.

```bash
pnpm --filter @amqp-contract/tests test:integration
```

Expected: PASS, 9 files / 45 tests, unchanged from before the move. Requires Docker.

- [ ] **Step 5: Verify the root gate now includes the package**

```bash
pnpm build && pnpm typecheck && pnpm test --concurrency=1 && pnpm lint
```

Expected: green, and the turbo summary now shows **13** tasks rather than 12 — `@amqp-contract/tests#test` is newly included. Confirm that count changed; it is the evidence this task worked.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test: give the tests package a unit project

globalSetup sat at the top level, so every spec there started a RabbitMQ
container, and the package had no test script at all — so the root gate
skipped it entirely. Splitting unit from integration lets a broker-free
suite run where most changes are actually gated."
```

---

### Task 2: The snippet parser

**Files:**

- Create: `tests/src/snippets/extract.ts`
- Create: `tests/src/snippets/discover.ts`
- Test: `tests/src/snippets/extract.spec.ts`

**Interfaces:**

- Produces: `type Snippet = { readonly file: string; readonly line: number; readonly code: string }`
- Produces: `parseSnippets(markdown: string, file: string): readonly Snippet[]`
- Produces: `discoverMarkdownFiles(repoRoot: string): readonly string[]`

**Why:** Splitting the parser from the executor makes the fiddly half testable without touching the filesystem or importing anything. Fence parsing looks trivial and is not: a regex over language-tagged and nested fences miscounts, which happened during this design's own exploration — a regex census reported 1928 blocks where a line-state machine found 244.

`line` is the 1-based line number of the **opening fence**, so a failure can name `docs/how-to/route-dead-letters.md:41`.

- [ ] **Step 1: Write the failing test**

Create `tests/src/snippets/extract.spec.ts`:

`````ts
import { describe, expect, it } from "vitest";

import { parseSnippets } from "./extract.js";

describe("parseSnippets", () => {
  it("extracts a ts block that calls defineContract, with its opening-fence line", () => {
    const md = ["# Title", "", "```ts", "defineContract({});", "```", ""].join("\n");

    expect(parseSnippets(md, "a.md")).toEqual([
      { file: "a.md", line: 3, code: "defineContract({});" },
    ]);
  });

  it("accepts the typescript alias", () => {
    const md = ["```typescript", "defineContract({});", "```"].join("\n");

    expect(parseSnippets(md, "a.md")).toHaveLength(1);
  });

  it("accepts a language tag with trailing metadata", () => {
    // VitePress allows ```ts twoslash and similar.
    const md = ["```ts twoslash", "defineContract({});", "```"].join("\n");

    expect(parseSnippets(md, "a.md")).toHaveLength(1);
  });

  it("ignores a block that never calls defineContract", () => {
    const md = ["```ts", "const x = 1;", "```"].join("\n");

    expect(parseSnippets(md, "a.md")).toEqual([]);
  });

  it("ignores non-TypeScript blocks", () => {
    const md = ["```bash", "defineContract({});", "```"].join("\n");

    expect(parseSnippets(md, "a.md")).toEqual([]);
  });

  it("finds several blocks in one document and numbers each correctly", () => {
    const md = [
      "```ts", // 1
      "defineContract({ a: 1 });",
      "```",
      "prose",
      "```ts", // 5
      "const ignored = 1;",
      "```",
      "```ts", // 8
      "defineContract({ b: 2 });",
      "```",
    ].join("\n");

    expect(parseSnippets(md, "a.md")).toEqual([
      { file: "a.md", line: 1, code: "defineContract({ a: 1 });" },
      { file: "a.md", line: 8, code: "defineContract({ b: 2 });" },
    ]);
  });

  it("does not treat a fence inside a four-backtick block as an opener", () => {
    // A regex-based parser gets this wrong and desynchronises for the rest of
    // the file — the failure mode that made a census report 1928 blocks where
    // there were 244.
    const md = [
      "````markdown",
      "```ts",
      "defineContract({ inner: 1 });",
      "```",
      "````",
      "```ts",
      "defineContract({ real: 1 });",
      "```",
    ].join("\n");

    const found = parseSnippets(md, "a.md");
    expect(found).toHaveLength(1);
    expect(found[0]?.code).toBe("defineContract({ real: 1 });");
  });

  it("preserves the block body verbatim, including blank lines and indentation", () => {
    const md = [
      "```ts",
      'import { defineContract } from "@amqp-contract/contract";',
      "",
      "const contract = defineContract({",
      "  publishers: {},",
      "});",
      "```",
    ].join("\n");

    expect(parseSnippets(md, "a.md")[0]?.code).toBe(
      [
        'import { defineContract } from "@amqp-contract/contract";',
        "",
        "const contract = defineContract({",
        "  publishers: {},",
        "});",
      ].join("\n"),
    );
  });

  it("ignores an unterminated block at end of file", () => {
    const md = ["```ts", "defineContract({});"].join("\n");

    expect(parseSnippets(md, "a.md")).toEqual([]);
  });
});
`````

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd tests && pnpm vitest run --project unit src/snippets/extract.spec.ts
```

Expected: FAIL — cannot resolve `./extract.js`.

- [ ] **Step 3: Implement the parser**

Create `tests/src/snippets/extract.ts`:

`````ts
/**
 * Turn markdown into located, executable snippets.
 *
 * Pure: no filesystem, no imports, no execution. That is what makes the
 * fiddly half testable, and fence parsing is fiddlier than it looks — a
 * regex over language-tagged and nested fences desynchronises and keeps
 * miscounting for the rest of the file. A regex census of this repo reported
 * 1928 blocks where a line-state machine found 244.
 */

/** A fenced TypeScript block that builds a contract. */
export type Snippet = {
  /** Path of the markdown file, as given to {@link parseSnippets}. */
  readonly file: string;
  /** 1-based line of the opening fence, so a failure can be located. */
  readonly line: number;
  /** The block body, verbatim. Never modified, never augmented. */
  readonly code: string;
};

/** True for an opening fence introducing TypeScript: ```ts, ```typescript, and tagged variants. */
function isTypeScriptFence(line: string): boolean {
  const match = /^```([A-Za-z]+)/.exec(line);
  return match?.[1] === "ts" || match?.[1] === "typescript";
}

/**
 * Every `defineContract` block in one markdown document.
 *
 * Blocks that never call `defineContract` are skipped: every guard in this
 * project throws from there, so a snippet that does not build a contract
 * cannot trip one.
 *
 * @internal
 */
export function parseSnippets(markdown: string, file: string): readonly Snippet[] {
  const lines = markdown.split("\n");
  const snippets: Snippet[] = [];

  let index = 0;
  while (index < lines.length) {
    const line = lines[index] ?? "";

    // A four-or-more-backtick fence wraps other fences. Skip the whole region:
    // treating its inner ```ts as an opener is what desynchronises a naive
    // parser for the rest of the file.
    if (line.startsWith("````")) {
      index += 1;
      while (index < lines.length && !(lines[index] ?? "").startsWith("````")) index += 1;
      index += 1;
      continue;
    }

    if (!isTypeScriptFence(line)) {
      index += 1;
      continue;
    }

    const openedAt = index + 1;
    const body: string[] = [];
    index += 1;
    while (index < lines.length && !(lines[index] ?? "").startsWith("```")) {
      body.push(lines[index] ?? "");
      index += 1;
    }

    // Unterminated at end of file: not a block, and nothing after it either.
    if (index >= lines.length) break;
    index += 1;

    const code = body.join("\n");
    if (code.includes("defineContract(")) {
      snippets.push({ file, line: openedAt, code });
    }
  }

  return snippets;
}
`````

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd tests && pnpm vitest run --project unit src/snippets/extract.spec.ts
```

Expected: PASS, 9 tests.

- [ ] **Step 5: Implement discovery**

Create `tests/src/snippets/discover.ts`:

```ts
import { readdirSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";

/**
 * Which markdown files carry hand-written examples.
 *
 * Discovery is a walk, never a hand-maintained list: a new page with a
 * contract snippet must be covered the moment it lands, without anyone
 * remembering to register it.
 *
 * @internal
 */

/** Roots to walk, relative to the repository root. */
const ROOTS = ["docs", "packages", ".agents", "README.md"] as const;

/**
 * Path fragments that disqualify a file.
 *
 * - `docs/api` is generated TypeDoc output: signature fragments, not programs.
 * - `docs/superpowers` holds specs and plans, deliberately illustrative.
 * - `node_modules`, `dist` and `.vitepress` are build artifacts.
 * - Only `packages/<name>/README.md` is wanted from `packages`, not sources.
 */
const EXCLUDED = ["node_modules", `docs${sep}api`, `docs${sep}superpowers`, "dist", ".vitepress"];

function walk(absolute: string, out: string[]): void {
  let entries: string[];
  try {
    entries = readdirSync(absolute);
  } catch {
    return;
  }
  for (const entry of entries) {
    const child = join(absolute, entry);
    if (statSync(child).isDirectory()) {
      walk(child, out);
    } else if (entry.endsWith(".md")) {
      out.push(child);
    }
  }
}

/**
 * Absolute paths of every in-scope markdown file under `repoRoot`.
 *
 * @internal
 */
export function discoverMarkdownFiles(repoRoot: string): readonly string[] {
  const found: string[] = [];
  for (const root of ROOTS) {
    const absolute = join(repoRoot, root);
    try {
      if (statSync(absolute).isDirectory()) walk(absolute, found);
      else if (absolute.endsWith(".md")) found.push(absolute);
    } catch {
      continue;
    }
  }
  return found
    .filter((file) => {
      const rel = relative(repoRoot, file);
      if (EXCLUDED.some((fragment) => rel.includes(fragment))) return false;
      // From packages, only the package READMEs.
      if (rel.startsWith(`packages${sep}`))
        return rel.split(sep).length === 3 && rel.endsWith("README.md");
      return true;
    })
    .sort();
}
```

- [ ] **Step 6: Verify discovery finds the expected corpus**

Add to `tests/src/snippets/extract.spec.ts`:

```ts
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { discoverMarkdownFiles } from "./discover.js";

describe("discoverMarkdownFiles", () => {
  const repoRoot = join(import.meta.dirname, "..", "..", "..");

  it("finds hand-written docs and excludes generated and planning ones", () => {
    const files = discoverMarkdownFiles(repoRoot).map((f) => f.slice(repoRoot.length + 1));

    expect(files).toContain("docs/how-to/define-a-contract.md");
    expect(files).toContain("README.md");
    expect(files).toContain("packages/core/README.md");
    expect(files.some((f) => f.startsWith("docs/api/"))).toBe(false);
    expect(files.some((f) => f.startsWith("docs/superpowers/"))).toBe(false);
    expect(files.some((f) => f.includes("node_modules"))).toBe(false);
  });

  it("finds contract snippets across the real corpus", () => {
    const found = discoverMarkdownFiles(repoRoot).flatMap((file) =>
      parseSnippets(readFileSync(file, "utf8"), file),
    );

    // A census at design time found 28. Asserting a floor rather than an
    // exact count keeps this from failing every time someone adds a page,
    // while still catching discovery silently collapsing to nothing.
    expect(found.length).toBeGreaterThanOrEqual(20);
  });
});
```

- [ ] **Step 7: Run the tests to verify they pass**

```bash
cd tests && pnpm vitest run --project unit src/snippets/extract.spec.ts
```

Expected: PASS, 11 tests.

- [ ] **Step 8: Verify the repo is still green**

Nothing executes snippets yet, so nothing should break.

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm build && pnpm typecheck && pnpm test --concurrency=1 && pnpm lint
```

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "test: parse located contract snippets out of the docs

Pure parser plus a discovery walk, split from execution so the fiddly half
is testable without touching the filesystem. Fence parsing is fiddlier than
it looks: a regex desynchronises on nested fences and kept miscounting — a
regex census reported 1928 blocks where there were 244."
```

---

### Task 3: Execute the snippets, and fix what fails

**Files:**

- Create: `tests/src/snippets/snippet-execution.spec.ts`
- Modify: `.gitignore`
- Modify: documentation and rule files across the repo

**Interfaces:**

- Consumes: `parseSnippets`, `discoverMarkdownFiles`, `Snippet` from Task 2

**Why:** This is the guard, and the task where the backlog lands. Twenty-four of the roughly 28 in-scope snippets have never been executed by anything committed.

**Verbatim is the whole point.** Write the snippet's own text and nothing else. No preamble, no injected imports, no scaffold. An earlier throwaway harness that injected a superset import preamble found **zero** defects; the version using each snippet's own imports immediately found three, including `packages/core/README.md` calling `defineQueueBinding` without importing it. If you find yourself adding anything to a snippet to make it run, that is the defect, and it belongs in the markdown.

- [ ] **Step 1: Ignore the generated directory**

Append to `.gitignore`:

```gitignore
# Snippets materialised from the docs by tests/src/snippets/snippet-execution.spec.ts
tests/.snippets/
```

The directory must sit **inside the repository**: Node resolves `@amqp-contract/*` by walking up from the importing file to `node_modules`, so a snippet written to the OS temp directory cannot import the workspace packages.

- [ ] **Step 2: Write the executing spec**

Create `tests/src/snippets/snippet-execution.spec.ts`:

```ts
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { discoverMarkdownFiles } from "./discover.js";
import { parseSnippets, type Snippet } from "./extract.js";

/**
 * Every documented contract must actually construct.
 *
 * Three branches of guard work each found our own documentation teaching the
 * shape the new guard forbids, and twice the fix for that documentation
 * reintroduced it. Each was caught by a throwaway harness and then lost.
 *
 * Snippets run **verbatim**, with only the imports they show. An earlier
 * harness that injected a shared import preamble found zero defects; the
 * version using each snippet's own imports immediately found three. A
 * harness that supplies imports the reader does not have proves nothing.
 */

const repoRoot = join(import.meta.dirname, "..", "..", "..");
const outputDir = join(repoRoot, "tests", ".snippets");

const snippets: readonly Snippet[] = discoverMarkdownFiles(repoRoot).flatMap((file) =>
  parseSnippets(readFileSync(file, "utf8"), file),
);

beforeAll(() => {
  rmSync(outputDir, { recursive: true, force: true });
  mkdirSync(outputDir, { recursive: true });
});

afterAll(() => {
  rmSync(outputDir, { recursive: true, force: true });
});

describe("documentation snippets", () => {
  it("finds snippets to check", () => {
    // Guards the guard: a discovery bug that returns nothing would otherwise
    // make this whole suite pass vacuously.
    expect(snippets.length).toBeGreaterThanOrEqual(20);
  });

  for (const [index, snippet] of snippets.entries()) {
    const where = `${snippet.file.slice(repoRoot.length + 1)}:${String(snippet.line)}`;

    it(`constructs: ${where}`, async () => {
      const file = join(outputDir, `snippet-${String(index)}.ts`);
      // Verbatim. Nothing prepended, nothing appended.
      writeFileSync(file, snippet.code, "utf8");

      await expect(
        import(pathToFileURL(file).href),
        `Snippet at ${where} did not construct. Run it with only the imports it shows.`,
      ).resolves.toBeDefined();
    });
  }
});
```

- [ ] **Step 3: Run it and collect the failures**

```bash
cd tests && pnpm vitest run --project unit src/snippets/snippet-execution.spec.ts
```

Expected: **several failures.** That is the guard working. Record every failing `file:line` and its error before changing anything — the list is the deliverable of this step, and your report needs it.

- [ ] **Step 4: Fix each failing snippet at the source**

For each failure, edit the markdown. Two shapes dominate:

- **`ReferenceError: X is not defined`** — the snippet uses a symbol it never imports. Add it to that snippet's own import block. If the page has no import block at all, add one.
- **A throw from `defineContract`** — the snippet documents a contract the guards reject: an unroutable publisher, a consumed queue with no dead-letter route, or a dead-letter exchange with nothing bound. Fix the topology the snippet teaches. The correct dead-letter shape is at `docs/how-to/define-a-contract.md`, in the standalone-topology section: declare the DLQ, then bind it.

Two rules while fixing:

- **Never adjust the harness to accommodate a snippet.** If a snippet needs something added to run, the markdown is what changes.
- **`#` is a topic wildcard.** On a `direct` exchange it is a literal key matching nothing — measured against a real broker: topic + `#` receives 1 message, direct + `#` receives 0. If a DLX is direct, bind the real routing key. This trap has produced three separate defects in this project, including inside the text written to fix it.

The four blocks known to show no imports are in `docs/explanation/core-concepts.md`, `docs/explanation/why-amqp-contract.md`, `docs/how-to/route-dead-letters.md` and `.agents/rules/contract-patterns.md`. Expect more than four failures in total.

Record every file you touch in your report as a table: file, line, the error, and the fix.

- [ ] **Step 5: Run until green**

```bash
cd tests && pnpm vitest run --project unit src/snippets/snippet-execution.spec.ts
```

Expected: PASS, every snippet constructing.

- [ ] **Step 6: Prove the guard bites**

Delete an import line from `packages/core/README.md`'s contract snippet and re-run. Expected: that snippet fails, naming `packages/core/README.md` and its fence line. Restore it.

Then add an unbound dead-letter exchange to any snippet — a `deadLetter: { exchange: someDlx }` with nothing bound to `someDlx` — and re-run. Expected: that snippet fails with the routability error. Restore it.

**Report both failure outputs.** A guard nobody has watched fail is a guard nobody knows works.

- [ ] **Step 7: Full verification**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm build && pnpm typecheck && pnpm test --concurrency=1 && pnpm lint && npx oxfmt --check .
pnpm --filter @amqp-contract/core test:integration
pnpm --filter @amqp-contract/client test:integration
pnpm --filter @amqp-contract/worker test:integration
pnpm --filter @amqp-contract/tests test:integration
```

Run the integration projects **one at a time** — there is a known pre-existing flake when several testcontainers share one Docker daemon. Do not try to fix it. Confirm `git status` is clean: the generated `tests/.snippets/` directory must not appear.

- [ ] **Step 8: Record the invariant**

Append to the "Load-bearing invariants" list in `AGENTS.md`:

```markdown
23. **Every documented `defineContract` example constructs, using only the imports it shows** (a snippet that needs anything the reader would not copy is a broken example; three shipped broken before this ran in CI) — `tests/src/snippets/snippet-execution.spec.ts`.
```

- [ ] **Step 9: Commit**

No changeset: this changes no published API.

```bash
git add -A
git commit -m "test: execute every documented contract example in CI

Three branches of guard work each found our own docs teaching the shape the
new guard forbids, and each was caught by a throwaway harness that was then
deleted. Snippets now run verbatim, with only the imports they show — the
distinction that matters, since a harness injecting a shared preamble found
zero defects where honest imports found three."
```

---

## Out of scope

Tracked in `docs/superpowers/specs/2026-08-01-robustness-hardening-design.md`:

- **Type-checking snippets.** Execution catches the class that has actually shipped — a missing import, and a guard violation. A snippet using a removed option that still runs has not shipped once across three branches.
- **Proving a snippet's topology routes.** `defineContract` accepts bindings it cannot decide, so a snippet can construct and still deliver nothing. That residual belongs to the guards.
- `MatchingBindingPattern`'s template-literal hole; `RoutableRoutingKey`'s status; the `setup.ts`/`asyncapi` DLX precedence inconsistency; the parallel-testcontainer flake; the coverage-floor ratchet.
- **H5, duplicate delivery** — still unaddressed, and still the largest unguarded risk.
