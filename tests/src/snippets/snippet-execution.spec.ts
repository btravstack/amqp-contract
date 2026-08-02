import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
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
