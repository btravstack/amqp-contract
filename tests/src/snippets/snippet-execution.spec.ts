import { randomUUID } from "node:crypto";
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
    // make this whole suite pass vacuously. The count is pinned to the CURRENT
    // corpus size (31), exactly, not a floor — a >= comparison only catches
    // coverage going down; it stays green if the corpus grows past it while
    // discovery silently drops pages, which is the same failure this suite
    // exists to prevent, one layer up. At 20 against a corpus of 31, dropping
    // two whole ROOTS still passed. Both directions now require a deliberate
    // edit here: adding a documented example fails until this number is
    // bumped, and losing coverage fails too.
    expect(snippets.length).toBe(31);
  });

  for (const [index, snippet] of snippets.entries()) {
    const where = `${snippet.file.slice(repoRoot.length + 1)}:${String(snippet.line)}`;

    it(`constructs: ${where}`, async () => {
      const file = join(outputDir, `snippet-${String(index)}.ts`);
      // Verbatim. Nothing prepended, nothing appended.
      writeFileSync(file, snippet.code, "utf8");

      // ES module resolution caches by URL for the life of the process, so a
      // plain `import(pathToFileURL(file).href)` would return the module from
      // a previous run of this same suite (e.g. watch mode) even though
      // `beforeAll` just rewrote the file on disk. Snippet indices are
      // positional, so a rewritten `snippet-N.ts` can hold entirely different
      // content between runs while keeping the same URL — a cache hit here
      // would report a fix as passing without re-executing it. The query
      // string busts the cache; `where` (derived from the doc, not this URL)
      // keeps the failure message's file:line attribution intact. The buster
      // must contain no `.` — the esbuild-backed loader used to run this
      // import reads the trailing dot-segment of the request as a loader
      // override, so a decimal value here would break the import itself
      // rather than merely bust the cache. `randomUUID()` is hex and hyphens
      // only.
      const importUrl = `${pathToFileURL(file).href}?t=${randomUUID()}`;

      await expect(
        import(importUrl),
        `Snippet at ${where} did not construct. Run it with only the imports it shows.`,
      ).resolves.toBeDefined();
    });
  }
});
