import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

/**
 * A path in the agent rules that no longer resolves is worse than no
 * reference: it sends a reader looking for a guarantee that has moved.
 *
 * This is not hypothetical. The snippet-execution branch relocated eleven spec
 * files and left nine `AGENTS.md` invariant references pointing at nothing —
 * while the same edit added a tenth invariant, directly under a line telling
 * the reader to extend the mapping. Three task reviews did not notice.
 */

const repoRoot = join(import.meta.dirname, "..", "..", "..");

/** Backticked tokens that look like a repo path: contain a slash and end in a known extension. */
const PATH_LIKE = /^[\w@./-]+\.(?:ts|tsx|js|mjs|cjs|md|json|ya?ml)(?::\d+)?$/;

/**
 * Top-level entries of the repo. A citation is rooted at one of these; an
 * illustrative path is not.
 *
 * Without this test, `` `path/to/file.ts` `` (a placeholder in a code-style
 * example) and `` `src/index.ts` `` (package-relative, in "add this to your
 * package" instructions) both read as dead references, and the suite fails on
 * documentation that is perfectly correct. Deriving the set from the
 * filesystem rather than hardcoding it means a new top-level directory needs
 * no edit here.
 */
const repoRootEntries = new Set(readdirSync(repoRoot));

function pathsIn(markdown: string): readonly string[] {
  return [...markdown.matchAll(/`([^`\n]+)`/g)]
    .map((match) => match[1] ?? "")
    .filter((token) => token.includes("/") && !token.includes("*"))
    .filter((token) => PATH_LIKE.test(token))
    .filter((token) => repoRootEntries.has(token.split("/")[0] ?? ""))
    .map((token) => token.replace(/:\d+$/, ""));
}

const sources: readonly string[] = [
  "AGENTS.md",
  ...readdirSync(join(repoRoot, ".agents", "rules"))
    .filter((name) => name.endsWith(".md"))
    .map((name) => join(".agents", "rules", name)),
];

const extracted = sources.map((source) => ({
  source,
  paths: pathsIn(readFileSync(join(repoRoot, source), "utf8")),
}));

describe("agent rule docs", () => {
  // `CLAUDE.md` is a symlink to `AGENTS.md`; listing only `AGENTS.md` avoids
  // asserting the same corpus twice.
  it("has rule files to check", () => {
    expect(sources.length).toBeGreaterThan(5);
  });

  // These two floors detect a broken extractor, nothing more. They sit far
  // below the real counts on purpose, so ordinary doc edits never trip them
  // and nobody is tempted to loosen them. They are deliberately NOT coverage
  // pins: several rule files legitimately cite no repo-rooted paths at all
  // (they link with Markdown syntax instead), so a per-file floor would fail
  // on correct documentation.
  it("extracts path references across the corpus", () => {
    expect(extracted.reduce((total, entry) => total + entry.paths.length, 0)).toBeGreaterThan(20);
  });

  it("extracts path references from AGENTS.md", () => {
    // The index cites every guarded invariant by file; it can never be empty.
    const agents = extracted.find((entry) => entry.source === "AGENTS.md");
    expect(agents?.paths.length ?? 0).toBeGreaterThan(10);
  });

  for (const { source, paths } of extracted) {
    for (const path of paths) {
      it(`${source}: ${path} resolves`, () => {
        expect(
          existsSync(join(repoRoot, path)),
          `${source} references ${path}, which does not exist`,
        ).toBe(true);
      });
    }
  }
});
