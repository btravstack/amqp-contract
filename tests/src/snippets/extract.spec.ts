import { mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { discoverMarkdownFiles } from "./discover.js";
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

    // The count is pinned to the CURRENT corpus size (31), exactly, not a
    // floor. A >= comparison only catches discovery collapsing to nothing —
    // at 20, dropping `.agents` and `packages` from ROOTS silently lost 8
    // snippets and still passed — but stays green if the corpus grows past
    // it while discovery quietly drops pages. Both directions now require a
    // deliberate edit here: adding a documented example fails until this
    // number is bumped, and removing coverage fails too.
    expect(found.length).toBe(31);
  });

  describe("path-segment anchoring", () => {
    // A plain substring match on EXCLUDED fragments would silently drop pages
    // whose name merely contains a fragment, or whose fragment is a prefix of
    // a longer, unrelated segment — a doc missing from discovery still leaves
    // this whole suite green, since nothing counts what should have been
    // found.
    let tmpRoot: string;

    afterEach(() => {
      rmSync(tmpRoot, { recursive: true, force: true });
    });

    it("keeps a doc whose path merely contains an excluded fragment as a substring", () => {
      tmpRoot = mkdtempSync(join(tmpdir(), "amqp-contract-discover-"));
      mkdirSync(join(tmpRoot, "docs", "how-to"), { recursive: true });
      mkdirSync(join(tmpRoot, "docs", "api"), { recursive: true });
      // "dist" is an EXCLUDED fragment; it must not disqualify a segment that
      // merely contains it, like "distributed-tracing.md".
      writeFileSync(join(tmpRoot, "docs", "how-to", "distributed-tracing.md"), "# ok\n");
      // "docs/api" is an EXCLUDED fragment; the real generated-docs directory
      // must still be excluded.
      writeFileSync(join(tmpRoot, "docs", "api", "whatever.md"), "# generated\n");

      const files = discoverMarkdownFiles(tmpRoot).map((f) => f.slice(tmpRoot.length + 1));

      expect(files).toContain(join("docs", "how-to", "distributed-tracing.md"));
      expect(files).not.toContain(join("docs", "api", "whatever.md"));
    });

    it("does not throw when a broken symlink appears mid-walk", () => {
      tmpRoot = mkdtempSync(join(tmpdir(), "amqp-contract-discover-"));
      mkdirSync(join(tmpRoot, "docs"), { recursive: true });
      writeFileSync(join(tmpRoot, "docs", "real.md"), "# ok\n");
      symlinkSync(join(tmpRoot, "docs", "nonexistent-target"), join(tmpRoot, "docs", "broken.md"));

      const files = discoverMarkdownFiles(tmpRoot).map((f) => f.slice(tmpRoot.length + 1));

      expect(files).toContain(join("docs", "real.md"));
      expect(files).not.toContain(join("docs", "broken.md"));
    });
  });
});
