import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

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

    // A census at design time found 28. Asserting a floor rather than an
    // exact count keeps this from failing every time someone adds a page,
    // while still catching discovery silently collapsing to nothing.
    expect(found.length).toBeGreaterThanOrEqual(20);
  });
});
