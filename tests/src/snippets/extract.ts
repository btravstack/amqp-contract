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
