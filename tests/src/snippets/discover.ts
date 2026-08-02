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
