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
 *
 * Matched against whole path segments via {@link matchesExcludedFragment}, never as a
 * plain substring — `"dist"` must not disqualify `docs/how-to/distributed-tracing.md`,
 * and `` `docs/api` `` must not disqualify a hypothetical `docs/apis-overview.md`.
 */
const EXCLUDED = ["node_modules", `docs${sep}api`, `docs${sep}superpowers`, "dist", ".vitepress"];

/**
 * True when `fragment` matches `rel` on path-segment boundaries: as the whole path, a
 * leading segment run, a trailing segment run, or a segment run in the middle. A plain
 * `rel.includes(fragment)` would also match `fragment` as a mere substring of some
 * unrelated, longer segment (`"dist"` inside `distributed-tracing.md`).
 */
function matchesExcludedFragment(rel: string, fragment: string): boolean {
  return (
    rel === fragment ||
    rel.startsWith(`${fragment}${sep}`) ||
    rel.endsWith(`${sep}${fragment}`) ||
    rel.includes(`${sep}${fragment}${sep}`)
  );
}

/** True when `rel` sits under any excluded fragment. */
function isExcluded(rel: string): boolean {
  return EXCLUDED.some((fragment) => matchesExcludedFragment(rel, fragment));
}

function walk(absolute: string, out: string[], repoRoot: string): void {
  let entries: string[];
  try {
    entries = readdirSync(absolute);
  } catch {
    return;
  }
  for (const entry of entries) {
    const child = join(absolute, entry);
    let stats: ReturnType<typeof statSync>;
    try {
      stats = statSync(child);
    } catch {
      // A broken symlink or a file removed mid-walk: skip it rather than take
      // the whole discovery run down.
      continue;
    }
    if (stats.isDirectory()) {
      // Prune before descending, not after collecting. The excluded fragments
      // include `node_modules`, and this repo has 344 of those directories
      // holding ~70 000 files — walking them to find ~60 markdown files and
      // then filtering them back out cost enough wall-clock to time this
      // suite out in CI while passing locally on a warm cache.
      if (isExcluded(relative(repoRoot, child))) continue;
      walk(child, out, repoRoot);
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
      if (statSync(absolute).isDirectory()) walk(absolute, found, repoRoot);
      else if (absolute.endsWith(".md")) found.push(absolute);
    } catch {
      continue;
    }
  }
  return found
    .filter((file) => {
      const rel = relative(repoRoot, file);
      if (isExcluded(rel)) return false;
      // From packages, only the package READMEs.
      if (rel.startsWith(`packages${sep}`))
        return rel.split(sep).length === 3 && rel.endsWith("README.md");
      return true;
    })
    .sort();
}
