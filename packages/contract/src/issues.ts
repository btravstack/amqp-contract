import type { StandardSchemaV1 } from "@standard-schema/spec";

/**
 * Render a single Standard Schema issue as `path.to.field: message` (or just
 * the message for root-level issues). Path segments may be raw property keys
 * or `{ key }` objects per the Standard Schema spec; both are handled.
 *
 * Single source of truth for issue rendering across the client and worker —
 * mirrors temporal-contract's shared formatter (org DNA).
 */
export function formatIssue(issue: StandardSchemaV1.Issue): string {
  const path = (issue.path ?? [])
    .map((segment) =>
      typeof segment === "object" && segment !== null && "key" in segment
        ? String(segment.key)
        : String(segment),
    )
    .join(".");
  return path.length > 0 ? `${path}: ${issue.message}` : issue.message;
}

/**
 * Render a list of Standard Schema issues as a single human-readable line:
 * the first `limit` issues joined with `; `, plus a `(+N more)` suffix when
 * truncated. Empty input renders as `"no issues"` (defensive — validation
 * failures always carry at least one issue).
 */
export function summarizeIssues(issues: readonly StandardSchemaV1.Issue[], limit = 3): string {
  if (issues.length === 0) {
    return "no issues";
  }
  const shown = issues.slice(0, limit).map(formatIssue).join("; ");
  const hidden = issues.length - limit;
  return hidden > 0 ? `${shown} (+${hidden} more)` : shown;
}
