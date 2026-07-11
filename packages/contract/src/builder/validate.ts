/**
 * Define-time structural validation shared by the builders.
 *
 * The type system already rejects wrong shapes for TypeScript callers, but a
 * JavaScript caller (or a cast) can pass misspelled option keys that were
 * previously ignored silently — a typo like `durabel: false` produced a
 * durable queue with no warning. Per the org DNA ("typo = define-time
 * failure", mirroring temporal-contract's contract-shape validation), the
 * builders now throw at definition time with an actionable message.
 *
 * All helpers use the `_internal_` prefix: exported for use across the
 * builder modules, no semver guarantee.
 */

/** Throw unless `name` is a non-empty string (AMQP names must not be blank). */
export function _internal_assertNonEmptyName(kind: string, name: unknown): void {
  if (typeof name !== "string" || name.length === 0) {
    throw new Error(`${kind} name must be a non-empty string, got ${JSON.stringify(name)}`);
  }
}

/**
 * Throw when an options bag carries keys outside the allowed set. Lists both
 * the offending keys and the allowed ones so a typo is a one-glance fix.
 */
export function _internal_assertKnownKeys(
  kind: string,
  name: string,
  bag: object | undefined,
  allowed: readonly string[],
): void {
  if (bag === undefined) return;
  const unknown = Object.keys(bag).filter((key) => !allowed.includes(key));
  if (unknown.length > 0) {
    throw new Error(
      `Unknown option${unknown.length > 1 ? "s" : ""} ${unknown.map((k) => `"${k}"`).join(", ")} ` +
        `on ${kind} "${name}". Allowed options: ${allowed.join(", ")}.`,
    );
  }
}

/**
 * Duck-check that a value implements Standard Schema v1 (has a `~standard`
 * object with a `validate` function) — catches passing a plain object or a
 * schema from an incompatible library where a payload schema is expected.
 */
export function _internal_assertStandardSchema(kind: string, schema: unknown): void {
  // ArkType schemas are callable, so accept functions as well as objects.
  const standard =
    (typeof schema === "object" || typeof schema === "function") && schema !== null
      ? (schema as Record<string, unknown>)["~standard"]
      : undefined;
  const validate =
    typeof standard === "object" && standard !== null
      ? (standard as Record<string, unknown>)["validate"]
      : undefined;
  if (typeof validate !== "function") {
    throw new Error(
      `${kind} must be a Standard Schema v1 (an object exposing "~standard".validate) — ` +
        "got a value without one. Zod, Valibot, and ArkType schemas all qualify.",
    );
  }
}
