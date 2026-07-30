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
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)
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
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)
    throw new Error(
      `Unknown option${unknown.length > 1 ? "s" : ""} ${unknown.map((k) => `"${k}"`).join(", ")} ` +
        `on ${kind} "${name}". Allowed options: ${allowed.join(", ")}.`,
    );
  }
}

/**
 * Throw when a direct/topic target is defined without a usable routing key.
 *
 * Direct and topic exchanges route on the routing key: a missing or empty key
 * is silently unroutable at runtime (the broker drops the message with no
 * error), so it must fail at define time instead. The type system already
 * requires the key for TypeScript callers — this guards JavaScript callers
 * and casts, which previously fell through to a silent `""` default.
 *
 * Callers must gate on the exchange type: fanout and headers exchanges ignore
 * routing keys and are exempt.
 */
export function _internal_assertRoutingKeyPresent(
  kind: string,
  exchangeName: string,
  exchangeType: string,
  routingKey: unknown,
): asserts routingKey is string {
  if (typeof routingKey !== "string" || routingKey.length === 0) {
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)
    throw new Error(
      `${kind} on ${exchangeType} exchange "${exchangeName}" requires a non-empty routingKey ` +
        `(got ${JSON.stringify(routingKey)}). Direct and topic exchanges route on the routing key — ` +
        `without one, messages are silently unroutable. ` +
        `Fanout and headers exchanges do not use routing keys.`,
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
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)
    throw new Error(
      `${kind} must be a Standard Schema v1 (an object exposing "~standard".validate) — ` +
        "got a value without one. Zod, Valibot, and ArkType schemas all qualify.",
    );
  }
}
