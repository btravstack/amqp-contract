/**
 * Nominal brand key for the builder-result config objects
 * (`EventPublisherConfig`, `EventConsumerResult`, `CommandConsumerConfig`,
 * `BridgedPublisherConfig`).
 *
 * A `unique symbol` key instead of the pre-3.0 `__brand` string field:
 *
 * - **Invisible in IDE hovers** — the brand is an implementation detail, not
 *   part of the shape users work with.
 * - **Unforgeable at the type level** — the symbol is deliberately NOT
 *   exported from the package root, so user code cannot spell the branded
 *   property. (`Symbol.for` at runtime returns plain `symbol`, which does not
 *   satisfy the `unique symbol` key either.)
 * - **Registry-scoped at runtime** (`Symbol.for`) so the ESM and CJS builds
 *   of this dual-format package agree on the key — a config created by one
 *   format is still recognized by the type guards of the other.
 *
 * Internal — no semver guarantee. Import only from within this package.
 */
export const brand: unique symbol = Symbol.for("amqp-contract.brand");

/**
 * The brand values carried by builder-result configs.
 * @internal
 */
export type BrandValue =
  | "EventPublisherConfig"
  | "EventConsumerResult"
  | "CommandConsumerConfig"
  | "BridgedPublisherConfig";

/**
 * Read a value's brand, if any. Shared runtime probe for the `is*` guards.
 * @internal
 */
export function brandOf(value: unknown): BrandValue | undefined {
  if ((typeof value !== "object" && typeof value !== "function") || value === null) {
    return undefined;
  }
  const candidate = (value as { readonly [brand]?: unknown })[brand];
  return typeof candidate === "string" ? (candidate as BrandValue) : undefined;
}
