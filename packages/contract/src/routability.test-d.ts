import { describe, expectTypeOf, it } from "vitest";

import type { RoutableRoutingKey } from "./builder/routing-types.js";

describe("RoutableRoutingKey", () => {
  it("resolves to the key when a pattern matches", () => {
    expectTypeOf<RoutableRoutingKey<"order.created", "order.#">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order.created", "order.*">>().toEqualTypeOf<"order.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.created">
    >().toEqualTypeOf<"order.created">();
  });

  it("resolves to the key when ANY pattern in the union matches", () => {
    expectTypeOf<
      RoutableRoutingKey<"order.created", "user.#" | "order.#">
    >().toEqualTypeOf<"order.created">();
  });

  it("resolves to a readable error when no pattern matches", () => {
    expectTypeOf<
      RoutableRoutingKey<"order.created", "user.#">
    >().toEqualTypeOf<"Error: routing key 'order.created' matches none of the declared binding patterns; the broker would confirm and discard every message">();
  });

  it("skips the check when the key is not a literal", () => {
    expectTypeOf<RoutableRoutingKey<string, "user.#">>().toEqualTypeOf<string>();
  });

  it("skips the check when the patterns are not literal", () => {
    expectTypeOf<RoutableRoutingKey<"order.created", string>>().toEqualTypeOf<"order.created">();
  });

  it("skips the check when there are no patterns", () => {
    expectTypeOf<RoutableRoutingKey<"order.created", never>>().toEqualTypeOf<"order.created">();
  });

  it("skips the check when either side is not a compile-time literal", () => {
    expectTypeOf<
      RoutableRoutingKey<`order.${string}`, "order.#">
    >().toEqualTypeOf<`order.${string}`>();
    expectTypeOf<
      RoutableRoutingKey<"order.created", `order.${string}`>
    >().toEqualTypeOf<"order.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.#" | `x.${string}`>
    >().toEqualTypeOf<"order.created">();
  });
});

/**
 * The corpus in `match-corpus.ts` is asserted against the runtime matcher in
 * `topic-match.spec.ts`. These assertions pin the same cases at the type
 * level, so the two implementations cannot diverge without a test failing.
 *
 * Kept as explicit lines rather than a loop: types cannot be generated from
 * a runtime array.
 */
describe("type-level matcher agrees with the runtime corpus", () => {
  it("matches the cases the runtime matcher matches", () => {
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.created">
    >().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order", "order">>().toEqualTypeOf<"order">();
    expectTypeOf<RoutableRoutingKey<"order.created", "order.*">>().toEqualTypeOf<"order.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "*.created">
    >().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order.created", "*.*">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order", "*">>().toEqualTypeOf<"order">();
    expectTypeOf<RoutableRoutingKey<"order.created", "#">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order", "#">>().toEqualTypeOf<"order">();
    expectTypeOf<RoutableRoutingKey<"order.created", "order.#">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order", "order.#">>().toEqualTypeOf<"order">();
    expectTypeOf<
      RoutableRoutingKey<"order.created.v2", "order.#">
    >().toEqualTypeOf<"order.created.v2">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "#.created">
    >().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"created", "#.created">>().toEqualTypeOf<"created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created.v2", "order.#.v2">
    >().toEqualTypeOf<"order.created.v2">();
    expectTypeOf<RoutableRoutingKey<"order.v2", "order.#.v2">>().toEqualTypeOf<"order.v2">();
    expectTypeOf<
      RoutableRoutingKey<"order.a.b.v2", "order.#.v2">
    >().toEqualTypeOf<"order.a.b.v2">();
    expectTypeOf<
      RoutableRoutingKey<"order.created.v2", "order.*.#">
    >().toEqualTypeOf<"order.created.v2">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.*.#">
    >().toEqualTypeOf<"order.created">();
  });

  it("rejects the cases the runtime matcher rejects", () => {
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.updated">
    >().not.toEqualTypeOf<"order.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created.v2", "order.*">
    >().not.toEqualTypeOf<"order.created.v2">();
    expectTypeOf<RoutableRoutingKey<"order.created", "*">>().not.toEqualTypeOf<"order.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.#.v2">
    >().not.toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order", "order.*.#">>().not.toEqualTypeOf<"order">();
    expectTypeOf<
      RoutableRoutingKey<"user.created", "order.#">
    >().not.toEqualTypeOf<"user.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.created.v2">
    >().not.toEqualTypeOf<"order.created">();
  });
});
