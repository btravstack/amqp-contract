import { describe, expectTypeOf, it } from "vitest";

import type { IsStringLiteral } from "./routing-types.js";

/**
 * The single decision procedure behind all three matcher types: can this
 * string be reasoned about at compile time?
 *
 * Answering it wrong in the "yes" direction is the expensive failure — the
 * matcher then runs on a type it cannot decide and reports a valid contract
 * as an error. Every case below that resolves to `false` is a case the
 * matchers must skip rather than guess at.
 */
describe("IsStringLiteral", () => {
  it("decides fully-known literals", () => {
    expectTypeOf<IsStringLiteral<"order.created">>().toEqualTypeOf<true>();
    expectTypeOf<IsStringLiteral<"">>().toEqualTypeOf<true>();
    expectTypeOf<IsStringLiteral<"order.*">>().toEqualTypeOf<true>();
    expectTypeOf<IsStringLiteral<"a" | "b">>().toEqualTypeOf<true>();
  });

  it("skips plain string", () => {
    expectTypeOf<IsStringLiteral<string>>().toEqualTypeOf<false>();
  });

  it("skips template literals, wherever the hole sits", () => {
    expectTypeOf<IsStringLiteral<`order.${string}`>>().toEqualTypeOf<false>();
    expectTypeOf<IsStringLiteral<`${string}.orders`>>().toEqualTypeOf<false>();
    expectTypeOf<IsStringLiteral<`a.${string}.b`>>().toEqualTypeOf<false>();
    expectTypeOf<IsStringLiteral<`${string}.orders.#`>>().toEqualTypeOf<false>();
    expectTypeOf<IsStringLiteral<`v${number}`>>().toEqualTypeOf<false>();
  });

  it("skips a union in which any member is not a literal", () => {
    // Without distribution this resolves to `true` and the templated member
    // reaches the matcher, which is the exact defect being fixed.
    expectTypeOf<IsStringLiteral<"a" | `b.${string}`>>().toEqualTypeOf<false>();
  });

  it("skips the empty union", () => {
    // `never` is vacuously assignable everywhere; without an explicit arm it
    // reports as a literal.
    expectTypeOf<IsStringLiteral<never>>().toEqualTypeOf<false>();
  });
});
