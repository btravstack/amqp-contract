import { describe, expect, expectTypeOf, it } from "vitest";
import { z } from "zod";

import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "./index.js";

/**
 * INVARIANT: the runtime object returned by `defineContract` has exactly the
 * keys `ContractOutput` says it has — for a retry-configured contract in
 * particular. Before 3.0, ttl-backoff wait queues/exchanges/bindings were
 * injected at runtime without appearing in the type (`contract.exchanges["wait-exchange"]`
 * existed at runtime but failed to typecheck). Both assertions below are tied
 * to the same literal lists, so the type level and the runtime level cannot
 * drift apart without failing this spec.
 */
describe("ContractOutput / defineContract parity", () => {
  const dlx = defineExchange("orders-dlx", { type: "direct" });
  const ordersExchange = defineExchange("orders");
  const orderMessage = defineMessage(z.object({ orderId: z.string() }));
  const orderQueue = defineQueue("order-processing", {
    deadLetter: { exchange: dlx },
    retry: { mode: "ttl-backoff", maxRetries: 5, initialDelayMs: 2000 },
  });
  const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
    routingKey: "order.created",
  });

  const contract = defineContract({
    publishers: { orderCreated },
    consumers: { processOrder: defineEventConsumer(orderCreated, orderQueue) },
  });

  it("exchanges: runtime keys match the type-level keys", () => {
    const expected = ["orders", "orders-dlx"] as const;
    expectTypeOf<keyof typeof contract.exchanges>().toEqualTypeOf<(typeof expected)[number]>();
    expect(Object.keys(contract.exchanges).sort()).toEqual([...expected].sort());
  });

  it("queues: runtime keys match the type-level keys (no injected wait queues)", () => {
    const expected = ["order-processing"] as const;
    expectTypeOf<keyof typeof contract.queues>().toEqualTypeOf<(typeof expected)[number]>();
    expect(Object.keys(contract.queues).sort()).toEqual([...expected].sort());
  });

  it("bindings: runtime keys match the type-level keys (no injected retry bindings)", () => {
    const expected = ["processOrderBinding"] as const;
    expectTypeOf<keyof typeof contract.bindings>().toEqualTypeOf<(typeof expected)[number]>();
    expect(Object.keys(contract.bindings).sort()).toEqual([...expected].sort());
  });
});
