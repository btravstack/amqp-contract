import { describe, expect, it } from "vitest";
import { z } from "zod";

import { defineQueueBinding } from "./binding.js";
import { defineContract } from "./contract.js";
import { defineEventConsumer, defineEventPublisher } from "./event.js";
import { defineExchange } from "./exchange.js";
import { defineMessage } from "./message.js";
import { defineQueue } from "./queue.js";

describe("defineContract standalone topology", () => {
  it("registers standalone exchanges, queues, and bindings keyed like extracted resources", () => {
    // GIVEN — the classic case: a DLQ bound to the DLX so dead-lettered
    // messages are actually retained, without any consumer in this service.
    const dlx = defineExchange("orders-dlx", { type: "direct", durable: false });
    const dlq = defineQueue("orders-dlq", { type: "classic", durable: false });
    const auditExchange = defineExchange("audit", { type: "fanout", durable: false });

    // WHEN
    const contract = defineContract({
      exchanges: { auditExchange },
      queues: { dlq },
      bindings: {
        dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "orders.dlq" }),
      },
    });

    // THEN — exchanges and queues are keyed by NAME (authoring labels are
    // dropped, matching extracted resources); binding labels are kept.
    expect(contract.exchanges).toMatchObject({ audit: auditExchange });
    expect(contract.queues).toMatchObject({ "orders-dlq": dlq });
    expect(contract.bindings.dlqBinding).toMatchObject({
      queue: dlq,
      exchange: dlx,
      routingKey: "orders.dlq",
    });
  });

  it("auto-extracts a standalone queue's dead-letter exchange but stores no TTL-backoff infrastructure", () => {
    // GIVEN
    const dlx = defineExchange("standalone-dlx", { type: "direct", durable: false });
    const queue = defineQueue("standalone-retry", {
      type: "classic",
      durable: false,
      // The DLX is `direct`, so the dead-letter routing key must be spelled out
      // and bound verbatim — `#` is a topic wildcard and matches nothing here.
      deadLetter: { exchange: dlx, routingKey: "standalone-retry.dead" },
      retry: { mode: "ttl-backoff", maxRetries: 3 },
    });
    const dlq = defineQueue("standalone-dlq", { type: "classic", durable: false });

    // WHEN
    const contract = defineContract({
      queues: { queue, dlq },
      bindings: {
        dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "standalone-retry.dead" }),
      },
    });

    // THEN — the DLX is extracted; wait queues are derived at topology-setup
    // time and never appear in the contract.
    expect(contract.exchanges).toMatchObject({ "standalone-dlx": dlx });
    expect(Object.keys(contract.queues)).toEqual(["standalone-retry", "standalone-dlq"]);
    expect(Object.keys(contract.bindings)).toEqual(["dlqBinding"]);
  });

  it("dedupes a standalone declaration against the same resource extracted from a consumer", () => {
    // GIVEN — the queue is declared standalone AND used by a consumer.
    const exchange = defineExchange("orders", { durable: false });
    const queue = defineQueue("order-processing", {
      type: "classic",
      durable: false,
      onPoison: "drop",
    });
    const message = defineMessage(z.object({ orderId: z.string() }));
    const orderCreated = defineEventPublisher(exchange, message, { routingKey: "order.created" });

    // WHEN
    const contract = defineContract({
      queues: { queue },
      publishers: { orderCreated },
      consumers: { processOrder: defineEventConsumer(orderCreated, queue) },
    });

    // THEN — one queue, no collision error.
    expect(Object.keys(contract.queues)).toEqual(["order-processing"]);
  });

  it("rejects a standalone declaration that conflicts with an extracted resource", () => {
    // GIVEN — same queue name, different definition.
    const exchange = defineExchange("orders", { durable: false });
    const standalone = defineQueue("order-processing", { type: "classic", durable: true });
    const consumed = defineQueue("order-processing", { type: "classic", durable: false });
    const message = defineMessage(z.object({ orderId: z.string() }));
    const orderCreated = defineEventPublisher(exchange, message, { routingKey: "order.created" });

    // WHEN / THEN
    expect(() =>
      defineContract({
        queues: { standalone },
        publishers: { orderCreated },
        consumers: { processOrder: defineEventConsumer(orderCreated, consumed) },
      }),
    ).toThrow(/queue "order-processing" was declared with conflicting definitions/);
  });
});
