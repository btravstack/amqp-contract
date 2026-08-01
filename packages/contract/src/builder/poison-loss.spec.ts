import { describe, expect, it } from "vitest";
import { z } from "zod";

import { defineQueueBinding } from "./binding.js";
import { defineConsumer } from "./consumer.js";
import { defineContract } from "./contract.js";
import { defineExchange } from "./exchange.js";
import { defineMessage } from "./message.js";
import { definePublisher } from "./publisher.js";
import { defineQueue } from "./queue.js";

const message = defineMessage(z.object({ orderId: z.string() }));
const orders = defineExchange("orders", { type: "topic" });
const dlx = defineExchange("orders-dlx", { type: "topic" });

/** A routable publisher, so the H1 check never fires in these tests. */
function contractWith(queue: ReturnType<typeof defineQueue>) {
  return {
    publishers: { orderCreated: definePublisher(orders, message, { routingKey: "order.created" }) },
    consumers: { processOrder: defineConsumer(queue, message) },
    bindings: { processOrder: defineQueueBinding(queue, orders, { routingKey: "order.created" }) },
  };
}

describe("silent poison-loss guard", () => {
  it("throws for a consumed queue with neither a DLX nor an explicit onPoison", () => {
    const queue = defineQueue("order-processing");

    expect(() => defineContract(contractWith(queue))).toThrow(/order-processing/);
    expect(() => defineContract(contractWith(queue))).toThrow(/onPoison/);
  });

  it("names the consumer that makes the queue poisonable", () => {
    const queue = defineQueue("order-processing");

    expect(() => defineContract(contractWith(queue))).toThrow(/processOrder/);
  });

  it("accepts a consumed queue with a dead-letter exchange", () => {
    const queue = defineQueue("order-processing", { deadLetter: { exchange: dlx } });

    expect(() => defineContract(contractWith(queue))).not.toThrow();
  });

  it('accepts a consumed queue that explicitly opts in to dropping with onPoison: "drop"', () => {
    const queue = defineQueue("order-processing", { onPoison: "drop" });

    expect(() => defineContract(contractWith(queue))).not.toThrow();
  });

  it("does NOT require a DLX on a declared-but-unconsumed queue (the dead-letter queue case)", () => {
    // A DLQ has no DLX of its own — that would be infinite regress — and is
    // typically inspected rather than consumed. It must not trip the check.
    const dlq = defineQueue("orders-dlq");
    const processing = defineQueue("order-processing", { deadLetter: { exchange: dlx } });

    expect(() =>
      defineContract({
        ...contractWith(processing),
        queues: { dlq },
        bindings: {
          processOrder: defineQueueBinding(processing, orders, { routingKey: "order.created" }),
          dlq: defineQueueBinding(dlq, dlx, { routingKey: "#" }),
        },
      }),
    ).not.toThrow();
  });

  it("DOES require it once that same dead-letter queue is consumed", () => {
    // A DLQ processor can itself poison-loop, so the check applies again.
    const dlq = defineQueue("orders-dlq");
    const processing = defineQueue("order-processing", { deadLetter: { exchange: dlx } });

    expect(() =>
      defineContract({
        ...contractWith(processing),
        consumers: {
          processOrder: defineConsumer(processing, message),
          inspectDlq: defineConsumer(dlq, message),
        },
        bindings: {
          processOrder: defineQueueBinding(processing, orders, { routingKey: "order.created" }),
          dlq: defineQueueBinding(dlq, dlx, { routingKey: "#" }),
        },
      }),
    ).toThrow(/orders-dlq/);
  });
});
