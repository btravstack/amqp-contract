import { describe, expect, it } from "vitest";
import { z } from "zod";

import { defineQueueBinding } from "./binding.js";
import { defineConsumer } from "./consumer.js";
import { defineContract } from "./contract.js";
import { defineExchange } from "./exchange.js";
import { defineMessage } from "./message.js";
import { definePublisher } from "./publisher.js";
import { defineQueue } from "./queue.js";
import { defineRpc } from "./rpc.js";

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
    const dlq = defineQueue("orders-dlq");

    expect(() =>
      defineContract({
        ...contractWith(queue),
        queues: { dlq },
        bindings: {
          processOrder: defineQueueBinding(queue, orders, { routingKey: "order.created" }),
          // The DLX check needs somewhere for the dead-lettered messages to land.
          dlq: defineQueueBinding(dlq, dlx, { routingKey: "#" }),
        },
      }),
    ).not.toThrow();
  });

  it('accepts a consumed queue that explicitly opts in to dropping with onPoison: "drop"', () => {
    const queue = defineQueue("order-processing", { onPoison: "drop" });

    expect(() => defineContract(contractWith(queue))).not.toThrow();
  });

  it("accepts a DLX set through the raw `arguments` passthrough", () => {
    // setup.ts spreads `queue.arguments` into the declare arguments, so this
    // queue genuinely dead-letters on the broker. Rejecting it would break a
    // contract that is valid and working today.
    const queue = defineQueue("order-processing", {
      arguments: { "x-dead-letter-exchange": "orders-dlx" },
    });

    expect(() => defineContract(contractWith(queue))).not.toThrow();
  });

  it("does NOT accept an empty or non-string `x-dead-letter-exchange` argument", () => {
    const empty = defineQueue("order-processing", {
      arguments: { "x-dead-letter-exchange": "" },
    });
    const notAString = defineQueue("order-processing", {
      arguments: { "x-dead-letter-exchange": 42 },
    });

    expect(() => defineContract(contractWith(empty))).toThrow(/order-processing/);
    expect(() => defineContract(contractWith(notAString))).toThrow(/order-processing/);
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

  it("applies to an RPC's queue, not just consumers", () => {
    // The rpcs arm is a separate loop in defineContract. Without this case the
    // whole `rpcs` loop could be deleted and every other spec would stay green.
    const queue = defineQueue("rpc.calculate");
    const calculate = defineRpc(queue, {
      request: defineMessage(z.object({ a: z.number() })),
      response: defineMessage(z.object({ sum: z.number() })),
    });

    expect(() => defineContract({ rpcs: { calculate } })).toThrow(/rpc\.calculate/);
    expect(() => defineContract({ rpcs: { calculate } })).toThrow(/calculate/);
    expect(() => defineContract({ rpcs: { calculate } })).toThrow(/onPoison/);
  });

  it("accepts an RPC queue with a dead-letter exchange", () => {
    const queue = defineQueue("rpc.calculate", { deadLetter: { exchange: dlx } });
    const dlq = defineQueue("orders-dlq");
    const calculate = defineRpc(queue, {
      request: defineMessage(z.object({ a: z.number() })),
      response: defineMessage(z.object({ sum: z.number() })),
    });

    expect(() =>
      defineContract({
        rpcs: { calculate },
        queues: { dlq },
        // The DLX check needs somewhere for the dead-lettered messages to land.
        bindings: { dlq: defineQueueBinding(dlq, dlx, { routingKey: "#" }) },
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
