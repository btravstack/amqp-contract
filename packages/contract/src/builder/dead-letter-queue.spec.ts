import { describe, expect, expectTypeOf, it } from "vitest";
import { z } from "zod";

import type { QueueDefinition } from "../types.js";
import { defineQueueBinding } from "./binding.js";
import { defineContract } from "./contract.js";
import { defineDeadLetterQueue } from "./dead-letter-queue.js";
import { defineEventConsumer, defineEventPublisher } from "./event.js";
import { defineExchange } from "./exchange.js";
import { defineMessage } from "./message.js";
import { defineQueue } from "./queue.js";

describe("defineDeadLetterQueue", () => {
  it("binds '#' on a topic exchange, so every dead letter is caught", () => {
    const dlx = defineExchange("orders-dlx");

    const dlq = defineDeadLetterQueue(dlx, "orders-dlq");

    expect(dlq).toEqual({
      queue: defineQueue("orders-dlq"),
      binding: defineQueueBinding(defineQueue("orders-dlq"), dlx, { routingKey: "#" }),
    });
    expectTypeOf(dlq.queue).toEqualTypeOf<QueueDefinition<"orders-dlq">>();
  });

  it("uses an explicit key on a topic exchange", () => {
    const dlx = defineExchange("orders-dlx");

    expect(defineDeadLetterQueue(dlx, "orders-dlq", { routingKey: "order.#" }).binding).toEqual(
      expect.objectContaining({ routingKey: "order.#" }),
    );
  });

  it("requires the key on a direct exchange, where '#' would match nothing", () => {
    const dlx = defineExchange("orders-dlx", { type: "direct" });

    expect(
      defineDeadLetterQueue(dlx, "orders-dlq", { routingKey: "order.failed" }).binding,
    ).toEqual(expect.objectContaining({ routingKey: "order.failed" }));
    // @ts-expect-error — a direct dead-letter exchange has no catch-all key
    expect(() => defineDeadLetterQueue(dlx, "orders-dlq")).toThrow(
      /requires a non-empty routingKey/,
    );
    expect(() => defineDeadLetterQueue(dlx, "orders-dlq", { routingKey: "#" })).toThrow(
      /"#" is a topic wildcard/,
    );
  });

  it("binds without a key on a fanout exchange", () => {
    const dlx = defineExchange("orders-dlx", { type: "fanout" });

    expect(defineDeadLetterQueue(dlx, "orders-dlq").binding).not.toHaveProperty("routingKey");
    // @ts-expect-error — a fanout exchange ignores the routing key
    defineDeadLetterQueue(dlx, "orders-dlq", { routingKey: "x" });
  });

  it("passes queue options through to defineQueue", () => {
    const dlx = defineExchange("orders-dlx");

    const dlq = defineDeadLetterQueue(dlx, "orders-dlq", {
      queue: { type: "classic", onPoison: "drop" },
    });

    expect(dlq.queue).toEqual(defineQueue("orders-dlq", { type: "classic", onPoison: "drop" }));
  });

  describe("with defineContract", () => {
    const orders = defineExchange("orders");
    const message = defineMessage(z.object({ id: z.string() }));
    const created = defineEventPublisher(orders, message, { routingKey: "order.created" });

    it("satisfies the dead-letter routability check that the bare DLX fails", () => {
      const dlx = defineExchange("orders-dlx");
      const queue = defineQueue("order-processing", { deadLetter: { exchange: dlx } });
      const dlq = defineDeadLetterQueue(dlx, "orders-dlq");
      const consumers = { process: defineEventConsumer(created, queue) };

      expect(() => defineContract({ publishers: { created }, consumers })).toThrow(
        /dead-letters to exchange "orders-dlx"/,
      );
      const contract = defineContract({
        publishers: { created },
        consumers,
        queues: { ordersDlq: dlq.queue },
        bindings: { ordersDlq: dlq.binding },
      });
      expect(Object.keys(contract.queues).sort()).toEqual(["order-processing", "orders-dlq"]);
    });

    it("is still checked: a key that misses the queues' dead-letter key is rejected", () => {
      const dlx = defineExchange("orders-dlx", { type: "direct" });
      const queue = defineQueue("order-processing", {
        deadLetter: { exchange: dlx, routingKey: "order.failed" },
      });
      const dlq = defineDeadLetterQueue(dlx, "orders-dlq", { routingKey: "order.dead" });

      expect(() =>
        defineContract({
          publishers: { created },
          consumers: { process: defineEventConsumer(created, queue) },
          queues: { ordersDlq: dlq.queue },
          bindings: { ordersDlq: dlq.binding },
        }),
      ).toThrow(/dead-letters to exchange "orders-dlx"/);
    });
  });
});
