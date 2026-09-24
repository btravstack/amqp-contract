import { TypedAmqpClient } from "@amqp-contract/client";
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { it } from "@amqp-contract/testing/extension";
import { RetryableError, TypedAmqpWorker } from "@amqp-contract/worker";
import { ErrAsync } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

/**
 * Quorum immediate-requeue retry vs RabbitMQ 4.x's default `x-delivery-limit`.
 *
 * Test 1 shows the hazard is genuine: on a quorum queue declared with no
 * delivery limit, the broker itself dead-letters a repeatedly requeued message
 * after 20 redeliveries — so a worker configured with `maxRetries: 25` would
 * never reach its own budget. Test 2 shows that a contract with that budget
 * now carries the limit to the broker, and the worker — not the broker — makes
 * the dead-letter decision after exactly `maxRetries` retries.
 */
describe("quorum delivery limit", () => {
  const MAX_RETRIES = 25;

  it("INVARIANT: an unconfigured quorum queue dead-letters on the broker's own limit, before 25 retries", async ({
    amqpChannel,
  }) => {
    await amqpChannel.assertExchange("dl-raw-dlx", "fanout", { durable: false });
    await amqpChannel.assertQueue("dl-raw-dlq", { durable: false });
    await amqpChannel.bindQueue("dl-raw-dlq", "dl-raw-dlx", "");
    await amqpChannel.assertQueue("dl-raw", {
      durable: true,
      arguments: { "x-queue-type": "quorum", "x-dead-letter-exchange": "dl-raw-dlx" },
    });

    amqpChannel.sendToQueue("dl-raw", Buffer.from("poison"));

    // Requeue the message exactly as the worker's quorum immediate-requeue
    // path does, until the broker stops handing it back.
    let deliveries = 0;
    await vi.waitFor(
      async () => {
        const msg = await amqpChannel.get("dl-raw", { noAck: false });
        if (msg !== false) {
          deliveries += 1;
          amqpChannel.nack(msg, false, true);
        }
        expect((await amqpChannel.checkQueue("dl-raw-dlq")).messageCount).toBe(1);
      },
      { timeout: 15_000, interval: 10 },
    );

    const deadLettered = await amqpChannel.get("dl-raw-dlq", { noAck: true });
    const reason = deadLettered === false ? undefined : deadLettered.properties.headers;
    // The broker decided, on its own limit — well short of a 25-retry budget.
    expect(reason?.["x-first-death-reason"]).toBe("delivery_limit");
    expect(deliveries).toBeLessThan(MAX_RETRIES + 1);
  }, 30_000);

  it("INVARIANT: a contract's quorum immediate-requeue budget is spent by the worker, not cut short by the broker", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    const exchange = defineExchange("dl-orders", { type: "direct", durable: false });
    const dlx = defineExchange("dl-orders-dlx", { type: "direct", durable: false });
    const queue = defineQueue("dl-processing", {
      deadLetter: { exchange: dlx, routingKey: "failed" },
      retry: { mode: "immediate-requeue", maxRetries: MAX_RETRIES },
    });
    const dlq = defineQueue("dl-processing-dlq", { type: "classic", durable: false });
    const message = defineMessage(z.object({ orderId: z.string() }));
    const created = defineEventPublisher(exchange, message, { routingKey: "order.created" });
    const contract = defineContract({
      publishers: { created },
      consumers: { process: defineEventConsumer(created, queue) },
      queues: { dlq },
      bindings: { dlq: defineQueueBinding(dlq, dlx, { routingKey: "failed" }) },
    });

    let attempts = 0;
    const worker = await TypedAmqpWorker.create({
      contract,
      urls: [amqpConnectionUrl],
      handlers: {
        process: () => {
          attempts += 1;
          return ErrAsync(new RetryableError("always fails"));
        },
      },
    }).getOrThrow();
    const client = await TypedAmqpClient.create({
      contract,
      urls: [amqpConnectionUrl],
    }).getOrThrow();

    try {
      await client.publish("created", { orderId: "o-1" }).getOrThrow();

      await vi.waitFor(
        async () => {
          expect((await amqpChannel.checkQueue("dl-processing-dlq")).messageCount).toBe(1);
        },
        { timeout: 20_000, interval: 50 },
      );

      const deadLettered = await amqpChannel.get("dl-processing-dlq", { noAck: true });
      const headers = deadLettered === false ? undefined : deadLettered.properties.headers;
      // `rejected` is the worker's nack(requeue: false); the broker's own cap
      // would read `delivery_limit`.
      expect({ attempts, reason: headers?.["x-first-death-reason"] }).toEqual({
        attempts: MAX_RETRIES + 1,
        reason: "rejected",
      });
    } finally {
      await worker.close().get();
      await client.close().get();
    }
  }, 40_000);
});
