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
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { OkAsync } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

/**
 * The client declares the queues its publishes route to, so a message sent
 * before any worker has started is retained, not confirmed-and-dropped.
 */
describe("publisher topology", () => {
  it("INVARIANT: a message published before any worker exists is delivered once the worker starts", async ({
    amqpConnectionUrl,
    amqpConnection,
    amqpChannel,
  }) => {
    const orders = defineExchange("orders");
    const dlx = defineExchange("orders-dlx");
    const dlq = defineQueue("orders-dlq");
    const processing = defineQueue("order-processing", {
      deadLetter: { exchange: dlx },
      retry: { mode: "ttl-backoff", maxRetries: 2 },
    });
    const created = defineEventPublisher(orders, defineMessage(z.object({ id: z.string() })), {
      routingKey: "order.created",
    });
    const contract = defineContract({
      publishers: { created },
      consumers: { process: defineEventConsumer(created, processing) },
      queues: { dlq },
      bindings: { dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "#" }) },
    });

    // GIVEN only a client: publish before any worker exists.
    const client = await TypedAmqpClient.create({
      contract,
      urls: [amqpConnectionUrl],
    }).getOrThrow();
    await client.publish("created", { id: "early" }).getOrThrow();
    await client.close().get();

    // The client declared the consumer's queue, and nothing of the consumer's
    // own infrastructure (DLX, DLQ, retry wait queues).
    expect((await amqpChannel.checkQueue("order-processing")).messageCount).toBe(1);
    const probe = await amqpConnection.createChannel();
    probe.on("error", () => {});
    await expect(probe.checkExchange("orders-dlx")).rejects.toThrow(/NOT_FOUND/);

    // WHEN the worker starts, THEN it receives the early message.
    const received: string[] = [];
    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: {
        process: ({ input: { payload } }) => {
          received.push(payload.id);
          return OkAsync(undefined);
        },
      },
      urls: [amqpConnectionUrl],
    }).getOrThrow();
    try {
      await vi.waitFor(() => expect(received).toEqual(["early"]), { timeout: 5_000 });
    } finally {
      await worker.close().get();
    }
  });
});
