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

    // The client declared the consumer's queue and its dead-letter path, but
    // none of the consumer's retry wait queues.
    expect((await amqpChannel.checkQueue("order-processing")).messageCount).toBe(1);
    await amqpChannel.checkExchange("orders-dlx");
    await amqpChannel.checkQueue("orders-dlq");
    const probe = await amqpConnection.createChannel();
    probe.on("error", () => {});
    await expect(probe.checkQueue("order-processing-wait-1000ms")).rejects.toThrow(/NOT_FOUND/);

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

  it("INVARIANT: a quorum immediate-requeue queue declared first by the client is re-declared by the worker without PRECONDITION_FAILED", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    // defineQueue puts `x-delivery-limit` in the queue's arguments; the client
    // (retain-only slice) and the worker (full queue, DLX and all) must send
    // the broker identical arguments, or the second declaration is refused.
    const orders = defineExchange("dl-first-orders", { type: "direct" });
    const dlx = defineExchange("dl-first-dlx", { type: "direct" });
    const dlq = defineQueue("dl-first-dlq");
    const processing = defineQueue("dl-first-processing", {
      deadLetter: { exchange: dlx, routingKey: "failed" },
      retry: { mode: "immediate-requeue", maxRetries: 25 },
    });
    const created = defineEventPublisher(orders, defineMessage(z.object({ id: z.string() })), {
      routingKey: "order.created",
    });
    const contract = defineContract({
      publishers: { created },
      consumers: { process: defineEventConsumer(created, processing) },
      queues: { dlq },
      bindings: { dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "failed" }) },
    });
    expect(processing.arguments?.["x-delivery-limit"]).toBe(26);

    // GIVEN the client declared the queue first and published into it.
    const client = await TypedAmqpClient.create({
      contract,
      urls: [amqpConnectionUrl],
    }).getOrThrow();
    await client.publish("created", { id: "early" }).getOrThrow();
    await client.close().get();
    expect((await amqpChannel.checkQueue("dl-first-processing")).messageCount).toBe(1);

    // WHEN the worker declares the same queue, THEN it starts and consumes.
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

  it("INVARIANT: a message the broker dead-letters before any worker exists reaches the DLQ, not a missing exchange", async ({
    amqpConnectionUrl,
    amqpConnection,
    amqpChannel,
  }) => {
    // A queue-level TTL expires the message long before a worker starts; the
    // broker dead-letters it to the queue's DLX — which only the worker used
    // to declare, so the expired message was discarded.
    const orders = defineExchange("ttl-orders");
    const dlx = defineExchange("ttl-orders-dlx");
    const dlq = defineQueue("ttl-orders-dlq");
    const processing = defineQueue("ttl-order-processing", {
      deadLetter: { exchange: dlx },
      arguments: { "x-message-ttl": 50 },
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

    // GIVEN only a client, WHEN its message expires unconsumed,
    const client = await TypedAmqpClient.create({
      contract,
      urls: [amqpConnectionUrl],
    }).getOrThrow();
    await client.publish("created", { id: "early" }).getOrThrow();
    await client.close().get();

    // THEN the broker dead-letters it into the DLQ, with reason "expired".
    // (A throwaway channel per probe: a missing DLQ answers 404, which closes
    // the channel it was asked on.)
    const dlqDepth = async () => {
      const probe = await amqpConnection.createChannel();
      probe.on("error", () => {});
      const { messageCount } = await probe.checkQueue("ttl-orders-dlq");
      await probe.close();
      return messageCount;
    };
    await vi.waitFor(async () => expect(await dlqDepth()).toBe(1), { timeout: 5_000 });
    const deadLetter = await amqpChannel.get("ttl-orders-dlq", { noAck: true });
    expect(deadLetter && deadLetter.properties.headers?.["x-death"]).toEqual([
      expect.objectContaining({ reason: "expired", queue: "ttl-order-processing" }),
    ]);
  });
});
