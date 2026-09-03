import { TypedAmqpClient } from "@amqp-contract/client";
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
  defineRpc,
} from "@amqp-contract/contract";
import { InMemoryAmqpBroker } from "@amqp-contract/testing/in-memory";
import { NonRetryableError, TypedAmqpWorker } from "@amqp-contract/worker";
import { ErrAsync, OkAsync } from "unthrown";
import { describe, expect, it, vi } from "vitest";
import { z } from "zod";

/**
 * The whole point of these: they exercise the contract pipeline — both
 * validation passes, serialization, RPC correlation, dead-lettering — in the
 * **unit** project, where nothing may need a broker. The integration suite
 * keeps the broker behaviours a fake cannot honestly claim.
 */
const orderPlaced = defineMessage(z.object({ orderId: z.string(), total: z.number() }));

const events = defineExchange("events", { type: "topic", durable: false });
const parked = defineExchange("parked", { durable: false });
const orders = defineQueue("orders", {
  type: "classic",
  durable: false,
  deadLetter: { exchange: parked },
});
const dlq = defineQueue("orders-dlq", { type: "classic", durable: false });

const placeOrder = defineEventPublisher(events, orderPlaced, { routingKey: "order.placed" });

const pubSubContract = defineContract({
  publishers: { placeOrder },
  consumers: { onOrder: defineEventConsumer(placeOrder, orders) },
  queues: { dlq },
  bindings: { dlqBinding: defineQueueBinding(dlq, parked, { routingKey: "#" }) },
});

describe("the in-memory broker carries a contract end to end", () => {
  it("delivers a published event to the consumer that is bound for it", async () => {
    // GIVEN a worker and a client on one broker, with no Docker anywhere
    const broker = new InMemoryAmqpBroker();
    const received: unknown[] = [];
    const worker = await TypedAmqpWorker.create({
      contract: pubSubContract,
      handlers: {
        onOrder: (_helpers, { payload }) => {
          received.push(payload);
          return OkAsync();
        },
      },
      transport: broker.createTransport(pubSubContract),
    }).getOrThrow();
    const client = await TypedAmqpClient.create({
      contract: pubSubContract,
      transport: broker.createTransport(pubSubContract),
    }).getOrThrow();

    // WHEN an event is published
    await client.publish("placeOrder", { orderId: "o-1", total: 42 }).getOrThrow();
    await vi.waitUntil(() => received.length > 0);

    // THEN it arrives validated and parsed, the routing key having matched
    expect(received).toEqual([{ orderId: "o-1", total: 42 }]);
    await worker.close().get();
    await client.close().get();
  });

  it("dead-letters a handler failure to the queue's dead-letter exchange", async () => {
    // GIVEN a handler that refuses every message
    const broker = new InMemoryAmqpBroker();
    const worker = await TypedAmqpWorker.create({
      contract: pubSubContract,
      handlers: { onOrder: () => ErrAsync(new NonRetryableError("nope")) },
      transport: broker.createTransport(pubSubContract),
    }).getOrThrow();
    const client = await TypedAmqpClient.create({
      contract: pubSubContract,
      transport: broker.createTransport(pubSubContract),
    }).getOrThrow();

    // WHEN an event is published
    await client.publish("placeOrder", { orderId: "o-2", total: 1 }).getOrThrow();
    await vi.waitUntil(() => broker.peek("orders-dlq").length > 0);

    // THEN the message is parked on the DLQ, body intact
    expect(
      broker.peek("orders-dlq").map((message) => JSON.parse(message.content.toString())),
    ).toEqual([{ orderId: "o-2", total: 1 }]);
    await worker.close({ drainTimeoutMs: 200 }).get();
    await client.close().get();
  });
});

const rpcQueue = defineQueue("calculate", {
  type: "classic",
  durable: false,
  deadLetter: { exchange: parked },
});
const calculate = defineRpc(rpcQueue, {
  request: defineMessage(z.object({ a: z.number(), b: z.number() })),
  response: defineMessage(z.object({ sum: z.number() })),
  errors: { OVERFLOW: { data: z.object({ limit: z.number() }) } },
});

const rpcContract = defineContract({
  rpcs: { calculate },
  queues: { dlq },
  bindings: { dlqBinding: defineQueueBinding(dlq, parked, { routingKey: "#" }) },
});

describe("the in-memory broker carries an RPC", () => {
  it("routes a reply back through direct reply-to", async () => {
    // GIVEN an RPC worker and a client, both on the in-memory broker
    const broker = new InMemoryAmqpBroker();
    const worker = await TypedAmqpWorker.create({
      contract: rpcContract,
      handlers: { calculate: (_helpers, { payload }) => OkAsync({ sum: payload.a + payload.b }) },
      transport: broker.createTransport(rpcContract),
    }).getOrThrow();
    const client = await TypedAmqpClient.create({
      contract: rpcContract,
      transport: broker.createTransport(rpcContract),
    }).getOrThrow();

    // WHEN the client calls
    // THEN the reply comes home, correlated, through the reply pseudo-queue
    const outcome = await client.call("calculate", { a: 2, b: 3 }, { timeoutMs: 2_000 });

    expect(outcome.isOk() ? outcome.value : outcome.isDefect() ? outcome.cause : outcome).toEqual({
      sum: 5,
    });
    await worker.close({ drainTimeoutMs: 200 }).get();
    await client.close().get();
  });

  it("carries a declared RPC error back as a typed failure", async () => {
    // GIVEN a handler that answers a declared error code
    const broker = new InMemoryAmqpBroker();
    const worker = await TypedAmqpWorker.create({
      contract: rpcContract,
      handlers: {
        calculate: (helpers) => ErrAsync(helpers.errors.OVERFLOW({ limit: 100 })),
      },
      transport: broker.createTransport(rpcContract),
    }).getOrThrow();
    const client = await TypedAmqpClient.create({
      contract: rpcContract,
      transport: broker.createTransport(rpcContract),
    }).getOrThrow();

    // WHEN the client calls
    const outcome = await client.call("calculate", { a: 1, b: 1 }, { timeoutMs: 2_000 });

    // THEN the failure arrives as the declared code, not as a timeout
    // `code` is the discriminator only on the declared-error arm; the others
    // are the timeout and validation failures the same channel can carry.
    expect(outcome.isErr() && "code" in outcome.error ? outcome.error.code : outcome).toBe(
      "OVERFLOW",
    );
    await worker.close({ drainTimeoutMs: 200 }).get();
    await client.close().get();
  });
});
