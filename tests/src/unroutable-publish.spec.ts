import { TypedAmqpClient } from "@amqp-contract/client";
import {
  defineContract,
  defineExchange,
  defineMessage,
  definePublisher,
} from "@amqp-contract/contract";
import { it } from "@amqp-contract/testing/extension";
import { describe, expect } from "vitest";
import { z } from "zod";

/**
 * H1, proven end to end against a real broker.
 *
 * Test 1 demonstrates the hazard is genuine: an unroutable publish is
 * confirmed by RabbitMQ and the message is gone. Test 2 demonstrates the
 * define-time guard catches the same contract before it can run.
 *
 * The first test is the reason the second exists; if anyone ever weakens
 * the guard, the pair reads as a complete argument for putting it back.
 */
describe("unroutable publish", () => {
  const message = defineMessage(z.object({ orderId: z.string() }));

  it("INVARIANT: an unroutable publish is confirmed by the broker and the message is lost", async ({
    amqpChannel,
    amqpConnectionUrl,
  }) => {
    // GIVEN a topic exchange whose only queue is bound with a pattern that
    // cannot match the publisher's routing key.
    const exchangeName = "orders";
    const queueName = "audit";
    await amqpChannel.assertExchange(exchangeName, "topic", { durable: false });
    await amqpChannel.assertQueue(queueName, { durable: false });
    await amqpChannel.bindQueue(queueName, exchangeName, "user.#");

    const orders = defineExchange(exchangeName, { type: "topic", durable: false });
    // Both publishers deliberately bypass the define-time guard: the binding
    // above lives on the broker, not in the contract, so the raw broker
    // behavior is what is under test here.
    const orderCreated = definePublisher(orders, message, {
      routingKey: "order.created",
      externalConsumers: true,
    });
    // A sentinel on a key the binding DOES match. Published second on the same
    // channel, so its arrival proves the broker finished routing the first
    // message — the "we checked the queue too early" explanation is ruled out
    // without a sleep.
    const sentinel = definePublisher(orders, message, {
      routingKey: "user.created",
      externalConsumers: true,
    });
    const contract = defineContract({ publishers: { orderCreated, sentinel } });

    const client = await TypedAmqpClient.create({
      contract,
      urls: [amqpConnectionUrl],
    }).get();

    try {
      // WHEN the unroutable message is published, and then the sentinel.
      const lost = await client.publish("orderCreated", { orderId: "lost" });
      const delivered = await client.publish("sentinel", { orderId: "sentinel" });

      // THEN the broker confirms BOTH — publish reports success either way.
      expect(lost.isOk()).toBe(true);
      expect(delivered.isOk()).toBe(true);

      // ...and only the sentinel reached the queue. The unroutable message was
      // acknowledged and discarded: at the API surface, loss is
      // indistinguishable from success.
      const stats = await amqpChannel.checkQueue(queueName);
      expect(stats.messageCount).toBe(1);

      const received = await amqpChannel.get(queueName, { noAck: true });
      expect(received).not.toBe(false);
      // `get` returns `false` when the queue is empty; the assertion above has
      // already failed the test in that case.
      const body = received === false ? undefined : JSON.parse(received.content.toString());
      expect(body).toEqual({ orderId: "sentinel" });

      // Nothing else is queued anywhere — the order message is simply gone.
      expect(await amqpChannel.get(queueName, { noAck: true })).toBe(false);
    } finally {
      await client.close().get();
    }
  });

  it("INVARIANT: the same contract is rejected at define time", () => {
    const orders = defineExchange("orders-guarded", { type: "topic", durable: false });
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() => defineContract({ publishers: { orderCreated } })).toThrow(/unroutable/i);
  });
});
