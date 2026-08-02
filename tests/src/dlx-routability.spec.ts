import {
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  definePublisher,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { it } from "@amqp-contract/testing/extension";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

/**
 * The dead-letter half of the routability guard, proven end to end.
 *
 * Test 1 shows the hazard is genuine: a message dead-lettered to an exchange
 * with nothing bound is discarded by the broker, with no error anywhere. Test 2
 * shows the guard rejects that contract before it can run. Test 3 measures the
 * `#`-on-a-direct-exchange trap the guard's error text warns about but cannot
 * detect, which is why the warning has to travel in the message.
 *
 * The first test is the reason the second exists; if anyone ever weakens the
 * guard, the pair reads as a complete argument for putting it back.
 */
describe("dead-letter routability", () => {
  const message = defineMessage(z.object({ orderId: z.string() }));

  const body = (orderId: string) => Buffer.from(JSON.stringify({ orderId }));

  it("INVARIANT: a message dead-lettered to an unbound exchange is discarded by the broker", async ({
    amqpChannel,
  }) => {
    // GIVEN a routable DLX — a topic exchange with a dead-letter queue bound to
    // it. This is the control: it proves dead-lettering is enabled, works on
    // this broker, and completes inside the wait below.
    await amqpChannel.assertExchange("dlx-bound", "topic", { durable: false });
    await amqpChannel.assertQueue("dlq-bound", { durable: false });
    await amqpChannel.bindQueue("dlq-bound", "dlx-bound", "#");

    // ...and an unroutable DLX: declared, existing, and bound to nothing. This
    // is the shape the guard now rejects.
    await amqpChannel.assertExchange("dlx-unbound", "topic", { durable: false });

    await amqpChannel.assertQueue("src-control", {
      durable: false,
      deadLetterExchange: "dlx-bound",
    });
    await amqpChannel.assertQueue("src-lost", {
      durable: false,
      deadLetterExchange: "dlx-unbound",
    });
    // A queue on the same channel that is bound to nothing of ours and never
    // rejected, so a "0 messages" result elsewhere cannot be explained by a
    // broker that stopped delivering.
    await amqpChannel.assertQueue("witness", { durable: false });

    // These are the only queues in this test's vhost, so checking all four is a
    // complete census of where a message can be.
    amqpChannel.sendToQueue("src-control", body("control"));
    amqpChannel.sendToQueue("src-lost", body("lost"));
    amqpChannel.sendToQueue("witness", body("witness"));

    const lost = await amqpChannel.get("src-lost", { noAck: false });
    const control = await amqpChannel.get("src-control", { noAck: false });
    expect(lost).not.toBe(false);
    expect(control).not.toBe(false);
    if (lost === false || control === false) {
      // oxlint-disable-next-line unthrown/no-throw -- test setup; the assertions above have already failed
      throw new Error("the broker did not deliver the messages under test");
    }

    // WHEN both are rejected without requeue, the broker dead-letters each to
    // its queue's own DLX.
    amqpChannel.nack(lost, false, false);
    amqpChannel.nack(control, false, false);

    // THEN wait for the broker to settle by polling for the state we expect,
    // not by sleeping. The control's arrival is the signal that dead-lettering
    // has run; a fixed sleep would either be slower or hide the race.
    await vi.waitFor(
      async () => {
        expect((await amqpChannel.checkQueue("dlq-bound")).messageCount).toBe(1);
        expect((await amqpChannel.checkQueue("src-lost")).messageCount).toBe(0);
        expect((await amqpChannel.checkQueue("src-control")).messageCount).toBe(0);
      },
      { timeout: 10_000, interval: 50 },
    );

    // The witness still holds its message: the broker was alive and delivering
    // for the whole window.
    expect((await amqpChannel.checkQueue("witness")).messageCount).toBe(1);

    // The control message really did travel the dead-letter path, so the test
    // can detect a dead letter arriving. It is the *control* message, not the
    // lost one — this assertion is what fails if `dlx-unbound` is ever made
    // routable, which is how we know the test is not passing vacuously.
    const deadLettered = await amqpChannel.get("dlq-bound", { noAck: true });
    expect(deadLettered).not.toBe(false);
    const deadLetteredBody =
      deadLettered === false ? undefined : JSON.parse(deadLettered.content.toString());
    expect(deadLetteredBody).toEqual({ orderId: "control" });
    expect(await amqpChannel.get("dlq-bound", { noAck: true })).toBe(false);

    // Nothing anywhere holds the lost message. Three messages were published;
    // the control is accounted for above, the witness below, and the third
    // reached no queue at all — RabbitMQ discarded it, and neither the nack nor
    // any other API call reported a thing. This does not depend on the wait
    // above: no queue in this vhost is bound to "dlx-unbound", so there is
    // nowhere the message could turn up later.
    expect(await amqpChannel.get("src-lost", { noAck: true })).toBe(false);
    expect(await amqpChannel.get("src-control", { noAck: true })).toBe(false);

    const survivor = await amqpChannel.get("witness", { noAck: true });
    const survivorBody = survivor === false ? undefined : JSON.parse(survivor.content.toString());
    expect(survivorBody).toEqual({ orderId: "witness" });
  }, 20_000);

  it("INVARIANT: the same contract is rejected at define time", () => {
    const orders = defineExchange("orders-dlxr", { type: "topic", durable: false });
    const dlx = defineExchange("orders-dlx-dlxr", { type: "topic", durable: false });
    const queue = defineQueue("order-processing-dlxr", {
      type: "classic",
      durable: false,
      deadLetter: { exchange: dlx },
    });

    expect(() =>
      defineContract({
        publishers: {
          orderCreated: definePublisher(orders, message, { routingKey: "order.created" }),
        },
        consumers: { processOrder: defineConsumer(queue, message) },
        bindings: {
          processOrder: defineQueueBinding(queue, orders, { routingKey: "order.created" }),
        },
      }),
    ).toThrow(/dead-letters to exchange/);
  });

  it("INVARIANT: on a direct dead-letter exchange, a '#' binding receives nothing", async ({
    amqpChannel,
  }) => {
    // GIVEN the same `#` binding on a topic DLX and on a direct one. `#` is a
    // topic wildcard; a direct exchange treats it as the literal key "#".
    await amqpChannel.assertExchange("dlx-topic", "topic", { durable: false });
    await amqpChannel.assertQueue("dlq-topic", { durable: false });
    await amqpChannel.bindQueue("dlq-topic", "dlx-topic", "#");

    await amqpChannel.assertExchange("dlx-direct", "direct", { durable: false });
    await amqpChannel.assertQueue("dlq-direct", { durable: false });
    await amqpChannel.bindQueue("dlq-direct", "dlx-direct", "#");

    // ...plus a positive control ON THE DIRECT EXCHANGE ITSELF, bound to the
    // literal key the dead letter will carry. Without it, `dlq-direct === 0`
    // would be equally consistent with "the direct source never dead-lettered
    // at all"; with it, the same dead letter is measured arriving on the same
    // exchange, so 0 on the `#` binding can only mean the key did not match.
    await amqpChannel.assertQueue("dlq-direct-literal", { durable: false });
    await amqpChannel.bindQueue("dlq-direct-literal", "dlx-direct", "order.created");

    // Both source queues are reached under the routing key "order.created" —
    // the key a dead letter keeps when the queue sets no dead-letter key.
    await amqpChannel.assertExchange("orders-measured", "topic", { durable: false });
    await amqpChannel.assertQueue("src-topic", {
      durable: false,
      deadLetterExchange: "dlx-topic",
    });
    await amqpChannel.assertQueue("src-direct", {
      durable: false,
      deadLetterExchange: "dlx-direct",
    });
    await amqpChannel.bindQueue("src-topic", "orders-measured", "order.created");
    await amqpChannel.bindQueue("src-direct", "orders-measured", "order.created");

    amqpChannel.publish("orders-measured", "order.created", body("measured"));

    const fromDirect = await amqpChannel.get("src-direct", { noAck: false });
    const fromTopic = await amqpChannel.get("src-topic", { noAck: false });
    expect(fromDirect).not.toBe(false);
    expect(fromTopic).not.toBe(false);
    if (fromDirect === false || fromTopic === false) {
      // oxlint-disable-next-line unthrown/no-throw -- test setup; the assertions above have already failed
      throw new Error("the broker did not deliver the messages under test");
    }

    amqpChannel.nack(fromDirect, false, false);
    amqpChannel.nack(fromTopic, false, false);

    // THEN the topic DLQ receives it and the `#`-bound direct DLQ does not.
    // Both waits are positive controls: the topic arrival proves dead-lettering
    // ran on the topic side, and `dlq-direct-literal` proves it ran on the
    // DIRECT side too — the same exchange, the same dead letter, delivered
    // under the literal key "order.created". Only with that second count does
    // `dlq-direct === 0` mean "the key did not match" rather than "nothing was
    // dead-lettered here".
    await vi.waitFor(
      async () => {
        expect((await amqpChannel.checkQueue("dlq-topic")).messageCount).toBe(1);
        expect((await amqpChannel.checkQueue("dlq-direct-literal")).messageCount).toBe(1);
      },
      { timeout: 10_000, interval: 50 },
    );
    expect((await amqpChannel.checkQueue("dlq-direct")).messageCount).toBe(0);

    // And it is the message under test that arrived, not a leftover: the
    // literal-key DLQ holds exactly the dead letter, and nothing follows it.
    const viaLiteral = await amqpChannel.get("dlq-direct-literal", { noAck: true });
    expect(viaLiteral).not.toBe(false);
    const viaLiteralBody =
      viaLiteral === false ? undefined : JSON.parse(viaLiteral.content.toString());
    expect(viaLiteralBody).toEqual({ orderId: "measured" });
    expect(await amqpChannel.get("dlq-direct-literal", { noAck: true })).toBe(false);
  }, 20_000);
});
