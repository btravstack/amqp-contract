import { defineExchange, defineQueue, defineQueueBinding } from "@amqp-contract/contract";
import type { ContractDefinition } from "@amqp-contract/contract";
import { it } from "@amqp-contract/testing/extension";
import { beforeEach, describe, expect, vi } from "vitest";
import { AmqpClient } from "../amqp-client.js";

/**
 * Regression test for the per-consumer prefetch handling in
 * `AmqpClient.consume`. The previous implementation just spread `prefetch`
 * into `channel.consume(...)` options where amqplib silently ignores it; the
 * fix routes it through `channel.prefetch(count, false)` registered on the
 * channel wrapper, before the consume runs.
 *
 * The assertion is made on the user-visible behaviour — how many times the
 * handler is invoked — rather than on the RabbitMQ management API, because
 * the management stats are sampled (default ~5s) and can lag actual broker
 * state. That lag was the source of an earlier CI flake.
 */
describe("AmqpClient prefetch integration", () => {
  beforeEach(async () => {
    await AmqpClient._resetConnectionCacheForTesting();
  });

  it("limits handler invocations to the configured prefetch", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    // GIVEN a topology with a single queue we'll consume with prefetch=2
    const exchange = defineExchange("prefetch-x", { durable: false });
    const queue = defineQueue("prefetch-q", { type: "classic", durable: false });
    const contract: ContractDefinition = {
      exchanges: { x: exchange },
      queues: { q: queue },
      bindings: {
        b: defineQueueBinding(queue, exchange, { routingKey: "k" }),
      },
    };

    const client = new AmqpClient(contract, { urls: [amqpConnectionUrl] });
    await client.waitForConnect().getOrThrow();

    // Hold every delivery: never ack, so the broker is forced to honour the
    // per-consumer prefetch cap (it cannot deliver more than `prefetch`
    // unacked messages over the consumer at a time).
    const heldDeliveryTags: number[] = [];
    await client
      .consume(
        "prefetch-q",
        (msg) => {
          if (msg) {
            heldDeliveryTags.push(msg.fields.deliveryTag);
          }
        },
        { prefetch: 2 },
      )
      .getOrThrow();

    // WHEN publishing more messages than the prefetch allows.
    for (let i = 0; i < 10; i++) {
      amqpChannel.publish(exchange.name, "k", Buffer.from(JSON.stringify({ i })), {
        contentType: "application/json",
      });
    }

    // THEN the handler is invoked exactly `prefetch` times: the broker stops
    // delivering once the unack'd window is full. Wait for it to reach the
    // cap…
    await vi.waitFor(
      () => {
        if (heldDeliveryTags.length < 2) {
          throw new Error(`expected handler called ≥ 2 times, got ${heldDeliveryTags.length}`);
        }
      },
      { timeout: 5000 },
    );

    // …then assert it stays there. If prefetch is not enforced, deliveries
    // would continue past 2 within this window.
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(heldDeliveryTags).toHaveLength(2);

    // CLEANUP — close releases unack'd messages back to the queue.
    await client.close().getOrThrow();
  });
});
