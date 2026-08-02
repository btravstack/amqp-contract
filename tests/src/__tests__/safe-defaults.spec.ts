import { TypedAmqpClient } from "@amqp-contract/client";
import {
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  definePublisher,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { DEFAULT_PREFETCH } from "@amqp-contract/core";
import { it } from "@amqp-contract/testing/extension";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { fromPromise } from "unthrown";
import { describe, expect } from "vitest";
import { z } from "zod";

/**
 * H3, proven against a real broker.
 *
 * Test 1 shows the hazard is genuine: with the opt-out, one consumer takes the
 * entire backlog unacked. Test 2 shows the default bounds it.
 */
describe("default prefetch", () => {
  /**
   * Deliberately larger than {@link DEFAULT_PREFETCH}, so the bounded run has
   * a backlog left to refuse and the pair cannot pass vacuously.
   */
  const MESSAGE_COUNT = 30;

  /**
   * Drives a real TypedAmqpWorker so this proves OUR default is applied, not
   * merely that RabbitMQ's basic.qos works. Asserting through `amqpChannel`
   * directly would pass even if the default were never wired.
   *
   * The handler blocks until released, so every delivered message stays
   * unacked and the in-flight count is exactly what prefetch allows.
   *
   * `prefetch` is `undefined` for the default case: nothing is passed to the
   * worker at all, so the bound observed there can only come from the library.
   */
  const message = defineMessage(z.object({ i: z.number() }));

  async function inFlightUnderPrefetch(
    amqpConnectionUrl: string,
    label: string,
    prefetch?: number | "unbounded",
  ): Promise<number> {
    const exchange = defineExchange(`prefetch-x-${label}`, { type: "topic", durable: false });
    const dlx = defineExchange(`prefetch-dlx-${label}`, { type: "topic", durable: false });
    const queue = defineQueue(`prefetch-q-${label}`, {
      type: "classic",
      durable: false,
      deadLetter: { exchange: dlx },
    });
    // A DLX with nothing bound discards every message routed to it, so declare
    // the dead-letter queue and bind it on the key the DLX will see.
    const dlq = defineQueue(`prefetch-q-${label}-dlq`, { type: "classic", durable: false });
    const contract = defineContract({
      publishers: { emit: definePublisher(exchange, message, { routingKey: "p.one" }) },
      consumers: { onOne: defineConsumer(queue, message) },
      queues: { dlq },
      bindings: {
        onOne: defineQueueBinding(queue, exchange, { routingKey: "p.one" }),
        dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "#" }),
      },
    });

    let inFlight = 0;
    let peak = 0;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });

    const client = await TypedAmqpClient.create({ contract, urls: [amqpConnectionUrl] }).get();
    const worker = await TypedAmqpWorker.create({
      contract,
      urls: [amqpConnectionUrl],
      ...(prefetch === undefined ? {} : { defaultConsumerOptions: { prefetch } }),
      handlers: {
        onOne: () =>
          fromPromise(
            (async () => {
              inFlight += 1;
              peak = Math.max(peak, inFlight);
              await gate;
              inFlight -= 1;
            })(),
            (cause, defect) => defect(cause),
          ),
      },
    }).get();

    try {
      for (let i = 0; i < MESSAGE_COUNT; i += 1) {
        await client.publish("emit", { i }).getOrThrow();
      }
      // Wait for the broker to stop pushing rather than sleeping a fixed
      // interval: the peak is read once it has held steady, which is the
      // moment the consumer has taken everything prefetch permits. Handlers
      // never complete (the gate is still shut), so a steady peak means the
      // broker has nothing more it is willing to deliver.
      await waitForDeliveriesToSettle(() => peak);
      return peak;
    } finally {
      release();
      await worker.close().get();
      await client.close().get();
    }
  }

  /**
   * Polls until the observed peak has not moved for `quietForMs`, or the
   * deadline expires. Returns either way — the caller asserts on the peak, so
   * a timeout surfaces as a failed expectation rather than an opaque throw.
   */
  async function waitForDeliveriesToSettle(
    readPeak: () => number,
    { quietForMs = 400, timeoutMs = 10_000, pollMs = 25 } = {},
  ): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    let last = -1;
    let lastChangedAt = Date.now();

    while (Date.now() < deadline) {
      const current = readPeak();
      if (current !== last) {
        last = current;
        lastChangedAt = Date.now();
      } else if (current > 0 && Date.now() - lastChangedAt >= quietForMs) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
  }

  it("INVARIANT: an unbounded consumer takes far more than the default in flight", async ({
    amqpConnectionUrl,
  }) => {
    const peak = await inFlightUnderPrefetch(amqpConnectionUrl, "unbounded", "unbounded");
    expect(peak).toBeGreaterThan(DEFAULT_PREFETCH);
    // Pinned, not merely "more than 10": with no bound the consumer takes the
    // whole backlog into its own memory, every message of it unacked.
    expect(peak).toBe(MESSAGE_COUNT);
  }, 20_000);

  it("INVARIANT: the default bounds in-flight handlers to DEFAULT_PREFETCH", async ({
    amqpConnectionUrl,
  }) => {
    // No prefetch passed anywhere — not to the worker, not per handler. The
    // bound observed here is the library's default doing it.
    const peak = await inFlightUnderPrefetch(amqpConnectionUrl, "default");
    expect(peak).toBe(DEFAULT_PREFETCH);
  }, 20_000);
});

describe("poison-loss guard", () => {
  const message = defineMessage(z.object({ orderId: z.string() }));

  it("INVARIANT: a consumed DLX-less queue is rejected at define time", () => {
    const orders = defineExchange("orders-poison", { type: "topic" });
    const queue = defineQueue("order-processing-poison");

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
    ).toThrow(/dead-letter exchange/);
  });
});
