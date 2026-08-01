import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import { OkAsync, fromSafePromise } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

import { TypedAmqpWorker } from "../worker.js";
import { it } from "./fixture.js";

/**
 * Guards the graceful-shutdown invariant: `close()` cancels the consumers,
 * then DRAINS in-flight handlers before closing the channel — so a handler
 * that completes during shutdown gets its ack onto the still-open channel and
 * the message is never redelivered. Before the fix, `close()` tore the channel
 * down under the in-flight handler: the ack was dropped (or threw, cascading
 * into an unhandled rejection from the defensive nack) and the broker
 * redelivered fully-processed work.
 */
const drainDlx = defineExchange("drain-dlx", { durable: false });

describe("Worker close drains in-flight handlers", () => {
  it("waits for an in-flight handler and lands its ack before closing", async ({
    amqpConnectionUrl,
    publishMessage,
  }) => {
    const exchange = defineExchange("drain-x", { durable: false });
    const queue = defineQueue("drain-q", {
      type: "classic",
      durable: false,
      deadLetter: { exchange: drainDlx },
    });
    const message = defineMessage(z.object({ id: z.string() }));
    const event = defineEventPublisher(exchange, message, { routingKey: "drain.test" });
    const contract = defineContract({
      publishers: { drainPublisher: event },
      consumers: { drainConsumer: defineEventConsumer(event, queue, { routingKey: "drain.#" }) },
    });

    let handlerStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      handlerStarted = resolve;
    });
    let releaseHandler!: () => void;
    const release = new Promise<void>((resolve) => {
      releaseHandler = resolve;
    });
    let handlerCompleted = false;

    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: {
        drainConsumer: () => {
          handlerStarted();
          return fromSafePromise(release).tap(() => {
            handlerCompleted = true;
          });
        },
      },
      urls: [amqpConnectionUrl],
    }).get();

    // WHEN a message is mid-handler as close() begins
    publishMessage({ exchange: exchange.name, routingKey: "drain.test" }, { id: "in-flight" });
    await started;

    const closing = worker.close().get();
    // Give close() time to cancel the consumer, then let the handler finish.
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(handlerCompleted).toBe(false);
    releaseHandler();
    await closing;

    // THEN close() waited for the handler
    expect(handlerCompleted).toBe(true);

    // AND the ack landed: a fresh worker on the same queue sees no redelivery.
    const redelivered = vi.fn().mockReturnValue(OkAsync(undefined));
    const secondWorker = await TypedAmqpWorker.create({
      contract,
      handlers: { drainConsumer: redelivered },
      urls: [amqpConnectionUrl],
    }).get();
    try {
      await new Promise((resolve) => setTimeout(resolve, 1_000));
      expect(redelivered).not.toHaveBeenCalled();
    } finally {
      await secondWorker.close().get();
    }
  }, 15_000);
});
