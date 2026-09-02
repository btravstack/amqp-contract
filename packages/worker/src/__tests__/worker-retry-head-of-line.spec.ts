import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { ErrAsync, OkAsync } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

import { RetryableError } from "../errors.js";
import { it } from "./fixture.js";

const holDlx = defineExchange("hol-dlx", { durable: false });
// Topic DLX with no dead-letter routing key on the queue, so `#` catches
// whatever key the rejected message arrived with. Without a queue bound here,
// defineContract rejects the contract.
const holDlq = defineQueue("hol-queue-dlq", { type: "classic", durable: false });

describe("TTL-backoff head-of-line blocking", () => {
  it("INVARIANT: a short-delay retry is not blocked by a long-delay retry parked ahead of it", async ({
    workerFactory,
    publishMessage,
    amqpChannel,
  }) => {
    // GIVEN a ttl-backoff queue whose schedule produces two delay tiers:
    // 1000ms (first retry) and 4000ms (second retry). RabbitMQ only
    // dead-letters expired messages at the HEAD of a queue, so with a single
    // shared wait queue a message parked for 4s would block a later message
    // parked for 1s — the short delay would silently degrade to ~4s. With
    // per-delay-tier wait queues each delay has its own head.
    const TestMessage = z.object({ id: z.string() });

    const exchange = defineExchange("hol-exchange", { durable: false });
    const queue = defineQueue("hol-queue", {
      type: "classic",
      durable: false,
      deadLetter: { exchange: holDlx },
      retry: {
        mode: "ttl-backoff",
        maxRetries: 2,
        initialDelayMs: 1000,
        maxDelayMs: 10_000,
        backoffMultiplier: 4,
        jitter: false, // Deterministic delays: tier skew must be zero
      },
    });

    const testMessage = defineMessage(TestMessage);
    const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

    const contract = defineContract({
      publishers: { testPublisher: testEvent },
      consumers: {
        testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
      },
      queues: { holDlq },
      bindings: { holDlqBinding: defineQueueBinding(holDlq, holDlx, { routingKey: "#" }) },
    });

    const deliveries: Array<{ id: string; at: number }> = [];
    let aAttempts = 0;
    let bAttempts = 0;

    await workerFactory(contract, {
      testConsumer: (_, { payload }) => {
        deliveries.push({ id: payload.id, at: Date.now() });
        if (payload.id === "A") {
          aAttempts++;
          // A fails twice: first retry waits in the 1000ms tier, second retry
          // waits in the 4000ms tier.
          return aAttempts <= 2 ? ErrAsync(new RetryableError("A fails")) : OkAsync(undefined);
        }
        bAttempts++;
        // B fails once: a single 1000ms-tier retry.
        return bAttempts <= 1 ? ErrAsync(new RetryableError("B fails")) : OkAsync(undefined);
      },
    });

    // Both delay tiers exist as separate wait queues.
    await amqpChannel.checkQueue("hol-queue-wait-1000ms");
    await amqpChannel.checkQueue("hol-queue-wait-4000ms");

    // WHEN message A is published and burns through its first retry so that
    // its second retry is parked with a LONG (4000ms) delay...
    publishMessage({ exchange: exchange.name, routingKey: "test.message" }, { id: "A" });
    await vi.waitFor(
      () => {
        if (aAttempts < 2) {
          throw new Error("A has not entered its long-delay retry yet");
        }
      },
      { timeout: 4000 },
    );

    // ...and message B is published AFTER A is parked, failing once with a
    // SHORT (1000ms) delay.
    const bPublishedAt = Date.now();
    publishMessage({ exchange: exchange.name, routingKey: "test.message" }, { id: "B" });

    // THEN B is redelivered in roughly its own delay. With a single shared
    // wait queue, B's 1s message would sit behind A's 4s message and only be
    // redelivered after ~4s.
    await vi.waitFor(
      () => {
        if (bAttempts < 2) {
          throw new Error("B has not been redelivered yet");
        }
      },
      { timeout: 8000 },
    );
    const bRedelivery = deliveries.find((d) => d.id === "B" && d.at > bPublishedAt + 500);
    expect(bRedelivery).toBeDefined();
    expect(bRedelivery!.at - bPublishedAt).toBeLessThan(2500);

    // AND A's final redelivery lands only after its full 4s delay — B was
    // redelivered well before A, not behind it.
    await vi.waitFor(
      () => {
        if (aAttempts < 3) {
          throw new Error("A has not completed its long-delay retry yet");
        }
      },
      { timeout: 8000 },
    );
    const aFinalDelivery = deliveries.filter((d) => d.id === "A").at(-1)!;
    expect(bRedelivery!.at).toBeLessThan(aFinalDelivery.at);

    expect(aAttempts).toBe(3);
    expect(bAttempts).toBe(2);
  }, 15_000);
});
