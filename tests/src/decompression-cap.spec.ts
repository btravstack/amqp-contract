import { TypedAmqpClient } from "@amqp-contract/client";
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import type { Logger } from "@amqp-contract/core";
import { it as baseIt } from "@amqp-contract/testing/extension";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { OkAsync } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

/**
 * End-to-end guard for two robustness items:
 *  - the `maxDecompressedBytes` cap threads from CreateWorkerOptions all the
 *    way to the decompress call (a compressed payload exceeding the cap must
 *    not reach the handler), and
 *  - a poison message on a queue with no DLX logs the "will be lost" warning
 *    (observability parity with the retry path's sendToDLQ).
 */
describe("worker decompression cap and poison-message loss warning", () => {
  const Message = z.object({ description: z.string() });
  const exchange = defineExchange("cap-x", { durable: false });
  // No DLX on this queue: a rejected message is dropped, and that must be logged.
  const queue = defineQueue("cap-q", { type: "classic", durable: false, onPoison: "drop" });
  const event = defineEventPublisher(exchange, defineMessage(Message), {
    routingKey: "cap.test",
  });
  const contract = defineContract({
    publishers: { publishThing: event },
    consumers: { consumeThing: defineEventConsumer(event, queue, { routingKey: "cap.#" }) },
  });

  baseIt(
    "drops an over-cap compressed message before the handler and warns about the missing DLX",
    async ({ amqpConnectionUrl }) => {
      const handlerCalls: unknown[] = [];
      const warnings: string[] = [];
      const logger: Logger = {
        debug: () => {},
        info: () => {},
        warn: (message) => warnings.push(message),
        error: () => {},
      };

      const worker = await TypedAmqpWorker.create({
        contract,
        // 64 bytes: any real JSON payload decompresses past this.
        maxDecompressedBytes: 64,
        logger,
        handlers: {
          consumeThing: ({ payload }) => {
            handlerCalls.push(payload);
            return OkAsync();
          },
        },
        urls: [amqpConnectionUrl],
      }).get();

      const client = await TypedAmqpClient.create({
        contract,
        urls: [amqpConnectionUrl],
      }).get();

      try {
        // A payload that comfortably exceeds 64 bytes once decompressed.
        await client
          .publish("publishThing", { description: "x".repeat(500) }, { compression: "gzip" })
          .getOrThrow();

        // Give the broker time to (fail to) deliver and nack.
        await vi.waitFor(
          () => {
            if (warnings.length === 0) throw new Error("no warning logged yet");
          },
          { timeout: 5000 },
        );

        expect(handlerCalls).toHaveLength(0);
        expect(warnings.some((w) => /will be lost/i.test(w))).toBe(true);
      } finally {
        await worker.close().get();
        await client.close().get();
      }
    },
    15_000,
  );
});
