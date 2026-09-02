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
 *  - a poison message on a queue declared `onPoison: "drop"` is still recorded
 *    in the logs before it is discarded (observability parity with the retry
 *    path's sendToDLQ). Since `defineContract` rejects any other consumed
 *    queue without a DLX, this line is now an `info` about intended behaviour
 *    rather than a `warn` about a misconfiguration — but it must still exist,
 *    because it is the only record the message ever arrived.
 */
describe("worker decompression cap and poison-message drop logging", () => {
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
    "drops an over-cap compressed message before the handler and records the discard",
    async ({ amqpConnectionUrl }) => {
      const handlerCalls: unknown[] = [];
      const infos: string[] = [];
      const warnings: string[] = [];
      const logger: Logger = {
        debug: () => {},
        info: (message) => infos.push(message),
        warn: (message) => warnings.push(message),
        error: () => {},
      };

      const worker = await TypedAmqpWorker.create({
        contract,
        // 64 bytes: any real JSON payload decompresses past this.
        maxDecompressedBytes: 64,
        logger,
        handlers: {
          consumeThing: (_, { payload }) => {
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
            if (!infos.some((m) => /onPoison/.test(m))) {
              throw new Error("no discard logged yet");
            }
          },
          { timeout: 5000 },
        );

        expect(handlerCalls).toHaveLength(0);
        expect(
          infos.some((m) =>
            /Discarding poison message: queue is declared onPoison: "drop" and has no DLX/.test(m),
          ),
        ).toBe(true);
        // The drop is declared configuration, so it must NOT raise a warning —
        // a warn on correct config is how real warnings get ignored.
        expect(warnings.some((w) => /DLX|lost|poison/i.test(w))).toBe(false);
      } finally {
        await worker.close().get();
        await client.close().get();
      }
    },
    15_000,
  );
});
