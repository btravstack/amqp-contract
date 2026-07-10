import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import type { TelemetryProvider } from "@amqp-contract/core";
import { Ok } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";
import { TypedAmqpWorker } from "../worker.js";
import { it } from "./fixture.js";

/**
 * Regression test for the audit's issue #4: the worker's defensive `nack` in
 * the consume callback's catch-all used to fire unconditionally, which meant a
 * throw from the telemetry tail (which runs *after* `ack`) would double-act
 * on the same delivery tag — RabbitMQ then closes the channel with 406
 * PRECONDITION_FAILED.
 *
 * The fix tracks `messageHandled` across the dispatch path and refuses to nack
 * once the message has already been ack'd or nack'd.
 */
describe("Worker defensive nack guard", () => {
  it("does not double-act on the delivery tag when telemetry throws after ack", async ({
    amqpConnectionUrl,
    publishMessage,
  }) => {
    // GIVEN a telemetry provider whose consume counter throws synchronously.
    // recordConsumeMetric is called AFTER the success-path ack, so the throw
    // would propagate up to consumeSingle's catch-all in the buggy version.
    const explodingCounter = {
      add: () => {
        throw new Error("simulated telemetry failure");
      },
    };

    const noopHistogram = {
      record: () => {},
    };

    const provider: TelemetryProvider = {
      getTracer: () => undefined,
      getPublishCounter: () => undefined,
      getConsumeCounter: () =>
        explodingCounter as unknown as ReturnType<TelemetryProvider["getConsumeCounter"]>,
      getPublishLatencyHistogram: () => undefined,
      getConsumeLatencyHistogram: () =>
        noopHistogram as unknown as ReturnType<TelemetryProvider["getConsumeLatencyHistogram"]>,
      getLateRpcReplyCounter: () => undefined,
    };

    const TestMessage = z.object({ id: z.string() });

    const exchange = defineExchange("doubleack-x", { durable: false });
    const queue = defineQueue("doubleack-q", { type: "classic", durable: false });
    const testMessage = defineMessage(TestMessage);
    const testEvent = defineEventPublisher(exchange, testMessage, {
      routingKey: "test.message",
    });

    const contract = defineContract({
      publishers: { testPublisher: testEvent },
      consumers: {
        testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
      },
    });

    const processed: string[] = [];

    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: {
        testConsumer: ({ payload }) => {
          processed.push(payload.id);
          return Ok(undefined).toAsync();
        },
      },
      urls: [amqpConnectionUrl],
      telemetry: provider,
    }).unwrapOrElse((e) => {
      throw e;
    });

    try {
      // WHEN we publish two messages back to back. The first triggers the
      // telemetry throw on the success-path tail — in the buggy version
      // this caused `consumeSingle`'s catch-all to nack the same delivery
      // tag the handler had just ack'd, RabbitMQ closed the channel with
      // 406 PRECONDITION_FAILED, and the second message either never
      // arrived or landed back in the queue after a reconnect.
      publishMessage(exchange.name, "test.message", { id: "first" });
      publishMessage(exchange.name, "test.message", { id: "second" });

      // THEN both messages should be processed exactly once each. If the
      // channel had been torn down by a double-act, processing would have
      // either stalled (no second message) or duplicated (after reconnect
      // the message would be re-delivered).
      await vi.waitFor(
        () => {
          if (processed.length < 2) {
            throw new Error(`only processed ${processed.length} messages so far`);
          }
        },
        { timeout: 5000 },
      );

      // Sort because parallel processing on a busy channel can interleave.
      expect([...processed].sort()).toEqual(["first", "second"]);
      // Each id should appear exactly once — no redelivery from a torn-down
      // channel.
      expect(processed.length).toBe(2);
    } finally {
      await worker.close().unwrapOrElse((e) => {
        throw e;
      });
    }
  }, 15_000);
});
