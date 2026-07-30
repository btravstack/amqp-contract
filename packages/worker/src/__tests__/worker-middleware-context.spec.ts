import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import { OkAsync } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

import type { AnyWorkerMiddleware, WorkerMiddleware } from "../middleware.js";
import { TypedAmqpWorker } from "../worker.js";
import { it } from "./fixture.js";

/**
 * Regression test: the dispatch terminal must MERGE a middleware's injected
 * context over the `createContext` seed, exactly as `composeMiddleware` does.
 * The buggy version replaced the seed (`opts?.context ?? seedContext`) when a
 * single middleware was passed un-composed, so `middleware: mw` and
 * `middleware: [mw]` disagreed: the bare form silently dropped every field the
 * seed provided.
 */
describe("Worker middleware context merge", () => {
  const TestMessage = z.object({ id: z.string() });

  function buildContract(exchangeName: string, queueName: string) {
    const exchange = defineExchange(exchangeName, { durable: false });
    const queue = defineQueue(queueName, { type: "classic", durable: false });
    const testEvent = defineEventPublisher(exchange, defineMessage(TestMessage), {
      routingKey: "test.message",
    });
    return {
      exchange,
      contract: defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
        },
      }),
    };
  }

  type Seed = { fromSeed: string };
  type Enriched = Seed & { fromMiddleware: string };

  const injectingMiddleware: WorkerMiddleware<Seed, Enriched> = (_args, next) =>
    next({ context: { fromMiddleware: "mw" } as Enriched });

  it("INVARIANT: a bare middleware's injected context merges over the createContext seed (middleware: mw ≡ middleware: [mw])", async ({
    amqpConnectionUrl,
    publishMessage,
  }) => {
    const seen: Record<string, unknown>[] = [];

    const { exchange, contract } = buildContract("mwctx-bare-x", "mwctx-bare-q");
    const worker = await TypedAmqpWorker.create({
      contract,
      createContext: () => ({ fromSeed: "seed" }),
      middleware: injectingMiddleware,
      handlers: {
        testConsumer: (_message, _raw, { context }) => {
          seen.push(context);
          return OkAsync();
        },
      },
      urls: [amqpConnectionUrl],
    }).get();

    try {
      publishMessage({ exchange: exchange.name, routingKey: "test.message" }, { id: "bare" });
      await vi.waitFor(
        () => {
          if (seen.length === 0) throw new Error("message not processed yet");
        },
        { timeout: 5000 },
      );

      // The seed field must survive alongside the middleware's injection.
      expect(seen[0]).toEqual({ fromSeed: "seed", fromMiddleware: "mw" });
    } finally {
      await worker.close().get();
    }
  }, 15_000);

  it("array-form middleware merges identically (parity contract)", async ({
    amqpConnectionUrl,
    publishMessage,
  }) => {
    const seen: Record<string, unknown>[] = [];

    const { exchange, contract } = buildContract("mwctx-array-x", "mwctx-array-q");
    const worker = await TypedAmqpWorker.create({
      contract,
      createContext: () => ({ fromSeed: "seed" }),
      // The array form erases stepwise context typing (documented limitation),
      // so a typed middleware needs the AnyWorkerMiddleware cast here.
      middleware: [injectingMiddleware as AnyWorkerMiddleware],
      handlers: {
        testConsumer: (_message, _raw, { context }) => {
          seen.push(context);
          return OkAsync();
        },
      },
      urls: [amqpConnectionUrl],
    }).get();

    try {
      publishMessage({ exchange: exchange.name, routingKey: "test.message" }, { id: "array" });
      await vi.waitFor(
        () => {
          if (seen.length === 0) throw new Error("message not processed yet");
        },
        { timeout: 5000 },
      );

      expect(seen[0]).toEqual({ fromSeed: "seed", fromMiddleware: "mw" });
    } finally {
      await worker.close().get();
    }
  }, 15_000);
});
