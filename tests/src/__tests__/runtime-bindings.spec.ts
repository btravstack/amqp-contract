import { TypedAmqpClient } from "@amqp-contract/client";
import {
  defineContract,
  defineContractBinding,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
  defineRpc,
} from "@amqp-contract/contract";
import { it } from "@amqp-contract/testing/extension";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { ErrAsync, OkAsync } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

describe("runtime resource bindings", () => {
  it("should isolate two deployments including retries, parking and RPC in one vhost", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    // GIVEN
    const exchange = defineExchange("events");
    const dlx = defineExchange("parked");
    const parked = defineQueue("parked");
    const queue = defineQueue("inbox", {
      deadLetter: { exchange: dlx },
      retry: { mode: "ttl-backoff", maxRetries: 1, initialDelayMs: 100, jitter: false },
    });
    const event = defineEventPublisher(exchange, defineMessage(z.string()), {
      routingKey: "event.v1",
    });
    const protocol = defineContract({
      publishers: { emit: event },
      consumers: { handle: defineEventConsumer(event, queue) },
      queues: { parked },
      bindings: { parked: defineQueueBinding(parked, dlx, { routingKey: "#" }) },
      rpcs: {
        identify: defineRpc(defineQueue("rpc", { onPoison: "drop" }), {
          request: defineMessage(z.string()),
          response: defineMessage(z.string()),
        }),
      },
    });
    const first = defineContractBinding(protocol, {
      exchanges: { events: "a.events", parked: "a.parked" },
      queues: { inbox: "a.inbox", parked: "a.parked", rpc: "a.rpc" },
    });
    const second = defineContractBinding(protocol, {
      exchanges: { events: "b.events", parked: "b.parked" },
      queues: { inbox: "b.inbox", parked: "b.parked", rpc: "b.rpc" },
    });
    const seen: string[][] = [[], []];
    const close: (() => Promise<unknown>)[] = [];
    try {
      const workerA = await TypedAmqpWorker.create({
        contract: first,
        urls: [amqpConnectionUrl],
        handlers: {
          handle: ({ input, retryable }) => {
            seen[0]!.push(input.payload);
            return ErrAsync(retryable("park first deployment"));
          },
          identify: () => OkAsync("a"),
        },
      }).getOrThrow();
      close.push(() => workerA.close().get());
      const workerB = await TypedAmqpWorker.create({
        contract: second,
        urls: [amqpConnectionUrl],
        handlers: {
          handle: ({ input }) => {
            seen[1]!.push(input.payload);
            return OkAsync();
          },
          identify: () => OkAsync("b"),
        },
      }).getOrThrow();
      close.push(() => workerB.close().get());
      const clientA = await TypedAmqpClient.create({
        contract: first,
        urls: [amqpConnectionUrl],
      }).getOrThrow();
      close.push(() => clientA.close().get());
      const clientB = await TypedAmqpClient.create({
        contract: second,
        urls: [amqpConnectionUrl],
      }).getOrThrow();
      close.push(() => clientB.close().get());
      // WHEN
      await clientA.publish("emit", "first").getOrThrow();
      await clientB.publish("emit", "second").getOrThrow();
      const replies = [
        await clientA.call("identify", "request", { timeoutMs: 5000 }).getOrThrow(),
        await clientB.call("identify", "request", { timeoutMs: 5000 }).getOrThrow(),
      ];
      await vi.waitUntil(
        async () =>
          (await amqpChannel.checkQueue("a.parked")).messageCount === 1 && seen[1]!.length === 1,
        { timeout: 10000 },
      );
      // THEN
      expect({
        seen,
        replies,
        parkedB: (await amqpChannel.checkQueue("b.parked")).messageCount,
        retryA: (await amqpChannel.checkQueue("a.inbox-wait-100ms")).queue,
        retryB: (await amqpChannel.checkQueue("b.inbox-wait-100ms")).queue,
      }).toEqual({
        seen: [["first", "first"], ["second"]],
        replies: ["a", "b"],
        parkedB: 0,
        retryA: "a.inbox-wait-100ms",
        retryB: "b.inbox-wait-100ms",
      });
    } finally {
      await Promise.all(close.map((release) => release()));
    }
  });
});
