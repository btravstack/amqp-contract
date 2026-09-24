/**
 * Load-bearing invariants, each guarded by a named test (org DNA — unthrown's
 * `invariants.spec.ts` pattern). Invariants whose natural guard lives in
 * another suite are listed in AGENTS.md ("Load-bearing invariants") with a
 * pointer instead of a duplicate here; this file adds direct unit guards for
 * the error-routing decisions of `handleError` (settled through `settle`, as
 * the dispatcher does).
 */
import {
  defineMessage,
  defineQueue,
  deriveTtlBackoffInfrastructure,
} from "@amqp-contract/contract";
import type { AmqpClient } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { OkAsync } from "unthrown";
import { describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { NonRetryableError, RetryableError } from "./errors.js";
import { settle } from "./outcome.js";
import { decideRetry, handleError, MAX_LAST_ERROR_LENGTH } from "./retry.js";

function mockMessage(headers: Record<string, unknown> = {}): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify({ id: "1" })),
    fields: {
      consumerTag: "tag",
      deliveryTag: 1,
      redelivered: false,
      exchange: "x",
      routingKey: "k",
    },
    properties: { headers, contentType: "application/json" },
  } as unknown as ConsumeMessage;
}

function mockClient(): {
  client: Pick<AmqpClient, "publish" | "ack" | "nack">;
  ack: ReturnType<typeof vi.fn>;
  nack: ReturnType<typeof vi.fn>;
  publish: ReturnType<typeof vi.fn>;
} {
  const ack = vi.fn();
  const nack = vi.fn();
  const publish = vi.fn(() => OkAsync(undefined));
  return { client: { publish, ack, nack } as never, ack, nack, publish };
}

const message = defineMessage(z.object({ id: z.string() }));

/** `handleError`, then `settle` its outcome — exactly what the dispatcher does with a handler failure. */
function routeAndSettle(...args: Parameters<typeof handleError>): ReturnType<typeof handleError> {
  const [ctx, , msg] = args;
  return handleError(...args).tap((outcome) =>
    settle(ctx.amqpClient, msg, outcome, undefined, undefined),
  );
}

describe("invariants: handler-error routing", () => {
  it("INVARIANT: a NonRetryableError is nacked exactly once with requeue=false (DLQ), never published or acked", async () => {
    const { client, ack, nack, publish } = mockClient();
    const consumer = { queue: defineQueue("orders"), message };

    const result = await routeAndSettle(
      { amqpClient: client as never },
      new NonRetryableError("permanent"),
      mockMessage(),
      "processOrder",
      consumer,
    );

    expect(result).toBeOk();
    expect(nack).toHaveBeenCalledTimes(1);
    expect(nack).toHaveBeenCalledWith(expect.anything(), {
      requeue: false,
      deliveryEpoch: undefined,
    });
    expect(publish).not.toHaveBeenCalled();
    expect(ack).not.toHaveBeenCalled();
  });

  it("INVARIANT: a RetryableError on a queue without retry config routes to DLQ (nack requeue=false), not an infinite requeue", async () => {
    const { client, nack, publish } = mockClient();
    const consumer = { queue: defineQueue("orders"), message };

    const result = await routeAndSettle(
      { amqpClient: client as never },
      new RetryableError("transient"),
      mockMessage(),
      "processOrder",
      consumer,
    );

    expect(result).toBeOk();
    expect(nack).toHaveBeenCalledTimes(1);
    expect(nack).toHaveBeenCalledWith(expect.anything(), {
      requeue: false,
      deliveryEpoch: undefined,
    });
    expect(publish).not.toHaveBeenCalled();
  });

  it("INVARIANT: immediate-requeue retries below the budget requeue (requeue=true); at the budget they DLQ (requeue=false)", async () => {
    const consumer = {
      queue: defineQueue("orders", { retry: { mode: "immediate-requeue", maxRetries: 2 } }),
      message,
    };

    // Below the budget: broker-side redelivery via nack(requeue=true).
    const below = mockClient();
    await routeAndSettle(
      { amqpClient: below.client as never },
      new RetryableError("transient"),
      mockMessage({ "x-delivery-count": 1 }),
      "processOrder",
      consumer,
    ).get();
    expect(below.nack).toHaveBeenCalledWith(expect.anything(), {
      requeue: true,
      deliveryEpoch: undefined,
    });

    // At the budget: permanent failure, DLQ.
    const at = mockClient();
    await routeAndSettle(
      { amqpClient: at.client as never },
      new RetryableError("transient"),
      mockMessage({ "x-delivery-count": 2 }),
      "processOrder",
      consumer,
    ).get();
    expect(at.nack).toHaveBeenCalledWith(expect.anything(), {
      requeue: false,
      deliveryEpoch: undefined,
    });
  });

  it("INVARIANT: a classic-queue immediate-requeue retry republishes to THIS queue via the default exchange, never the original exchange", async () => {
    // Republishing to the original exchange would fan the retry out to every
    // bound queue — sibling consumers would process duplicates and inherit the
    // retry's `x-retry-count` header into their own accounting.
    const { client, publish } = mockClient();
    const consumer = {
      queue: defineQueue("orders", {
        type: "classic",
        retry: { mode: "immediate-requeue", maxRetries: 2 },
      }),
      message,
    };

    const result = await routeAndSettle(
      { amqpClient: client as never },
      new RetryableError("transient"),
      mockMessage({ "x-retry-count": 0 }),
      "processOrder",
      consumer,
    );

    expect(result).toBeOk();
    expect(publish).toHaveBeenCalledTimes(1);
    expect(publish).toHaveBeenCalledWith(
      { exchange: "", routingKey: "orders" },
      expect.anything(),
      expect.anything(),
    );
  });

  const malformedCounts = [
    ["a string", "abc"],
    ["NaN", Number.NaN],
    ["a negative", -5],
    ["a fraction", 1.5],
    ["a table", { nested: 1 }],
    ["an unsafe integer", 2 ** 60],
  ] as const;

  it.for(malformedCounts)(
    "INVARIANT: a malformed x-retry-count (%s) counts as 0 — the classic immediate-requeue budget still holds",
    async ([, forged]) => {
      const { client, publish } = mockClient();
      const consumer = {
        queue: defineQueue("orders", {
          type: "classic",
          retry: { mode: "immediate-requeue", maxRetries: 2 },
        }),
        message,
      };

      await routeAndSettle(
        { amqpClient: client as never },
        new RetryableError("transient"),
        mockMessage({ "x-retry-count": forged }),
        "processOrder",
        consumer,
      ).get();

      // The copy restarts the count at 1 — never `"abc1"`, which would never
      // reach the budget and loop forever.
      expect(publish.mock.calls[0]?.[2]).toMatchObject({ headers: { "x-retry-count": 1 } });
    },
  );

  it.for(malformedCounts)(
    "INVARIANT: a malformed x-retry-count (%s) never computes an undeclared ttl-backoff wait-queue tier",
    ([, forged]) => {
      const queue = defineQueue("orders", {
        retry: { mode: "ttl-backoff", maxRetries: 3, initialDelayMs: 1000 },
      });
      const declared = deriveTtlBackoffInfrastructure(queue)!.waitQueues.map((w) => w.name);

      const action = decideRetry(new RetryableError("transient"), queue, {
        "x-retry-count": forged,
      });

      expect(action).toMatchObject({ kind: "republish", routingKey: declared[0] });
    },
  );

  it("INVARIANT: malformed x-first-failure-timestamp / x-original-routing-key are replaced, never propagated", async () => {
    const { client, publish } = mockClient();
    const consumer = {
      queue: defineQueue("orders", { retry: { mode: "ttl-backoff", maxRetries: 3 } }),
      message,
    };

    await routeAndSettle(
      { amqpClient: client as never },
      new RetryableError("transient"),
      mockMessage({ "x-first-failure-timestamp": "yesterday", "x-original-routing-key": 42 }),
      "processOrder",
      consumer,
    ).get();

    expect(publish.mock.calls[0]?.[2]).toMatchObject({
      headers: { "x-first-failure-timestamp": expect.any(Number), "x-original-routing-key": "k" },
    });
  });

  it("INVARIANT: the x-last-error header is bounded (a huge handler error cannot exceed frame_max)", async () => {
    const { client, publish } = mockClient();
    const consumer = {
      queue: defineQueue("orders", { retry: { mode: "ttl-backoff", maxRetries: 3 } }),
      message,
    };

    await routeAndSettle(
      { amqpClient: client as never },
      new RetryableError("x".repeat(1_000_000)),
      mockMessage(),
      "processOrder",
      consumer,
    ).get();

    expect(publish.mock.calls[0]?.[2]).toMatchObject({
      headers: { "x-last-error": "x".repeat(MAX_LAST_ERROR_LENGTH) },
    });
  });
});
