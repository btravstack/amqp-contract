/**
 * Load-bearing invariants, each guarded by a named test (org DNA — unthrown's
 * `invariants.spec.ts` pattern). Invariants whose natural guard lives in
 * another suite are listed in AGENTS.md ("Load-bearing invariants") with a
 * pointer instead of a duplicate here; this file adds direct unit guards for
 * the error-routing decisions of `handleError`.
 */
import { defineMessage, defineQueue } from "@amqp-contract/contract";
import type { AmqpClient } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { OkAsync } from "unthrown";
import { describe, expect, it, vi } from "vitest";
import { z } from "zod";
import { NonRetryableError, RetryableError } from "./errors.js";
import { handleError } from "./retry.js";

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
  const publish = vi.fn(() => OkAsync(true));
  return { client: { publish, ack, nack } as never, ack, nack, publish };
}

const message = defineMessage(z.object({ id: z.string() }));

describe("invariants: handler-error routing", () => {
  it("INVARIANT: a NonRetryableError is nacked exactly once with requeue=false (DLQ), never published or acked", async () => {
    const { client, ack, nack, publish } = mockClient();
    const consumer = { queue: defineQueue("orders"), message };

    const result = await handleError(
      { amqpClient: client as never },
      new NonRetryableError("permanent"),
      mockMessage(),
      "processOrder",
      consumer,
    );

    expect(result).toBeOk();
    expect(nack).toHaveBeenCalledTimes(1);
    expect(nack).toHaveBeenCalledWith(expect.anything(), false, false);
    expect(publish).not.toHaveBeenCalled();
    expect(ack).not.toHaveBeenCalled();
  });

  it("INVARIANT: a RetryableError on a queue without retry config routes to DLQ (nack requeue=false), not an infinite requeue", async () => {
    const { client, nack, publish } = mockClient();
    const consumer = { queue: defineQueue("orders"), message };

    const result = await handleError(
      { amqpClient: client as never },
      new RetryableError("transient"),
      mockMessage(),
      "processOrder",
      consumer,
    );

    expect(result).toBeOk();
    expect(nack).toHaveBeenCalledTimes(1);
    expect(nack).toHaveBeenCalledWith(expect.anything(), false, false);
    expect(publish).not.toHaveBeenCalled();
  });

  it("INVARIANT: immediate-requeue retries below the budget requeue (requeue=true); at the budget they DLQ (requeue=false)", async () => {
    const consumer = {
      queue: defineQueue("orders", { retry: { mode: "immediate-requeue", maxRetries: 2 } }),
      message,
    };

    // Below the budget: broker-side redelivery via nack(requeue=true).
    const below = mockClient();
    await handleError(
      { amqpClient: below.client as never },
      new RetryableError("transient"),
      mockMessage({ "x-delivery-count": 1 }),
      "processOrder",
      consumer,
    ).getOrThrow();
    expect(below.nack).toHaveBeenCalledWith(expect.anything(), false, true);

    // At the budget: permanent failure, DLQ.
    const at = mockClient();
    await handleError(
      { amqpClient: at.client as never },
      new RetryableError("transient"),
      mockMessage({ "x-delivery-count": 2 }),
      "processOrder",
      consumer,
    ).getOrThrow();
    expect(at.nack).toHaveBeenCalledWith(expect.anything(), false, false);
  });
});
