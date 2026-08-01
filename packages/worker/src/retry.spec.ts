import {
  defineExchange,
  defineMessage,
  defineQueue,
  type ResolvedTtlBackoffRetryOptions,
} from "@amqp-contract/contract";
import { TechnicalError, type AmqpClient } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { fromSafeThrowable, OkAsync } from "unthrown";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { NonRetryableError, RetryableError } from "./errors.js";
import { _internalForTesting, handleError } from "./retry.js";

const { calculateRetryDelay, publishForRetry } = _internalForTesting;

describe("calculateRetryDelay", () => {
  const baseConfig: ResolvedTtlBackoffRetryOptions = {
    mode: "ttl-backoff",
    maxRetries: 5,
    initialDelayMs: 1000,
    maxDelayMs: 60_000,
    backoffMultiplier: 2,
    jitter: false,
  };

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("returns the exponential delay when jitter is disabled", () => {
    expect(calculateRetryDelay(0, baseConfig)).toBe(1000);
    expect(calculateRetryDelay(1, baseConfig)).toBe(2000);
    expect(calculateRetryDelay(2, baseConfig)).toBe(4000);
    expect(calculateRetryDelay(3, baseConfig)).toBe(8000);
  });

  it("clamps the delay to maxDelayMs without jitter", () => {
    // 1000 * 2^7 = 128_000, clamped to 60_000.
    expect(calculateRetryDelay(7, baseConfig)).toBe(60_000);
  });

  describe("jitter distribution", () => {
    const jitterConfig: ResolvedTtlBackoffRetryOptions = {
      ...baseConfig,
      jitter: true,
    };

    it("multiplies the base delay by 0.5 at the lower jitter bound", () => {
      // Math.random() === 0  →  multiplier = 0.5 + 0 = 0.5
      vi.spyOn(Math, "random").mockReturnValue(0);
      // Use a base delay well below maxDelayMs so the clamp doesn't engage.
      expect(calculateRetryDelay(0, jitterConfig)).toBe(500);
    });

    it("multiplies the base delay by ~1.5 at the upper jitter bound", () => {
      // Math.random() returns [0, 1) — the supremum is just under 1, so the
      // multiplier approaches 1.5 but never quite reaches it. Use a value
      // very close to 1 to assert the upper end of the jitter range. The
      // previous (buggy) formula `0.5 + Math.random() * 0.5` would have
      // produced ~1.0 here — never above 1.0 — so this assertion fails on
      // the old code.
      vi.spyOn(Math, "random").mockReturnValue(0.999_999);
      // initialDelayMs * (0.5 + 0.999_999) ≈ 1000 * 1.499_999 ≈ 1499 (floored)
      const delay = calculateRetryDelay(0, jitterConfig);
      expect(delay).toBeGreaterThan(1400);
      expect(delay).toBeLessThan(1500);
    });

    it("never overshoots maxDelayMs even at the upper jitter bound", () => {
      // Base delay 1000 * 2^6 = 64_000, jitter would multiply to ~96_000,
      // but clamp must hold the result at maxDelayMs (60_000).
      vi.spyOn(Math, "random").mockReturnValue(0.999_999);
      expect(calculateRetryDelay(6, jitterConfig)).toBeLessThanOrEqual(jitterConfig.maxDelayMs);
    });

    it("produces a symmetric distribution centred near 1.0x over many samples", () => {
      // Real, unmocked Math.random — sample enough to assert the empirical
      // mean is near 1.0x of the base delay (within a few percent), which
      // would not hold for the previous one-sided 0.75x-mean formula.
      const samples = 5000;
      let sum = 0;
      for (let i = 0; i < samples; i++) {
        sum += calculateRetryDelay(0, jitterConfig);
      }
      const mean = sum / samples;
      // initialDelayMs of jitterConfig is 1000 — assert mean is near 1.0x.
      expect(mean).toBeGreaterThan(900);
      expect(mean).toBeLessThan(1100);
    });

    it("produces values in the [0.5x, 1.5x] range over many samples", () => {
      const samples = 1000;
      const base = 1000; // initialDelayMs of jitterConfig
      let min = Infinity;
      let max = -Infinity;
      for (let i = 0; i < samples; i++) {
        const value = calculateRetryDelay(0, jitterConfig);
        if (value < min) min = value;
        if (value > max) max = value;
      }
      // Lower bound is exactly 0.5x (when Math.random() === 0).
      expect(min).toBeGreaterThanOrEqual(base * 0.5);
      // Upper bound approaches 1.5x but stays strictly below it.
      expect(max).toBeLessThan(base * 1.5);
      // The previous (broken) formula capped at 1.0x; assert we exceed that.
      expect(max).toBeGreaterThan(base * 1.0);
    });
  });
});

// Helpers for publishForRetry tests
function createMockConsumeMessage(overrides: Partial<ConsumeMessage> = {}): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify({ id: "msg-1" })),
    fields: {
      consumerTag: "test-consumer-tag",
      deliveryTag: 42,
      redelivered: false,
      exchange: "test-exchange",
      routingKey: "test.key",
      ...overrides.fields,
    },
    properties: {
      contentType: "application/json",
      contentEncoding: undefined,
      headers: {},
      deliveryMode: undefined,
      priority: undefined,
      correlationId: undefined,
      replyTo: undefined,
      expiration: undefined,
      messageId: undefined,
      timestamp: undefined,
      type: undefined,
      userId: undefined,
      appId: undefined,
      clusterId: undefined,
      ...overrides.properties,
    },
  } as ConsumeMessage;
}

type MockAmqpClient = Pick<AmqpClient, "publish" | "ack" | "nack">;

function createMockClient(publishImpl: () => ReturnType<AmqpClient["publish"]>): {
  client: MockAmqpClient;
  ack: ReturnType<typeof vi.fn>;
  nack: ReturnType<typeof vi.fn>;
  publish: ReturnType<typeof vi.fn>;
} {
  const ack = vi.fn();
  const nack = vi.fn();
  const publish = vi.fn(publishImpl);
  return {
    client: { publish, ack, nack } as unknown as MockAmqpClient,
    ack,
    nack,
    publish,
  };
}

describe("publishForRetry", () => {
  it("acks the original message only AFTER a successful retry publish", async () => {
    const { client, ack, publish } = createMockClient(() => OkAsync(undefined));
    const callOrder: string[] = [];
    (client.ack as ReturnType<typeof vi.fn>).mockImplementation(() => callOrder.push("ack"));
    (client.publish as ReturnType<typeof vi.fn>).mockImplementation(() => {
      callOrder.push("publish");
      return OkAsync(undefined);
    });

    const msg = createMockConsumeMessage();

    const result = await publishForRetry(
      { amqpClient: client as unknown as AmqpClient },
      {
        msg,
        exchange: "retry-x",
        routingKey: "test.key",
        queueName: "test-queue",
        error: new Error("boom"),
      },
    );

    expect(result).toBeOk();
    expect(publish).toHaveBeenCalledTimes(1);
    expect(ack).toHaveBeenCalledTimes(1);
    // Critical ordering: publish must complete before ack runs.
    expect(callOrder).toEqual(["publish", "ack"]);
  });

  it("does NOT ack the original when publish surfaces a full write buffer (core-level Defect)", async () => {
    // Since the buffer-full unification, AmqpClient.publish absorbs the
    // channel wrapper's boolean and surfaces a full write buffer as a Defect
    // with a TechnicalError cause — mirror exactly that shape here.
    const { client, ack, nack, publish } = createMockClient(() =>
      fromSafeThrowable((): void => {
        throw new TechnicalError(
          'Failed to publish message to queue "test-queue": channel write buffer full',
        );
      })().toAsync(),
    );

    const msg = createMockConsumeMessage();

    const result = await publishForRetry(
      { amqpClient: client as unknown as AmqpClient },
      {
        msg,
        exchange: "retry-x",
        routingKey: "test.key",
        queueName: "test-queue",
        error: new Error("boom"),
      },
    );

    // A full write buffer is an unexpected publish failure — it surfaces as a
    // Defect (with a TechnicalError cause) from the core layer, not a
    // modeled Err.
    expect(result).toBeDefect();
    expect(publish).toHaveBeenCalledTimes(1);
    // The whole point of the fix: the original message must remain un-ack'd
    // so amqp-connection-manager / the broker can redeliver it instead of
    // losing it forever.
    expect(ack).not.toHaveBeenCalled();
    expect(nack).not.toHaveBeenCalled();
  });

  it("does NOT ack the original when publish itself rejects", async () => {
    // `amqpClient.publish` routes every rejection to the defect channel (with a
    // `TechnicalError` cause), so the retry publish surfaces a Defect here.
    const { client, ack, nack, publish } = createMockClient(() =>
      fromSafeThrowable((): void => {
        throw new TechnicalError("publish exploded");
      })().toAsync(),
    );

    const msg = createMockConsumeMessage();

    const result = await publishForRetry(
      { amqpClient: client as unknown as AmqpClient },
      {
        msg,
        exchange: "retry-x",
        routingKey: "test.key",
        queueName: "test-queue",
        delayMs: 500,
        error: new Error("boom"),
      },
    );

    expect(result).toBeDefect();
    expect(publish).toHaveBeenCalledTimes(1);
    expect(ack).not.toHaveBeenCalled();
    expect(nack).not.toHaveBeenCalled();
  });

  it("propagates retry headers and increments x-retry-count on publish", async () => {
    const { client, publish } = createMockClient(() => OkAsync(undefined));

    const msg = createMockConsumeMessage({
      properties: {
        contentType: "application/json",
        headers: {
          "x-retry-count": 2,
          "x-first-failure-timestamp": 1234,
        },
      } as unknown as ConsumeMessage["properties"],
    });

    await publishForRetry(
      { amqpClient: client as unknown as AmqpClient },
      {
        msg,
        exchange: "retry-x",
        routingKey: "test.key",
        queueName: "test-queue",
        delayMs: 750,
        error: new Error("third failure"),
      },
    );

    expect(publish).toHaveBeenCalledWith(
      { exchange: "retry-x", routingKey: "test.key" },
      expect.anything(),
      expect.objectContaining({
        expiration: "750",
        headers: expect.objectContaining({
          "x-retry-count": 3,
          "x-last-error": "third failure",
          "x-first-failure-timestamp": 1234,
          "x-original-routing-key": "test.key",
        }),
      }),
    );
  });
});

describe("terminal-nack logging", () => {
  // The wording matters: since defineContract rejects a consumed queue with
  // neither a DLX nor `onPoison: "drop"`, the no-DLX branch is reachable ONLY
  // on a queue whose author declared the drop. It must therefore read as a
  // recorded fact at `info`, not as a warning about a misconfiguration.
  function loggerSpy(): { info: ReturnType<typeof vi.fn>; warn: ReturnType<typeof vi.fn> } {
    return { info: vi.fn(), warn: vi.fn() };
  }

  it("logs a DLQ hand-off at info when the queue has a dead-letter exchange", async () => {
    const { client } = createMockClient(() => OkAsync(undefined));
    const logger = loggerSpy();
    const dlx = defineExchange("orders-dlx");

    await handleError(
      { amqpClient: client as unknown as AmqpClient, logger: logger as never },
      new NonRetryableError("permanent"),
      createMockConsumeMessage(),
      "processOrder",
      {
        queue: defineQueue("orders", { deadLetter: { exchange: dlx } }),
        message: defineMessage(z.object({ id: z.string() })),
      },
    );

    expect(logger.info).toHaveBeenCalledWith("Sending message to DLQ", expect.anything());
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it('logs a declared discard at info — never warn — when the queue is onPoison: "drop"', async () => {
    const { client } = createMockClient(() => OkAsync(undefined));
    const logger = loggerSpy();

    await handleError(
      { amqpClient: client as unknown as AmqpClient, logger: logger as never },
      new NonRetryableError("permanent"),
      createMockConsumeMessage(),
      "processOrder",
      {
        queue: defineQueue("orders", { onPoison: "drop" }),
        message: defineMessage(z.object({ id: z.string() })),
      },
    );

    expect(logger.info).toHaveBeenCalledWith(
      'Discarding message: queue is declared onPoison: "drop" and has no DLX',
      expect.objectContaining({ queueName: "orders" }),
    );
    // A deliberate configuration must not raise an operational warning.
    expect(logger.warn).not.toHaveBeenCalled();
  });
});

describe("delivery-epoch stamping (reconnect-safe settles)", () => {
  // Delivery tags are per-channel: an ack/nack that lands after a reconnect
  // must carry the epoch captured at delivery time so AmqpClient can refuse
  // to settle a foreign tag on the new channel (guarded core-side by
  // packages/core/src/channel-epoch.spec.ts). These tests pin the worker's
  // half of the contract: every settle in the retry pipeline is stamped.
  it("INVARIANT: the post-retry-publish ack carries the delivery epoch", async () => {
    const { client, ack } = createMockClient(() => OkAsync(undefined));
    const msg = createMockConsumeMessage();

    await publishForRetry(
      { amqpClient: client as unknown as AmqpClient, deliveryEpoch: 7 },
      {
        msg,
        exchange: "retry-x",
        routingKey: "test.key",
        queueName: "test-queue",
        error: new Error("boom"),
      },
    );

    expect(ack).toHaveBeenCalledWith(msg, { deliveryEpoch: 7 });
  });

  it("INVARIANT: DLQ and requeue nacks carry the delivery epoch", async () => {
    const consumer = {
      queue: defineQueue("orders"),
      message: defineMessage(z.object({ id: z.string() })),
    };

    // No retry config → DLQ nack.
    const dlq = createMockClient(() => OkAsync(undefined));
    await handleError(
      { amqpClient: dlq.client as unknown as AmqpClient, deliveryEpoch: 3 },
      new NonRetryableError("permanent"),
      createMockConsumeMessage(),
      "processOrder",
      consumer,
    );
    expect(dlq.nack).toHaveBeenCalledWith(expect.anything(), { requeue: false, deliveryEpoch: 3 });

    // Immediate-requeue below budget → requeue nack.
    const requeue = createMockClient(() => OkAsync(undefined));
    await handleError(
      { amqpClient: requeue.client as unknown as AmqpClient, deliveryEpoch: 4 },
      new RetryableError("transient"),
      createMockConsumeMessage({
        properties: {
          headers: { "x-delivery-count": 0 },
          contentType: "application/json",
        } as never,
      }),
      "processOrder",
      {
        ...consumer,
        queue: defineQueue("orders", { retry: { mode: "immediate-requeue", maxRetries: 2 } }),
      },
    );
    expect(requeue.nack).toHaveBeenCalledWith(expect.anything(), {
      requeue: true,
      deliveryEpoch: 4,
    });
  });
});
