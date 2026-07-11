import type { ResolvedTtlBackoffRetryOptions } from "@amqp-contract/contract";
import type { AmqpClient } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { ErrAsync, OkAsync } from "unthrown";
import { afterEach, describe, expect, it, vi } from "vitest";
import { _internalForTesting } from "./retry.js";

const { calculateRetryDelay, publishForRetry } = _internalForTesting;

describe("calculateRetryDelay", () => {
  const baseConfig: ResolvedTtlBackoffRetryOptions = {
    mode: "ttl-backoff",
    maxRetries: 5,
    initialDelayMs: 1000,
    maxDelayMs: 60_000,
    backoffMultiplier: 2,
    jitter: false,
    waitQueueName: "q-wait",
    waitExchangeName: "x-wait",
    retryExchangeName: "x-retry",
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
    const { client, ack, publish } = createMockClient(() => OkAsync(true));
    const callOrder: string[] = [];
    (client.ack as ReturnType<typeof vi.fn>).mockImplementation(() => callOrder.push("ack"));
    (client.publish as ReturnType<typeof vi.fn>).mockImplementation(() => {
      callOrder.push("publish");
      return OkAsync(true);
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

  it("does NOT ack the original when publish reports the channel buffer is full", async () => {
    const { client, ack, nack, publish } = createMockClient(() => OkAsync(false));

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

    expect(result).toBeErr();
    expect(publish).toHaveBeenCalledTimes(1);
    // The whole point of the fix: the original message must remain un-ack'd
    // so amqp-connection-manager / the broker can redeliver it instead of
    // losing it forever.
    expect(ack).not.toHaveBeenCalled();
    expect(nack).not.toHaveBeenCalled();
  });

  it("does NOT ack the original when publish itself rejects", async () => {
    const { client, ack, nack, publish } = createMockClient(() =>
      ErrAsync(new Error("publish exploded") as never),
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
        waitQueueName: "test-queue-wait",
        error: new Error("boom"),
      },
    );

    expect(result).toBeErr();
    expect(publish).toHaveBeenCalledTimes(1);
    expect(ack).not.toHaveBeenCalled();
    expect(nack).not.toHaveBeenCalled();
  });

  it("propagates retry headers and increments x-retry-count on publish", async () => {
    const { client, publish } = createMockClient(() => OkAsync(true));

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
        waitQueueName: "test-queue-wait",
        error: new Error("third failure"),
      },
    );

    expect(publish).toHaveBeenCalledWith(
      "retry-x",
      "test.key",
      expect.anything(),
      expect.objectContaining({
        expiration: "750",
        headers: expect.objectContaining({
          "x-retry-count": 3,
          "x-last-error": "third failure",
          "x-first-failure-timestamp": 1234,
          "x-wait-queue": "test-queue-wait",
          "x-retry-queue": "test-queue",
        }),
      }),
    );
  });
});
