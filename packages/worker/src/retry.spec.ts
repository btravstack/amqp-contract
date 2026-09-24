import {
  defineExchange,
  defineMessage,
  defineQueue,
  type ResolvedTtlBackoffRetryOptions,
} from "@amqp-contract/contract";
import { PublishError, TechnicalError, type AmqpClient } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { ErrAsync, fromSafeThrowable, OkAsync } from "unthrown";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { NonRetryableError, RetryableError } from "./errors.js";
import { _internalForTesting, decideRetry, handleError } from "./retry.js";

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
      expect(calculateRetryDelay(0, jitterConfig, () => 0)).toBe(500);
    });

    it("multiplies the base delay by ~1.5 at the upper jitter bound", () => {
      // The random source is [0, 1): the multiplier approaches 1.5 but never
      // reaches it. The previous (buggy) formula `0.5 + rand * 0.5` would
      // have produced ~1.0 here.
      const delay = calculateRetryDelay(0, jitterConfig, () => 0.999_999);
      expect([delay > 1400, delay < 1500]).toEqual([true, true]);
    });

    it("never overshoots maxDelayMs even at the upper jitter bound", () => {
      // Base delay 1000 * 2^6 = 64_000, jitter would multiply to ~96_000,
      // but clamp must hold the result at maxDelayMs (60_000).
      expect(calculateRetryDelay(6, jitterConfig, () => 0.999_999)).toBe(jitterConfig.maxDelayMs);
    });

    it("spreads symmetrically over [0.5x, 1.5x), centred near 1.0x (seeded samples)", () => {
      const rand = seededRandom(42);
      const samples = Array.from({ length: 5000 }, () =>
        calculateRetryDelay(0, jitterConfig, rand),
      );
      const mean = samples.reduce((sum, value) => sum + value, 0) / samples.length;

      // initialDelayMs is 1000; the previous one-sided formula had a 0.75x
      // mean and never exceeded 1.0x.
      expect({
        min: Math.min(...samples) >= 500,
        max: Math.max(...samples) < 1500,
        overshoots: Math.max(...samples) > 1000,
        centred: mean > 950 && mean < 1050,
      }).toEqual({ min: true, max: true, overshoots: true, centred: true });
    });
  });
});

/** mulberry32 — a tiny seeded PRNG, so jitter samples are reproducible. */
function seededRandom(seed: number): () => number {
  let state = seed;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
}

describe("decideRetry (pure)", () => {
  const ttl = defineQueue("orders", {
    retry: { mode: "ttl-backoff", maxRetries: 3, initialDelayMs: 1000, jitter: true },
  });

  it("is deterministic for a given random source", () => {
    const decide = () =>
      decideRetry(new RetryableError("x"), ttl, { "x-retry-count": 1 }, seededRandom(7));
    expect(decide()).toEqual(decide());
  });

  it("picks the wait-queue tier from the BASE delay and puts the jittered delay on the copy", () => {
    expect(decideRetry(new RetryableError("x"), ttl, { "x-retry-count": 1 }, () => 0)).toEqual({
      kind: "republish",
      routingKey: "orders-wait-2000ms",
      retryCount: 2,
      delayMs: 1000,
    });
  });

  it.for([
    ["a NonRetryableError", new NonRetryableError("x"), ttl, {}],
    ["no retry config", new RetryableError("x"), defineQueue("orders"), {}],
    ["a spent budget", new RetryableError("x"), ttl, { "x-retry-count": 3 }],
  ] as const)("dead-letters on %s", ([, error, queue, headers]) => {
    expect(decideRetry(error, queue, headers).kind).toBe("dead-letter");
  });

  it("requeues on a quorum immediate-requeue queue and republishes to itself on a classic one", () => {
    const retry = { mode: "immediate-requeue", maxRetries: 3 } as const;
    expect([
      decideRetry(new RetryableError("x"), defineQueue("q", { retry }), { "x-delivery-count": 1 }),
      decideRetry(new RetryableError("x"), defineQueue("c", { type: "classic", retry }), {
        "x-retry-count": 1,
      }),
    ]).toEqual([
      { kind: "requeue", retryCount: 1 },
      { kind: "republish", routingKey: "c", retryCount: 2 },
    ]);
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

  it("requeues the original (nack requeue=true) when the retry publish fails with a PublishError", async () => {
    const { client, ack, nack } = createMockClient(() =>
      ErrAsync(new PublishError({ reason: "timeout", target: 'queue "test-queue"' })),
    );
    const msg = createMockConsumeMessage();

    const result = await publishForRetry(
      { amqpClient: client as unknown as AmqpClient },
      {
        msg,
        exchange: "",
        routingKey: "test-queue",
        queueName: "test-queue",
        error: new Error("boom"),
      },
    );

    expect(result).toBeOk();
    expect(ack).not.toHaveBeenCalled();
    expect(nack).toHaveBeenCalledExactlyOnceWith(msg, { requeue: true, deliveryEpoch: undefined });
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

  it("logs the same hand-off when the DLX comes only from the `arguments` passthrough", async () => {
    // `defineContract` accepts this queue — setupAmqpTopology spreads
    // `queue.arguments` into the declare arguments, so it really does
    // dead-letter. Warning here would page an operator over a queue that is
    // working, on the one line add-logging.md calls a bug report.
    const { client } = createMockClient(() => OkAsync(undefined));
    const logger = loggerSpy();

    await handleError(
      { amqpClient: client as unknown as AmqpClient, logger: logger as never },
      new NonRetryableError("permanent"),
      createMockConsumeMessage(),
      "processOrder",
      {
        queue: defineQueue("orders", { arguments: { "x-dead-letter-exchange": "orders-dlx" } }),
        message: defineMessage(z.object({ id: z.string() })),
      },
    );

    expect(logger.info).toHaveBeenCalledWith(
      "Sending message to DLQ",
      expect.objectContaining({ queueName: "orders" }),
    );
    expect(logger.warn).not.toHaveBeenCalled();
    // And it must not claim the message was discarded — the broker has it.
    expect(logger.info).not.toHaveBeenCalledWith(
      expect.stringContaining("Discarding"),
      expect.anything(),
    );
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

  it("keeps the warning when the queue carries neither a DLX nor onPoison", async () => {
    // defineContract rejects this queue, so it can only arrive via a
    // hand-built ContractDefinition that bypassed the guard — an undeclared
    // loss, which is exactly what the warning is for.
    const { client } = createMockClient(() => OkAsync(undefined));
    const logger = loggerSpy();

    await handleError(
      { amqpClient: client as unknown as AmqpClient, logger: logger as never },
      new NonRetryableError("permanent"),
      createMockConsumeMessage(),
      "processOrder",
      {
        queue: defineQueue("orders"),
        message: defineMessage(z.object({ id: z.string() })),
      },
    );

    expect(logger.warn).toHaveBeenCalledWith(
      "Queue has no dead-letter exchange and no onPoison declaration - message will be lost on nack",
      expect.objectContaining({ queueName: "orders" }),
    );
    // It must NOT claim a declaration the queue does not carry.
    expect(logger.info).not.toHaveBeenCalledWith(
      expect.stringContaining("onPoison"),
      expect.anything(),
    );
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
