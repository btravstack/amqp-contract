import { describe, expect, it } from "vitest";

import { defineQueue } from "./queue.js";
import {
  deriveTtlBackoffInfrastructure,
  ttlBackoffBaseDelay,
  ttlBackoffWaitQueueName,
} from "./ttl-backoff.js";

describe("ttlBackoffBaseDelay", () => {
  const retry = {
    mode: "ttl-backoff" as const,
    maxRetries: 10,
    initialDelayMs: 1000,
    maxDelayMs: 30000,
    backoffMultiplier: 2,
    jitter: false,
  };

  it("grows exponentially from initialDelayMs", () => {
    expect(ttlBackoffBaseDelay(retry, 0)).toBe(1000);
    expect(ttlBackoffBaseDelay(retry, 1)).toBe(2000);
    expect(ttlBackoffBaseDelay(retry, 2)).toBe(4000);
    expect(ttlBackoffBaseDelay(retry, 3)).toBe(8000);
  });

  it("caps at maxDelayMs", () => {
    expect(ttlBackoffBaseDelay(retry, 5)).toBe(30000); // 32000 capped
    expect(ttlBackoffBaseDelay(retry, 9)).toBe(30000);
  });
});

describe("ttlBackoffWaitQueueName", () => {
  it("derives the per-tier wait queue name from queue name and delay", () => {
    expect(ttlBackoffWaitQueueName("orders", 1000)).toBe("orders-wait-1000ms");
  });
});

describe("deriveTtlBackoffInfrastructure", () => {
  it("returns undefined for a queue without ttl-backoff retry", () => {
    expect(deriveTtlBackoffInfrastructure(defineQueue("plain"))).toBeUndefined();
    expect(
      deriveTtlBackoffInfrastructure(
        defineQueue("requeue", { retry: { mode: "immediate-requeue" } }),
      ),
    ).toBeUndefined();
  });

  it("derives one wait queue per distinct backoff delay, ascending", () => {
    const queue = defineQueue("orders", {
      retry: {
        mode: "ttl-backoff",
        maxRetries: 3,
        initialDelayMs: 1000,
        backoffMultiplier: 2,
        jitter: false,
      },
    });

    expect(deriveTtlBackoffInfrastructure(queue)).toEqual({
      queueName: "orders",
      queueType: "quorum",
      durable: true,
      waitQueues: [
        { name: "orders-wait-1000ms", delayMs: 1000, messageTtlMs: 1000 },
        { name: "orders-wait-2000ms", delayMs: 2000, messageTtlMs: 2000 },
        { name: "orders-wait-4000ms", delayMs: 4000, messageTtlMs: 4000 },
      ],
    });
  });

  it("dedupes delays once the maxDelayMs cap engages", () => {
    const queue = defineQueue("orders", {
      retry: {
        mode: "ttl-backoff",
        maxRetries: 6,
        initialDelayMs: 1000,
        maxDelayMs: 4000,
        backoffMultiplier: 2,
        jitter: false,
      },
    });

    const infra = deriveTtlBackoffInfrastructure(queue);
    // Delays: 1000, 2000, 4000, 4000, 4000, 4000 → three distinct tiers.
    expect(infra?.waitQueues.map((w) => w.delayMs)).toEqual([1000, 2000, 4000]);
  });

  it("sets the queue-level TTL backstop to the jitter ceiling when jitter is enabled", () => {
    const queue = defineQueue("orders", {
      retry: {
        mode: "ttl-backoff",
        maxRetries: 2,
        initialDelayMs: 1001,
        backoffMultiplier: 2,
        jitter: true,
      },
    });

    const infra = deriveTtlBackoffInfrastructure(queue);
    // ceil(1001 * 1.5) = 1502; ceil(2002 * 1.5) = 3003.
    expect(infra?.waitQueues).toEqual([
      { name: "orders-wait-1001ms", delayMs: 1001, messageTtlMs: 1502 },
      { name: "orders-wait-2002ms", delayMs: 2002, messageTtlMs: 3003 },
    ]);
  });

  it("mirrors the main queue's type and durability", () => {
    const queue = defineQueue("orders", {
      type: "classic",
      durable: false,
      retry: { mode: "ttl-backoff", maxRetries: 1, initialDelayMs: 500 },
    });

    expect(deriveTtlBackoffInfrastructure(queue)).toMatchObject({
      queueType: "classic",
      durable: false,
    });
  });
});
