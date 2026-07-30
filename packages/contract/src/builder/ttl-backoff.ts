import type {
  QueueDefinition,
  ResolvedTtlBackoffRetryOptions,
  TtlBackoffInfrastructure,
  TtlBackoffWaitQueueDefinition,
} from "../types.js";

/**
 * Base (pre-jitter) backoff delay for a given retry attempt:
 * `min(initialDelayMs * backoffMultiplier ^ retryCount, maxDelayMs)`.
 *
 * `retryCount` is zero-based — the delay applied before retry attempt
 * `retryCount + 1`.
 *
 * @param retry - Resolved TTL-backoff retry options
 * @param retryCount - Number of retries already attempted (0 for the first retry)
 * @returns The base delay in milliseconds
 */
export function ttlBackoffBaseDelay(
  retry: ResolvedTtlBackoffRetryOptions,
  retryCount: number,
): number {
  return Math.floor(
    Math.min(
      retry.initialDelayMs * Math.pow(retry.backoffMultiplier, retryCount),
      retry.maxDelayMs,
    ),
  );
}

/**
 * Broker name of the wait queue for a delay tier: `{queueName}-wait-{delayMs}ms`.
 *
 * @param queueName - The main queue name
 * @param delayMs - The tier's base delay in milliseconds
 * @returns The wait queue name
 */
export function ttlBackoffWaitQueueName(queueName: string, delayMs: number): string {
  return `${queueName}-wait-${delayMs}ms`;
}

/**
 * Derive the TTL-backoff retry infrastructure for a queue: one wait queue per
 * distinct backoff delay in the retry schedule.
 *
 * The infrastructure is **derived, not stored** — `defineQueue` returns a
 * plain `QueueDefinition` and the contract output contains only the queues
 * you declared. `setupAmqpTopology` calls this helper at channel-setup time
 * to declare the wait queues, and the worker's retry pipeline uses it to
 * publish the retry copy to the tier queue matching the attempt's base delay.
 *
 * **How the retry hop works:**
 * 1. The worker publishes the failed message to the tier's wait queue via the
 *    default exchange (routing key = wait queue name), with a per-message
 *    `expiration` carrying the (jittered) delay.
 * 2. The wait queue is declared with `x-message-ttl` set to the tier's jitter
 *    ceiling as a backstop, and dead-letters to the default exchange with
 *    `x-dead-letter-routing-key` set to the main queue name.
 * 3. When the TTL expires, RabbitMQ routes the message straight back to the
 *    main queue.
 *
 * Because every message in a tier shares the same base delay, a long-delay
 * retry can never block a short-delay retry: head-of-line skew within a tier
 * is bounded by the jitter spread (at most `delayMs`), and is zero when
 * jitter is disabled.
 *
 * @param queue - The main queue definition
 * @returns The derived infrastructure, or `undefined` when the queue's retry
 *   mode is not `ttl-backoff`
 *
 * @example
 * ```typescript
 * const queue = defineQueue('order-processing', {
 *   retry: { mode: 'ttl-backoff', maxRetries: 3, initialDelayMs: 1000 },
 * });
 * const infra = deriveTtlBackoffInfrastructure(queue);
 * // infra.waitQueues → [
 * //   { name: 'order-processing-wait-1000ms', delayMs: 1000, messageTtlMs: 1500 },
 * //   { name: 'order-processing-wait-2000ms', delayMs: 2000, messageTtlMs: 3000 },
 * //   { name: 'order-processing-wait-4000ms', delayMs: 4000, messageTtlMs: 6000 },
 * // ]
 * ```
 */
export function deriveTtlBackoffInfrastructure(
  queue: QueueDefinition,
): TtlBackoffInfrastructure | undefined {
  const retry = queue.retry;
  if (retry.mode !== "ttl-backoff") {
    return undefined;
  }

  // Distinct base delays across the whole retry budget, ascending. Delays
  // repeat once the maxDelayMs cap engages; dedupe to one tier per delay.
  const delays = [
    ...new Set(Array.from({ length: retry.maxRetries }, (_, k) => ttlBackoffBaseDelay(retry, k))),
  ];

  const waitQueues: TtlBackoffWaitQueueDefinition[] = delays.map((delayMs) => ({
    name: ttlBackoffWaitQueueName(queue.name, delayMs),
    delayMs,
    // Jitter spreads the per-message expiration over [0.5x, 1.5x] of the base
    // delay; the queue-level TTL backstop must cover the jitter ceiling so no
    // message outlives its tier. Without jitter the expiration is exactly the
    // base delay.
    messageTtlMs: retry.jitter ? Math.ceil(delayMs * 1.5) : delayMs,
  }));

  return {
    queueName: queue.name,
    queueType: queue.type,
    durable: queue.durable,
    waitQueues,
  };
}
