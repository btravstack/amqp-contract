import type {
  BaseQueueDefinition,
  QueueDefinition,
  QueueEntry,
  QueueWithTtlBackoffInfrastructure,
  TtlBackoffRetryInfrastructure,
} from "../types.js";
import { defineQueueBindingInternal } from "./binding.js";
import { defineExchange } from "./exchange.js";

/**
 * Type guard to check if a queue entry is a QueueWithTtlBackoffInfrastructure.
 *
 * When you configure a queue with TTL-backoff retry,
 * `defineQueue` returns a `QueueWithTtlBackoffInfrastructure` instead of a plain
 * `QueueDefinition`. This type guard helps you distinguish between the two.
 *
 * **When to use:**
 * - When you need to check the type of a queue entry at runtime
 * - When writing generic code that handles both plain queues and infrastructure wrappers
 *
 * **Related functions:**
 * - `extractQueue()` - Use this to get the underlying queue definition from either type
 *
 * @param entry - The queue entry to check
 * @returns True if the entry is a QueueWithTtlBackoffInfrastructure, false otherwise
 *
 * @example
 * ```typescript
 * const queue = defineQueue('orders', {
 *   retry: { mode: 'ttl-backoff' },
 * });
 *
 * if (isQueueWithTtlBackoffInfrastructure(queue)) {
 *   // queue has .queue, .waitQueue, .waitQueueBinding, .retryQueueBinding, .waitExchange, .retryExchange
 *   console.log('Wait queue:', queue.waitQueue.name);
 * } else {
 *   // queue is a plain QueueDefinition
 *   console.log('Queue:', queue.name);
 * }
 * ```
 */
export function isQueueWithTtlBackoffInfrastructure(
  entry: QueueEntry,
): entry is QueueWithTtlBackoffInfrastructure {
  return (
    typeof entry === "object" &&
    entry !== null &&
    "__brand" in entry &&
    entry.__brand === "QueueWithTtlBackoffInfrastructure"
  );
}

/**
 * Wrap a queue definition with TTL-backoff retry infrastructure.
 */
export function wrapWithTtlBackoffInfrastructure(
  queue: QueueDefinition,
): QueueWithTtlBackoffInfrastructure {
  const infra = createTtlBackoffInfrastructure(queue);

  return {
    __brand: "QueueWithTtlBackoffInfrastructure",
    queue,
    ...infra,
  };
}

/**
 * Create TTL-backoff retry infrastructure for a queue.
 *
 * This builder helper generates the wait queue, exchanges, and bindings needed for TTL-backoff retry.
 * The generated infrastructure can be spread into a contract definition.
 *
 * TTL-backoff retry works by:
 * 1. Failed messages are sent to the wait exchange with header `x-wait-queue` set to the wait queue name
 * 2. The wait queue receives these messages and holds them for a TTL period
 * 3. After TTL expires, messages are dead-lettered back to the retry exchange with header `x-retry-queue` set to the main queue name
 * 4. The main queue receives the retried message via its binding to the retry exchange
 *
 * @param queue - The main queue definition
 * @param options - Optional configuration for the wait queue
 * @returns TTL-backoff retry infrastructure containing wait queue and bindings
 * @throws {Error} If the queue does not have retry mode set to `ttl-backoff`
 *
 * @example
 * ```typescript
 * const orderQueue = defineQueue('order-processing', {
 *   type: 'quorum',
 *   retry: {
 *     mode: 'ttl-backoff',
 *     maxRetries: 5,
 *     initialDelayMs: 1000,
 *   },
 * });
 *
 * // Infrastructure is auto-extracted when using defineContract:
 * const contract = defineContract({
 *   publishers: { ... },
 *   consumers: { processOrder: defineEventConsumer(event, orderQueue) },
 * });
 * // contract.queues includes the wait queue, contract.exchanges includes retry exchanges, contract.bindings includes retry bindings
 * ```
 */
export function createTtlBackoffInfrastructure(
  queue: QueueDefinition,
): TtlBackoffRetryInfrastructure {
  // Ensure queue retry mode is ttl-backoff
  if (queue.retry.mode !== "ttl-backoff") {
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
    throw new Error(
      `Queue ${queue.name} does not have ttl-backoff retry mode. Infrastructure can only be created for queues with ttl-backoff retry.`,
    );
  }

  // Create wait exchange (headers exchange) for routing failed messages to the wait queue
  const waitExchange = defineExchange(queue.retry.waitExchangeName, {
    type: "headers",
  });

  // Create retry exchange (headers exchange) for routing messages to retry back to main queue
  const retryExchange = defineExchange(queue.retry.retryExchangeName, {
    type: "headers",
  });

  // Create the wait queue (of same type as main queue)
  const baseWaitQueue: BaseQueueDefinition = {
    name: queue.retry.waitQueueName,
    deadLetter: {
      exchange: retryExchange, // Routes back to retry exchange after TTL (will preserve original message routing key)
    },
    retry: { mode: "none" }, // No retry for wait queue itself
  };

  const waitQueue: QueueDefinition =
    queue.type === "quorum"
      ? {
          ...baseWaitQueue,
          type: queue.type,
          durable: true, // Quorum queues are always durable
        }
      : {
          ...baseWaitQueue,
          type: queue.type,
          durable: queue.durable,
        };

  // Create binding for wait queue to receive failed messages
  const waitQueueBinding = defineQueueBindingInternal(waitQueue, waitExchange, {
    arguments: {
      "x-match": "all",
      "x-wait-queue": waitQueue.name, // Custom header to specify the wait queue to which messages should be routed
    },
  });

  // Create binding for main queue to receive messages to retry
  const retryQueueBinding = defineQueueBindingInternal(queue, retryExchange, {
    arguments: {
      "x-match": "all",
      "x-retry-queue": queue.name, // Custom header to specify the retry queue to which messages should be routed
    },
  });

  return {
    waitQueue,
    waitExchange,
    retryExchange,
    waitQueueBinding,
    retryQueueBinding,
  };
}
