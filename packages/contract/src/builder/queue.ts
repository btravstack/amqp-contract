import type {
  BaseQueueDefinition,
  DeadLetterConfig,
  DefineQueueOptions,
  DefineQueueOptionsWithDeadLetterExchange,
  ExchangeDefinition,
  ImmediateRequeueRetryOptions,
  QueueDefinition,
  QueueEntry,
  QueueEntryWithDeadLetterExchange,
  ResolvedImmediateRequeueRetryOptions,
  ResolvedTtlBackoffRetryOptions,
  TtlBackoffRetryOptions,
} from "../types.js";
import { _internal_assertKnownKeys, _internal_assertNonEmptyName } from "./validate.js";
import { wrapWithTtlBackoffInfrastructure } from "./ttl-backoff.js";

/**
 * Resolve immediate-requeue retry options with defaults.
 * @internal
 */
function resolveImmediateRequeueOptions(
  options: ImmediateRequeueRetryOptions | undefined,
): ResolvedImmediateRequeueRetryOptions {
  return {
    mode: "immediate-requeue",
    maxRetries: options?.maxRetries ?? 3,
  };
}

/**
 * Resolve TTL-backoff retry options with defaults.
 * @internal
 */
function resolveTtlBackoffOptions(
  queueName: string,
  options: TtlBackoffRetryOptions | undefined,
): ResolvedTtlBackoffRetryOptions {
  return {
    mode: "ttl-backoff",
    maxRetries: options?.maxRetries ?? 3,
    initialDelayMs: options?.initialDelayMs ?? 1000,
    maxDelayMs: options?.maxDelayMs ?? 30000,
    backoffMultiplier: options?.backoffMultiplier ?? 2,
    jitter: options?.jitter ?? true,
    waitQueueName: options?.waitQueueName ?? `${queueName}-wait`,
    waitExchangeName: options?.waitExchangeName ?? "wait-exchange",
    retryExchangeName: options?.retryExchangeName ?? "retry-exchange",
  };
}

/**
 * Define an AMQP queue.
 *
 * A queue stores messages until they are consumed by workers. Queues can be bound to exchanges
 * to receive messages based on routing rules.
 *
 * By default, queues are created as quorum queues which provide better durability and
 * high-availability. Use `type: 'classic'` for special cases like non-durable queues
 * or priority queues.
 *
 * @param name - The name of the queue
 * @param options - Optional queue configuration
 * @param options.type - Queue type: 'quorum' (default, recommended) or 'classic'
 * @param options.durable - If true, the queue survives broker restarts. Quorum queues only support durable queues (default: true)
 * @param options.exclusive - If true, the queue can only be used by the declaring connection and is deleted when that connection closes. Only supported with classic queues.
 * @param options.autoDelete - If true, the queue is deleted when the last consumer unsubscribes. Only supported with classic queues.
 * @param options.maxPriority - Maximum priority level for priority queue (1-255, recommended: 1-10). Only supported with classic queues.
 * @param options.deadLetter - Dead letter configuration for handling failed messages
 * @param options.retry - Retry configuration for handling failed message processing
 * @param options.arguments - Additional AMQP arguments (e.g., x-message-ttl)
 * @returns A queue definition
 *
 * @example
 * ```typescript
 * // Quorum queue (default, recommended for production)
 * const orderQueue = defineQueue('order-processing');
 *
 * // Explicit quorum queue with dead letter exchange
 * const dlx = defineExchange('orders-dlx');
 * const orderQueueWithDLX = defineQueue('order-processing', {
 *   type: 'quorum',
 *   deadLetter: {
 *     exchange: dlx,
 *     routingKey: 'order.failed'
 *   },
 *   arguments: {
 *     'x-message-ttl': 86400000, // 24 hours
 *   }
 * });
 *
 * // Classic queue (for special cases)
 * const tempQueue = defineQueue('temp-queue', {
 *   type: 'classic',
 *   durable: false,
 *   autoDelete: true,
 * });
 *
 * // Priority queue (requires classic type)
 * const taskQueue = defineQueue('urgent-tasks', {
 *   type: 'classic',
 *   maxPriority: 10,
 * });
 *
 * // Queue with TTL-backoff retry (returns infrastructure automatically)
 * const dlx = defineExchange('orders-dlx', { type: 'direct' });
 * const orderQueue = defineQueue('order-processing', {
 *   deadLetter: { exchange: dlx },
 *   retry: { mode: 'ttl-backoff', maxRetries: 5 },
 * });
 * // orderQueue is QueueWithTtlBackoffInfrastructure, pass directly to defineContract
 * ```
 */
export function defineQueue<TName extends string, TDlx extends ExchangeDefinition>(
  name: TName,
  options: DefineQueueOptionsWithDeadLetterExchange<TDlx>,
): QueueEntryWithDeadLetterExchange<TName, TDlx>;

export function defineQueue<TName extends string>(
  name: TName,
  options?: DefineQueueOptions,
): QueueEntry<TName>;

export function defineQueue(name: string, options?: DefineQueueOptions): QueueEntry {
  _internal_assertNonEmptyName("Queue", name);
  _internal_assertKnownKeys("queue", name, options, [
    "type",
    "durable",
    "exclusive",
    "autoDelete",
    "maxPriority",
    "deadLetter",
    "retry",
    "arguments",
  ]);
  if (options?.deadLetter !== undefined) {
    _internal_assertKnownKeys("queue deadLetter config of", name, options.deadLetter, [
      "exchange",
      "routingKey",
    ]);
  }
  if (options?.retry !== undefined) {
    _internal_assertKnownKeys("queue retry config of", name, options.retry, [
      "mode",
      "maxRetries",
      "initialDelayMs",
      "maxDelayMs",
      "backoffMultiplier",
      "jitter",
      "waitQueueName",
      "waitExchangeName",
      "retryExchangeName",
    ]);
  }
  const opts = options ?? {};
  const type = opts.type ?? "quorum";
  const durable = opts.durable ?? true;

  // Build base properties shared by both queue types
  const baseProps: {
    name: string;
    deadLetter?: DeadLetterConfig;
    arguments?: Record<string, unknown>;
  } = {
    name,
    ...(opts.deadLetter !== undefined && { deadLetter: opts.deadLetter }),
    ...(opts.arguments !== undefined && { arguments: opts.arguments }),
  };

  // Build specific properties for classic queues
  const classicProps: {
    exclusive?: boolean;
    autoDelete?: boolean;
    maxPriority?: number;
  } = {
    ...(opts.exclusive !== undefined && { exclusive: opts.exclusive }),
    ...(opts.autoDelete !== undefined && { autoDelete: opts.autoDelete }),
    ...(opts.maxPriority !== undefined && { maxPriority: opts.maxPriority }),
  };

  if (type === "quorum") {
    // Quorum queues do not support non-durable, exclusive, autoDelete, or maxPriority
    if (opts.durable === false) {
      throw new Error("Non-durable queues are not supported with quorum type.");
    }
    if (opts.exclusive !== undefined) {
      throw new Error("Exclusive queues are not supported with quorum type.");
    }
    if (opts.autoDelete !== undefined) {
      throw new Error("Auto-deleting queues are not supported with quorum type.");
    }
    if (opts.maxPriority !== undefined) {
      throw new Error("Priority queues are not supported with quorum type.");
    }
  } else {
    // Validate maxPriority
    if (opts.maxPriority !== undefined) {
      if (opts.maxPriority < 1 || opts.maxPriority > 255) {
        throw new Error(
          `Invalid maxPriority: ${opts.maxPriority}. Must be between 1 and 255. Recommended range: 1-10.`,
        );
      }
    }
  }

  const inputRetry = opts.retry ?? { mode: "none" as const };

  // Validate retry requirements
  if (inputRetry.mode === "immediate-requeue" || inputRetry.mode === "ttl-backoff") {
    if (inputRetry.maxRetries !== undefined) {
      if (inputRetry.maxRetries < 1 || !Number.isInteger(inputRetry.maxRetries)) {
        throw new Error(
          `Queue "${name}" uses ${inputRetry.mode} retry mode with invalid maxRetries: ${inputRetry.maxRetries}. Must be a positive integer.`,
        );
      }
    }
  }

  // Resolve retry options with defaults
  const retry =
    inputRetry.mode === "immediate-requeue"
      ? resolveImmediateRequeueOptions(inputRetry)
      : inputRetry.mode === "ttl-backoff"
        ? resolveTtlBackoffOptions(name, inputRetry)
        : inputRetry;

  const baseQueueDefinition: BaseQueueDefinition = {
    ...baseProps,
    retry,
  };

  const queueDefinition: QueueDefinition =
    type === "quorum"
      ? {
          ...baseQueueDefinition,
          type,
          durable: true, // Quorum queues are always durable
        }
      : {
          ...baseQueueDefinition,
          ...classicProps,
          type,
          durable,
        };

  // If TTL-backoff retry, wrap with infrastructure
  if (retry.mode === "ttl-backoff") {
    return wrapWithTtlBackoffInfrastructure(queueDefinition);
  }

  return queueDefinition;
}
