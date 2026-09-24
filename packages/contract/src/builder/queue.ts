import type {
  BaseQueueDefinition,
  DeadLetterConfig,
  DefineQueueOptions,
  DefineQueueOptionsWithDeadLetterExchange,
  ExchangeDefinition,
  ImmediateRequeueRetryOptions,
  QueueDefinition,
  QueueDefinitionWithDeadLetterExchange,
  ResolvedImmediateRequeueRetryOptions,
  ResolvedTtlBackoffRetryOptions,
  TtlBackoffRetryOptions,
} from "../types.js";
import { _internal_assertKnownKeys, _internal_assertNonEmptyName } from "./validate.js";

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
  options: TtlBackoffRetryOptions | undefined,
): ResolvedTtlBackoffRetryOptions {
  return {
    mode: "ttl-backoff",
    maxRetries: options?.maxRetries ?? 3,
    initialDelayMs: options?.initialDelayMs ?? 1000,
    maxDelayMs: options?.maxDelayMs ?? 30000,
    backoffMultiplier: options?.backoffMultiplier ?? 2,
    jitter: options?.jitter ?? true,
  };
}

/**
 * Align a quorum queue's broker-side redelivery cap with immediate-requeue retry.
 *
 * Immediate-requeue on a quorum queue is `nack(requeue: true)`, and the worker
 * dead-letters once `x-delivery-count` reaches `maxRetries`. RabbitMQ 4.x also
 * caps redeliveries itself: `x-delivery-limit` defaults to 20, past which the
 * broker dead-letters (or drops) the message on its own. With `maxRetries >= 20`
 * the broker would win, the worker's budget would silently never be reached,
 * and its "max retries exceeded" log and DLQ hand-off would never happen.
 * Setting the limit one above `maxRetries` keeps the worker the one deciding.
 *
 * An explicit `x-delivery-limit` is kept; one too low to let the worker finish
 * is rejected. A negative value is RabbitMQ's "unlimited" and always passes.
 * (A `delivery-limit` applied by broker policy is invisible here.)
 */
function withQuorumDeliveryLimit(
  name: string,
  args: Record<string, unknown> | undefined,
  maxRetries: number,
): Record<string, unknown> {
  const required = maxRetries + 1;
  const explicit = args?.["x-delivery-limit"];
  if (explicit === undefined) {
    return { ...args, "x-delivery-limit": required };
  }
  if (typeof explicit === "number" && explicit >= 0 && explicit < required) {
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
    throw new Error(
      `Queue "${name}": arguments["x-delivery-limit"] is ${explicit}, but its immediate-requeue ` +
        `retry needs at least ${required} (maxRetries ${maxRetries} + 1). RabbitMQ would dead-letter ` +
        `the message after ${explicit} redeliveries, before the worker's retry budget runs out. ` +
        `Remove the argument (it defaults to ${required} for this queue), raise it to at least ` +
        `${required}, or lower maxRetries.`,
    );
  }
  return { ...args };
}

/**
 * Define an AMQP queue.
 *
 * A queue stores messages until they are consumed by workers. Queues can be bound to exchanges
 * to receive messages based on routing rules.
 *
 * By default, queues are created as quorum queues which provide better durability and
 * high-availability. Use `type: 'classic'` for special cases like non-durable queues
 * or `x-max-priority` priority levels.
 *
 * @param name - The name of the queue
 * @param options - Optional queue configuration
 * @param options.type - Queue type: 'quorum' (default, recommended) or 'classic'
 * @param options.durable - If true, the queue survives broker restarts. Quorum queues only support durable queues (default: true)
 * @param options.exclusive - If true, the queue can only be used by the declaring connection and is deleted when that connection closes. Only supported with classic queues.
 * @param options.autoDelete - If true, the queue is deleted when the last consumer unsubscribes. Only supported with classic queues.
 * @param options.maxPriority - Maximum priority level for priority queue (1-255, recommended: 1-10). Only supported with classic queues: quorum queues ignore `x-max-priority` and honor the per-message `priority` property natively on RabbitMQ 4.0+.
 * @param options.deadLetter - Dead letter configuration for handling failed messages
 * @param options.onPoison - Set to 'drop' to declare that poison messages on this queue are deliberately discarded. `defineContract` requires either this or `deadLetter` on any queue it sees consumed.
 * @param options.retry - Retry configuration for handling failed message processing. On a quorum queue, immediate-requeue retry also sets `arguments["x-delivery-limit"]` to `maxRetries + 1` (see {@link ImmediateRequeueRetryOptions})
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
 * // Classic priority levels (x-max-priority). A quorum queue needs none of
 * // this: it honors the per-message `priority` natively on RabbitMQ 4.0+.
 * const taskQueue = defineQueue('urgent-tasks', {
 *   type: 'classic',
 *   maxPriority: 10,
 * });
 *
 * // Queue with TTL-backoff retry (wait queues are derived at setup time)
 * const retryDlx = defineExchange('payments-dlx', { type: 'direct' });
 * const paymentQueue = defineQueue('payment-processing', {
 *   deadLetter: { exchange: retryDlx },
 *   retry: { mode: 'ttl-backoff', maxRetries: 5 },
 * });
 * // paymentQueue is a plain QueueDefinition; setupAmqpTopology declares the
 * // per-delay wait queues derived from its retry config
 * ```
 */
export function defineQueue<TName extends string, TDlx extends ExchangeDefinition>(
  name: TName,
  options: DefineQueueOptionsWithDeadLetterExchange<TDlx>,
): QueueDefinitionWithDeadLetterExchange<TName, TDlx>;

export function defineQueue<TName extends string>(
  name: TName,
  options?: DefineQueueOptions,
): QueueDefinition<TName>;

export function defineQueue(name: string, options?: DefineQueueOptions): QueueDefinition {
  _internal_assertNonEmptyName("Queue", name);
  _internal_assertKnownKeys("queue", name, options, [
    "type",
    "durable",
    "exclusive",
    "autoDelete",
    "maxPriority",
    "deadLetter",
    "onPoison",
    "retry",
    "arguments",
  ]);
  if (options?.deadLetter !== undefined) {
    _internal_assertKnownKeys("queue deadLetter config of", name, options.deadLetter, [
      "exchange",
      "routingKey",
      "externalConsumers",
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
    ]);
  }
  const opts = options ?? {};
  const type = opts.type ?? "quorum";
  const durable = opts.durable ?? true;

  // Build base properties shared by both queue types
  const baseProps: {
    name: string;
    deadLetter?: DeadLetterConfig;
    onPoison?: "drop";
    arguments?: Record<string, unknown>;
  } = {
    name,
    ...(opts.deadLetter !== undefined && { deadLetter: opts.deadLetter }),
    ...(opts.onPoison !== undefined && { onPoison: opts.onPoison }),
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
    // Quorum queues do not support non-durable, exclusive, autoDelete, or maxPriority.
    // The default type is quorum, so the remedy must say so: an author who never
    // wrote `type` does not know which type rejected the option.
    const quorumRejects = (option: string, why: string, remedy = "Set"): Error =>
      new Error(
        `Queue "${name}": ${option} is not supported on quorum queues (the default type)${why}. ` +
          `${remedy} \`type: "classic"\` on this queue.`,
      );
    if (opts.durable === false) {
      // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
      throw quorumRejects("durable: false", " — quorum queues are always durable");
    }
    if (opts.exclusive !== undefined) {
      // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
      throw quorumRejects("exclusive", "");
    }
    if (opts.autoDelete !== undefined) {
      // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
      throw quorumRejects("autoDelete", "");
    }
    if (opts.maxPriority !== undefined) {
      // Quorum queues DO prioritise messages (RabbitMQ 4.0+), but with no
      // queue argument: `x-max-priority` is classic-only and silently ignored
      // on a quorum queue, so accepting it would be a no-op option.
      // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
      throw quorumRejects(
        "maxPriority",
        " — `x-max-priority` is a classic-queue argument that quorum queues ignore",
        "Quorum queues already honor the per-message `priority` property natively on " +
          "RabbitMQ 4.0+ (normal vs high above 4 on 4.0–4.2; 32 strict levels on 4.3+): remove " +
          "maxPriority to keep this a quorum queue, or for classic priority levels set",
      );
    }
  } else if (
    opts.maxPriority !== undefined &&
    (!Number.isInteger(opts.maxPriority) || opts.maxPriority < 1 || opts.maxPriority > 255)
  ) {
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
    throw new Error(
      `Queue "${name}": maxPriority must be an integer between 1 and 255 (got ${opts.maxPriority}). ` +
        `Use 1-10: each level costs broker memory and CPU.`,
    );
  }

  const inputRetry = opts.retry ?? { mode: "none" as const };

  // Validate retry requirements
  if (inputRetry.mode === "immediate-requeue" || inputRetry.mode === "ttl-backoff") {
    if (inputRetry.maxRetries !== undefined) {
      if (inputRetry.maxRetries < 1 || !Number.isInteger(inputRetry.maxRetries)) {
        // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
        throw new Error(
          `Queue "${name}" uses ${inputRetry.mode} retry mode with invalid maxRetries: ${inputRetry.maxRetries}. ` +
            `Must be a positive integer — omit maxRetries for the default (3), or use \`mode: "none"\` to disable retries.`,
        );
      }
    }
  }

  // Resolve retry options with defaults
  const retry =
    inputRetry.mode === "immediate-requeue"
      ? resolveImmediateRequeueOptions(inputRetry)
      : inputRetry.mode === "ttl-backoff"
        ? resolveTtlBackoffOptions(inputRetry)
        : inputRetry;

  const queueArguments =
    type === "quorum" && retry.mode === "immediate-requeue"
      ? withQuorumDeliveryLimit(name, opts.arguments, retry.maxRetries)
      : opts.arguments;

  const baseQueueDefinition: BaseQueueDefinition = {
    ...baseProps,
    ...(queueArguments !== undefined && { arguments: queueArguments }),
    retry,
  };

  return type === "quorum"
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
}
