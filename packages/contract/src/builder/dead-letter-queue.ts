import type {
  DefineQueueOptions,
  DirectExchangeDefinition,
  ExchangeDefinition,
  QueueBindingDefinition,
  QueueDefinition,
  TopicExchangeDefinition,
} from "../types.js";
import { defineQueueBindingInternal } from "./binding.js";
import { defineQueue } from "./queue.js";

/**
 * Options for {@link defineDeadLetterQueue}, chosen by the dead-letter
 * exchange's type.
 *
 * - **direct**: `routingKey` is required — a direct exchange matches keys
 *   literally, so there is no catch-all to default to.
 * - **topic**: `routingKey` defaults to `#`, which catches every dead letter
 *   whatever key it carries.
 * - **fanout / headers**: no routing key.
 */
type DeadLetterQueueOptionsArgs<TExchange extends ExchangeDefinition> = [TExchange] extends [
  DirectExchangeDefinition,
]
  ? [options: { routingKey: string; queue?: DefineQueueOptions }]
  : [TExchange] extends [TopicExchangeDefinition]
    ? [options?: { routingKey?: string; queue?: DefineQueueOptions }]
    : [options?: { queue?: DefineQueueOptions }];

/**
 * A dead-letter queue and the binding that delivers to it — add both to the
 * contract: `queues: { dlq: dlq.queue }, bindings: { dlq: dlq.binding }`.
 */
export type DeadLetterQueue<TName extends string = string> = {
  /** The dead-letter queue. */
  queue: QueueDefinition<TName>;
  /** The binding from the dead-letter exchange to {@link DeadLetterQueue.queue}. */
  binding: QueueBindingDefinition;
};

/**
 * Define a dead-letter queue bound to a dead-letter exchange.
 *
 * Collapses the usual pair — `defineQueue(name)` plus
 * `defineQueueBinding(queue, dlx, { routingKey })` — into one call, and picks
 * the binding key that actually receives dead letters: `#` on a topic
 * exchange, none on fanout / headers, and a required explicit key on a direct
 * exchange (where `#` would be a literal key that matches nothing).
 *
 * The result is ordinary topology: add `queue` and `binding` to the contract
 * and `defineContract` validates them exactly as if they had been written out
 * — including the dead-letter routability check on every queue that
 * dead-letters to this exchange.
 *
 * @param deadLetterExchange - The exchange queues dead-letter to (their `deadLetter.exchange`)
 * @param name - The dead-letter queue's name
 * @param options - Required for a direct exchange
 * @param options.routingKey - The binding key: required on a direct exchange
 *   (use the queues' `deadLetter.routingKey`), defaults to `#` on a topic exchange
 * @param options.queue - Options for the dead-letter queue itself (see {@link defineQueue})
 * @returns The dead-letter queue and its binding
 *
 * @example
 * ```typescript
 * const ordersDlx = defineExchange('orders-dlx');
 * const orderQueue = defineQueue('order-processing', { deadLetter: { exchange: ordersDlx } });
 * const ordersDlq = defineDeadLetterQueue(ordersDlx, 'orders-dlq');
 *
 * const contract = defineContract({
 *   consumers: { processOrder: defineEventConsumer(orderCreatedEvent, orderQueue) },
 *   queues: { ordersDlq: ordersDlq.queue },
 *   bindings: { ordersDlq: ordersDlq.binding },
 * });
 * ```
 */
export function defineDeadLetterQueue<TName extends string, TExchange extends ExchangeDefinition>(
  deadLetterExchange: TExchange,
  name: TName,
  ...options: DeadLetterQueueOptionsArgs<TExchange>
): DeadLetterQueue<TName>;

/*
 * Implementation signature of defineDeadLetterQueue. (Deliberately a plain
 * comment: a JSDoc `@internal` here makes TypeDoc drop the whole function
 * from the generated API docs.)
 */
export function defineDeadLetterQueue(
  deadLetterExchange: ExchangeDefinition,
  name: string,
  options?: { routingKey?: string; queue?: DefineQueueOptions },
): DeadLetterQueue {
  const queue = defineQueue(name, options?.queue);
  const routingKey = options?.routingKey ?? (deadLetterExchange.type === "topic" ? "#" : undefined);
  // The shared binding builder, not an object literal: its define-time checks
  // (missing key on direct, wildcard on direct) apply here too.
  const binding = defineQueueBindingInternal(
    queue,
    deadLetterExchange,
    routingKey === undefined ? undefined : { routingKey },
  );
  return { queue, binding };
}
