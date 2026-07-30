import type {
  DirectExchangeDefinition,
  ExchangeBindingDefinition,
  ExchangeDefinition,
  FanoutExchangeDefinition,
  HeadersExchangeDefinition,
  QueueBindingDefinition,
  QueueDefinition,
  TopicExchangeDefinition,
} from "../types.js";
import { _internal_assertRoutingKeyPresent } from "./validate.js";

/**
 * Define a binding between a queue and a fanout or headers exchange.
 *
 * Binds a queue to a fanout or headers exchange (no routing key needed).
 * Fanout and headers exchanges ignore routing keys, so this overload doesn't require one.
 *
 * @param queue - The queue definition to bind
 * @param exchange - The fanout or headers exchange definition
 * @param options - Optional binding configuration
 * @param options.arguments - Additional AMQP arguments for the binding
 * @returns A queue binding definition
 *
 * @example
 * ```typescript
 * const logsQueue = defineQueue('logs-queue');
 * const logsExchange = defineExchange('logs', { type: 'fanout' });
 *
 * const binding = defineQueueBinding(logsQueue, logsExchange);
 * ```
 */
export function defineQueueBinding(
  queue: QueueDefinition,
  exchange: FanoutExchangeDefinition | HeadersExchangeDefinition,
  options?: Omit<
    Extract<
      QueueBindingDefinition,
      { exchange: FanoutExchangeDefinition | HeadersExchangeDefinition }
    >,
    "type" | "queue" | "exchange" | "routingKey"
  >,
): Extract<
  QueueBindingDefinition,
  { exchange: FanoutExchangeDefinition | HeadersExchangeDefinition }
>;

/**
 * Define a binding between a queue and a direct or topic exchange.
 *
 * Binds a queue to an exchange with a specific routing key pattern.
 * Messages are only routed to the queue if the routing key matches the pattern.
 *
 * For direct exchanges: The routing key must match exactly.
 * For topic exchanges: The routing key can include wildcards:
 * - `*` matches exactly one word
 * - `#` matches zero or more words
 *
 * @param queue - The queue definition to bind
 * @param exchange - The direct or topic exchange definition
 * @param options - Binding configuration (routingKey is required)
 * @param options.routingKey - The routing key pattern for message routing
 * @param options.arguments - Additional AMQP arguments for the binding
 * @returns A queue binding definition
 *
 * @example
 * ```typescript
 * const orderQueue = defineQueue('order-processing');
 * const ordersExchange = defineExchange('orders');
 *
 * // Bind with exact routing key
 * const binding = defineQueueBinding(orderQueue, ordersExchange, {
 *   routingKey: 'order.created'
 * });
 *
 * // Bind with wildcard pattern
 * const allOrdersBinding = defineQueueBinding(orderQueue, ordersExchange, {
 *   routingKey: 'order.*'  // Matches order.created, order.updated, etc.
 * });
 * ```
 */
export function defineQueueBinding(
  queue: QueueDefinition,
  exchange: DirectExchangeDefinition | TopicExchangeDefinition,
  options: Omit<
    Extract<
      QueueBindingDefinition,
      { exchange: DirectExchangeDefinition | TopicExchangeDefinition }
    >,
    "type" | "queue" | "exchange"
  >,
): Extract<
  QueueBindingDefinition,
  { exchange: DirectExchangeDefinition | TopicExchangeDefinition }
>;

/*
 * Implementation signature of defineQueueBinding — not part of the public
 * overload set; use the type-specific overloads above. (Deliberately a plain
 * comment: a JSDoc `@internal` here makes TypeDoc drop the whole function
 * from the generated API docs.)
 */
export function defineQueueBinding(
  queue: QueueDefinition,
  exchange: ExchangeDefinition,
  options?: {
    routingKey?: string;
    arguments?: Record<string, unknown>;
  },
): QueueBindingDefinition {
  if (exchange.type === "fanout" || exchange.type === "headers") {
    return {
      type: "queue",
      queue,
      exchange,
      ...(options?.arguments && { arguments: options.arguments }),
    } as QueueBindingDefinition;
  }

  const routingKey = options?.routingKey;
  _internal_assertRoutingKeyPresent("Queue binding", exchange.name, exchange.type, routingKey);

  return {
    type: "queue",
    queue,
    exchange,
    routingKey,
    ...(options?.arguments && { arguments: options.arguments }),
  } as QueueBindingDefinition;
}

/**
 * Internal helper to call defineQueueBinding with proper type handling.
 * Used by queue.ts to avoid circular dependency.
 * @internal
 */
export function defineQueueBindingInternal(
  queue: QueueDefinition,
  exchange: ExchangeDefinition,
  options?: {
    routingKey?: string;
    arguments?: Record<string, unknown>;
  },
): QueueBindingDefinition {
  if (exchange.type === "fanout" || exchange.type === "headers") {
    return defineQueueBinding(queue, exchange, options);
  }
  return defineQueueBinding(queue, exchange, options as { routingKey: string });
}

/**
 * Define a binding between two exchanges (exchange-to-exchange routing).
 *
 * Binds a destination exchange to a fanout or headers source exchange.
 * Messages published to the source exchange will be forwarded to the destination exchange.
 * Fanout and headers exchanges ignore routing keys, so this overload doesn't require one.
 *
 * @param destination - The destination exchange definition
 * @param source - The fanout or headers source exchange definition
 * @param options - Optional binding configuration
 * @param options.arguments - Additional AMQP arguments for the binding
 * @returns An exchange binding definition
 *
 * @example
 * ```typescript
 * const sourceExchange = defineExchange('logs', { type: 'fanout' });
 * const destExchange = defineExchange('all-logs', { type: 'fanout' });
 *
 * const binding = defineExchangeBinding(destExchange, sourceExchange);
 * ```
 */
export function defineExchangeBinding(
  destination: ExchangeDefinition,
  source: FanoutExchangeDefinition | HeadersExchangeDefinition,
  options?: Omit<
    Extract<
      ExchangeBindingDefinition,
      { source: FanoutExchangeDefinition | HeadersExchangeDefinition }
    >,
    "type" | "source" | "destination" | "routingKey"
  >,
): Extract<
  ExchangeBindingDefinition,
  { source: FanoutExchangeDefinition | HeadersExchangeDefinition }
>;

/**
 * Define a binding between two exchanges (exchange-to-exchange routing).
 *
 * Binds a destination exchange to a direct or topic source exchange with a routing key pattern.
 * Messages are forwarded from source to destination only if the routing key matches the pattern.
 *
 * @param destination - The destination exchange definition
 * @param source - The direct or topic source exchange definition
 * @param options - Binding configuration (routingKey is required)
 * @param options.routingKey - The routing key pattern for message routing
 * @param options.arguments - Additional AMQP arguments for the binding
 * @returns An exchange binding definition
 *
 * @example
 * ```typescript
 * const ordersExchange = defineExchange('orders');
 * const importantExchange = defineExchange('important-orders');
 *
 * // Forward only high-value orders
 * const binding = defineExchangeBinding(importantExchange, ordersExchange, {
 *   routingKey: 'order.high-value.*'
 * });
 * ```
 */
export function defineExchangeBinding(
  destination: ExchangeDefinition,
  source: DirectExchangeDefinition | TopicExchangeDefinition,
  options: Omit<
    Extract<
      ExchangeBindingDefinition,
      { source: DirectExchangeDefinition | TopicExchangeDefinition }
    >,
    "type" | "source" | "destination"
  >,
): Extract<
  ExchangeBindingDefinition,
  { source: DirectExchangeDefinition | TopicExchangeDefinition }
>;

/*
 * Implementation signature of defineExchangeBinding — not part of the public
 * overload set; use the type-specific overloads above. (Deliberately a plain
 * comment: a JSDoc `@internal` here makes TypeDoc drop the whole function
 * from the generated API docs.)
 */
export function defineExchangeBinding(
  destination: ExchangeDefinition,
  source: ExchangeDefinition,
  options?: {
    routingKey?: string;
    arguments?: Record<string, unknown>;
  },
): ExchangeBindingDefinition {
  if (source.type === "fanout" || source.type === "headers") {
    return {
      type: "exchange",
      source,
      destination,
      ...(options?.arguments && { arguments: options.arguments }),
    } as ExchangeBindingDefinition;
  }

  return {
    type: "exchange",
    source,
    destination,
    routingKey: options?.routingKey ?? "",
    ...(options?.arguments && { arguments: options.arguments }),
  } as ExchangeBindingDefinition;
}
