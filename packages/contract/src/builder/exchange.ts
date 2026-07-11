import type {
  BaseExchangeDefinition,
  DirectExchangeDefinition,
  ExchangeDefinition,
  FanoutExchangeDefinition,
  HeadersExchangeDefinition,
  TopicExchangeDefinition,
} from "../types.js";
import { _internal_assertKnownKeys, _internal_assertNonEmptyName } from "./validate.js";

/**
 * Define a topic exchange.
 *
 * A topic exchange routes messages to queues based on routing key patterns.
 * Routing keys can use wildcards: `*` matches one word, `#` matches zero or more words.
 * This exchange type is ideal for flexible message routing based on hierarchical topics.
 *
 * @param name - The name of the exchange
 * @param options - Optional exchange configuration
 * @param options.type - Exchange type (must be "topic", or omitted for default topic exchange)
 * @param options.durable - If true, the exchange survives broker restarts (default: true)
 * @param options.autoDelete - If true, the exchange is deleted when no queues are bound
 * @param options.internal - If true, the exchange cannot be directly published to
 * @param options.arguments - Additional AMQP arguments for the exchange
 * @returns A topic exchange definition
 *
 * @example
 * ```typescript
 * const ordersExchange = defineExchange('orders', { type: 'topic' });
 *
 * // Or omit type for default topic exchange
 * const ordersExchange = defineExchange('orders');
 * ```
 */
export function defineExchange<TName extends string>(
  name: TName,
  options?: { type?: "topic" } & Omit<BaseExchangeDefinition, "name" | "type">,
): TopicExchangeDefinition<TName>;

/**
 * Define a direct exchange.
 *
 * A direct exchange routes messages to queues based on exact routing key matches.
 * This exchange type is ideal for point-to-point messaging.
 *
 * @param name - The name of the exchange
 * @param options - Exchange configuration
 * @param options.type - Exchange type (must be "direct")
 * @param options.durable - If true, the exchange survives broker restarts (default: true)
 * @param options.autoDelete - If true, the exchange is deleted when no queues are bound
 * @param options.internal - If true, the exchange cannot be directly published to
 * @param options.arguments - Additional AMQP arguments for the exchange
 * @returns A direct exchange definition
 *
 * @example
 * ```typescript
 * const tasksExchange = defineExchange('tasks', { type: 'direct' });
 * ```
 */
export function defineExchange<TName extends string>(
  name: TName,
  options: { type: "direct" } & Omit<BaseExchangeDefinition, "name" | "type">,
): DirectExchangeDefinition<TName>;

/**
 * Define a fanout exchange.
 *
 * A fanout exchange routes messages to all bound queues without considering routing keys.
 * This exchange type is ideal for broadcasting messages to multiple consumers.
 *
 * @param name - The name of the exchange
 * @param options - Exchange configuration
 * @param options.type - Exchange type (must be "fanout")
 * @param options.durable - If true, the exchange survives broker restarts (default: true)
 * @param options.autoDelete - If true, the exchange is deleted when no queues are bound
 * @param options.internal - If true, the exchange cannot be directly published to
 * @param options.arguments - Additional AMQP arguments for the exchange
 * @returns A fanout exchange definition
 *
 * @example
 * ```typescript
 * const logsExchange = defineExchange('logs', { type: 'fanout' });
 * ```
 */
export function defineExchange<TName extends string>(
  name: TName,
  options: { type: "fanout" } & Omit<BaseExchangeDefinition, "name" | "type">,
): FanoutExchangeDefinition<TName>;

/**
 * Define a headers exchange.
 *
 * A headers exchange routes messages to all bound queues based on header matching.
 * This exchange type is ideal for complex routing scenarios.
 *
 * @param name - The name of the exchange
 * @param options - Exchange configuration
 * @param options.type - Exchange type (must be "headers")
 * @param options.durable - If true, the exchange survives broker restarts (default: true)
 * @param options.autoDelete - If true, the exchange is deleted when no queues are bound
 * @param options.internal - If true, the exchange cannot be directly published to
 * @param options.arguments - Additional AMQP arguments for the exchange
 * @returns A headers exchange definition
 *
 * @example
 * ```typescript
 * const routesExchange = defineExchange('routes', { type: 'headers' });
 * ```
 */
export function defineExchange<TName extends string>(
  name: TName,
  options: { type: "headers" } & Omit<BaseExchangeDefinition, "name" | "type">,
): HeadersExchangeDefinition<TName>;

/**
 * Define an AMQP exchange.
 *
 * An exchange receives messages from publishers and routes them to queues based on the exchange type
 * and routing rules. This is the implementation function - use the type-specific overloads for better
 * type safety.
 *
 * @param name - The name of the exchange
 * @param options - Optional exchange configuration
 * @param options.type - Exchange type (one of "topic", "direct", "fanout", "headers") (default: "topic")
 * @param options.durable - If true, the exchange survives broker restarts (default: true)
 * @param options.autoDelete - If true, the exchange is deleted when no queues are bound
 * @param options.internal - If true, the exchange cannot be directly published to
 * @param options.arguments - Additional AMQP arguments for the exchange
 * @returns An exchange definition
 * @internal
 */
export function defineExchange(
  name: string,
  options?: { type?: "topic" | "direct" | "fanout" | "headers" } & Omit<
    BaseExchangeDefinition,
    "name" | "type"
  >,
): ExchangeDefinition {
  _internal_assertNonEmptyName("Exchange", name);
  _internal_assertKnownKeys("exchange", name, options, [
    "type",
    "durable",
    "autoDelete",
    "internal",
    "arguments",
  ]);
  const type = options?.type ?? "topic";
  if (!["topic", "direct", "fanout", "headers"].includes(type)) {
    throw new Error(
      `Unknown exchange type "${String(type)}" for exchange "${name}". ` +
        "Allowed types: topic, direct, fanout, headers.",
    );
  }
  return {
    name,
    type,
    durable: true,
    ...options,
  };
}
