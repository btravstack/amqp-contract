import type {
  DirectExchangeDefinition,
  ExchangeDefinition,
  FanoutExchangeDefinition,
  HeadersExchangeDefinition,
  MessageDefinition,
  PublisherDefinition,
  TopicExchangeDefinition,
} from "../types.js";

/**
 * Define a message publisher for a fanout or headers exchange.
 *
 * A publisher sends messages to an exchange. For fanout exchanges, messages are broadcast
 * to all bound queues regardless of routing key, so no routing key is required. For headers exchanges,
 * routing is based on message headers rather than routing keys, so no routing key is required either.
 *
 * The message schema is validated when publishing to ensure type safety.
 *
 * **Which pattern to use:**
 *
 * | Pattern | Best for | Description |
 * |---------|----------|-------------|
 * | `definePublisher` + `defineConsumer` | Independent definition | Define publishers and consumers separately with manual schema consistency |
 * | `defineEventPublisher` + `defineEventConsumer` | Event broadcasting | Define event publisher first, create consumers that subscribe to it |
 * | `defineCommandConsumer` + `defineCommandPublisher` | Task queues | Define command consumer first, create publishers that send commands to it |
 *
 * Use `defineEventPublisher` when:
 * - One publisher feeds multiple consumers
 * - You want automatic schema consistency between publisher and consumers
 * - You're building event-driven architectures
 *
 * @param exchange - The fanout or headers exchange definition to publish to
 * @param message - The message definition with payload schema
 * @param options - Optional publisher configuration
 * @returns A publisher definition with inferred message types
 *
 * @example
 * ```typescript
 * import { z } from 'zod';
 *
 * const logsExchange = defineExchange('logs', { type: 'fanout' });
 * const logMessage = defineMessage(
 *   z.object({
 *     level: z.enum(['info', 'warn', 'error']),
 *     message: z.string(),
 *     timestamp: z.string().datetime(),
 *   })
 * );
 *
 * const logPublisher = definePublisher(logsExchange, logMessage);
 * ```
 *
 * @see defineEventPublisher - For event-driven patterns with automatic schema consistency
 * @see defineCommandConsumer - For task queue patterns with automatic schema consistency
 */
export function definePublisher<TMessage extends MessageDefinition>(
  exchange: FanoutExchangeDefinition | HeadersExchangeDefinition,
  message: TMessage,
  options?: Omit<
    Extract<
      PublisherDefinition<TMessage>,
      { exchange: FanoutExchangeDefinition | HeadersExchangeDefinition }
    >,
    "exchange" | "message" | "routingKey"
  >,
): Extract<
  PublisherDefinition<TMessage>,
  { exchange: FanoutExchangeDefinition | HeadersExchangeDefinition }
>;

/**
 * Define a message publisher for a direct or topic exchange.
 *
 * A publisher sends messages to an exchange with a specific routing key.
 * The routing key determines which queues receive the message.
 *
 * The message schema is validated when publishing to ensure type safety.
 *
 * **Which pattern to use:**
 *
 * | Pattern | Best for | Description |
 * |---------|----------|-------------|
 * | `definePublisher` + `defineConsumer` | Independent definition | Define publishers and consumers separately with manual schema consistency |
 * | `defineEventPublisher` + `defineEventConsumer` | Event broadcasting | Define event publisher first, create consumers that subscribe to it |
 * | `defineCommandConsumer` + `defineCommandPublisher` | Task queues | Define command consumer first, create publishers that send commands to it |
 *
 * Use `defineEventPublisher` when:
 * - One publisher feeds multiple consumers
 * - You want automatic schema consistency between publisher and consumers
 * - You're building event-driven architectures
 *
 * @param exchange - The direct or topic exchange definition to publish to
 * @param message - The message definition with payload schema
 * @param options - Publisher configuration (routingKey is required)
 * @param options.routingKey - The routing key for message routing
 * @returns A publisher definition with inferred message types
 *
 * @example
 * ```typescript
 * import { z } from 'zod';
 *
 * const ordersExchange = defineExchange('orders');
 * const orderMessage = defineMessage(
 *   z.object({
 *     orderId: z.string().uuid(),
 *     amount: z.number().positive(),
 *   }),
 *   {
 *     summary: 'Order created event',
 *     description: 'Emitted when a new order is created'
 *   }
 * );
 *
 * const orderCreatedPublisher = definePublisher(ordersExchange, orderMessage, {
 *   routingKey: 'order.created'
 * });
 * ```
 *
 * @see defineEventPublisher - For event-driven patterns with automatic schema consistency
 * @see defineCommandConsumer - For task queue patterns with automatic schema consistency
 */
export function definePublisher<TMessage extends MessageDefinition>(
  exchange: DirectExchangeDefinition | TopicExchangeDefinition,
  message: TMessage,
  options: Omit<
    Extract<
      PublisherDefinition<TMessage>,
      { exchange: DirectExchangeDefinition | TopicExchangeDefinition }
    >,
    "exchange" | "message"
  >,
): Extract<
  PublisherDefinition<TMessage>,
  { exchange: DirectExchangeDefinition | TopicExchangeDefinition }
>;

/*
 * Implementation signature of definePublisher — not part of the public
 * overload set; use the type-specific overloads above. (Deliberately a plain
 * comment: a JSDoc `@internal` here makes TypeDoc drop the whole function
 * from the generated API docs.)
 */
export function definePublisher<TMessage extends MessageDefinition>(
  exchange: ExchangeDefinition,
  message: TMessage,
  options?: { routingKey?: string },
): PublisherDefinition<TMessage> {
  if (exchange.type === "fanout" || exchange.type === "headers") {
    return {
      exchange,
      message,
    } as PublisherDefinition<TMessage>;
  }

  return {
    exchange,
    message,
    routingKey: options?.routingKey ?? "",
  } as PublisherDefinition<TMessage>;
}

/**
 * Helper to call definePublisher with proper type handling.
 * Type safety is enforced by overloaded public function signatures.
 * @internal
 */
export function definePublisherInternal<TMessage extends MessageDefinition>(
  exchange: ExchangeDefinition,
  message: TMessage,
  options?: {
    routingKey?: string;
    arguments?: Record<string, unknown>;
  },
): PublisherDefinition<TMessage> {
  // Type assertion is safe because overloaded signatures enforce routingKey requirement
  if (exchange.type === "fanout" || exchange.type === "headers") {
    return definePublisher(exchange, message, options);
  }
  return definePublisher(exchange, message, options as { routingKey: string });
}
