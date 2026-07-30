import type {
  ConsumerDefinition,
  DirectExchangeDefinition,
  ExchangeBindingDefinition,
  ExchangeDefinition,
  FanoutExchangeDefinition,
  HeadersExchangeDefinition,
  MessageDefinition,
  QueueBindingDefinition,
  QueueDefinition,
  TopicExchangeDefinition,
} from "../types.js";
import { defineExchangeBinding, defineQueueBindingInternal } from "./binding.js";
import { defineConsumer } from "./consumer.js";
import type { BindingPattern, RoutingKey } from "./routing-types.js";

/**
 * Configuration for an event publisher.
 *
 * Events are published without knowing who consumes them. Multiple consumers
 * can subscribe to the same event. This follows the pub/sub pattern where
 * publishers broadcast events and consumers subscribe to receive them.
 *
 * @template TMessage - The message definition
 * @template TExchange - The exchange definition
 * @template TRoutingKey - The routing key type (undefined for fanout and headers exchanges)
 */
export type EventPublisherConfig<
  TMessage extends MessageDefinition,
  TExchange extends ExchangeDefinition,
  TRoutingKey extends string | undefined = undefined,
> = {
  /** Discriminator to identify this as an event publisher config */
  __brand: "EventPublisherConfig";
  /** The exchange to publish to */
  exchange: TExchange;
  /** The message definition */
  message: TMessage;
  /** The routing key for direct/topic exchanges */
  routingKey: TRoutingKey;
  /** Additional AMQP arguments */
  arguments?: Record<string, unknown>;
};

/**
 * Result from defineEventConsumer.
 *
 * Contains the consumer definition and binding needed to subscribe to an event.
 * Can be used directly in defineContract's consumers section - the binding
 * will be automatically extracted.
 *
 * @template TMessage - The message definition
 */
export type EventConsumerResult<
  TMessage extends MessageDefinition,
  TExchange extends ExchangeDefinition = ExchangeDefinition,
  TQueue extends QueueDefinition = QueueDefinition,
  TExchangeBinding extends ExchangeBindingDefinition | undefined =
    | ExchangeBindingDefinition
    | undefined,
  TBridgeExchange extends ExchangeDefinition | undefined = ExchangeDefinition | undefined,
> = {
  /** Discriminator to identify this as an event consumer result */
  __brand: "EventConsumerResult";
  /** The consumer definition for processing messages */
  consumer: ConsumerDefinition<TMessage>;
  /** The binding connecting the queue to the exchange */
  binding: QueueBindingDefinition;
  /** The source exchange this consumer subscribes to */
  exchange: TExchange;
  /** The queue this consumer reads from */
  queue: TQueue;
  /** The exchange-to-exchange binding when bridging, if configured */
  exchangeBinding: TExchangeBinding;
  /** The bridge (local domain) exchange when bridging, if configured */
  bridgeExchange: TBridgeExchange;
};

/**
 * Define an event publisher for broadcasting messages via fanout exchange.
 *
 * Events are published without knowing who consumes them. Multiple consumers
 * can subscribe to the same event using `defineEventConsumer`.
 *
 * @param exchange - The fanout exchange to publish to
 * @param message - The message definition (schema and metadata)
 * @param options - Optional binding configuration
 * @param options.arguments - Additional AMQP arguments
 * @returns An event publisher configuration
 *
 * @example
 * ```typescript
 * const logsExchange = defineExchange('logs', { type: 'fanout' });
 * const logMessage = defineMessage(z.object({
 *   level: z.enum(['info', 'warn', 'error']),
 *   message: z.string(),
 * }));
 *
 * // Create event publisher
 * const logEvent = defineEventPublisher(logsExchange, logMessage);
 *
 * // Multiple consumers can subscribe
 * const { consumer: fileConsumer, binding: fileBinding } =
 *   defineEventConsumer(logEvent, fileLogsQueue);
 * const { consumer: alertConsumer, binding: alertBinding } =
 *   defineEventConsumer(logEvent, alertsQueue);
 * ```
 */
export function defineEventPublisher<
  TMessage extends MessageDefinition,
  TExchange extends FanoutExchangeDefinition,
>(
  exchange: TExchange,
  message: TMessage,
  options?: {
    arguments?: Record<string, unknown>;
  },
): EventPublisherConfig<TMessage, TExchange, undefined>;

/**
 * Define an event publisher for broadcasting messages via headers exchange.
 *
 * Events are published without knowing who consumes them. Multiple consumers
 * can subscribe to the same event using `defineEventConsumer`.
 *
 * @param exchange - The headers exchange to publish to
 * @param message - The message definition (schema and metadata)
 * @param options - Optional binding configuration
 * @param options.arguments - Additional AMQP arguments
 * @returns An event publisher configuration
 *
 * @example
 * ```typescript
 * const logsExchange = defineExchange('logs', { type: 'headers' });
 * const logMessage = defineMessage(z.object({
 *   level: z.enum(['info', 'warn', 'error']),
 *   message: z.string(),
 * }));
 *
 * // Create event publisher
 * const logEvent = defineEventPublisher(logsExchange, logMessage);
 *
 * // Multiple consumers can subscribe
 * const { consumer: fileConsumer, binding: fileBinding } =
 *   defineEventConsumer(logEvent, fileLogsQueue);
 * const { consumer: alertConsumer, binding: alertBinding } =
 *   defineEventConsumer(logEvent, alertsQueue);
 * ```
 */
export function defineEventPublisher<
  TMessage extends MessageDefinition,
  TExchange extends HeadersExchangeDefinition,
>(
  exchange: TExchange,
  message: TMessage,
  options?: {
    arguments?: Record<string, unknown>;
  },
): EventPublisherConfig<TMessage, TExchange, undefined>;

/**
 * Define an event publisher for broadcasting messages via direct exchange.
 *
 * Events are published with a specific routing key. Consumers will receive
 * messages that match the routing key exactly.
 *
 * @param exchange - The direct exchange to publish to
 * @param message - The message definition (schema and metadata)
 * @param options - Configuration with required routing key
 * @param options.routingKey - The routing key for message routing
 * @param options.arguments - Additional AMQP arguments
 * @returns An event publisher configuration
 *
 * @example
 * ```typescript
 * const tasksExchange = defineExchange('tasks', { type: 'direct' });
 * const taskMessage = defineMessage(z.object({ taskId: z.string() }));
 *
 * const taskEvent = defineEventPublisher(tasksExchange, taskMessage, {
 *   routingKey: 'task.execute',
 * });
 * ```
 */
export function defineEventPublisher<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TExchange extends DirectExchangeDefinition,
>(
  exchange: TExchange,
  message: TMessage,
  options: {
    routingKey: RoutingKey<TRoutingKey>;
    arguments?: Record<string, unknown>;
  },
): EventPublisherConfig<TMessage, TExchange, TRoutingKey>;

/**
 * Define an event publisher for broadcasting messages via topic exchange.
 *
 * Events are published with a concrete routing key. Consumers can subscribe
 * using patterns (with * and # wildcards) to receive matching messages.
 *
 * @param exchange - The topic exchange to publish to
 * @param message - The message definition (schema and metadata)
 * @param options - Configuration with required routing key
 * @param options.routingKey - The concrete routing key (no wildcards)
 * @param options.arguments - Additional AMQP arguments
 * @returns An event publisher configuration
 *
 * @example
 * ```typescript
 * const ordersExchange = defineExchange('orders', { type: 'topic' });
 * const orderMessage = defineMessage(z.object({
 *   orderId: z.string(),
 *   amount: z.number(),
 * }));
 *
 * // Publisher uses concrete routing key
 * const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
 *   routingKey: 'order.created',
 * });
 *
 * // Consumer can use pattern
 * const { consumer, binding } = defineEventConsumer(
 *   orderCreatedEvent,
 *   allOrdersQueue,
 *   { routingKey: 'order.*' },
 * );
 * ```
 */
export function defineEventPublisher<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TExchange extends TopicExchangeDefinition,
>(
  exchange: TExchange,
  message: TMessage,
  options: {
    routingKey: RoutingKey<TRoutingKey>;
    arguments?: Record<string, unknown>;
  },
): EventPublisherConfig<TMessage, TExchange, TRoutingKey>;

/*
 * Implementation signature of defineEventPublisher. (Deliberately a plain
 * comment: a JSDoc `@internal` here makes TypeDoc drop the whole function
 * from the generated API docs.)
 */
export function defineEventPublisher<TMessage extends MessageDefinition>(
  exchange: ExchangeDefinition,
  message: TMessage,
  options?: {
    routingKey?: string;
    arguments?: Record<string, unknown>;
  },
): EventPublisherConfig<TMessage, ExchangeDefinition, string | undefined> {
  const config: EventPublisherConfig<TMessage, ExchangeDefinition, string | undefined> = {
    __brand: "EventPublisherConfig",
    exchange,
    message,
    routingKey: options?.routingKey,
  };

  if (options?.arguments !== undefined) {
    config.arguments = options.arguments;
  }

  return config;
}

/**
 * Create a consumer that subscribes to an event from a fanout exchange via a bridge exchange.
 *
 * When `bridgeExchange` is provided, the queue binds to the bridge exchange instead of the
 * source exchange, and an exchange-to-exchange binding is created from the source to the bridge.
 *
 * @param eventPublisher - The event publisher configuration
 * @param queue - The queue that will receive messages
 * @param options - Binding configuration with required bridgeExchange
 * @param options.bridgeExchange - The fanout bridge exchange (must be fanout to match source)
 * @param options.arguments - Additional AMQP arguments
 * @returns An object with the consumer definition, queue binding, and exchange binding
 */
export function defineEventConsumer<
  TMessage extends MessageDefinition,
  TExchange extends FanoutExchangeDefinition,
  TQueueDefinition extends QueueDefinition,
  TBridgeExchange extends FanoutExchangeDefinition,
>(
  eventPublisher: EventPublisherConfig<TMessage, TExchange, undefined>,
  queue: TQueueDefinition,
  options: {
    bridgeExchange: TBridgeExchange;
    arguments?: Record<string, unknown>;
  },
): EventConsumerResult<
  TMessage,
  TExchange,
  TQueueDefinition,
  ExchangeBindingDefinition,
  TBridgeExchange
>;

/**
 * Create a consumer that subscribes to an event from a headers exchange via a bridge exchange.
 *
 * When `bridgeExchange` is provided, the queue binds to the bridge exchange instead of the
 * source exchange, and an exchange-to-exchange binding is created from the source to the bridge.
 *
 * @param eventPublisher - The event publisher configuration
 * @param queue - The queue that will receive messages
 * @param options - Binding configuration with required bridgeExchange
 * @param options.bridgeExchange - The headers bridge exchange (must be headers to match source)
 * @param options.arguments - Additional AMQP arguments
 * @returns An object with the consumer definition, queue binding, and exchange binding
 */
export function defineEventConsumer<
  TMessage extends MessageDefinition,
  TExchange extends HeadersExchangeDefinition,
  TQueueDefinition extends QueueDefinition,
  TBridgeExchange extends HeadersExchangeDefinition,
>(
  eventPublisher: EventPublisherConfig<TMessage, TExchange, undefined>,
  queue: TQueueDefinition,
  options: {
    bridgeExchange: TBridgeExchange;
    arguments?: Record<string, unknown>;
  },
): EventConsumerResult<
  TMessage,
  TExchange,
  TQueueDefinition,
  ExchangeBindingDefinition,
  TBridgeExchange
>;

/**
 * Create a consumer that subscribes to an event from a direct exchange via a bridge exchange.
 *
 * @param eventPublisher - The event publisher configuration
 * @param queue - The queue that will receive messages
 * @param options - Binding configuration with required bridgeExchange
 * @param options.bridgeExchange - The bridge exchange (must be direct or topic to preserve routing keys)
 * @param options.arguments - Additional AMQP arguments
 * @returns An object with the consumer definition, queue binding, and exchange binding
 */
export function defineEventConsumer<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TExchange extends DirectExchangeDefinition,
  TQueueDefinition extends QueueDefinition,
  TBridgeExchange extends DirectExchangeDefinition | TopicExchangeDefinition,
>(
  eventPublisher: EventPublisherConfig<TMessage, TExchange, TRoutingKey>,
  queue: TQueueDefinition,
  options: {
    bridgeExchange: TBridgeExchange;
    arguments?: Record<string, unknown>;
  },
): EventConsumerResult<
  TMessage,
  TExchange,
  TQueueDefinition,
  ExchangeBindingDefinition,
  TBridgeExchange
>;

/**
 * Create a consumer that subscribes to an event from a topic exchange via a bridge exchange.
 *
 * @param eventPublisher - The event publisher configuration
 * @param queue - The queue that will receive messages
 * @param options - Binding configuration with required bridgeExchange
 * @param options.bridgeExchange - The bridge exchange (must be direct or topic to preserve routing keys)
 * @param options.routingKey - Override routing key with pattern (defaults to publisher's key)
 * @param options.arguments - Additional AMQP arguments
 * @returns An object with the consumer definition, queue binding, and exchange binding
 */
export function defineEventConsumer<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TExchange extends TopicExchangeDefinition,
  TQueueDefinition extends QueueDefinition,
  TBridgeExchange extends DirectExchangeDefinition | TopicExchangeDefinition,
  TConsumerRoutingKey extends string = TRoutingKey,
>(
  eventPublisher: EventPublisherConfig<TMessage, TExchange, TRoutingKey>,
  queue: TQueueDefinition,
  options: {
    bridgeExchange: TBridgeExchange;
    routingKey?: BindingPattern<TConsumerRoutingKey>;
    arguments?: Record<string, unknown>;
  },
): EventConsumerResult<
  TMessage,
  TExchange,
  TQueueDefinition,
  ExchangeBindingDefinition,
  TBridgeExchange
>;

/**
 * Create a consumer that subscribes to an event from a fanout exchange.
 *
 * @param eventPublisher - The event publisher configuration
 * @param queue - The queue that will receive messages
 * @param options - Optional binding configuration
 * @param options.arguments - Additional AMQP arguments
 * @returns An object with the consumer definition and binding
 *
 * @example
 * ```typescript
 * const logEvent = defineEventPublisher(logsExchange, logMessage);
 * const { consumer, binding } = defineEventConsumer(logEvent, logsQueue);
 * ```
 */
export function defineEventConsumer<
  TMessage extends MessageDefinition,
  TExchange extends FanoutExchangeDefinition,
  TQueueDefinition extends QueueDefinition,
>(
  eventPublisher: EventPublisherConfig<TMessage, TExchange, undefined>,
  queue: TQueueDefinition,
  options?: {
    arguments?: Record<string, unknown>;
  },
): EventConsumerResult<TMessage, TExchange, TQueueDefinition>;

/**
 * Create a consumer that subscribes to an event from a headers exchange.
 *
 * @param eventPublisher - The event publisher configuration
 * @param queue - The queue that will receive messages
 * @param options - Optional binding configuration
 * @param options.arguments - Additional AMQP arguments
 * @returns An object with the consumer definition and binding
 *
 * @example
 * ```typescript
 * const logEvent = defineEventPublisher(logsExchange, logMessage);
 * const { consumer, binding } = defineEventConsumer(logEvent, logsQueue);
 * ```
 */
export function defineEventConsumer<
  TMessage extends MessageDefinition,
  TExchange extends HeadersExchangeDefinition,
  TQueueDefinition extends QueueDefinition,
>(
  eventPublisher: EventPublisherConfig<TMessage, TExchange, undefined>,
  queue: TQueueDefinition,
  options?: {
    arguments?: Record<string, unknown>;
  },
): EventConsumerResult<TMessage, TExchange, TQueueDefinition>;

/**
 * Create a consumer that subscribes to an event from a direct exchange.
 *
 * @param eventPublisher - The event publisher configuration
 * @param queue - The queue that will receive messages
 * @param options - Optional binding configuration
 * @param options.arguments - Additional AMQP arguments
 * @returns An object with the consumer definition and binding
 */
export function defineEventConsumer<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TExchange extends DirectExchangeDefinition,
  TQueueDefinition extends QueueDefinition,
>(
  eventPublisher: EventPublisherConfig<TMessage, TExchange, TRoutingKey>,
  queue: TQueueDefinition,
  options?: {
    arguments?: Record<string, unknown>;
  },
): EventConsumerResult<TMessage, TExchange, TQueueDefinition>;

/**
 * Create a consumer that subscribes to an event from a topic exchange.
 *
 * For topic exchanges, the consumer can optionally override the routing key
 * with a pattern to subscribe to multiple events.
 *
 * @param eventPublisher - The event publisher configuration
 * @param queue - The queue that will receive messages
 * @param options - Optional binding configuration
 * @param options.routingKey - Override routing key with pattern (defaults to publisher's key)
 * @param options.arguments - Additional AMQP arguments
 * @returns An object with the consumer definition and binding
 *
 * @example
 * ```typescript
 * const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
 *   routingKey: 'order.created',
 * });
 *
 * // Use exact routing key from publisher
 * const { consumer: exactConsumer } = defineEventConsumer(orderCreatedEvent, exactQueue);
 *
 * // Override with pattern to receive all order events
 * const { consumer: allConsumer } = defineEventConsumer(orderCreatedEvent, allQueue, {
 *   routingKey: 'order.*',
 * });
 * ```
 */
export function defineEventConsumer<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TExchange extends TopicExchangeDefinition,
  TQueueDefinition extends QueueDefinition,
  TConsumerRoutingKey extends string = TRoutingKey,
>(
  eventPublisher: EventPublisherConfig<TMessage, TExchange, TRoutingKey>,
  queue: TQueueDefinition,
  options?: {
    routingKey?: BindingPattern<TConsumerRoutingKey>;
    arguments?: Record<string, unknown>;
  },
): EventConsumerResult<TMessage, TExchange, TQueueDefinition>;

/*
 * Implementation signature of defineEventConsumer. (Deliberately a plain
 * comment: a JSDoc `@internal` here makes TypeDoc drop the whole function
 * from the generated API docs.)
 */
export function defineEventConsumer<TMessage extends MessageDefinition>(
  eventPublisher: EventPublisherConfig<TMessage, ExchangeDefinition, string | undefined>,
  queue: QueueDefinition,
  options?: {
    routingKey?: string;
    bridgeExchange?: ExchangeDefinition;
    arguments?: Record<string, unknown>;
  },
): EventConsumerResult<TMessage> {
  const { exchange: sourceExchange, message, routingKey: publisherRoutingKey } = eventPublisher;

  // For topic exchanges, consumer can override the routing key
  const bindingRoutingKey = options?.routingKey ?? publisherRoutingKey;

  const bindingOptions: { routingKey?: string; arguments?: Record<string, unknown> } = {};
  if (bindingRoutingKey !== undefined) {
    bindingOptions.routingKey = bindingRoutingKey;
  }
  const bindingArguments = options?.arguments ?? eventPublisher.arguments;
  if (bindingArguments !== undefined) {
    bindingOptions.arguments = bindingArguments;
  }

  const bridgeExchange = options?.bridgeExchange;

  if (bridgeExchange) {
    // Bridged: queue binds to bridge exchange, e2e binding from source → bridge
    const binding = defineQueueBindingInternal(queue, bridgeExchange, bindingOptions);
    const consumer = defineConsumer(queue, message);

    // Create e2e binding: bridge ← source (destination ← source)
    const exchangeBindingOptions: { routingKey?: string } = {};
    if (bindingRoutingKey !== undefined) {
      exchangeBindingOptions.routingKey = bindingRoutingKey;
    }
    const e2eBinding =
      sourceExchange.type === "fanout" || sourceExchange.type === "headers"
        ? defineExchangeBinding(bridgeExchange, sourceExchange)
        : defineExchangeBinding(
            bridgeExchange,
            sourceExchange as DirectExchangeDefinition | TopicExchangeDefinition,
            exchangeBindingOptions as { routingKey: string },
          );

    return {
      __brand: "EventConsumerResult",
      consumer,
      binding,
      exchange: sourceExchange,
      queue,
      exchangeBinding: e2eBinding,
      bridgeExchange,
    } as EventConsumerResult<TMessage>;
  }

  const binding = defineQueueBindingInternal(queue, sourceExchange, bindingOptions);
  const consumer = defineConsumer(queue, message);

  return {
    __brand: "EventConsumerResult",
    consumer,
    binding,
    exchange: sourceExchange,
    queue,
    exchangeBinding: undefined,
    bridgeExchange: undefined,
  };
}

/**
 * Type guard to check if a value is an EventPublisherConfig.
 *
 * @param value - The value to check
 * @returns True if the value is an EventPublisherConfig
 */
export function isEventPublisherConfig(
  value: unknown,
): value is EventPublisherConfig<MessageDefinition, ExchangeDefinition, string | undefined> {
  return (
    typeof value === "object" &&
    value !== null &&
    "__brand" in value &&
    value.__brand === "EventPublisherConfig"
  );
}

/**
 * Type guard to check if a value is an EventConsumerResult.
 *
 * @param value - The value to check
 * @returns True if the value is an EventConsumerResult
 */
export function isEventConsumerResult(
  value: unknown,
): value is EventConsumerResult<MessageDefinition> {
  return (
    typeof value === "object" &&
    value !== null &&
    "__brand" in value &&
    value.__brand === "EventConsumerResult"
  );
}
