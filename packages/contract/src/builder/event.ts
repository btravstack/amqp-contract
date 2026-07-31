import { brand, brandOf } from "../brand.js";
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
import type { MatchingBindingPattern, RoutingKey } from "./routing-types.js";
import { _internal_assertRoutingKeyPresent } from "./validate.js";

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
  readonly [brand]: "EventPublisherConfig";
  /** The exchange to publish to */
  exchange: TExchange;
  /** The message definition */
  message: TMessage;
  /** The routing key for direct/topic exchanges */
  routingKey: TRoutingKey;
  /**
   * Default AMQP binding arguments for consumers of this event.
   *
   * These are NOT publish arguments — they are applied to the queue binding
   * of every `defineEventConsumer` of this event that does not pass its own
   * `arguments` option.
   */
  bindingArguments?: Record<string, unknown>;
  /**
   * Declares that this event's consumers live outside this contract — a
   * separate service or deployment owns the binding.
   *
   * Carried onto the publisher definition that `defineContract` extracts, so
   * it opts the event out of the define-time routability check.
   *
   * @see PublisherDefinition.externalConsumers
   */
  externalConsumers?: boolean | undefined;
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
  readonly [brand]: "EventConsumerResult";
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
 * @param options - Optional configuration
 * @param options.bindingArguments - Default AMQP binding arguments applied to
 *   this event's consumers' queue bindings (a consumer's own `arguments`
 *   option takes precedence)
 * @param options.externalConsumers - Declare that this event's consumers are
 *   owned by another service, opting the event out of `defineContract`'s
 *   define-time routability check
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
    bindingArguments?: Record<string, unknown>;
    externalConsumers?: boolean;
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
 * @param options - Optional configuration
 * @param options.bindingArguments - Default AMQP binding arguments applied to
 *   this event's consumers' queue bindings (a consumer's own `arguments`
 *   option takes precedence)
 * @param options.externalConsumers - Declare that this event's consumers are
 *   owned by another service, opting the event out of `defineContract`'s
 *   define-time routability check
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
    bindingArguments?: Record<string, unknown>;
    externalConsumers?: boolean;
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
 * @param options.bindingArguments - Default AMQP binding arguments applied to
 *   this event's consumers' queue bindings (a consumer's own `arguments`
 *   option takes precedence)
 * @param options.externalConsumers - Declare that this event's consumers are
 *   owned by another service, opting the event out of `defineContract`'s
 *   define-time routability check
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
    bindingArguments?: Record<string, unknown>;
    externalConsumers?: boolean;
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
 * @param options.bindingArguments - Default AMQP binding arguments applied to
 *   this event's consumers' queue bindings (a consumer's own `arguments`
 *   option takes precedence)
 * @param options.externalConsumers - Declare that this event's consumers are
 *   owned by another service, opting the event out of `defineContract`'s
 *   define-time routability check
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
    bindingArguments?: Record<string, unknown>;
    externalConsumers?: boolean;
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
    bindingArguments?: Record<string, unknown>;
    externalConsumers?: boolean | undefined;
  },
): EventPublisherConfig<TMessage, ExchangeDefinition, string | undefined> {
  if (exchange.type === "direct" || exchange.type === "topic") {
    _internal_assertRoutingKeyPresent(
      "Event publisher",
      exchange.name,
      exchange.type,
      options?.routingKey,
    );
  }

  const config: EventPublisherConfig<TMessage, ExchangeDefinition, string | undefined> = {
    [brand]: "EventPublisherConfig",
    exchange,
    message,
    routingKey: options?.routingKey,
  };

  if (options?.bindingArguments !== undefined) {
    config.bindingArguments = options.bindingArguments;
  }
  if (options?.externalConsumers !== undefined) {
    config.externalConsumers = options.externalConsumers;
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
 * @param options.routingKey - Override routing key with a pattern that can
 *   match the publisher's routing key (defaults to the publisher's key). A
 *   pattern that can never match is a compile-time error.
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
    routingKey?: MatchingBindingPattern<TConsumerRoutingKey, TRoutingKey>;
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
 * @param options.routingKey - Override routing key with a pattern that can
 *   match the publisher's routing key (defaults to the publisher's key). A
 *   pattern that can never match the publisher's concrete routing key — e.g.
 *   `user.*` against `order.created` — is a compile-time error, because the
 *   binding would silently receive nothing at runtime.
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
 *
 * // A pattern that can never match the publisher's key fails to compile:
 * // defineEventConsumer(orderCreatedEvent, allQueue, { routingKey: 'user.*' });
 * // Error: binding pattern 'user.*' can never match the publisher routing key 'order.created'
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
    routingKey?: MatchingBindingPattern<TConsumerRoutingKey, TRoutingKey>;
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
  const bindingArguments = options?.arguments ?? eventPublisher.bindingArguments;
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
      [brand]: "EventConsumerResult",
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
    [brand]: "EventConsumerResult",
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
  return brandOf(value) === "EventPublisherConfig";
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
  return brandOf(value) === "EventConsumerResult";
}
