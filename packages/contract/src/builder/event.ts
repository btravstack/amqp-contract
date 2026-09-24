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

/** Exchange types that ignore the routing key. */
type KeylessExchange = FanoutExchangeDefinition | HeadersExchangeDefinition;

/** Exchange types that route on the routing key. */
type KeyedExchange = DirectExchangeDefinition | TopicExchangeDefinition;

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
 * Options for an event publisher on a keyless exchange (fanout or headers).
 */
type KeylessEventPublisherOptions = {
  bindingArguments?: Record<string, unknown>;
  externalConsumers?: boolean;
};

/**
 * Options for an event publisher on a direct or topic exchange: the routing
 * key is required and must be concrete (no `*` / `#` wildcards).
 */
type KeyedEventPublisherOptions<TRoutingKey extends string> = KeylessEventPublisherOptions & {
  routingKey: RoutingKey<TRoutingKey>;
};

/**
 * The trailing options argument of {@link defineEventPublisher}, chosen by the
 * exchange type. One signature with a conditional options argument, rather
 * than one overload per exchange type, so a mistake is reported against the
 * options — "Property 'routingKey' is missing" — instead of against whichever
 * overload happened to match the argument count ("DirectExchangeDefinition is
 * not assignable to FanoutExchangeDefinition | HeadersExchangeDefinition").
 */
type EventPublisherOptionsArgs<TExchange extends ExchangeDefinition, TRoutingKey extends string> = [
  TExchange,
] extends [KeylessExchange]
  ? [options?: KeylessEventPublisherOptions]
  : [options: KeyedEventPublisherOptions<TRoutingKey>];

/**
 * Define an event publisher.
 *
 * Events are published without knowing who consumes them. Multiple consumers
 * can subscribe to the same event using `defineEventConsumer`.
 *
 * The exchange type decides the options:
 * - **fanout / headers**: no routing key — fanout broadcasts to every bound
 *   queue, headers routes on header values. `options` is optional.
 * - **direct**: `routingKey` is required; consumers receive messages whose key
 *   matches it exactly.
 * - **topic**: `routingKey` is required and concrete (no wildcards);
 *   consumers can subscribe with `*` / `#` patterns via `defineEventConsumer`.
 *
 * @param exchange - The exchange to publish to
 * @param message - The message definition (schema and metadata)
 * @param options - Publisher configuration (required for direct and topic exchanges)
 * @param options.routingKey - The concrete routing key (direct and topic exchanges only)
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
 * // Keyless exchange: no routing key
 * const logsExchange = defineExchange('logs', { type: 'fanout' });
 * const logMessage = defineMessage(z.object({
 *   level: z.enum(['info', 'warn', 'error']),
 *   message: z.string(),
 * }));
 * const logEvent = defineEventPublisher(logsExchange, logMessage);
 *
 * // Direct exchange: exact routing key
 * const tasksExchange = defineExchange('tasks', { type: 'direct' });
 * const taskMessage = defineMessage(z.object({ taskId: z.string() }));
 * const taskEvent = defineEventPublisher(tasksExchange, taskMessage, {
 *   routingKey: 'task.execute',
 * });
 *
 * // Topic exchange: concrete key; consumers may bind with patterns
 * const ordersExchange = defineExchange('orders', { type: 'topic' });
 * const orderMessage = defineMessage(z.object({
 *   orderId: z.string(),
 *   amount: z.number(),
 * }));
 * const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
 *   routingKey: 'order.created',
 * });
 * const { consumer, binding } = defineEventConsumer(
 *   orderCreatedEvent,
 *   allOrdersQueue,
 *   { routingKey: 'order.*' },
 * );
 * ```
 */
export function defineEventPublisher<
  TMessage extends MessageDefinition,
  TExchange extends ExchangeDefinition,
  TRoutingKey extends string = never,
>(
  exchange: TExchange,
  message: TMessage,
  ...options: EventPublisherOptionsArgs<TExchange, TRoutingKey>
): EventPublisherConfig<
  TMessage,
  TExchange,
  [TExchange] extends [KeylessExchange] ? undefined : TRoutingKey
>;

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
 * The bridge exchanges an event on `TExchange` may be routed through: the
 * bridge must preserve the source's routing semantics, so a fanout source
 * needs a fanout bridge, a headers source a headers bridge, and a direct or
 * topic source a direct or topic bridge (which keeps the routing key).
 */
type EventBridgeExchange<TExchange extends ExchangeDefinition> = TExchange["type"] extends "fanout"
  ? FanoutExchangeDefinition
  : TExchange["type"] extends "headers"
    ? HeadersExchangeDefinition
    : KeyedExchange;

/**
 * Options for {@link defineEventConsumer}, chosen by the source exchange type.
 * Only a topic source accepts a `routingKey` override: a direct exchange
 * matches its key exactly, and fanout / headers exchanges ignore it.
 */
type EventConsumerOptions<
  TExchange extends ExchangeDefinition,
  TRoutingKey extends string | undefined,
  TBridgeExchange extends ExchangeDefinition,
  TConsumerRoutingKey extends string,
> =
  // One conditional around the whole object, not `base & (cond ? {routingKey} : unknown)`:
  // inference through that intersection widens TConsumerRoutingKey to `string`,
  // which silently disables the can-never-match check.
  [TExchange] extends [TopicExchangeDefinition]
    ? {
        bridgeExchange?: TBridgeExchange;
        routingKey?: MatchingBindingPattern<TConsumerRoutingKey, TRoutingKey & string>;
        arguments?: Record<string, unknown>;
      }
    : { bridgeExchange?: TBridgeExchange; arguments?: Record<string, unknown> };

/**
 * Create a consumer that subscribes to an event.
 *
 * The consumer's queue is bound to the event's exchange with the publisher's
 * routing key. The source exchange type decides the options:
 * - **fanout / headers**: no routing key.
 * - **direct**: the publisher's key, which cannot be overridden.
 * - **topic**: `routingKey` may override the publisher's key with a pattern
 *   (`*` one word, `#` zero or more). A pattern that can never match the
 *   publisher's concrete key — e.g. `user.*` against `order.created` — is a
 *   compile-time error, because the binding would silently receive nothing.
 *
 * When `bridgeExchange` is provided, the queue binds to the bridge exchange
 * instead of the source exchange, and an exchange-to-exchange binding is
 * created from the source to the bridge. The bridge must preserve the
 * source's routing semantics: fanout↔fanout, headers↔headers, and
 * direct/topic↔direct/topic.
 *
 * @param eventPublisher - The event publisher configuration
 * @param queue - The queue that will receive messages
 * @param options - Optional binding configuration
 * @param options.routingKey - Override routing key with a pattern that can
 *   match the publisher's routing key (topic exchanges only; defaults to the
 *   publisher's key)
 * @param options.bridgeExchange - Route through this local exchange instead of
 *   binding the queue to the source exchange directly
 * @param options.arguments - Additional AMQP arguments
 * @returns An object with the consumer definition and binding (plus the
 *   exchange-to-exchange binding and bridge exchange when bridging)
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
 *
 * // Keyless exchange
 * const logEvent = defineEventPublisher(logsExchange, logMessage);
 * const { consumer, binding } = defineEventConsumer(logEvent, logsQueue);
 * ```
 */
export function defineEventConsumer<
  TMessage extends MessageDefinition,
  TExchange extends ExchangeDefinition,
  TRoutingKey extends string | undefined,
  TQueueDefinition extends QueueDefinition,
  TBridgeExchange extends EventBridgeExchange<TExchange> = never,
  TConsumerRoutingKey extends string = TRoutingKey & string,
>(
  eventPublisher: EventPublisherConfig<TMessage, TExchange, TRoutingKey>,
  queue: TQueueDefinition,
  options?: EventConsumerOptions<TExchange, TRoutingKey, TBridgeExchange, TConsumerRoutingKey>,
  // NoInfer: inside `defineContract({ consumers: { … } })` the call has a
  // contextual type, and TypeScript would otherwise infer TBridgeExchange from
  // it (its `bridgeExchange` slot), typing an unbridged consumer as bridged.
): [NoInfer<TBridgeExchange>] extends [never]
  ? EventConsumerResult<TMessage, TExchange, TQueueDefinition>
  : EventConsumerResult<
      TMessage,
      TExchange,
      TQueueDefinition,
      ExchangeBindingDefinition,
      NoInfer<TBridgeExchange>
    >;

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
