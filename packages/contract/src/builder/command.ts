import { brand, brandOf } from "../brand.js";
import type {
  ConsumerDefinition,
  DirectExchangeDefinition,
  ExchangeBindingDefinition,
  ExchangeDefinition,
  FanoutExchangeDefinition,
  HeadersExchangeDefinition,
  MessageDefinition,
  PublisherDefinition,
  QueueBindingDefinition,
  QueueDefinition,
  TopicExchangeDefinition,
} from "../types.js";
import { defineExchangeBinding, defineQueueBindingInternal } from "./binding.js";
import { defineConsumer } from "./consumer.js";
import { definePublisherInternal } from "./publisher.js";
import type { BindingPattern, RoutingKey } from "./routing-types.js";

/** Exchange types that ignore the routing key. */
type KeylessExchange = FanoutExchangeDefinition | HeadersExchangeDefinition;

/**
 * Configuration for a command consumer.
 *
 * Commands are sent by one or more publishers to a single consumer (task queue pattern).
 * The consumer "owns" the queue, and publishers send commands to it.
 *
 * @template TMessage - The message definition
 * @template TExchange - The exchange definition
 * @template TRoutingKey - The routing key type (undefined for fanout and headers exchanges)
 */
export type CommandConsumerConfig<
  TMessage extends MessageDefinition,
  TExchange extends ExchangeDefinition,
  TRoutingKey extends string | undefined = undefined,
  TQueue extends QueueDefinition = QueueDefinition,
> = {
  /** Discriminator to identify this as a command consumer config */
  readonly [brand]: "CommandConsumerConfig";
  /** The consumer definition for processing commands */
  consumer: ConsumerDefinition<TMessage>;
  /** The binding connecting the queue to the exchange */
  binding: QueueBindingDefinition;
  /** The exchange that receives commands */
  exchange: TExchange;
  /** The queue this consumer reads from */
  queue: TQueue;
  /** The message definition */
  message: TMessage;
  /** The routing key pattern for the binding */
  routingKey: TRoutingKey;
};

/**
 * Configuration for a bridged command publisher.
 *
 * A bridged publisher publishes to a bridge exchange (local domain), which forwards
 * messages to the target exchange (remote domain) via an exchange-to-exchange binding.
 *
 * @template TMessage - The message definition
 * @template TBridgeExchange - The bridge (local domain) exchange definition
 * @template TTargetExchange - The target (remote domain) exchange definition
 */
export type BridgedPublisherConfig<
  TMessage extends MessageDefinition,
  TBridgeExchange extends ExchangeDefinition,
  TTargetExchange extends ExchangeDefinition,
> = {
  /** Discriminator to identify this as a bridged publisher config */
  readonly [brand]: "BridgedPublisherConfig";
  /** The publisher definition (publishes to bridge exchange) */
  publisher: PublisherDefinition<TMessage>;
  /** The exchange-to-exchange binding (bridge → target) */
  exchangeBinding: ExchangeBindingDefinition;
  /** The bridge (local domain) exchange */
  bridgeExchange: TBridgeExchange;
  /** The target (remote domain) exchange */
  targetExchange: TTargetExchange;
};

/**
 * The trailing options argument of {@link defineCommandConsumer}, chosen by
 * the exchange type: optional for fanout / headers (no routing key), required
 * with a concrete `routingKey` for direct, required with a binding pattern for
 * topic. One signature rather than one overload per exchange type, so a
 * forgotten routing key is reported against the options instead of as "the
 * exchange is not a fanout or headers exchange".
 */
type CommandConsumerOptionsArgs<
  TExchange extends ExchangeDefinition,
  TRoutingKey extends string,
> = [TExchange] extends [KeylessExchange]
  ? [options?: { arguments?: Record<string, unknown> }]
  : [TExchange] extends [DirectExchangeDefinition]
    ? [options: { routingKey: RoutingKey<TRoutingKey>; arguments?: Record<string, unknown> }]
    : [options: { routingKey: BindingPattern<TRoutingKey>; arguments?: Record<string, unknown> }];

/**
 * Define a command consumer.
 *
 * Commands are sent by publishers to a specific queue. The consumer "owns" the
 * queue and defines what commands it accepts; publishers are derived from it
 * with {@link defineCommandPublisher}.
 *
 * The exchange type decides the options:
 * - **fanout / headers**: no routing key. `options` is optional.
 * - **direct**: `routingKey` is required and concrete (matched exactly).
 * - **topic**: `routingKey` is required and may be a pattern (`*` one word,
 *   `#` zero or more); publishers then send concrete keys matching it.
 *
 * @param queue - The queue that will receive commands
 * @param exchange - The exchange that routes commands
 * @param message - The message definition (schema and metadata)
 * @param options - Binding configuration (required for direct and topic exchanges)
 * @param options.routingKey - The routing key (direct) or pattern (topic) for the binding
 * @param options.arguments - Additional AMQP arguments
 * @returns A command consumer configuration
 *
 * @example
 * ```typescript
 * // Keyless exchange
 * const tasksExchange = defineExchange('tasks', { type: 'fanout' });
 * const taskMessage = defineMessage(z.object({ taskId: z.string() }));
 * const executeTask = defineCommandConsumer(taskQueue, tasksExchange, taskMessage);
 * const sendTask = defineCommandPublisher(executeTask);
 *
 * // Topic exchange: the consumer binds a pattern, publishers send concrete keys
 * const ordersExchange = defineExchange('orders', { type: 'topic' });
 * const orderMessage = defineMessage(z.object({ orderId: z.string() }));
 * const processOrder = defineCommandConsumer(orderQueue, ordersExchange, orderMessage, {
 *   routingKey: 'order.*',
 * });
 * const createOrder = defineCommandPublisher(processOrder, {
 *   routingKey: 'order.create',
 * });
 * ```
 */
export function defineCommandConsumer<
  TMessage extends MessageDefinition,
  TQueueDefinition extends QueueDefinition,
  TExchange extends ExchangeDefinition,
  TRoutingKey extends string = never,
>(
  queue: TQueueDefinition,
  exchange: TExchange,
  message: TMessage,
  ...options: CommandConsumerOptionsArgs<TExchange, TRoutingKey>
): CommandConsumerConfig<
  TMessage,
  TExchange,
  [TExchange] extends [KeylessExchange] ? undefined : TRoutingKey,
  TQueueDefinition
>;

/*
 * Implementation signature of defineCommandConsumer. (Deliberately a plain
 * comment: a JSDoc `@internal` here makes TypeDoc drop the whole function
 * from the generated API docs.)
 */
export function defineCommandConsumer<TMessage extends MessageDefinition>(
  queue: QueueDefinition,
  exchange: ExchangeDefinition,
  message: TMessage,
  options?: {
    routingKey?: string;
    arguments?: Record<string, unknown>;
  },
): CommandConsumerConfig<TMessage, ExchangeDefinition, string | undefined> {
  const consumer = defineConsumer(queue, message);
  const binding = defineQueueBindingInternal(queue, exchange, options);

  return {
    [brand]: "CommandConsumerConfig",
    consumer,
    binding,
    exchange,
    queue,
    message,
    routingKey: options?.routingKey,
  };
}

/**
 * The bridge exchanges a command to `TExchange` may be published through: the
 * bridge must preserve the target's routing semantics, so a fanout target
 * needs a fanout bridge, a headers target a headers bridge, and a direct or
 * topic target a direct or topic bridge (which keeps the routing key).
 */
type CommandBridgeExchange<TExchange extends ExchangeDefinition> =
  TExchange["type"] extends "fanout"
    ? FanoutExchangeDefinition
    : TExchange["type"] extends "headers"
      ? HeadersExchangeDefinition
      : DirectExchangeDefinition | TopicExchangeDefinition;

/**
 * Options for {@link defineCommandPublisher}, chosen by the target exchange
 * type. Only a topic target accepts a `routingKey` override (a concrete key,
 * typically one matching the consumer's pattern).
 */
type CommandPublisherOptions<
  TExchange extends ExchangeDefinition,
  TBridgeExchange extends ExchangeDefinition,
  TPublisherRoutingKey extends string,
> = [TExchange] extends [TopicExchangeDefinition]
  ? {
      bridgeExchange?: TBridgeExchange;
      routingKey?: RoutingKey<TPublisherRoutingKey>;
      externalConsumers?: boolean;
    }
  : { bridgeExchange?: TBridgeExchange; externalConsumers?: boolean };

/**
 * The publisher {@link defineCommandPublisher} returns when not bridging.
 */
type CommandPublisherResult<
  TMessage extends MessageDefinition,
  TExchange extends ExchangeDefinition,
  TPublisherRoutingKey extends string,
> = [TExchange] extends [KeylessExchange]
  ? { message: TMessage; exchange: TExchange; externalConsumers?: boolean }
  : [TExchange] extends [DirectExchangeDefinition]
    ? {
        message: TMessage;
        exchange: DirectExchangeDefinition;
        routingKey: TPublisherRoutingKey;
        externalConsumers?: boolean;
      }
    : {
        message: TMessage;
        exchange: TopicExchangeDefinition;
        routingKey: TPublisherRoutingKey;
        externalConsumers?: boolean;
      };

/**
 * Create a publisher that sends commands to a command consumer.
 *
 * The publisher targets the consumer's exchange with the consumer's routing
 * key. On a topic exchange, `routingKey` may override it with a concrete key —
 * typically one matching the consumer's binding pattern.
 *
 * When `bridgeExchange` is provided, the publisher publishes to the bridge
 * (local domain) exchange instead, and an exchange-to-exchange binding is
 * created from the bridge to the target. The bridge must preserve the
 * target's routing semantics: fanout↔fanout, headers↔headers, and
 * direct/topic↔direct/topic.
 *
 * @param commandConsumer - The command consumer configuration
 * @param options - Optional publisher configuration
 * @param options.routingKey - Override routing key (topic exchanges only)
 * @param options.bridgeExchange - Publish through this local exchange
 * @param options.externalConsumers - Declare that the command's owner lives in
 *   another service, opting this publisher out of `defineContract`'s
 *   define-time routability check
 * @returns A publisher definition, or a bridged publisher configuration when
 *   `bridgeExchange` is set
 *
 * @example
 * ```typescript
 * // Consumer binds with pattern
 * const processOrder = defineCommandConsumer(orderQueue, topicExchange, orderMessage, {
 *   routingKey: 'order.*',
 * });
 *
 * // Publisher uses concrete key matching the pattern
 * const createOrder = defineCommandPublisher(processOrder, {
 *   routingKey: 'order.create',
 * });
 *
 * // Keyless exchange
 * const executeTask = defineCommandConsumer(taskQueue, fanoutExchange, taskMessage);
 * const sendTask = defineCommandPublisher(executeTask);
 * ```
 */
export function defineCommandPublisher<
  TMessage extends MessageDefinition,
  TExchange extends ExchangeDefinition,
  TRoutingKey extends string | undefined,
  TBridgeExchange extends CommandBridgeExchange<TExchange> = never,
  TPublisherRoutingKey extends string = TRoutingKey & string,
>(
  commandConsumer: CommandConsumerConfig<TMessage, TExchange, TRoutingKey>,
  options?: CommandPublisherOptions<TExchange, TBridgeExchange, TPublisherRoutingKey>,
  // NoInfer: inside `defineContract({ publishers: { … } })` the call has a
  // contextual type, and TypeScript would otherwise infer TBridgeExchange from
  // it, typing an unbridged publisher as bridged.
): [NoInfer<TBridgeExchange>] extends [never]
  ? CommandPublisherResult<TMessage, TExchange, TPublisherRoutingKey>
  : BridgedPublisherConfig<TMessage, NoInfer<TBridgeExchange>, TExchange>;

/*
 * Implementation signature of defineCommandPublisher. (Deliberately a plain
 * comment: a JSDoc `@internal` here makes TypeDoc drop the whole function
 * from the generated API docs.)
 */
export function defineCommandPublisher<TMessage extends MessageDefinition>(
  commandConsumer: CommandConsumerConfig<TMessage, ExchangeDefinition, string | undefined>,
  options?: {
    routingKey?: string;
    bridgeExchange?: ExchangeDefinition;
    externalConsumers?: boolean | undefined;
  },
):
  | PublisherDefinition<TMessage>
  | BridgedPublisherConfig<TMessage, ExchangeDefinition, ExchangeDefinition> {
  const { exchange: targetExchange, message, routingKey: consumerRoutingKey } = commandConsumer;

  // For topic exchanges, publisher can override the routing key
  const publisherRoutingKey = options?.routingKey ?? consumerRoutingKey;

  const bridgeExchange = options?.bridgeExchange;

  // Carried onto the publisher definition in both the bridged and direct
  // forms: whether the command's owner declares its queue in *this* contract
  // is the caller's knowledge, not something either form can infer.
  const externalConsumers: { externalConsumers?: boolean } =
    options?.externalConsumers !== undefined
      ? { externalConsumers: options.externalConsumers }
      : {};

  if (bridgeExchange) {
    // Bridged: publisher publishes to bridge exchange, e2e binding from bridge → target
    const publisherOptions: { routingKey?: string; externalConsumers?: boolean } = {
      ...externalConsumers,
    };
    if (publisherRoutingKey !== undefined) {
      publisherOptions.routingKey = publisherRoutingKey;
    }

    const publisher = definePublisherInternal(bridgeExchange, message, publisherOptions);

    // Create e2e binding: target ← bridge (destination = target, source = bridge)
    const e2eBindingOptions: { routingKey?: string } = {};
    if (publisherRoutingKey !== undefined) {
      e2eBindingOptions.routingKey = publisherRoutingKey;
    }
    const e2eBinding =
      bridgeExchange.type === "fanout" || bridgeExchange.type === "headers"
        ? defineExchangeBinding(targetExchange, bridgeExchange)
        : defineExchangeBinding(
            targetExchange,
            bridgeExchange as DirectExchangeDefinition | TopicExchangeDefinition,
            e2eBindingOptions as { routingKey: string },
          );

    return {
      [brand]: "BridgedPublisherConfig",
      publisher,
      exchangeBinding: e2eBinding,
      bridgeExchange,
      targetExchange,
    };
  }

  const publisherOptions: { routingKey?: string; externalConsumers?: boolean } = {
    ...externalConsumers,
  };
  if (publisherRoutingKey !== undefined) {
    publisherOptions.routingKey = publisherRoutingKey;
  }

  return definePublisherInternal(targetExchange, message, publisherOptions);
}

/**
 * Type guard to check if a value is a CommandConsumerConfig.
 *
 * @param value - The value to check
 * @returns True if the value is a CommandConsumerConfig
 */
export function isCommandConsumerConfig(
  value: unknown,
): value is CommandConsumerConfig<MessageDefinition, ExchangeDefinition, string | undefined> {
  return brandOf(value) === "CommandConsumerConfig";
}

/**
 * Type guard to check if a value is a BridgedPublisherConfig.
 *
 * @param value - The value to check
 * @returns True if the value is a BridgedPublisherConfig
 */
export function isBridgedPublisherConfig(
  value: unknown,
): value is BridgedPublisherConfig<MessageDefinition, ExchangeDefinition, ExchangeDefinition> {
  return brandOf(value) === "BridgedPublisherConfig";
}
