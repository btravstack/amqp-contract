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
  __brand: "CommandConsumerConfig";
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
  __brand: "BridgedPublisherConfig";
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
 * Define a command consumer for receiving commands via fanout exchange.
 *
 * Commands are sent by publishers to a specific queue. The consumer "owns" the
 * queue and defines what commands it accepts.
 *
 * @param queue - The queue that will receive commands
 * @param exchange - The fanout exchange that routes commands
 * @param message - The message definition (schema and metadata)
 * @param options - Optional binding configuration
 * @param options.arguments - Additional AMQP arguments
 * @returns A command consumer configuration
 *
 * @example
 * ```typescript
 * const tasksExchange = defineExchange('tasks', { type: 'fanout' });
 * const taskMessage = defineMessage(z.object({ taskId: z.string() }));
 *
 * // Consumer owns the queue
 * const executeTask = defineCommandConsumer(taskQueue, tasksExchange, taskMessage);
 *
 * // Publishers send commands to it
 * const sendTask = defineCommandPublisher(executeTask);
 * ```
 */
export function defineCommandConsumer<
  TMessage extends MessageDefinition,
  TQueueDefinition extends QueueDefinition,
  TExchange extends FanoutExchangeDefinition,
>(
  queue: TQueueDefinition,
  exchange: TExchange,
  message: TMessage,
  options?: {
    arguments?: Record<string, unknown>;
  },
): CommandConsumerConfig<TMessage, TExchange, undefined, TQueueDefinition>;

/**
 * Define a command consumer for receiving commands via headers exchange.
 *
 * Commands are sent by publishers to a specific queue. The consumer "owns" the
 * queue and defines what commands it accepts.
 *
 * @param queue - The queue that will receive commands
 * @param exchange - The headers exchange that routes commands
 * @param message - The message definition (schema and metadata)
 * @param options - Optional binding configuration
 * @param options.arguments - Additional AMQP arguments
 * @returns A command consumer configuration
 *
 * @example
 * ```typescript
 * const tasksExchange = defineExchange('tasks', { type: 'headers' });
 * const taskMessage = defineMessage(z.object({ taskId: z.string() }));
 *
 * // Consumer owns the queue
 * const executeTask = defineCommandConsumer(taskQueue, tasksExchange, taskMessage);
 *
 * // Publishers send commands to it
 * const sendTask = defineCommandPublisher(executeTask);
 * ```
 */
export function defineCommandConsumer<
  TMessage extends MessageDefinition,
  TQueueDefinition extends QueueDefinition,
  TExchange extends HeadersExchangeDefinition,
>(
  queue: TQueueDefinition,
  exchange: TExchange,
  message: TMessage,
  options?: {
    arguments?: Record<string, unknown>;
  },
): CommandConsumerConfig<TMessage, TExchange, undefined, TQueueDefinition>;

/**
 * Define a command consumer for receiving commands via direct exchange.
 *
 * Commands are sent by publishers with a specific routing key that matches
 * the binding pattern.
 *
 * @param queue - The queue that will receive commands
 * @param exchange - The direct exchange that routes commands
 * @param message - The message definition (schema and metadata)
 * @param options - Configuration with required routing key
 * @param options.routingKey - The routing key for the binding
 * @param options.arguments - Additional AMQP arguments
 * @returns A command consumer configuration
 *
 * @example
 * ```typescript
 * const tasksExchange = defineExchange('tasks', { type: 'direct' });
 * const taskMessage = defineMessage(z.object({ taskId: z.string() }));
 *
 * const executeTask = defineCommandConsumer(taskQueue, tasksExchange, taskMessage, {
 *   routingKey: 'task.execute',
 * });
 *
 * const sendTask = defineCommandPublisher(executeTask);
 * ```
 */
export function defineCommandConsumer<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TQueueDefinition extends QueueDefinition,
  TExchange extends DirectExchangeDefinition,
>(
  queue: TQueueDefinition,
  exchange: TExchange,
  message: TMessage,
  options: {
    routingKey: RoutingKey<TRoutingKey>;
    arguments?: Record<string, unknown>;
  },
): CommandConsumerConfig<TMessage, TExchange, TRoutingKey, TQueueDefinition>;

/**
 * Define a command consumer for receiving commands via topic exchange.
 *
 * The consumer binds with a routing key pattern (can use * and # wildcards).
 * Publishers then send commands with concrete routing keys that match the pattern.
 *
 * @param queue - The queue that will receive commands
 * @param exchange - The topic exchange that routes commands
 * @param message - The message definition (schema and metadata)
 * @param options - Configuration with required routing key pattern
 * @param options.routingKey - The routing key pattern for the binding
 * @param options.arguments - Additional AMQP arguments
 * @returns A command consumer configuration
 *
 * @example
 * ```typescript
 * const ordersExchange = defineExchange('orders', { type: 'topic' });
 * const orderMessage = defineMessage(z.object({ orderId: z.string() }));
 *
 * // Consumer uses pattern to receive multiple command types
 * const processOrder = defineCommandConsumer(orderQueue, ordersExchange, orderMessage, {
 *   routingKey: 'order.*',
 * });
 *
 * // Publishers send with concrete keys
 * const createOrder = defineCommandPublisher(processOrder, {
 *   routingKey: 'order.create',
 * });
 * const updateOrder = defineCommandPublisher(processOrder, {
 *   routingKey: 'order.update',
 * });
 * ```
 */
export function defineCommandConsumer<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TQueueDefinition extends QueueDefinition,
  TExchange extends TopicExchangeDefinition,
>(
  queue: TQueueDefinition,
  exchange: TExchange,
  message: TMessage,
  options: {
    routingKey: BindingPattern<TRoutingKey>;
    arguments?: Record<string, unknown>;
  },
): CommandConsumerConfig<TMessage, TExchange, TRoutingKey, TQueueDefinition>;

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
    __brand: "CommandConsumerConfig",
    consumer,
    binding,
    exchange,
    queue,
    message,
    routingKey: options?.routingKey,
  };
}

/**
 * Create a bridged publisher that sends commands to a fanout exchange consumer via a bridge exchange.
 *
 * @param commandConsumer - The command consumer configuration
 * @param options - Configuration with required bridgeExchange
 * @param options.bridgeExchange - The local domain exchange to bridge through (must be fanout to match target)
 * @returns A bridged publisher configuration
 */
export function defineCommandPublisher<
  TMessage extends MessageDefinition,
  TExchange extends FanoutExchangeDefinition,
  TBridgeExchange extends FanoutExchangeDefinition,
>(
  commandConsumer: CommandConsumerConfig<TMessage, TExchange, undefined>,
  options: {
    bridgeExchange: TBridgeExchange;
  },
): BridgedPublisherConfig<TMessage, TBridgeExchange, TExchange>;

/**
 * Create a bridged publisher that sends commands to a headers exchange consumer via a bridge exchange.
 *
 * @param commandConsumer - The command consumer configuration
 * @param options - Configuration with required bridgeExchange
 * @param options.bridgeExchange - The local domain exchange to bridge through (must be headers to match target)
 * @returns A bridged publisher configuration
 */
export function defineCommandPublisher<
  TMessage extends MessageDefinition,
  TExchange extends HeadersExchangeDefinition,
  TBridgeExchange extends HeadersExchangeDefinition,
>(
  commandConsumer: CommandConsumerConfig<TMessage, TExchange, undefined>,
  options: {
    bridgeExchange: TBridgeExchange;
  },
): BridgedPublisherConfig<TMessage, TBridgeExchange, TExchange>;

/**
 * Create a bridged publisher that sends commands to a direct exchange consumer via a bridge exchange.
 *
 * @param commandConsumer - The command consumer configuration
 * @param options - Configuration with required bridgeExchange
 * @param options.bridgeExchange - The bridge exchange (must be direct or topic to preserve routing keys)
 * @returns A bridged publisher configuration
 */
export function defineCommandPublisher<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TExchange extends DirectExchangeDefinition,
  TBridgeExchange extends DirectExchangeDefinition | TopicExchangeDefinition,
>(
  commandConsumer: CommandConsumerConfig<TMessage, TExchange, TRoutingKey>,
  options: {
    bridgeExchange: TBridgeExchange;
  },
): BridgedPublisherConfig<TMessage, TBridgeExchange, TExchange>;

/**
 * Create a bridged publisher that sends commands to a topic exchange consumer via a bridge exchange.
 *
 * @param commandConsumer - The command consumer configuration
 * @param options - Configuration with required bridgeExchange and optional routingKey override
 * @param options.bridgeExchange - The bridge exchange (must be direct or topic to preserve routing keys)
 * @param options.routingKey - Override routing key (must match consumer's pattern)
 * @returns A bridged publisher configuration
 */
export function defineCommandPublisher<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TExchange extends TopicExchangeDefinition,
  TBridgeExchange extends DirectExchangeDefinition | TopicExchangeDefinition,
  TPublisherRoutingKey extends string = TRoutingKey,
>(
  commandConsumer: CommandConsumerConfig<TMessage, TExchange, TRoutingKey>,
  options: {
    bridgeExchange: TBridgeExchange;
    routingKey?: RoutingKey<TPublisherRoutingKey>;
  },
): BridgedPublisherConfig<TMessage, TBridgeExchange, TExchange>;

/**
 * Create a publisher that sends commands to a fanout exchange consumer.
 *
 * @param commandConsumer - The command consumer configuration
 * @returns A publisher definition
 *
 * @example
 * ```typescript
 * const executeTask = defineCommandConsumer(taskQueue, fanoutExchange, taskMessage);
 * const sendTask = defineCommandPublisher(executeTask);
 * ```
 */
export function defineCommandPublisher<TMessage extends MessageDefinition>(
  commandConsumer: CommandConsumerConfig<TMessage, FanoutExchangeDefinition, undefined>,
): { message: TMessage; exchange: FanoutExchangeDefinition };

/**
 * Create a publisher that sends commands to a headers exchange consumer.
 *
 * @param commandConsumer - The command consumer configuration
 * @returns A publisher definition
 *
 * @example
 * ```typescript
 * const executeTask = defineCommandConsumer(taskQueue, headersExchange, taskMessage);
 * const sendTask = defineCommandPublisher(executeTask);
 * ```
 */
export function defineCommandPublisher<TMessage extends MessageDefinition>(
  commandConsumer: CommandConsumerConfig<TMessage, HeadersExchangeDefinition, undefined>,
): { message: TMessage; exchange: HeadersExchangeDefinition };

/**
 * Create a publisher that sends commands to a direct exchange consumer.
 *
 * @param commandConsumer - The command consumer configuration
 * @returns A publisher definition
 */
export function defineCommandPublisher<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
>(
  commandConsumer: CommandConsumerConfig<TMessage, DirectExchangeDefinition, TRoutingKey>,
): { message: TMessage; exchange: DirectExchangeDefinition; routingKey: TRoutingKey };

/**
 * Create a publisher that sends commands to a topic exchange consumer.
 *
 * For topic exchanges where the consumer uses a pattern, the publisher can
 * optionally specify a concrete routing key that matches the pattern.
 *
 * @param commandConsumer - The command consumer configuration
 * @param options - Optional binding configuration
 * @param options.routingKey - Override routing key (must match consumer's pattern)
 * @returns A publisher definition
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
 * ```
 */
export function defineCommandPublisher<
  TMessage extends MessageDefinition,
  TRoutingKey extends string,
  TPublisherRoutingKey extends string = TRoutingKey,
>(
  commandConsumer: CommandConsumerConfig<TMessage, TopicExchangeDefinition, TRoutingKey>,
  options?: {
    routingKey?: RoutingKey<TPublisherRoutingKey>;
  },
): { message: TMessage; exchange: TopicExchangeDefinition; routingKey: TPublisherRoutingKey };

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
  },
):
  | PublisherDefinition<TMessage>
  | BridgedPublisherConfig<TMessage, ExchangeDefinition, ExchangeDefinition> {
  const { exchange: targetExchange, message, routingKey: consumerRoutingKey } = commandConsumer;

  // For topic exchanges, publisher can override the routing key
  const publisherRoutingKey = options?.routingKey ?? consumerRoutingKey;

  const bridgeExchange = options?.bridgeExchange;

  if (bridgeExchange) {
    // Bridged: publisher publishes to bridge exchange, e2e binding from bridge → target
    const publisherOptions: { routingKey?: string } = {};
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
      __brand: "BridgedPublisherConfig",
      publisher,
      exchangeBinding: e2eBinding,
      bridgeExchange,
      targetExchange,
    };
  }

  const publisherOptions: { routingKey?: string } = {};
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
  return (
    typeof value === "object" &&
    value !== null &&
    "__brand" in value &&
    value.__brand === "CommandConsumerConfig"
  );
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
  return (
    typeof value === "object" &&
    value !== null &&
    "__brand" in value &&
    value.__brand === "BridgedPublisherConfig"
  );
}
