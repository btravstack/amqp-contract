import type { StandardSchemaV1 } from "@standard-schema/spec";

import type { brand } from "./brand.js";

/**
 * Any schema that conforms to Standard Schema v1.
 *
 * This library supports any validation library that implements the Standard Schema v1 specification,
 * including Zod, Valibot, and ArkType. This allows you to use your preferred validation library
 * while maintaining type safety.
 *
 * @see https://github.com/standard-schema/standard-schema
 */
export type AnySchema = StandardSchemaV1;

/**
 * Infer a Standard Schema's INPUT type — what callers hand to validation
 * (publish payloads, RPC requests) before defaults and transforms run.
 *
 * Canonical home of the helper the client and worker packages alias, so a
 * contract-only package can derive payload types without depending on either:
 * `type OrderCreated = InferSchemaInput<typeof contract.publishers.orderCreated.message.payload>`.
 */
export type InferSchemaInput<TSchema extends StandardSchemaV1> =
  TSchema extends StandardSchemaV1<infer TInput> ? TInput : never;

/**
 * Infer a Standard Schema's OUTPUT type — what validation produces (defaults
 * applied, transforms run); the shape handlers and RPC callers receive.
 */
export type InferSchemaOutput<TSchema extends StandardSchemaV1> =
  TSchema extends StandardSchemaV1<infer _TInput, infer TOutput> ? TOutput : never;

// =============================================================================
// Retry Configuration Types
// =============================================================================

/**
 * TTL-Backoff retry options for exponential backoff with configurable delays.
 *
 * Uses the TTL + wait queue pattern. Failed messages are published to a
 * per-delay-tier wait queue with per-message TTL, then dead-lettered back to
 * the main queue after the TTL expires. One wait queue per distinct backoff
 * delay means a long-delay retry never blocks a short-delay retry; within a
 * tier, head-of-line skew is bounded by the jitter spread (zero when jitter
 * is disabled).
 *
 * **Benefits:** Configurable delays with exponential backoff and jitter.
 * **Limitation:** More topology (one wait queue per distinct delay).
 */
export type TtlBackoffRetryOptions = {
  /**
   * TTL-Backoff mode uses wait queues with per-message TTL for exponential backoff.
   */
  mode: "ttl-backoff";
  /**
   * Maximum retry attempts before sending to DLQ.
   * @minimum 1 - Must be a positive integer (1 or greater)
   * @default 3
   */
  maxRetries?: number;
  /**
   * Initial delay in ms before first retry.
   * @default 1000
   */
  initialDelayMs?: number;
  /**
   * Maximum delay in ms between retries.
   * @default 30000
   */
  maxDelayMs?: number;
  /**
   * Exponential backoff multiplier.
   * @default 2
   */
  backoffMultiplier?: number;
  /**
   * Add jitter to prevent thundering herd.
   * @default true
   */
  jitter?: boolean;
};

/**
 * Immediate-Requeue retry options.
 *
 * Failed messages are requeued immediately.
 * For quorum queues, messages are requeued with `nack(requeue=true)`, and the worker tracks delivery count via the native RabbitMQ `x-delivery-count` header.
 * For classic queues, messages are re-published on the same queue, and the worker tracks delivery count via a custom `x-retry-count` header.
 * When the count exceeds `maxRetries`, the message is automatically dead-lettered (if DLX is configured) or dropped.
 *
 * **Benefits:** Simpler architecture, no wait queues needed, no head-of-queue blocking.
 * **Limitation:** Immediate retries only (no exponential backoff).
 *
 * @see https://www.rabbitmq.com/docs/quorum-queues#poison-message-handling
 */
export type ImmediateRequeueRetryOptions = {
  /**
   * Immediate-Requeue mode.
   */
  mode: "immediate-requeue";
  /**
   * Maximum retry attempts before sending to DLQ.
   * @minimum 1 - Must be a positive integer (1 or greater)
   * @default 3
   */
  maxRetries?: number;
};

/**
 * No retry mode. Failed messages are not retried and are sent
 * directly to DLQ (if configured) or rejected.
 */
export type NoneRetryOptions = {
  /**
   * None mode disables retry attempts entirely.
   */
  mode: "none";
};

/**
 * Retry configuration options.
 *
 * This is a discriminated union based on the `mode` field:
 * - `none` (default): No retry attempts are made; failed messages are handled by DLQ/reject
 * - `immediate-requeue`: Requeues failed messages immediately
 * - `ttl-backoff`: Uses wait queues with exponential backoff
 */
export type RetryOptions = NoneRetryOptions | ImmediateRequeueRetryOptions | TtlBackoffRetryOptions;

/**
 * Resolved TTL-Backoff retry options with all defaults applied.
 *
 * This is what queue definitions carry after `defineQueue` has applied
 * default values. All fields are required.
 */
export type ResolvedTtlBackoffRetryOptions = {
  mode: "ttl-backoff";
  maxRetries: number;
  initialDelayMs: number;
  maxDelayMs: number;
  backoffMultiplier: number;
  jitter: boolean;
};

/**
 * Resolved Immediate-Requeue retry options with all defaults applied.
 *
 * This is what queue definitions carry after `defineQueue` has applied
 * default values. All fields are required.
 */
export type ResolvedImmediateRequeueRetryOptions = {
  mode: "immediate-requeue";
  maxRetries: number;
};

/**
 * Resolved retry configuration stored in queue definitions.
 *
 * This is a discriminated union based on the `mode` field:
 * - `none`: No retry attempts are made; failed messages are handled by DLQ/reject
 * - `immediate-requeue`: Has all immediate-requeue retry options with default applied
 * - `ttl-backoff`: Has all TTL-backoff retry options with defaults applied
 *
 * When using `ttl-backoff` mode, `setupAmqpTopology` derives and declares one
 * wait queue per distinct backoff delay (see `deriveTtlBackoffInfrastructure`).
 */
export type ResolvedRetryOptions =
  | NoneRetryOptions
  | ResolvedImmediateRequeueRetryOptions
  | ResolvedTtlBackoffRetryOptions;

/**
 * Supported compression algorithms for message payloads.
 *
 * - `gzip`: GZIP compression (standard, widely supported, good compression ratio)
 * - `deflate`: DEFLATE compression (faster than gzip, slightly less compression)
 *
 * Compression is configured at runtime via PublishOptions when calling
 * AmqpClient.publish, not at publisher definition time.
 *
 * When compression is enabled, the message payload is compressed before publishing
 * and automatically decompressed when consuming. The `content-encoding` AMQP
 * message property is set to indicate the compression algorithm used.
 *
 * To disable compression, simply omit the `compression` option (it's optional).
 *
 * @example
 * ```typescript
 * // Define a publisher without compression configuration
 * const orderCreatedPublisher = definePublisher(exchange, message, {
 *   routingKey: "order.created",
 * });
 *
 * // Later, choose whether to compress at publish time
 * await client.publish("orderCreated", payload, {
 *   compression: "gzip",
 * });
 * ```
 */
export type CompressionAlgorithm = "gzip" | "deflate";

/**
 * Supported queue types in RabbitMQ.
 *
 * - `quorum`: Quorum queues (default, recommended) - Provide better durability and high-availability
 *   using the Raft consensus algorithm. Best for most production use cases.
 * - `classic`: Classic queues - The traditional RabbitMQ queue type. Use only when you need
 *   specific features not supported by quorum queues (e.g., non-durable queues, priority queues).
 *
 * Note: Quorum queues only support durable queues, and do not support exclusive, auto-deleting, or priority queues.
 *
 * @see https://www.rabbitmq.com/docs/quorum-queues
 *
 * @example
 * ```typescript
 * // Create a quorum queue (default, recommended)
 * const orderQueue = defineQueue('order-processing', {
 *   type: 'quorum', // This is the default
 * });
 *
 * // Create a classic queue (for special cases)
 * const tempQueue = defineQueue('temp-queue', {
 *   type: 'classic',
 *   durable: false, // Only supported with classic queues
 * });
 * ```
 */
export type QueueType = "quorum" | "classic";

/**
 * Common queue options shared between quorum and classic queues.
 */
type BaseQueueOptions = {
  /**
   * Dead letter configuration for handling failed or rejected messages.
   */
  deadLetter?: DeadLetterConfig;

  /**
   * Retry configuration for handling failed message processing.
   *
   * @example
   * ```typescript
   * // No retry
   * const orderQueue = defineQueue('order-processing', {
   *   retry: { mode: 'none' },
   * });
   *
   * // Immediate-requeue mode
   * const orderQueue = defineQueue('order-processing', {
   *   retry: { mode: 'immediate-requeue', maxRetries: 5 },
   * });
   *
   * // TTL-backoff mode with custom options
   * const orderQueue = defineQueue('order-processing', {
   *   retry: {
   *     mode: 'ttl-backoff',
   *     maxRetries: 5,
   *     initialDelayMs: 1000,
   *     maxDelayMs: 30000,
   *   },
   * });
   * ```
   */
  retry?: RetryOptions;

  /**
   * Additional AMQP arguments for advanced configuration.
   */
  arguments?: Record<string, unknown>;
};

/**
 * Options for creating a quorum queue.
 *
 * Quorum queues do not support:
 * - `exclusive` - Use classic queues for connection-scoped queues
 * - `autoDelete` - Use classic queues for auto-deleting queues when consumers disconnect
 * - `maxPriority` - Use classic queues for priority queues
 * - `durable: false` - Use classic queues for non-durable queues
 *
 * Quorum queues provide native retry support for immediate-requeue retry mode:
 * - RabbitMQ tracks delivery count automatically via `x-delivery-count` header
 * - When the limit is exceeded, messages are dead-lettered (if DLX is configured) or dropped
 * - This is simpler than TTL-based retry and avoids head-of-queue blocking issues
 *
 * @example
 * ```typescript
 * const orderQueue = defineQueue('orders', {
 *   type: 'quorum',
 *   deadLetter: { exchange: dlx },
 *   retry: { mode: 'immediate-requeue', maxRetries: 3 } // Message dead-lettered after 3 retry attempts
 * });
 * ```
 */
export type QuorumQueueOptions = BaseQueueOptions & {
  /**
   * Queue type: quorum (default, recommended)
   */
  type?: "quorum";

  /**
   * Quorum queues only support durable queues.
   */
  durable?: true;

  /**
   * Quorum queues do not support exclusive mode.
   * Use type: 'classic' if you need exclusive queues.
   */
  exclusive?: never;

  /**
   * Quorum queues do not support auto-delete mode.
   * Use type: 'classic' if you need auto-deleting queues.
   */
  autoDelete?: never;

  /**
   * Quorum queues do not support priority queues.
   * Use type: 'classic' if you need priority queues.
   */
  maxPriority?: never;
};

/**
 * Options for creating a classic queue.
 *
 * Classic queues support all traditional RabbitMQ features including:
 * - `exclusive` - For connection-scoped queues
 * - `autoDelete` - For auto-deleting queues when consumers disconnect
 * - `maxPriority` - For priority queues
 * - `durable: false` - For non-durable queues
 *
 * @example
 * ```typescript
 * const priorityQueue = defineQueue('tasks', {
 *   type: 'classic',
 *   maxPriority: 10,
 * });
 * ```
 */
export type ClassicQueueOptions = BaseQueueOptions & {
  /**
   * Queue type: classic (for special cases)
   */
  type: "classic";

  /**
   * If true, the queue survives broker restarts. Durable queues are persisted to disk.
   * @default true
   */
  durable?: boolean;

  /**
   * If true, the queue can only be used by the declaring connection and is deleted when
   * that connection closes. Exclusive queues are private to the connection.
   */
  exclusive?: boolean;

  /**
   * If true, the queue is deleted when the last consumer unsubscribes.
   */
  autoDelete?: boolean;

  /**
   * Maximum priority level for priority queue (1-255, recommended: 1-10).
   * Sets x-max-priority argument.
   */
  maxPriority?: number;
};

/**
 * Options for defining a queue. Uses a discriminated union based on the `type` property
 * to enforce quorum queue constraints at compile time.
 *
 * - Quorum queues (default): Do not support `exclusive`, `autoDelete`, or `maxPriority`
 * - Classic queues: Support all options including `exclusive`, `autoDelete`, and `maxPriority`
 */
export type DefineQueueOptions = QuorumQueueOptions | ClassicQueueOptions;

/**
 * Options for defining a queue with a dead letter exchange.
 */
export type DefineQueueOptionsWithDeadLetterExchange<
  TDlx extends ExchangeDefinition = ExchangeDefinition,
> = DefineQueueOptions & { deadLetter: { exchange: TDlx } };

/**
 * Base definition of an AMQP exchange.
 *
 * An exchange receives messages from publishers and routes them to queues based on the exchange
 * type and routing rules. This type contains properties common to all exchange types.
 */
export type BaseExchangeDefinition<TName extends string = string> = {
  /**
   * The name of the exchange. Must be unique within the RabbitMQ virtual host.
   */
  name: TName;

  /**
   * If true, the exchange survives broker restarts. Durable exchanges are persisted to disk.
   * @default true
   */
  durable?: boolean;

  /**
   * If true, the exchange is deleted when all queues have finished using it.
   */
  autoDelete?: boolean;

  /**
   * If true, the exchange cannot be directly published to by clients.
   * It can only receive messages from other exchanges via exchange-to-exchange bindings.
   */
  internal?: boolean;

  /**
   * Additional AMQP arguments for advanced configuration.
   * Common arguments include alternate-exchange for handling unroutable messages.
   */
  arguments?: Record<string, unknown>;
};

/**
 * A topic exchange definition.
 *
 * Topic exchanges route messages to queues based on routing key patterns with wildcards:
 * - `*` (star) matches exactly one word
 * - `#` (hash) matches zero or more words
 *
 * Words are separated by dots (e.g., `order.created.high-value`).
 *
 * @example
 * ```typescript
 * const ordersExchange: TopicExchangeDefinition = defineExchange('orders', {
 *   type: 'topic', // This is the default type, so it can be omitted
 * });
 * // Can be bound with patterns like 'order.*' or 'order.#'
 * ```
 */
export type TopicExchangeDefinition<TName extends string = string> =
  BaseExchangeDefinition<TName> & {
    type: "topic";
  };

/**
 * A direct exchange definition.
 *
 * Direct exchanges route messages to queues based on exact routing key matches.
 * This is ideal for point-to-point messaging where each message should go to specific queues.
 *
 * @example
 * ```typescript
 * const tasksExchange: DirectExchangeDefinition = defineExchange('tasks', {
 *   type: 'direct',
 * });
 * ```
 */
export type DirectExchangeDefinition<TName extends string = string> =
  BaseExchangeDefinition<TName> & {
    type: "direct";
  };

/**
 * A fanout exchange definition.
 *
 * Fanout exchanges broadcast all messages to all bound queues, ignoring routing keys.
 * This is the simplest exchange type for pub/sub messaging patterns.
 *
 * @example
 * ```typescript
 * const logsExchange: FanoutExchangeDefinition = defineExchange('logs', {
 *   type: 'fanout',
 * });
 * ```
 */
export type FanoutExchangeDefinition<TName extends string = string> =
  BaseExchangeDefinition<TName> & {
    type: "fanout";
  };

/**
 * A headers exchange definition.
 *
 * Headers exchanges route messages based on header values rather than routing keys.
 * This is useful for more complex routing scenarios where metadata is important.
 *
 * @example
 * ```typescript
 * const routesExchange: HeadersExchangeDefinition = defineExchange('routes', {
 *   type: 'headers',
 * });
 * ```
 */
export type HeadersExchangeDefinition<TName extends string = string> =
  BaseExchangeDefinition<TName> & {
    type: "headers";
  };

/**
 * Union type of all exchange definitions.
 *
 * Represents any type of AMQP exchange: topic, direct, fanout, headers.
 */
export type ExchangeDefinition<TName extends string = string> =
  | TopicExchangeDefinition<TName>
  | DirectExchangeDefinition<TName>
  | FanoutExchangeDefinition<TName>
  | HeadersExchangeDefinition<TName>;

/**
 * Configuration for dead letter exchange (DLX) on a queue.
 *
 * When a message in a queue is rejected, expires, or exceeds the queue length limit,
 * it can be automatically forwarded to a dead letter exchange for further processing
 * or storage.
 */
export type DeadLetterConfig = {
  /**
   * The exchange to send dead-lettered messages to.
   * This exchange must be declared in the contract.
   */
  exchange: ExchangeDefinition;

  /**
   * Optional routing key to use when forwarding messages to the dead letter exchange.
   * If not specified, the original message routing key is used.
   */
  routingKey?: string;
};

/**
 * Common properties shared by all queue definitions.
 */
export type BaseQueueDefinition<TName extends string = string> = {
  /**
   * The name of the queue. Must be unique within the RabbitMQ virtual host.
   */
  name: TName;

  /**
   * Dead letter configuration for handling failed or rejected messages.
   *
   * When configured, messages that are rejected, expire, or exceed queue limits
   * will be automatically forwarded to the specified dead letter exchange.
   */
  deadLetter?: DeadLetterConfig;

  /**
   * Retry configuration for handling failed message processing.
   * When the queue is created, defaults are applied.
   */
  retry: ResolvedRetryOptions;

  /**
   * Additional AMQP arguments for advanced configuration.
   *
   * Common arguments include:
   * - `x-message-ttl`: Message time-to-live in milliseconds
   * - `x-expires`: Queue expiration time in milliseconds
   * - `x-max-length`: Maximum number of messages in the queue
   * - `x-max-length-bytes`: Maximum size of the queue in bytes
   */
  arguments?: Record<string, unknown>;
};

/**
 * Definition of a quorum queue.
 *
 * Quorum queues provide better durability and high-availability using the Raft consensus algorithm.
 */
export type QuorumQueueDefinition<TName extends string = string> = BaseQueueDefinition<TName> & {
  /**
   * Queue type discriminator: quorum queue.
   */
  type: "quorum";

  /**
   * Quorum queues only support durable queues.
   */
  durable: true;

  /**
   * Quorum queues do not support exclusive mode.
   * Use type: 'classic' if you need exclusive queues.
   */
  exclusive?: never;

  /**
   * Quorum queues do not support auto-delete mode.
   * Use type: 'classic' if you need auto-deleting queues.
   */
  autoDelete?: never;

  /**
   * Quorum queues do not support priority queues.
   * Use type: 'classic' if you need priority queues.
   */
  maxPriority?: never;
};

/**
 * Definition of a classic queue.
 *
 * Classic queues are the traditional RabbitMQ queue type. Use them when you need
 * specific features not supported by quorum queues (e.g., exclusive queues, auto-deleting queues, priority queues).
 */
export type ClassicQueueDefinition<TName extends string = string> = BaseQueueDefinition<TName> & {
  /**
   * Queue type discriminator: classic queue.
   */
  type: "classic";

  /**
   * If true, the queue survives broker restarts. Durable queues are persisted to disk.
   */
  durable: boolean;

  /**
   * If true, the queue can only be used by the declaring connection and is deleted when
   * that connection closes. Exclusive queues are private to the connection.
   */
  exclusive?: boolean;

  /**
   * If true, the queue is deleted when the last consumer unsubscribes.
   */
  autoDelete?: boolean;

  /**
   * Maximum priority level for priority queue (1-255, recommended: 1-10).
   * Sets x-max-priority argument.
   */
  maxPriority?: number;
};

/**
 * Definition of an AMQP queue.
 *
 * A discriminated union based on queue type:
 * - `QuorumQueueDefinition`: For quorum queues (type: "quorum")
 * - `ClassicQueueDefinition`: For classic queues (type: "classic")
 *
 * Use `queue.type` as the discriminator to narrow the type.
 */
export type QueueDefinition<TName extends string = string> =
  | QuorumQueueDefinition<TName>
  | ClassicQueueDefinition<TName>;

/**
 * One derived TTL-backoff wait queue — a per-delay-tier holding queue.
 *
 * Each distinct backoff delay in a queue's retry schedule gets its own wait
 * queue so a long-delay retry can never block a short-delay retry behind it
 * (RabbitMQ only dead-letters expired messages at the head of a queue).
 */
export type TtlBackoffWaitQueueDefinition = {
  /**
   * Broker name of the tier's wait queue: `{queueName}-wait-{delayMs}ms`.
   */
  name: string;
  /**
   * The tier's base backoff delay in ms (pre-jitter).
   */
  delayMs: number;
  /**
   * Queue-level `x-message-ttl` backstop. With jitter enabled this is the
   * jitter ceiling (`ceil(delayMs * 1.5)`); without jitter it equals
   * `delayMs`. The per-message `expiration` carries the actual (jittered)
   * delay; this queue-level TTL bounds head-of-line skew within the tier to
   * the jitter spread (zero when jitter is disabled).
   */
  messageTtlMs: number;
};

/**
 * TTL-backoff retry infrastructure derived from a queue definition.
 *
 * This is computed (never stored on the contract) by
 * `deriveTtlBackoffInfrastructure`: `setupAmqpTopology` declares the wait
 * queues at channel-setup time, and the worker's retry pipeline publishes the
 * retry copy to the tier queue matching the attempt's base delay. Each wait
 * queue dead-letters back to the main queue via the default exchange.
 */
export type TtlBackoffInfrastructure = {
  /**
   * Name of the main queue messages return to after the delay.
   */
  queueName: string;
  /**
   * Queue type shared by the wait queues (mirrors the main queue).
   */
  queueType: QueueType;
  /**
   * Durability shared by the wait queues (mirrors the main queue).
   */
  durable: boolean;
  /**
   * One wait queue per distinct backoff delay, ascending by `delayMs`.
   */
  waitQueues: TtlBackoffWaitQueueDefinition[];
};

/**
 * A queue definition with a dead letter exchange.
 */
export type QueueDefinitionWithDeadLetterExchange<
  TName extends string = string,
  TDlx extends ExchangeDefinition = ExchangeDefinition,
> = QueueDefinition<TName> & {
  deadLetter: { exchange: TDlx };
};

/**
 * Definition of a message with typed payload and optional headers.
 *
 * @template TPayload - The Standard Schema v1 compatible schema for the message payload
 * @template THeaders - The Standard Schema v1 compatible schema for the message headers (optional)
 */
export type MessageDefinition<
  TPayload extends AnySchema = AnySchema,
  THeaders extends StandardSchemaV1<Record<string, unknown>> | undefined =
    | StandardSchemaV1<Record<string, unknown>>
    | undefined,
> = {
  /**
   * The payload schema for validating message content.
   * Must be a Standard Schema v1 compatible schema (Zod, Valibot, ArkType, etc.).
   */
  payload: TPayload;

  /**
   * Optional headers schema for validating message metadata.
   * Must be a Standard Schema v1 compatible schema.
   */
  headers?: THeaders;

  /**
   * Brief description of the message for documentation purposes.
   * Used in AsyncAPI specification generation.
   */
  summary?: string;

  /**
   * Detailed description of the message for documentation purposes.
   * Used in AsyncAPI specification generation.
   */
  description?: string;
};

/**
 * Binding between a queue and an exchange.
 *
 * Defines how messages from an exchange should be routed to a queue.
 * For direct and topic exchanges, a routing key is required.
 * For fanout and headers exchanges, no routing key is needed.
 */
export type QueueBindingDefinition = {
  /** Discriminator indicating this is a queue-to-exchange binding */
  type: "queue";

  /** The queue that will receive messages */
  queue: QueueDefinition;

  /**
   * Additional AMQP arguments for the binding.
   * Can be used for advanced routing scenarios with the headers exchange type.
   */
  arguments?: Record<string, unknown>;
} & (
  | {
      /** Direct or topic exchange requiring a routing key */
      exchange: DirectExchangeDefinition | TopicExchangeDefinition;
      /**
       * The routing key pattern for message routing.
       * For direct exchanges: Must match exactly.
       * For topic exchanges: Can use wildcards (* for one word, # for zero or more words).
       */
      routingKey: string;
    }
  | {
      /** Fanout or headers exchange (no routing key needed) */
      exchange: FanoutExchangeDefinition | HeadersExchangeDefinition;
      /** Fanout and headers exchanges don't use routing keys */
      routingKey?: never;
    }
);

/**
 * Binding between two exchanges (exchange-to-exchange routing).
 *
 * Defines how messages should be forwarded from a source exchange to a destination exchange.
 * This allows for more complex routing topologies.
 *
 * @example
 * ```typescript
 * // Forward high-priority orders to a special processing exchange
 * const binding: ExchangeBindingDefinition = {
 *   type: 'exchange',
 *   source: ordersExchange,
 *   destination: highPriorityExchange,
 *   routingKey: 'order.high-priority.*'
 * };
 * ```
 */
export type ExchangeBindingDefinition = {
  /** Discriminator indicating this is an exchange-to-exchange binding */
  type: "exchange";

  /** The destination exchange that will receive forwarded messages */
  destination: ExchangeDefinition;

  /**
   * Additional AMQP arguments for the binding.
   */
  arguments?: Record<string, unknown>;
} & (
  | {
      /** Direct or topic source exchange requiring a routing key */
      source: DirectExchangeDefinition | TopicExchangeDefinition;
      /**
       * The routing key pattern for message routing.
       * Messages matching this pattern will be forwarded to the destination exchange.
       */
      routingKey: string;
    }
  | {
      /** Fanout or headers source exchange (no routing key needed) */
      source: FanoutExchangeDefinition | HeadersExchangeDefinition;
      /** Fanout and headers exchanges don't use routing keys */
      routingKey?: never;
    }
);

/**
 * Union type of all binding definitions.
 *
 * A binding can be either:
 * - Queue-to-exchange binding: Routes messages from an exchange to a queue
 * - Exchange-to-exchange binding: Forwards messages from one exchange to another
 */
export type BindingDefinition = QueueBindingDefinition | ExchangeBindingDefinition;

/**
 * Definition of a message publisher.
 *
 * A publisher sends messages to an exchange with automatic schema validation.
 * The message payload is validated against the schema before being sent to RabbitMQ.
 *
 * Compression can be optionally applied at publish time by specifying a compression
 * algorithm when calling the publish method.
 *
 * @template TMessage - The message definition with payload schema
 *
 * @example
 * ```typescript
 * const publisher: PublisherDefinition = {
 *   exchange: ordersExchange,
 *   message: orderMessage,
 *   routingKey: 'order.created'
 * };
 * ```
 */
export type PublisherDefinition<TMessage extends MessageDefinition = MessageDefinition> = {
  /** The message definition including the payload schema */
  message: TMessage;
} & (
  | {
      /** Direct or topic exchange requiring a routing key */
      exchange: DirectExchangeDefinition | TopicExchangeDefinition;
      /**
       * The routing key for message routing.
       * Determines which queues will receive the published message.
       */
      routingKey: string;
    }
  | {
      /** Fanout or headers exchange (no routing key needed) */
      exchange: FanoutExchangeDefinition | HeadersExchangeDefinition;
      /** Fanout and headers exchanges don't use routing keys */
      routingKey?: never;
    }
);

/**
 * Definition of a message consumer.
 *
 * A consumer receives and processes messages from a queue with automatic schema validation.
 * The message payload is validated against the schema before being passed to your handler.
 * If the message is compressed (indicated by the content-encoding header), it will be
 * automatically decompressed before validation.
 *
 * @template TMessage - The message definition with payload schema
 *
 * @example
 * ```typescript
 * const consumer: ConsumerDefinition = {
 *   queue: orderProcessingQueue,
 *   message: orderMessage
 * };
 * ```
 */
export type ConsumerDefinition<TMessage extends MessageDefinition = MessageDefinition> = {
  /** The queue to consume messages from */
  queue: QueueDefinition;

  /** The message definition including the payload schema */
  message: TMessage;
};

// =============================================================================
// Event and Command Configuration Types
// =============================================================================

/**
 * Base type for event publisher configuration.
 *
 * This is a simplified type used in ContractDefinition. The full generic type
 * is defined in the builder module.
 *
 * @see defineEventPublisher for creating event publishers
 */
export type EventPublisherConfigBase = {
  readonly [brand]: "EventPublisherConfig";
  exchange: ExchangeDefinition;
  message: MessageDefinition;
  routingKey: string | undefined;
  /** Default binding arguments for this event's consumers (not publish arguments). */
  bindingArguments?: Record<string, unknown>;
};

/**
 * Base type for command consumer configuration.
 *
 * This is a simplified type used in ContractDefinition. The full generic type
 * is defined in the builder module.
 *
 * @see defineCommandConsumer for creating command consumers
 */
export type CommandConsumerConfigBase = {
  readonly [brand]: "CommandConsumerConfig";
  consumer: ConsumerDefinition;
  binding: QueueBindingDefinition;
  exchange: ExchangeDefinition;
  queue: QueueDefinition;
  message: MessageDefinition;
  routingKey: string | undefined;
};

/**
 * Base type for event consumer result.
 *
 * This is a simplified type used in ContractDefinitionInput. The full generic type
 * is defined in the builder module.
 *
 * @see defineEventConsumer for creating event consumers
 */
export type EventConsumerResultBase = {
  readonly [brand]: "EventConsumerResult";
  consumer: ConsumerDefinition;
  binding: QueueBindingDefinition;
  exchange: ExchangeDefinition;
  queue: QueueDefinition;
  exchangeBinding: ExchangeBindingDefinition | undefined;
  bridgeExchange: ExchangeDefinition | undefined;
};

/**
 * Base type for bridged publisher configuration.
 *
 * A bridged publisher publishes to a bridge exchange, which forwards messages
 * to the target exchange via an exchange-to-exchange binding.
 *
 * @see defineCommandPublisher with bridgeExchange option
 */
export type BridgedPublisherConfigBase = {
  readonly [brand]: "BridgedPublisherConfig";
  publisher: PublisherDefinition;
  exchangeBinding: ExchangeBindingDefinition;
  bridgeExchange: ExchangeDefinition;
  targetExchange: ExchangeDefinition;
};

/**
 * A single declared RPC error: the Standard Schema for its `data` payload,
 * plus an optional default human-readable message.
 *
 * The default `message` is used when the handler constructs the error without
 * one (`helpers.errors.CODE(data)`) and as the client-side fallback when a
 * reply arrives without a message on the wire.
 *
 * @see defineRpc for declaring errors on an RPC
 */
export type RpcErrorDefinition<TData extends StandardSchemaV1 = StandardSchemaV1> = {
  /** Schema validating the error's `data` payload (worker before replying, client on receipt). */
  data: TData;
  /** Default human-readable message when the handler does not supply one. */
  message?: string;
};

/**
 * Typed error map for an RPC: error code → {@link RpcErrorDefinition}.
 */
export type RpcErrorMap = Record<string, RpcErrorDefinition>;

/**
 * Definition of an RPC operation: a request/response pair flowing over a
 * request queue with replies routed back via direct reply-to.
 *
 * An RPC is bidirectional on both ends — the server consumes requests and
 * publishes responses; the client publishes requests and consumes responses —
 * so it has its own slot in the contract (`rpcs`) rather than being shoehorned
 * into `consumers` or `publishers`.
 *
 * @template TRequestMessage - The request message definition
 * @template TResponseMessage - The response message definition
 * @template TQueue - The request queue entry
 * @template TErrors - The typed error map (undefined when the RPC declares none)
 *
 * @see defineRpc for creating RPC definitions
 */
export type RpcDefinition<
  TRequestMessage extends MessageDefinition = MessageDefinition,
  TResponseMessage extends MessageDefinition = MessageDefinition,
  TQueue extends QueueDefinition = QueueDefinition,
  TErrors extends RpcErrorMap | undefined = RpcErrorMap | undefined,
> = {
  /** The queue that receives RPC requests. Replies are routed back via direct reply-to. */
  queue: TQueue;
  /** Schema for the request payload (validated on both publish and consume). */
  request: TRequestMessage;
  /** Schema for the response payload (validated on both worker reply and client receive). */
  response: TResponseMessage;
  /**
   * Typed business errors the handler may return via `Err(rpcError(code, data))`.
   * Error data is validated against the declared schema on the worker before
   * the error reply is published, and re-validated on the client when it
   * arrives. Business errors are replied and acked — never retried.
   */
  errors?: TErrors;
};

/**
 * Complete AMQP contract definition (output type).
 *
 * A contract brings together all AMQP resources into a single, type-safe definition.
 * It defines the complete messaging topology including exchanges, queues, bindings,
 * publishers, and consumers.
 *
 * The contract is used by:
 * - Clients (TypedAmqpClient) for type-safe message publishing
 * - Workers (TypedAmqpWorker) for type-safe message consumption
 * - AsyncAPI generator for documentation
 *
 * @example
 * ```typescript
 * const contract: ContractDefinition = {
 *   exchanges: {
 *     orders: ordersExchange,
 *   },
 *   queues: {
 *     orderProcessing: orderProcessingQueue,
 *   },
 *   bindings: {
 *     orderBinding: orderQueueBinding,
 *   },
 *   publishers: {
 *     orderCreated: orderCreatedPublisher,
 *   },
 *   consumers: {
 *     processOrder: processOrderConsumer,
 *   },
 * };
 * ```
 */
export type ContractDefinition = {
  /**
   * Named exchange definitions.
   * Each key becomes available as a named resource in the contract.
   */
  exchanges?: Record<string, ExchangeDefinition>;

  /**
   * Named queue definitions.
   * Each key becomes available as a named resource in the contract.
   *
   * Queues with TTL-backoff retry configured are plain `QueueDefinition`s;
   * their wait queues are derived and declared at topology-setup time, not
   * stored in the contract.
   */
  queues?: Record<string, QueueDefinition>;

  /**
   * Named binding definitions.
   * Bindings can be queue-to-exchange or exchange-to-exchange.
   */
  bindings?: Record<string, BindingDefinition>;

  /**
   * Named publisher definitions.
   * Each key becomes a method on the TypedAmqpClient for publishing messages.
   * The method will be fully typed based on the message schema.
   */
  publishers?: Record<string, PublisherDefinition>;

  /**
   * Named consumer definitions.
   * Each key requires a corresponding handler in the TypedAmqpWorker.
   * The handler will be fully typed based on the message schema.
   */
  consumers?: Record<string, ConsumerDefinition>;

  /**
   * Named RPC definitions. Each key gets:
   * - A handler in the TypedAmqpWorker that returns the typed response.
   * - A `client.call(name, request, options)` method on the TypedAmqpClient.
   *
   * RPC entries do not appear in `publishers` or `consumers` because each
   * end of an RPC plays both roles (publisher of one direction, consumer of
   * the other).
   */
  rpcs?: Record<string, RpcDefinition>;
};

/**
 * Publisher entry that can be passed to defineContract's publishers section.
 *
 * Can be either:
 * - A plain PublisherDefinition from definePublisher
 * - An EventPublisherConfig from defineEventPublisher (auto-extracted to publisher)
 * - An BridgedPublisherConfig from defineCommandPublisher (auto-extracted to publisher)
 */
export type PublisherEntry =
  | PublisherDefinition
  | EventPublisherConfigBase
  | BridgedPublisherConfigBase;

/**
 * Consumer entry that can be passed to defineContract's consumers section.
 *
 * Can be either:
 * - A plain ConsumerDefinition from defineConsumer
 * - An EventConsumerResult from defineEventConsumer (binding auto-extracted)
 * - A CommandConsumerConfig from defineCommandConsumer (binding auto-extracted)
 */
export type ConsumerEntry =
  | ConsumerDefinition
  | EventConsumerResultBase
  | CommandConsumerConfigBase;

/**
 * Contract definition input type with automatic extraction of event/command patterns.
 *
 * Users only define publishers and consumers. Exchanges, queues, and bindings are
 * automatically extracted from these definitions.
 *
 * @example
 * ```typescript
 * const contract = defineContract({
 *   publishers: {
 *     // EventPublisherConfig → auto-extracted to publisher
 *     orderCreated: defineEventPublisher(ordersExchange, orderMessage, { routingKey: "order.created" }),
 *   },
 *   consumers: {
 *     // CommandConsumerConfig → auto-extracted to consumer + binding
 *     processOrder: defineCommandConsumer(orderQueue, ordersExchange, orderMessage, { routingKey: "order.process" }),
 *     // EventConsumerResult → auto-extracted to consumer + binding
 *     notify: defineEventConsumer(orderCreatedEvent, notificationQueue),
 *   },
 * });
 * ```
 *
 * @see defineContract - Processes this input and returns a ContractDefinition
 */
export type ContractDefinitionInput = {
  /**
   * Named publisher definitions.
   *
   * Can accept:
   * - PublisherDefinition from definePublisher
   * - EventPublisherConfig from defineEventPublisher (auto-extracted to publisher)
   */
  publishers?: Record<string, PublisherEntry>;

  /**
   * Named consumer definitions.
   *
   * Can accept:
   * - ConsumerDefinition from defineConsumer
   * - EventConsumerResult from defineEventConsumer (binding auto-extracted)
   * - CommandConsumerConfig from defineCommandConsumer (binding auto-extracted)
   */
  consumers?: Record<string, ConsumerEntry>;

  /**
   * Named RPC definitions from `defineRpc`. Each entry contributes its queue
   * (and DLX if any) to the contract topology and exposes a typed
   * `client.call(name, ...)` / worker handler pair.
   */
  rpcs?: Record<string, RpcDefinition>;

  /**
   * Standalone exchanges — topology this service asserts without publishing
   * to it (e.g. an exchange owned by another domain that bindings reference).
   * Keys are authoring labels; the contract output keys exchanges by name.
   */
  exchanges?: Record<string, ExchangeDefinition>;

  /**
   * Standalone queues — queues with no consumer in this service, asserted by
   * `setupAmqpTopology` all the same. The classic cases: a DLQ bound to the
   * auto-extracted dead-letter exchange, or an audit queue that another
   * process drains. Dead-letter exchanges are auto-extracted exactly as for
   * consumer queues.
   * Keys are authoring labels; the contract output keys queues by name.
   */
  queues?: Record<string, QueueDefinition>;

  /**
   * Standalone bindings — e.g. binding a declared DLQ to the dead-letter
   * exchange. Keys are used verbatim as the binding labels in the output.
   */
  bindings?: Record<string, BindingDefinition>;
};

// =============================================================================
// Contract Output Type Inference Helpers
// =============================================================================

/**
 * Extract the exchange from a publisher entry.
 * @internal
 */
type ExtractPublisherExchange<T extends PublisherEntry> = T extends BridgedPublisherConfigBase
  ? T["bridgeExchange"]
  : T extends EventPublisherConfigBase
    ? T["exchange"]
    : T extends PublisherDefinition
      ? T["exchange"]
      : never;

/**
 * Extract the dead letter exchange from a queue definition type.
 * Handles the DLX intersection from defineQueue overloads.
 * @internal
 */
export type ExtractDlxFromEntry<T extends QueueDefinition> = T extends {
  deadLetter: { exchange: infer E extends ExchangeDefinition };
}
  ? E
  : never;

/**
 * Extract the queue from a consumer entry.
 * @internal
 */
type ExtractConsumerQueue<T extends ConsumerEntry> = T extends EventConsumerResultBase
  ? T["queue"]
  : T extends CommandConsumerConfigBase
    ? T["queue"]
    : T extends ConsumerDefinition
      ? T["queue"]
      : never;

/**
 * Extract the exchange from a consumer entry (from binding).
 * @internal
 */
type ExtractConsumerExchange<T extends ConsumerEntry> = T extends EventConsumerResultBase
  ? T["exchange"]
  : T extends CommandConsumerConfigBase
    ? T["exchange"]
    : never;

/**
 * Extract the binding from a consumer entry.
 * @internal
 */
type ExtractConsumerBinding<T extends ConsumerEntry> = T extends EventConsumerResultBase
  ? T["binding"]
  : T extends CommandConsumerConfigBase
    ? T["binding"]
    : never;

/**
 * Check if a consumer entry has a binding.
 * @internal
 */
type HasBinding<T extends ConsumerEntry> = T extends EventConsumerResultBase
  ? true
  : T extends CommandConsumerConfigBase
    ? true
    : false;

/**
 * Extract exchanges from all publishers in a contract.
 * @internal
 */
type ExtractExchangesFromPublishers<TPublishers extends Record<string, PublisherEntry>> = {
  [K in keyof TPublishers as ExtractPublisherExchange<
    TPublishers[K]
  >["name"]]: ExtractPublisherExchange<TPublishers[K]>;
};

/**
 * Extract exchanges from all consumers in a contract.
 * @internal
 */
type ExtractExchangesFromConsumers<TConsumers extends Record<string, ConsumerEntry>> = {
  [K in keyof TConsumers as ExtractConsumerExchange<TConsumers[K]> extends ExchangeDefinition
    ? ExtractConsumerExchange<TConsumers[K]>["name"]
    : never]: ExtractConsumerExchange<TConsumers[K]> extends ExchangeDefinition
    ? ExtractConsumerExchange<TConsumers[K]>
    : never;
};

/**
 * Extract the dead letter exchange from a consumer entry.
 * @internal
 */
type ExtractDeadLetterExchange<T extends ConsumerEntry> = ExtractDlxFromEntry<T["queue"]>;

/**
 * Extract dead letter exchanges from all consumers in a contract.
 * @internal
 */
type ExtractDeadLetterExchangesFromConsumers<TConsumers extends Record<string, ConsumerEntry>> = {
  [K in keyof TConsumers as ExtractDeadLetterExchange<TConsumers[K]> extends never
    ? never
    : ExtractDeadLetterExchange<TConsumers[K]>["name"]]: ExtractDeadLetterExchange<TConsumers[K]>;
};

/**
 * Extract queues from all consumers in a contract.
 * @internal
 */
type ExtractQueuesFromConsumers<TConsumers extends Record<string, ConsumerEntry>> = {
  [K in keyof TConsumers as ExtractConsumerQueue<TConsumers[K]>["name"]]: ExtractConsumerQueue<
    TConsumers[K]
  >;
};

/**
 * Extract bindings from all consumers in a contract.
 * @internal
 */
type ExtractBindingsFromConsumers<TConsumers extends Record<string, ConsumerEntry>> = {
  [K in keyof TConsumers as HasBinding<TConsumers[K]> extends true
    ? `${K & string}Binding`
    : never]: ExtractConsumerBinding<TConsumers[K]>;
};

/**
 * Extract the consumer definition from a consumer entry.
 * @internal
 */
type ExtractConsumerDefinition<T extends ConsumerEntry> = T extends EventConsumerResultBase
  ? T["consumer"]
  : T extends CommandConsumerConfigBase
    ? T["consumer"]
    : T extends ConsumerDefinition
      ? T
      : never;

/**
 * Extract consumer definitions from all consumers in a contract.
 * @internal
 */
type ExtractConsumerDefinitions<TConsumers extends Record<string, ConsumerEntry>> = {
  [K in keyof TConsumers]: ExtractConsumerDefinition<TConsumers[K]>;
};

/**
 * Extract the publisher definition from a publisher entry.
 * @internal
 */
type ExtractPublisherDefinition<T extends PublisherEntry> = T extends BridgedPublisherConfigBase
  ? T["publisher"]
  : T extends EventPublisherConfigBase
    ? PublisherDefinition<T["message"]> &
        (T["exchange"] extends DirectExchangeDefinition | TopicExchangeDefinition
          ? { exchange: T["exchange"]; routingKey: T["routingKey"] & string }
          : { exchange: T["exchange"]; routingKey?: never })
    : T extends PublisherDefinition
      ? T
      : never;

/**
 * Extract publisher definitions from all publishers in a contract.
 * @internal
 */
type ExtractPublisherDefinitions<TPublishers extends Record<string, PublisherEntry>> = {
  [K in keyof TPublishers]: ExtractPublisherDefinition<TPublishers[K]>;
};

/**
 * Extract the bridge exchange from a consumer entry (when bridgeExchange is set).
 * @internal
 */
type ExtractBridgeExchangeFromConsumer<T extends ConsumerEntry> = T extends EventConsumerResultBase
  ? T["bridgeExchange"] extends ExchangeDefinition
    ? T["bridgeExchange"]
    : never
  : never;

/**
 * Extract bridge exchanges from all consumers in a contract.
 * @internal
 */
type ExtractBridgeExchangesFromConsumers<TConsumers extends Record<string, ConsumerEntry>> = {
  [K in keyof TConsumers as ExtractBridgeExchangeFromConsumer<TConsumers[K]> extends never
    ? never
    : ExtractBridgeExchangeFromConsumer<TConsumers[K]>["name"]]: ExtractBridgeExchangeFromConsumer<
    TConsumers[K]
  >;
};

/**
 * Extract the target exchange from a bridged publisher entry.
 * @internal
 */
type ExtractTargetExchangeFromPublisher<T extends PublisherEntry> =
  T extends BridgedPublisherConfigBase ? T["targetExchange"] : never;

/**
 * Extract target exchanges from all publishers in a contract.
 * @internal
 */
type ExtractTargetExchangesFromPublishers<TPublishers extends Record<string, PublisherEntry>> = {
  [K in keyof TPublishers as ExtractTargetExchangeFromPublisher<TPublishers[K]> extends never
    ? never
    : ExtractTargetExchangeFromPublisher<
        TPublishers[K]
      >["name"]]: ExtractTargetExchangeFromPublisher<TPublishers[K]>;
};

/**
 * Check if a consumer entry has an exchange binding (e2e).
 * @internal
 */
type HasConsumerExchangeBinding<T extends ConsumerEntry> = T extends EventConsumerResultBase
  ? T["exchangeBinding"] extends ExchangeBindingDefinition
    ? true
    : false
  : false;

/**
 * Extract the exchange binding from a consumer entry.
 * @internal
 */
type ExtractConsumerExchangeBinding<T extends ConsumerEntry> = T extends EventConsumerResultBase
  ? T["exchangeBinding"] extends ExchangeBindingDefinition
    ? T["exchangeBinding"]
    : never
  : never;

/**
 * Extract exchange bindings from all consumers in a contract.
 * @internal
 */
type ExtractExchangeBindingsFromConsumers<TConsumers extends Record<string, ConsumerEntry>> = {
  [K in keyof TConsumers as HasConsumerExchangeBinding<TConsumers[K]> extends true
    ? `${K & string}ExchangeBinding`
    : never]: ExtractConsumerExchangeBinding<TConsumers[K]>;
};

/**
 * Check if a publisher entry has an exchange binding (bridged).
 * @internal
 */
type HasPublisherExchangeBinding<T extends PublisherEntry> = T extends BridgedPublisherConfigBase
  ? true
  : false;

/**
 * Extract the exchange binding from a bridged publisher entry.
 * @internal
 */
type ExtractPublisherExchangeBinding<T extends PublisherEntry> =
  T extends BridgedPublisherConfigBase ? T["exchangeBinding"] : never;

/**
 * Extract exchange bindings from all publishers in a contract.
 * @internal
 */
type ExtractExchangeBindingsFromPublishers<TPublishers extends Record<string, PublisherEntry>> = {
  [K in keyof TPublishers as HasPublisherExchangeBinding<TPublishers[K]> extends true
    ? `${K & string}ExchangeBinding`
    : never]: ExtractPublisherExchangeBinding<TPublishers[K]>;
};

/**
 * Extract queues from all RPC entries in a contract.
 * @internal
 */
type ExtractQueuesFromRpcs<TRpcs extends Record<string, RpcDefinition>> = {
  [K in keyof TRpcs as TRpcs[K]["queue"]["name"]]: TRpcs[K]["queue"];
};

/**
 * Extract dead letter exchanges from all RPC entries in a contract.
 * @internal
 */
type ExtractDeadLetterExchangesFromRpcs<TRpcs extends Record<string, RpcDefinition>> = {
  [K in keyof TRpcs as ExtractDlxFromEntry<TRpcs[K]["queue"]> extends never
    ? never
    : ExtractDlxFromEntry<TRpcs[K]["queue"]>["name"]]: ExtractDlxFromEntry<TRpcs[K]["queue"]>;
};

/**
 * Re-key standalone exchange declarations by exchange name.
 * @internal
 */
type ExtractStandaloneExchanges<T extends Record<string, ExchangeDefinition>> = {
  [K in keyof T as T[K]["name"]]: T[K];
};

/**
 * Re-key standalone queue declarations by queue name.
 * @internal
 */
type ExtractStandaloneQueues<T extends Record<string, QueueDefinition>> = {
  [K in keyof T as T[K]["name"]]: T[K];
};

/**
 * Dead-letter exchanges of standalone queue declarations.
 * @internal
 */
type ExtractDeadLetterExchangesFromStandaloneQueues<T extends Record<string, QueueDefinition>> = {
  [K in keyof T as ExtractDlxFromEntry<T[K]> extends never
    ? never
    : ExtractDlxFromEntry<T[K]>["name"]]: ExtractDlxFromEntry<T[K]>;
};

/**
 * Contract output type with all resources extracted and properly typed.
 *
 * This type represents the fully expanded contract with:
 * - exchanges: Extracted from publishers and consumer bindings, plus standalone declarations
 * - queues: Extracted from consumers and RPCs, plus standalone declarations
 * - bindings: Extracted from event/command consumers, plus standalone declarations
 * - publishers: Normalized publisher definitions
 * - consumers: Normalized consumer definitions
 */
export type ContractOutput<TContract extends ContractDefinitionInput> = {
  exchanges: (TContract["publishers"] extends Record<string, PublisherEntry>
    ? ExtractExchangesFromPublishers<TContract["publishers"]>
    : {}) &
    (TContract["exchanges"] extends Record<string, ExchangeDefinition>
      ? ExtractStandaloneExchanges<TContract["exchanges"]>
      : {}) &
    (TContract["queues"] extends Record<string, QueueDefinition>
      ? ExtractDeadLetterExchangesFromStandaloneQueues<TContract["queues"]>
      : {}) &
    (TContract["consumers"] extends Record<string, ConsumerEntry>
      ? ExtractExchangesFromConsumers<TContract["consumers"]>
      : {}) &
    (TContract["consumers"] extends Record<string, ConsumerEntry>
      ? ExtractDeadLetterExchangesFromConsumers<TContract["consumers"]>
      : {}) &
    (TContract["consumers"] extends Record<string, ConsumerEntry>
      ? ExtractBridgeExchangesFromConsumers<TContract["consumers"]>
      : {}) &
    (TContract["publishers"] extends Record<string, PublisherEntry>
      ? ExtractTargetExchangesFromPublishers<TContract["publishers"]>
      : {}) &
    (TContract["rpcs"] extends Record<string, RpcDefinition>
      ? ExtractDeadLetterExchangesFromRpcs<TContract["rpcs"]>
      : {});
  queues: (TContract["consumers"] extends Record<string, ConsumerEntry>
    ? ExtractQueuesFromConsumers<TContract["consumers"]>
    : {}) &
    (TContract["rpcs"] extends Record<string, RpcDefinition>
      ? ExtractQueuesFromRpcs<TContract["rpcs"]>
      : {}) &
    (TContract["queues"] extends Record<string, QueueDefinition>
      ? ExtractStandaloneQueues<TContract["queues"]>
      : {});
  bindings: (TContract["consumers"] extends Record<string, ConsumerEntry>
    ? ExtractBindingsFromConsumers<TContract["consumers"]>
    : {}) &
    (TContract["consumers"] extends Record<string, ConsumerEntry>
      ? ExtractExchangeBindingsFromConsumers<TContract["consumers"]>
      : {}) &
    (TContract["publishers"] extends Record<string, PublisherEntry>
      ? ExtractExchangeBindingsFromPublishers<TContract["publishers"]>
      : {}) &
    (TContract["bindings"] extends Record<string, BindingDefinition> ? TContract["bindings"] : {});
  publishers: TContract["publishers"] extends Record<string, PublisherEntry>
    ? ExtractPublisherDefinitions<TContract["publishers"]>
    : {};
  consumers: TContract["consumers"] extends Record<string, ConsumerEntry>
    ? ExtractConsumerDefinitions<TContract["consumers"]>
    : {};
  rpcs: TContract["rpcs"] extends Record<string, RpcDefinition> ? TContract["rpcs"] : {};
};

/**
 * Extract publisher names from a contract.
 *
 * This utility type extracts the keys of all publishers defined in a contract.
 * It's used internally for type inference in the TypedAmqpClient.
 *
 * @template TContract - The contract definition
 * @returns Union of publisher names, or never if no publishers defined
 *
 * @example
 * ```typescript
 * type PublisherNames = InferPublisherNames<typeof myContract>;
 * // Result: 'orderCreated' | 'orderUpdated' | 'orderCancelled'
 * ```
 */
export type InferPublisherNames<TContract extends ContractDefinition> =
  TContract["publishers"] extends Record<string, unknown> ? keyof TContract["publishers"] : never;

/**
 * Extract consumer names from a contract.
 *
 * This utility type extracts the keys of all consumers defined in a contract.
 * It's used internally for type inference in the TypedAmqpWorker.
 *
 * @template TContract - The contract definition
 * @returns Union of consumer names, or never if no consumers defined
 *
 * @example
 * ```typescript
 * type ConsumerNames = InferConsumerNames<typeof myContract>;
 * // Result: 'processOrder' | 'sendNotification' | 'updateInventory'
 * ```
 */
export type InferConsumerNames<TContract extends ContractDefinition> =
  TContract["consumers"] extends Record<string, unknown> ? keyof TContract["consumers"] : never;

/**
 * Extract RPC names from a contract.
 *
 * Each name in this union has a typed worker handler and a `client.call(name, ...)`
 * method. RPC names are disjoint from `InferConsumerNames` and `InferPublisherNames`.
 *
 * @template TContract - The contract definition
 * @returns Union of RPC names, or never if no RPCs defined
 */
export type InferRpcNames<TContract extends ContractDefinition> =
  TContract["rpcs"] extends Record<string, RpcDefinition> ? keyof TContract["rpcs"] : never;
