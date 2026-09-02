import { brandOf } from "../brand.js";
import type {
  CommandConsumerConfigBase,
  ConsumerDefinition,
  ConsumerEntry,
  EventConsumerResultBase,
  MessageDefinition,
  QueueDefinition,
} from "../types.js";

/**
 * Type guard to check if an entry is an EventConsumerResult.
 */
function isEventConsumerResultEntry(entry: ConsumerEntry): entry is EventConsumerResultBase {
  return brandOf(entry) === "EventConsumerResult";
}

/**
 * Type guard to check if an entry is a CommandConsumerConfig.
 */
function isCommandConsumerConfigEntry(entry: ConsumerEntry): entry is CommandConsumerConfigBase {
  return brandOf(entry) === "CommandConsumerConfig";
}

/**
 * Extract the ConsumerDefinition from any ConsumerEntry type.
 *
 * Handles the following entry types:
 * - ConsumerDefinition: returned as-is
 * - EventConsumerResult: returns the nested `.consumer` property
 * - CommandConsumerConfig: returns the nested `.consumer` property
 *
 * Use this function when you need to access the underlying ConsumerDefinition
 * from a consumer entry that may have been created with defineEventConsumer
 * or defineCommandConsumer.
 *
 * @param entry - The consumer entry to extract from
 * @returns The underlying ConsumerDefinition
 *
 * @example
 * ```typescript
 * // Works with plain ConsumerDefinition
 * const consumer1 = defineConsumer(queue, message);
 * extractConsumer(consumer1).queue.name; // "my-queue"
 *
 * // Works with EventConsumerResult
 * const consumer2 = defineEventConsumer(eventPublisher, queue);
 * extractConsumer(consumer2).queue.name; // "my-queue"
 *
 * // Works with CommandConsumerConfig
 * const consumer3 = defineCommandConsumer(queue, exchange, message, { routingKey: "cmd" });
 * extractConsumer(consumer3).queue.name; // "my-queue"
 * ```
 */
export function extractConsumer(entry: ConsumerEntry): ConsumerDefinition {
  if (isEventConsumerResultEntry(entry) || isCommandConsumerConfigEntry(entry)) {
    return entry.consumer;
  }
  // Otherwise it's a plain ConsumerDefinition
  return entry;
}

/**
 * Define a message consumer.
 *
 * A consumer receives and processes messages from a queue. The message schema is validated
 * automatically when messages are consumed, ensuring type safety for your handlers.
 *
 * Consumers are associated with a specific queue and message type. When you create a worker
 * with this consumer, it will process messages from the queue according to the schema.
 *
 * **Which pattern to use:**
 *
 * | Pattern | Best for | Description |
 * |---------|----------|-------------|
 * | `definePublisher` + `defineConsumer` | Independent definition | Define publishers and consumers separately with manual schema consistency |
 * | `defineEventPublisher` + `defineEventConsumer` | Event broadcasting | Define event publisher first, create consumers that subscribe to it |
 * | `defineCommandConsumer` + `defineCommandPublisher` | Task queues | Define command consumer first, create publishers that send commands to it |
 *
 * Use `defineCommandConsumer` when:
 * - One consumer receives from multiple publishers
 * - You want automatic schema consistency between consumer and publishers
 * - You're building task queue or command patterns
 *
 * @param queue - The queue definition to consume from
 * @param message - The message definition with payload schema
 * @param options - Optional consumer configuration
 * @returns A consumer definition with inferred message types
 *
 * @example
 * ```typescript
 * import { z } from 'zod';
 *
 * const orderQueue = defineQueue('order-processing');
 * const orderMessage = defineMessage(
 *   z.object({
 *     orderId: z.string().uuid(),
 *     customerId: z.string().uuid(),
 *     amount: z.number().positive(),
 *   })
 * );
 *
 * const processOrderConsumer = defineConsumer(orderQueue, orderMessage);
 *
 * // Later, when creating a worker, you'll provide a handler for this consumer.
 * // Handlers return AsyncResult<void, HandlerError> (from unthrown):
 * // const worker = await TypedAmqpWorker.create({
 * //   contract,
 * //   handlers: {
 * //     processOrder: (_, { payload }) => {
 * //       // payload is automatically typed based on the schema
 * //       console.log(payload.orderId); // string
 * //       return OkAsync();
 * //     },
 * //   },
 * //   urls: ['amqp://localhost'],
 * // }).get();
 * ```
 *
 * @see defineCommandConsumer - For task queue patterns with automatic schema consistency
 * @see defineEventPublisher - For event-driven patterns with automatic schema consistency
 */
export function defineConsumer<TMessage extends MessageDefinition>(
  queue: QueueDefinition,
  message: TMessage,
  options?: Omit<ConsumerDefinition<TMessage>, "queue" | "message">,
): ConsumerDefinition<TMessage> {
  return {
    queue,
    message,
    ...options,
  };
}
