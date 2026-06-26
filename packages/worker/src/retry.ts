import {
  type ConsumerDefinition,
  type ResolvedImmediateRequeueRetryOptions,
  type ResolvedTtlBackoffRetryOptions,
  extractQueue,
  isQueueWithTtlBackoffInfrastructure,
} from "@amqp-contract/contract";
import { type AmqpClient, type Logger, TechnicalError } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { err, ok, type AsyncResult } from "unthrown";
import { NonRetryableError } from "./errors.js";

type RetryContext = {
  amqpClient: AmqpClient;
  logger?: Logger | undefined;
};

/**
 * Handle error in message processing with retry logic.
 *
 * Flow depends on retry mode:
 *
 * **immediate-requeue mode:**
 * 1. If NonRetryableError -> send directly to DLQ (no retry)
 * 2. If max retries exceeded -> send to DLQ
 * 3. Otherwise -> requeue immediately for retry
 *
 * **ttl-backoff mode:**
 * 1. If NonRetryableError -> send directly to DLQ (no retry)
 * 2. If max retries exceeded -> send to DLQ
 * 3. Otherwise -> publish to wait queue with TTL for retry
 *
 * **none mode (no retry config):**
 * 1. send directly to DLQ (no retry)
 */
export function handleError(
  ctx: RetryContext,
  error: Error,
  msg: ConsumeMessage,
  consumerName: string,
  consumer: ConsumerDefinition,
): AsyncResult<void, TechnicalError> {
  // NonRetryableError -> send directly to DLQ without retrying.
  // The caller already logged the original error; we only emit a routing
  // decision log inside `sendToDLQ`.
  if (error instanceof NonRetryableError) {
    sendToDLQ(ctx, msg, consumer);
    return ok(undefined).toAsync();
  }

  // Get retry config from the queue definition in the contract
  const config = extractQueue(consumer.queue).retry;

  // Immediate-requeue mode: requeue the message immediately
  if (config.mode === "immediate-requeue") {
    return handleErrorImmediateRequeue(ctx, error, msg, consumerName, consumer, config);
  }

  // TTL-backoff mode: use wait queue with exponential backoff
  if (config.mode === "ttl-backoff") {
    return handleErrorTtlBackoff(ctx, error, msg, consumerName, consumer, config);
  }

  // None mode: no retry, send directly to DLQ or reject. The caller already
  // logged the original error; emit an info-level routing-decision log so
  // operators can distinguish this DLQ path from `NonRetryableError` and
  // max-retries exhaustion paths in retry.ts.
  ctx.logger?.info("Retry disabled (none mode), sending to DLQ", {
    consumerName,
    queueName: extractQueue(consumer.queue).name,
  });
  sendToDLQ(ctx, msg, consumer);
  return ok(undefined).toAsync();
}

/**
 * Handle error by requeuing immediately.
 *
 * For quorum queues, messages are requeued with `nack(requeue=true)`, and the worker tracks delivery count via the native RabbitMQ `x-delivery-count` header.
 * For classic queues, messages are re-published on the same queue, and the worker tracks delivery count via a custom `x-retry-count` header.
 * When the count exceeds `maxRetries`, the message is automatically dead-lettered (if DLX is configured) or dropped.
 *
 * This is simpler than TTL-based retry but provides immediate retries only.
 */
function handleErrorImmediateRequeue(
  ctx: RetryContext,
  error: Error,
  msg: ConsumeMessage,
  consumerName: string,
  consumer: ConsumerDefinition,
  config: ResolvedImmediateRequeueRetryOptions,
): AsyncResult<void, TechnicalError> {
  const queue = extractQueue(consumer.queue);
  const queueName = queue.name;

  // Get retry count from headers
  // For quorum queues, the header x-delivery-count is automatically incremented on each delivery attempt
  // For classic queues, the header x-retry-count is manually incremented by the worker when re-publishing messages
  const retryCount =
    queue.type === "quorum"
      ? ((msg.properties.headers?.["x-delivery-count"] as number) ?? 0)
      : ((msg.properties.headers?.["x-retry-count"] as number) ?? 0);

  // Max retries exceeded -> DLQ. The caller already logged the original error;
  // emit only the routing decision here.
  if (retryCount >= config.maxRetries) {
    ctx.logger?.info("Max retries exceeded, sending to DLQ (immediate-requeue mode)", {
      consumerName,
      queueName,
      retryCount,
      maxRetries: config.maxRetries,
    });
    sendToDLQ(ctx, msg, consumer);
    return ok(undefined).toAsync();
  }

  ctx.logger?.info("Retrying message (immediate-requeue mode)", {
    consumerName,
    queueName,
    retryCount,
    maxRetries: config.maxRetries,
  });

  if (queue.type === "quorum") {
    // For quorum queues, nack with requeue=true to trigger native retry mechanism
    ctx.amqpClient.nack(msg, false, true);
    return ok(undefined).toAsync();
  } else {
    // For classic queues, re-publish the message to the same exchange / routing key immediately with an incremented x-retry-count header
    return publishForRetry(ctx, {
      msg,
      exchange: msg.fields.exchange,
      routingKey: msg.fields.routingKey,
      queueName,
      error,
    });
  }
}

/**
 * Handle error using TTL + wait queue pattern for exponential backoff.
 *
 * ┌─────────────────────────────────────────────────────────────────┐
 * │ Retry Flow (Native RabbitMQ TTL + Wait queue pattern)           │
 * ├─────────────────────────────────────────────────────────────────┤
 * │                                                                 │
 * │ 1. Handler throws any Error                                     │
 * │    ↓                                                            │
 * │ 2. Worker publishes to wait exchange                            |
 * |    (with header `x-wait-queue` set to the wait queue name)      │
 * │    ↓                                                            │
 * │ 3. Wait exchange routes to wait queue                           │
 * │    (with expiration: calculated backoff delay)                  │
 * │    ↓                                                            │
 * │ 4. Message waits in queue until TTL expires                     │
 * │    ↓                                                            │
 * │ 5. Expired message dead-lettered to retry exchange              |
 * |    (with header `x-retry-queue` set to the main queue name)     │
 * │    ↓                                                            │
 * │ 6. Retry exchange routes back to main queue → RETRY             │
 * │    ↓                                                            │
 * │ 7. If retries exhausted: nack without requeue → DLQ             │
 * │                                                                 │
 * └─────────────────────────────────────────────────────────────────┘
 */
function handleErrorTtlBackoff(
  ctx: RetryContext,
  error: Error,
  msg: ConsumeMessage,
  consumerName: string,
  consumer: ConsumerDefinition,
  config: ResolvedTtlBackoffRetryOptions,
): AsyncResult<void, TechnicalError> {
  if (!isQueueWithTtlBackoffInfrastructure(consumer.queue)) {
    ctx.logger?.error("Queue does not have TTL-backoff infrastructure", {
      consumerName,
      queueName: consumer.queue.name,
    });
    return err(new TechnicalError("Queue does not have TTL-backoff infrastructure")).toAsync();
  }

  const queueEntry = consumer.queue;
  const queue = extractQueue(queueEntry);
  const queueName = queue.name;

  // Get retry count from headers
  const retryCount = (msg.properties.headers?.["x-retry-count"] as number) ?? 0;

  // Max retries exceeded -> DLQ. The caller already logged the original error;
  // emit only the routing decision here.
  if (retryCount >= config.maxRetries) {
    ctx.logger?.info("Max retries exceeded, sending to DLQ (ttl-backoff mode)", {
      consumerName,
      queueName,
      retryCount,
      maxRetries: config.maxRetries,
    });
    sendToDLQ(ctx, msg, consumer);
    return ok(undefined).toAsync();
  }

  // Retry with exponential backoff
  const delayMs = calculateRetryDelay(retryCount, config);
  ctx.logger?.info("Retrying message (ttl-backoff mode)", {
    consumerName,
    queueName,
    retryCount: retryCount + 1,
    maxRetries: config.maxRetries,
    delayMs,
  });

  // Re-publish the message to the wait exchange with TTL and incremented x-retry-count header
  return publishForRetry(ctx, {
    msg,
    exchange: queueEntry.waitExchange.name,
    routingKey: msg.fields.routingKey, // Preserve original routing key
    waitQueueName: queueEntry.waitQueue.name,
    queueName,
    delayMs,
    error,
  });
}

/**
 * Calculate retry delay with exponential backoff and optional jitter.
 */
function calculateRetryDelay(retryCount: number, config: ResolvedTtlBackoffRetryOptions): number {
  const { initialDelayMs, maxDelayMs, backoffMultiplier, jitter } = config;

  let delay = initialDelayMs * Math.pow(backoffMultiplier, retryCount);

  if (jitter) {
    // ± 50% jitter, centred on the calculated delay (range: [0.5x, 1.5x],
    // mean 1.0x). The previous formula `0.5 + Math.random() * 0.5` produced
    // [0.5x, 1.0x] (mean 0.75x) and never overshot — that's a one-sided bias,
    // not real jitter.
    delay = delay * (0.5 + Math.random());
  }

  // Clamp AFTER jitter so the upper jitter bound cannot push the delay past
  // `maxDelayMs`.
  return Math.floor(Math.min(delay, maxDelayMs));
}

/**
 * Parse message content for republishing.
 *
 * The channel is configured with `json: true`, so values published as plain
 * objects are encoded once at publish time. Re-publishing the raw `Buffer`
 * would then trigger a *second* JSON.stringify (turning the bytes into a
 * stringified base64 blob), so for JSON payloads we must round-trip back to
 * the parsed value. For any other content type — or when the message is
 * compressed — we pass the bytes through untouched, since re-parsing would
 * either fail or silently corrupt binary data.
 */
function parseMessageContentForRetry(
  ctx: RetryContext,
  msg: ConsumeMessage,
  queueName: string,
): Buffer | unknown {
  if (msg.properties.contentEncoding) {
    // Compressed (gzip, brotli, …) — opaque to us; keep the buffer as-is so
    // the consumer's decompressor sees the same bytes the producer sent.
    return msg.content;
  }

  const contentType = msg.properties.contentType;
  const isJson =
    contentType === undefined ||
    contentType === "application/json" ||
    contentType.startsWith("application/json;") ||
    contentType.endsWith("+json");

  if (!isJson) {
    // Binary or other text payload — preserve bytes exactly.
    return msg.content;
  }

  try {
    return JSON.parse(msg.content.toString());
  } catch (parseErr) {
    ctx.logger?.warn("Failed to parse JSON message for retry, using original buffer", {
      queueName,
      error: parseErr,
    });
    return msg.content;
  }
}

/**
 * Publish message with an incremented x-retry-count header and optional TTL.
 */
function publishForRetry(
  ctx: RetryContext,
  {
    msg,
    exchange,
    routingKey,
    queueName,
    waitQueueName,
    delayMs,
    error,
  }: {
    msg: ConsumeMessage;
    exchange: string;
    routingKey: string;
    queueName: string;
    waitQueueName?: string;
    delayMs?: number;
    error: Error;
  },
): AsyncResult<void, TechnicalError> {
  // Get retry count from headers
  const retryCount = (msg.properties.headers?.["x-retry-count"] as number) ?? 0;
  const newRetryCount = retryCount + 1;

  const content = parseMessageContentForRetry(ctx, msg, queueName);

  // Publish FIRST, then ack the original only if the publish succeeded.
  //
  // Acking before publishing would lose the message if the publish then fails:
  // the broker has already discarded the original delivery and the retry copy
  // never made it out. By publishing first and acking on success, we ensure the
  // message is not lost on a publish failure — leaving the original un-ack'd
  // makes amqp-connection-manager redeliver it (or, on channel close, the
  // broker re-enqueues), so we either get the retry through or get another
  // chance at the original.
  return ctx.amqpClient
    .publish(exchange, routingKey, content, {
      ...msg.properties,
      ...(delayMs !== undefined ? { expiration: delayMs.toString() } : {}), // Per-message TTL
      headers: {
        ...msg.properties.headers,
        "x-retry-count": newRetryCount,
        "x-last-error": error.message,
        "x-first-failure-timestamp":
          msg.properties.headers?.["x-first-failure-timestamp"] ?? Date.now(),
        ...(waitQueueName !== undefined
          ? {
              "x-wait-queue": waitQueueName, // For wait exchange routing
              "x-retry-queue": queueName, // For retry exchange routing
            }
          : {}),
      },
    })
    .flatMap((published) => {
      if (!published) {
        // Publish was rejected (channel buffer full / channel error). Do NOT
        // ack the original — leave it un-ack'd so the broker / channel manager
        // can redeliver it once the channel recovers.
        ctx.logger?.error("Failed to publish message for retry (write buffer full)", {
          queueName,
          retryCount: newRetryCount,
          ...(delayMs !== undefined ? { delayMs } : {}),
        });
        return err(new TechnicalError("Failed to publish message for retry (write buffer full)"));
      }

      // Publish confirmed by the broker — safe to ack the original now.
      ctx.amqpClient.ack(msg);

      ctx.logger?.info("Message published for retry", {
        queueName,
        retryCount: newRetryCount,
        ...(delayMs !== undefined ? { delayMs } : {}),
      });
      return ok(undefined);
    })
    .orElse((publishError) => {
      // Publish threw (network error, channel close, etc.). Same policy: do
      // not ack the original; the redelivery path is the recovery mechanism.
      ctx.logger?.error("Publish for retry failed; leaving original un-ack'd for redelivery", {
        queueName,
        retryCount: newRetryCount,
        ...(delayMs !== undefined ? { delayMs } : {}),
        error: publishError,
      });
      return err(publishError);
    });
}

/**
 * Send message to dead letter queue.
 * Nacks the message without requeue, relying on DLX configuration.
 */
function sendToDLQ(ctx: RetryContext, msg: ConsumeMessage, consumer: ConsumerDefinition): void {
  const queue = extractQueue(consumer.queue);
  const queueName = queue.name;
  const hasDeadLetter = queue.deadLetter !== undefined;

  if (!hasDeadLetter) {
    ctx.logger?.warn("Queue does not have DLX configured - message will be lost on nack", {
      queueName,
    });
  }

  ctx.logger?.info("Sending message to DLQ", {
    queueName,
    deliveryTag: msg.fields.deliveryTag,
  });

  // Nack without requeue - relies on DLX configuration
  ctx.amqpClient.nack(msg, false, false);
}

/**
 * Internal helpers exposed for unit testing only. Not part of the public API.
 *
 * @internal
 */
export const _internalForTesting = {
  calculateRetryDelay,
  publishForRetry,
};
