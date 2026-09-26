import {
  type ConsumerDefinition,
  type ResolvedImmediateRequeueRetryOptions,
  type ResolvedTtlBackoffRetryOptions,
  ttlBackoffBaseDelay,
  ttlBackoffWaitQueueName,
} from "@amqp-contract/contract";
import { _internal_queueHasDeadLetterExchange } from "@amqp-contract/contract/internal";
import type { AmqpTransport, Logger } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { OkAsync, type AsyncResult } from "unthrown";

import { NonRetryableError } from "./errors.js";

type RetryContext = {
  amqpClient: AmqpTransport;
  logger?: Logger | undefined;
  /**
   * Channel epoch captured when the message was delivered
   * ({@link AmqpTransport.currentChannelEpoch}). Stamped onto every ack/nack so
   * a settle that lands after a reconnect is skipped instead of targeting a
   * foreign delivery tag on the new channel.
   */
  deliveryEpoch?: number | undefined;
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
 * 3. Otherwise -> publish to the per-delay-tier wait queue with TTL for retry
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
): AsyncResult<void, never> {
  // NonRetryableError -> send directly to DLQ without retrying.
  // The caller already logged the original error; we only emit a routing
  // decision log inside `sendToDLQ`.
  if (error instanceof NonRetryableError) {
    sendToDLQ(ctx, msg, consumer);
    return OkAsync(undefined);
  }

  // Get retry config from the queue definition in the contract
  const config = consumer.queue.retry;

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
    queueName: consumer.queue.name,
  });
  sendToDLQ(ctx, msg, consumer);
  return OkAsync(undefined);
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
): AsyncResult<void, never> {
  const queue = consumer.queue;
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
    return OkAsync(undefined);
  }

  ctx.logger?.info("Retrying message (immediate-requeue mode)", {
    consumerName,
    queueName,
    retryCount,
    maxRetries: config.maxRetries,
  });

  if (queue.type === "quorum") {
    // For quorum queues, nack with requeue=true to trigger native retry mechanism
    ctx.amqpClient.nack(msg, { requeue: true, deliveryEpoch: ctx.deliveryEpoch });
    return OkAsync(undefined);
  } else {
    // For classic queues, re-publish the retry copy straight back to THIS
    // queue via the default exchange (routing key = queue name). Republishing
    // to the original exchange would fan the retry out to every queue bound
    // to it — sibling consumers would process duplicates and inherit our
    // `x-retry-count` header into their own retry accounting.
    return publishForRetry(ctx, {
      msg,
      exchange: "",
      routingKey: queueName,
      queueName,
      error,
    });
  }
}

/**
 * Handle error using the TTL + per-delay-tier wait queue pattern for
 * exponential backoff.
 *
 * ┌─────────────────────────────────────────────────────────────────┐
 * │ Retry Flow (Native RabbitMQ TTL + per-tier wait queues)         │
 * ├─────────────────────────────────────────────────────────────────┤
 * │                                                                 │
 * │ 1. Handler fails with a retryable error                         │
 * │    ↓                                                            │
 * │ 2. Worker computes the attempt's BASE delay and publishes the   │
 * │    retry copy to that tier's wait queue via the default         │
 * │    exchange (`{queue}-wait-{delay}ms`), with per-message        │
 * │    `expiration` carrying the jittered delay                     │
 * │    ↓                                                            │
 * │ 3. Message waits until its TTL expires (queue-level             │
 * │    `x-message-ttl` on the tier is the jitter-ceiling backstop)  │
 * │    ↓                                                            │
 * │ 4. Expired message is dead-lettered back to the main queue via  │
 * │    the default exchange (`x-dead-letter-routing-key`) → RETRY   │
 * │    ↓                                                            │
 * │ 5. If retries exhausted: nack without requeue → DLQ             │
 * │                                                                 │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * One wait queue per distinct base delay means a long-delay retry can never
 * block a short-delay retry behind it (RabbitMQ only dead-letters expired
 * messages at the head of a queue). Within a tier, head-of-line skew is
 * bounded by the jitter spread — zero when jitter is disabled.
 *
 * The retried delivery arrives via the default exchange, so its
 * `fields.routingKey` is the main queue name; the original routing key is
 * preserved in the `x-original-routing-key` header.
 */
function handleErrorTtlBackoff(
  ctx: RetryContext,
  error: Error,
  msg: ConsumeMessage,
  consumerName: string,
  consumer: ConsumerDefinition,
  config: ResolvedTtlBackoffRetryOptions,
): AsyncResult<void, never> {
  const queueName = consumer.queue.name;

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
    return OkAsync(undefined);
  }

  // Retry with exponential backoff: the base delay selects the wait-queue
  // tier; jitter only affects the per-message expiration within that tier.
  const baseDelayMs = ttlBackoffBaseDelay(config, retryCount);
  const waitQueueName = ttlBackoffWaitQueueName(queueName, baseDelayMs);
  const delayMs = calculateRetryDelay(retryCount, config);
  ctx.logger?.info("Retrying message (ttl-backoff mode)", {
    consumerName,
    queueName,
    waitQueueName,
    retryCount: retryCount + 1,
    maxRetries: config.maxRetries,
    delayMs,
  });

  // Re-publish the message to the tier's wait queue (default exchange,
  // routing key = wait queue name) with TTL and incremented x-retry-count.
  return publishForRetry(ctx, {
    msg,
    exchange: "",
    routingKey: waitQueueName,
    queueName,
    delayMs,
    error,
  });
}

/**
 * Calculate the per-message retry delay: the attempt's base delay
 * ({@link ttlBackoffBaseDelay}) with optional jitter applied.
 *
 * The jittered value only spreads messages WITHIN their delay tier — tier
 * selection uses the base delay, so all copies of an attempt land in the same
 * wait queue and the tier's queue-level TTL (the jitter ceiling) bounds the
 * head-of-line skew.
 */
function calculateRetryDelay(retryCount: number, config: ResolvedTtlBackoffRetryOptions): number {
  const { maxDelayMs, jitter } = config;

  let delay: number = ttlBackoffBaseDelay(config, retryCount);

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
 * Publish message with an incremented x-retry-count header and optional TTL.
 *
 * The retry copy republishes `msg.content` — the exact bytes the broker
 * delivered. `AmqpClient.publish` passes Buffers through untouched, so JSON,
 * compressed, and binary payloads all survive the retry hop byte-for-byte.
 *
 * Retry republish paths route via the default exchange (classic-queue
 * immediate-requeue republishes to the queue itself; ttl-backoff publishes to
 * the tier wait queue), so the redelivered `fields.routingKey` is no longer
 * the original one. The first republish stamps it into
 * `x-original-routing-key`, and subsequent republishes preserve that header.
 */
function publishForRetry(
  ctx: RetryContext,
  {
    msg,
    exchange,
    routingKey,
    queueName,
    delayMs,
    error,
  }: {
    msg: ConsumeMessage;
    exchange: string;
    routingKey: string;
    queueName: string;
    delayMs?: number;
    error: Error;
  },
): AsyncResult<void, never> {
  // Get retry count from headers
  const retryCount = (msg.properties.headers?.["x-retry-count"] as number) ?? 0;
  const newRetryCount = retryCount + 1;

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
    .publish({ exchange, routingKey }, msg.content, {
      ...msg.properties,
      ...(delayMs !== undefined ? { expiration: delayMs.toString() } : {}), // Per-message TTL
      headers: {
        ...msg.properties.headers,
        "x-retry-count": newRetryCount,
        "x-last-error": error.message,
        "x-first-failure-timestamp":
          msg.properties.headers?.["x-first-failure-timestamp"] ?? Date.now(),
        "x-original-routing-key":
          msg.properties.headers?.["x-original-routing-key"] ?? msg.fields.routingKey,
      },
    })
    .map(() => {
      // Publish confirmed by the broker — safe to ack the original now. The
      // epoch stamp keeps this safe even when the confirm arrived on a NEW
      // channel (the publish buffer survives reconnects; delivery tags do not).
      ctx.amqpClient.ack(msg, { deliveryEpoch: ctx.deliveryEpoch });

      ctx.logger?.info("Message published for retry", {
        queueName,
        retryCount: newRetryCount,
        ...(delayMs !== undefined ? { delayMs } : {}),
      });
    })
    .tapDefect((publishError) => {
      // The retry publish failed — core surfaces every publish-side
      // infrastructure fault (full write buffer included) as a Defect. Same
      // policy for all of them: do not ack the original; the redelivery path
      // is the recovery mechanism. Observed here so the failure is logged
      // before the defect flows on unchanged.
      ctx.logger?.error("Publish for retry failed; leaving original un-ack'd for redelivery", {
        queueName,
        retryCount: newRetryCount,
        ...(delayMs !== undefined ? { delayMs } : {}),
        error: publishError,
      });
    });
}

/**
 * Send message to dead letter queue.
 * Nacks the message without requeue, relying on DLX configuration.
 *
 * Three outcomes, logged as distinct facts:
 *
 * - a DLX is configured — the message is handed off, `info`;
 * - no DLX but `onPoison: "drop"` — the author declared the loss, `info`;
 * - neither — an undeclared loss. `defineContract` rejects such a queue, so
 *   this is only reachable via a hand-built `ContractDefinition` that bypassed
 *   it. That is exactly the accident the guard exists to catch, so it keeps the
 *   `warn`.
 *
 * The branch must test what the message claims: asserting a declaration the
 * queue does not carry would be a lie in the operator's logs. "Is there a DLX?"
 * is therefore the guard's own question, asked through the shared
 * {@link _internal_queueHasDeadLetterExchange} — a queue dead-lettering through
 * the raw `arguments` passthrough is handed off, not reported as lost.
 */
function sendToDLQ(ctx: RetryContext, msg: ConsumeMessage, consumer: ConsumerDefinition): void {
  const queue = consumer.queue;
  const queueName = queue.name;
  const fields = { queueName, deliveryTag: msg.fields.deliveryTag };

  if (_internal_queueHasDeadLetterExchange(queue)) {
    ctx.logger?.info("Sending message to DLQ", fields);
  } else if (queue.onPoison === "drop") {
    ctx.logger?.info(
      'Discarding message: queue is declared onPoison: "drop" and has no DLX',
      fields,
    );
  } else {
    ctx.logger?.warn(
      "Queue has no dead-letter exchange and no onPoison declaration - message will be lost on nack",
      fields,
    );
  }

  // Nack without requeue - relies on DLX configuration
  ctx.amqpClient.nack(msg, { requeue: false, deliveryEpoch: ctx.deliveryEpoch });
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
