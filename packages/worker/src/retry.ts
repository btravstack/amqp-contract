import {
  type ConsumerDefinition,
  type QueueDefinition,
  type ResolvedTtlBackoffRetryOptions,
  deriveTtlBackoffInfrastructure,
  ttlBackoffBaseDelay,
  ttlBackoffWaitQueueName,
} from "@amqp-contract/contract";
import { _internal_queueHasDeadLetterExchange } from "@amqp-contract/contract/internal";
import { type AmqpClient, type Logger, PublishError } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { OkAsync, P, type AsyncResult } from "unthrown";

import { NonRetryableError } from "./errors.js";

type RetryContext = {
  amqpClient: AmqpClient;
  logger?: Logger | undefined;
  /**
   * Channel epoch captured when the message was delivered
   * ({@link AmqpClient.currentChannelEpoch}). Stamped onto every ack/nack so
   * a settle that lands after a reconnect is skipped instead of targeting a
   * foreign delivery tag on the new channel.
   */
  deliveryEpoch?: number | undefined;
};

/** Cap on the `x-last-error` header, in characters (≤ 4 KiB of UTF-8). */
export const MAX_LAST_ERROR_LENGTH = 1024;

/**
 * What to do with a delivery whose handler failed — decided by
 * {@link decideRetry} without touching the broker, carried out by
 * {@link handleError}.
 *
 * - `dead-letter` — `nack(requeue: false)`: the queue's DLX gets it.
 * - `requeue` — `nack(requeue: true)`: a quorum queue's native retry, counted
 *   by the broker in `x-delivery-count`.
 * - `republish` — publish a retry copy via the default exchange to
 *   `routingKey` (the queue itself, or a ttl-backoff wait-queue tier), then
 *   ack the original. `retryCount` is the copy's new `x-retry-count`.
 */
export type RetryAction =
  | { kind: "dead-letter"; reason: string }
  | { kind: "requeue"; retryCount: number }
  | { kind: "republish"; routingKey: string; retryCount: number; delayMs?: number | undefined };

/**
 * A delivery's retry counter header, as a count. Headers arrive from the wire:
 * anything but a non-negative safe integer (a string, NaN, a negative, a
 * table — a producer bug or a forged header) counts as 0, so it can neither
 * bypass the retry budget (`"abc" >= 3` is false forever) nor compute an
 * undeclared wait-queue tier (`initialDelay * 2 ** NaN`).
 */
export function readCount(headers: Record<string, unknown> | undefined, name: string): number {
  const value = headers?.[name];
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : 0;
}

/**
 * Decide what a handler failure does to its delivery. Pure: the same error,
 * queue, headers and random source always give the same action.
 *
 * - A `NonRetryableError`, or a queue without retry (`mode: "none"`), dead-letters.
 * - A spent retry budget dead-letters.
 * - **immediate-requeue**: a quorum queue requeues (the broker counts
 *   deliveries in `x-delivery-count`); a classic queue republishes to ITSELF
 *   via the default exchange with an incremented `x-retry-count` —
 *   republishing to the original exchange would fan the copy out to every
 *   sibling queue bound to it.
 * - **ttl-backoff**: republish to the wait-queue tier of the attempt's base
 *   delay ({@link ttlBackoffBaseDelay}); the jittered delay rides on the
 *   copy's per-message `expiration`.
 *
 * @param rand - Random source for jitter, `[0, 1)`; defaults to `Math.random`.
 */
export function decideRetry(
  error: Error,
  queue: QueueDefinition,
  headers: Record<string, unknown> | undefined,
  rand: () => number = Math.random,
): RetryAction {
  if (error instanceof NonRetryableError) {
    return { kind: "dead-letter", reason: "non-retryable error" };
  }
  const config = queue.retry;
  if (config.mode === "none") {
    return { kind: "dead-letter", reason: "retry disabled (none mode)" };
  }

  if (config.mode === "immediate-requeue") {
    const retryCount =
      queue.type === "quorum"
        ? readCount(headers, "x-delivery-count")
        : readCount(headers, "x-retry-count");
    if (retryCount >= config.maxRetries) {
      return { kind: "dead-letter", reason: "max retries exceeded" };
    }
    return queue.type === "quorum"
      ? { kind: "requeue", retryCount }
      : { kind: "republish", routingKey: queue.name, retryCount: retryCount + 1 };
  }

  const retryCount = readCount(headers, "x-retry-count");
  if (retryCount >= config.maxRetries) {
    return { kind: "dead-letter", reason: "max retries exceeded" };
  }
  const waitQueue = ttlBackoffWaitQueueName(queue.name, ttlBackoffBaseDelay(config, retryCount));
  // The copy goes through the default exchange without `mandatory`: a tier
  // that was never declared would swallow it silently. Only publish to one
  // setup declared; anything else is a bug, and the DLQ keeps the message.
  if (!deriveTtlBackoffInfrastructure(queue)?.waitQueues.some((w) => w.name === waitQueue)) {
    return { kind: "dead-letter", reason: `wait queue "${waitQueue}" is not a declared tier` };
  }
  return {
    kind: "republish",
    routingKey: waitQueue,
    retryCount: retryCount + 1,
    delayMs: calculateRetryDelay(retryCount, config, rand),
  };
}

/**
 * Route a handler failure: {@link decideRetry}, then carry the action out on
 * the broker. The caller already logged the original error; this logs only
 * the routing decision.
 */
export function handleError(
  ctx: RetryContext,
  error: Error,
  msg: ConsumeMessage,
  consumerName: string,
  consumer: ConsumerDefinition,
): AsyncResult<void, never> {
  const queue = consumer.queue;
  const action = decideRetry(error, queue, msg.properties.headers);
  const fields = { consumerName, queueName: queue.name };

  switch (action.kind) {
    case "dead-letter":
      ctx.logger?.info(`Sending to DLQ: ${action.reason}`, fields);
      sendToDLQ(ctx, msg, consumer);
      return OkAsync(undefined);
    case "requeue":
      ctx.logger?.info("Retrying message (requeue)", {
        ...fields,
        retryCount: action.retryCount,
      });
      ctx.amqpClient.nack(msg, { requeue: true, deliveryEpoch: ctx.deliveryEpoch });
      return OkAsync(undefined);
    case "republish":
      ctx.logger?.info("Retrying message (republish)", {
        ...fields,
        routingKey: action.routingKey,
        retryCount: action.retryCount,
        ...(action.delayMs !== undefined ? { delayMs: action.delayMs } : {}),
      });
      return publishForRetry(ctx, {
        msg,
        exchange: "",
        routingKey: action.routingKey,
        queueName: queue.name,
        delayMs: action.delayMs,
        error,
      });
  }
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
function calculateRetryDelay(
  retryCount: number,
  config: ResolvedTtlBackoffRetryOptions,
  rand: () => number = Math.random,
): number {
  const { maxDelayMs, jitter } = config;

  let delay: number = ttlBackoffBaseDelay(config, retryCount);

  if (jitter) {
    // ± 50% jitter, centred on the calculated delay (range: [0.5x, 1.5x],
    // mean 1.0x).
    delay = delay * (0.5 + rand());
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
    delayMs?: number | undefined;
    error: Error;
  },
): AsyncResult<void, never> {
  const headers = msg.properties.headers;
  const newRetryCount = readCount(headers, "x-retry-count") + 1;
  const firstFailure = headers?.["x-first-failure-timestamp"];
  const originalRoutingKey = headers?.["x-original-routing-key"];

  // Publish FIRST, then ack the original only if the publish succeeded.
  //
  // Acking before publishing would lose the message if the publish then fails:
  // the broker has already discarded the original delivery and the retry copy
  // never made it out. By publishing first and acking on success, we ensure the
  // message is not lost on a publish failure — the original is requeued
  // (`nack(requeue: true)`), so we either get the retry through or get another
  // chance at the original. Never dead-lettered for an infrastructure fault.
  return ctx.amqpClient
    .publish({ exchange, routingKey }, msg.content, {
      ...msg.properties,
      ...(delayMs !== undefined ? { expiration: delayMs.toString() } : {}), // Per-message TTL
      headers: {
        ...headers,
        "x-retry-count": newRetryCount,
        // Bounded: headers ride in one AMQP frame, and a handler error
        // carrying a stack or payload dump could exceed frame_max — a
        // connection error on every retry, a poison loop.
        "x-last-error": error.message.slice(0, MAX_LAST_ERROR_LENGTH),
        // Carried over only when well-formed; a forged or corrupt value is
        // replaced, never propagated down the retry chain.
        "x-first-failure-timestamp":
          typeof firstFailure === "number" && Number.isSafeInteger(firstFailure) && firstFailure > 0
            ? firstFailure
            : Date.now(),
        "x-original-routing-key":
          typeof originalRoutingKey === "string" ? originalRoutingKey : msg.fields.routingKey,
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
    .recoverErrCases((matcher) =>
      matcher.with(P.tag(PublishError.tag), (publishError) => {
        // The broker did not take the retry copy (timeout, nack, channel
        // closed). Requeue the ORIGINAL: it is redelivered with its retry
        // headers unchanged, so the retry budget is intact and nothing is
        // lost or dead-lettered for an infrastructure hiccup.
        ctx.logger?.error("Publish for retry failed; requeueing the original for redelivery", {
          queueName,
          retryCount: newRetryCount,
          ...(delayMs !== undefined ? { delayMs } : {}),
          error: publishError,
        });
        ctx.amqpClient.nack(msg, { requeue: true, deliveryEpoch: ctx.deliveryEpoch });
      }),
    );
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
