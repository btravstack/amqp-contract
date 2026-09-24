import type { AmqpClient, Logger, TelemetryProvider } from "@amqp-contract/core";
import { endSpanError, endSpanSuccess, recordConsumeMetric } from "@amqp-contract/core/internal";
import type { ConsumeMessage } from "amqplib";

/**
 * How the processing of one delivery ended. Every path through the dispatch
 * pipeline — success, handler failure, poison message, RPC reply failure —
 * produces exactly one outcome, and the outcome alone decides how the
 * delivery is settled ({@link settle}) and what telemetry records
 * ({@link recordOutcome}). Defects are left for genuine bugs.
 *
 * - `acked` — the handler succeeded (for an RPC, its reply — or its declared
 *   typed error — was published).
 * - `retried` — a retry copy was published and confirmed; the original is acked.
 * - `requeued` — `nack(requeue: true)`: a quorum queue's native retry, or a
 *   retry publish the broker refused (the original comes back, budget intact).
 * - `dead-lettered` — `nack(requeue: false)`: the queue's DLX gets it (or, on an
 *   `onPoison: "drop"` queue, it is discarded).
 */
export type Outcome =
  | { kind: "acked" }
  | { kind: "retried"; error: Error; delayMs?: number | undefined }
  | { kind: "requeued"; error: Error; reason: string }
  | { kind: "dead-lettered"; error: Error; reason: string };

export type DeadLettered = Extract<Outcome, { kind: "dead-lettered" }>;

export const ACKED: Outcome = { kind: "acked" };

/** A defect's cause as an `Error`, for logs and the span's exception. */
export function asError(cause: unknown): Error {
  return cause instanceof Error ? cause : new Error(String(cause));
}

/**
 * Settle a delivery according to its outcome — the ONE place the dispatch
 * path acks or nacks, so a message is settled exactly once by construction.
 *
 * Never throws: during `close()` — or a server-initiated channel teardown —
 * the channel may already reject writes, and a throw would escape the consume
 * callback as an unhandled rejection. Dropping the settle is safe: an unacked
 * delivery is redelivered once the channel is gone.
 */
export function settle(
  amqpClient: AmqpClient,
  msg: ConsumeMessage,
  outcome: Outcome,
  deliveryEpoch: number | undefined,
  logger: Logger | undefined,
): void {
  try {
    if (outcome.kind === "acked" || outcome.kind === "retried") {
      amqpClient.ack(msg, { deliveryEpoch });
    } else {
      amqpClient.nack(msg, { requeue: outcome.kind === "requeued", deliveryEpoch });
    }
  } catch (error: unknown) {
    logger?.warn("Failed to settle message (channel closing?); broker will redeliver instead", {
      deliveryTag: msg.fields.deliveryTag,
      outcome: outcome.kind,
      error,
    });
  }
}

/**
 * Close the consume span and record the consume metric from the outcome:
 * only `acked` is a success. A failure records the error that caused it, so
 * the span's `exception.type` is the discriminating class
 * (`RetryableError`, `NonRetryableError`, `MessageValidationError`, …).
 * Every helper swallows its own throws (invariant 16).
 */
export function recordOutcome(
  telemetry: TelemetryProvider,
  span: Parameters<typeof endSpanSuccess>[0],
  outcome: Outcome,
  queueName: string,
  consumerName: string,
  startTime: number,
): void {
  if (outcome.kind === "acked") {
    endSpanSuccess(span);
  } else {
    endSpanError(span, outcome.error);
  }
  recordConsumeMetric(
    telemetry,
    queueName,
    consumerName,
    outcome.kind === "acked",
    Date.now() - startTime,
  );
}
