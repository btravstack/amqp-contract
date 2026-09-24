import type { RpcErrorMap } from "@amqp-contract/contract";
import {
  type AmqpClient,
  type Logger,
  PublishError,
  RPC_ERROR_CODE_HEADER,
  type RpcError,
} from "@amqp-contract/core";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import { fromSchemaAsync } from "@unthrown/standard-schema";
import type { ConsumeMessage } from "amqplib";
import { Err, ErrAsync, Ok, P, type AsyncResult, type Result } from "unthrown";

import type { HandlerError } from "./errors.js";
import { MessageValidationError, NonRetryableError } from "./errors.js";

/**
 * RPC reply publishing: validate what the handler produced against the
 * contract, then publish it to the caller's `replyTo` with its
 * `correlationId` via the default exchange. Every failure on this path is a
 * `NonRetryableError` (→ DLQ): the caller is gone or the handler produced
 * something unusable, and retrying re-runs the handler against a stale caller.
 */
export type ReplyContext = {
  amqpClient: AmqpClient;
  logger?: Logger | undefined;
  /** Which `replyTo` addresses the worker may publish a reply to. */
  allowReplyTo: (replyTo: string) => boolean;
};

/**
 * The default `replyTo` allowlist: RabbitMQ direct reply-to only
 * (`amq.rabbitmq.reply-to`, which the broker rewrites to
 * `amq.rabbitmq.reply-to.<token>` on delivery) — what `client.call()` uses.
 * Anything else would let whoever can publish a request make the worker
 * publish into an arbitrary queue through the default exchange.
 */
export function isDirectReplyTo(replyTo: string): boolean {
  return replyTo === "amq.rabbitmq.reply-to" || replyTo.startsWith("amq.rabbitmq.reply-to.");
}

/**
 * Validate an RPC handler's response and publish it back to the caller's reply
 * queue with the same `correlationId`. Published via the AMQP default exchange
 * with `routingKey = msg.properties.replyTo` — only when the worker's
 * `rpc.allowReplyTo` accepts that address (by default: direct reply-to only).
 *
 * Failure semantics:
 * - **Missing, or disallowed, replyTo / missing correlationId**:
 *   NonRetryableError. The caller is
 *   already lost; retrying the original message cannot recover the reply
 *   path. The poison message lands in DLQ for inspection rather than being
 *   silently ack'd (which would mask a contract violation).
 * - **Schema validation failure**: NonRetryableError — the handler returned
 *   the wrong shape; retrying the same input will not fix it.
 * - **Publish failure**: NonRetryableError. The caller has already timed out
 *   (or will shortly), so retrying the message wastes the queue's retry
 *   budget on a reply that no one is waiting for. The message is logged and
 *   DLQ'd; the original work is treated as completed for the purpose of the
 *   inbox.
 */
export function publishRpcResponse(
  ctx: ReplyContext,
  msg: ConsumeMessage,
  queueName: string,
  rpcName: string,
  responseSchema: StandardSchemaV1,
  response: unknown,
): AsyncResult<void, HandlerError> {
  return requireReplyAddress(ctx, msg, rpcName, queueName)
    .toAsync()
    .flatMap(({ replyTo, correlationId }) =>
      validateReplyPayload(
        responseSchema,
        response,
        `RPC response for "${rpcName}"`,
        rpcName,
      ).flatMap((validatedResponse) =>
        publishReply(ctx, validatedResponse, replyTo, {
          correlationId,
          contentType: "application/json",
        }),
      ),
    );
}

/**
 * Validate a declared `RpcError` returned by an RPC handler and publish it
 * as an error reply: same `replyTo` / `correlationId` routing as a success
 * reply, plus the error code in the `RPC_ERROR_CODE_HEADER` header and a
 * `{ message, data }` body with `data` validated against the error's schema
 * from the RPC's `errors` map.
 *
 * Failure semantics mirror {@link publishRpcResponse} (NonRetryableError →
 * DLQ), with one addition: an error code absent from the `errors` map is a
 * contract violation by the handler — the type system prevents it, but a
 * cast or untyped call site can bypass that — and is routed to the DLQ
 * rather than sent to a caller that has no schema for it.
 */
export function publishRpcErrorReply(
  ctx: ReplyContext,
  msg: ConsumeMessage,
  queueName: string,
  errorSchemas: RpcErrorMap | undefined,
  rpcName: string,
  error: RpcError,
): AsyncResult<void, HandlerError> {
  // `Object.hasOwn` rather than plain indexing so prototype properties
  // (e.g. "toString") are not misclassified as declared error codes.
  const errorSchema =
    errorSchemas && Object.hasOwn(errorSchemas, error.code) ? errorSchemas[error.code] : undefined;
  if (!errorSchema) {
    return ErrAsync<HandlerError>(
      new NonRetryableError(
        `RPC "${rpcName}" returned undeclared error code "${error.code}"`,
        error,
      ),
    );
  }

  return requireReplyAddress(ctx, msg, rpcName, queueName)
    .toAsync()
    .flatMap(({ replyTo, correlationId }) =>
      validateReplyPayload(
        errorSchema.data as StandardSchemaV1,
        error.data,
        `RPC error data for "${rpcName}" code "${error.code}"`,
        rpcName,
      ).flatMap((validatedData) =>
        publishReply(ctx, { message: error.message, data: validatedData }, replyTo, {
          correlationId,
          contentType: "application/json",
          headers: { [RPC_ERROR_CODE_HEADER]: error.code },
        }),
      ),
    );
}

/**
 * Extract and require the `replyTo` / `correlationId` pair an RPC reply is
 * routed by, with `replyTo` on the worker's allowlist. Missing either, or a
 * disallowed `replyTo`, is a NonRetryableError: the caller is already
 * lost (or cannot demultiplex the reply), so retrying the original message
 * cannot recover the reply path — the poison message lands in DLQ for
 * inspection rather than being silently ack'd.
 */
function requireReplyAddress(
  ctx: ReplyContext,
  msg: ConsumeMessage,
  rpcName: string,
  queueName: string,
): Result<{ replyTo: string; correlationId: string }, HandlerError> {
  const replyTo = msg.properties.replyTo;
  const correlationId = msg.properties.correlationId;
  if (typeof replyTo !== "string" || replyTo.length === 0) {
    ctx.logger?.error("RPC handler returned a reply but the incoming message has no replyTo", {
      rpcName,
      queueName,
    });
    return Err(
      new NonRetryableError(
        `RPC "${rpcName}" received a message without replyTo; cannot deliver response`,
      ),
    );
  }
  if (!ctx.allowReplyTo(replyTo)) {
    // Never publish into an address the operator did not allow: a request
    // could otherwise make the worker write its reply into any queue.
    ctx.logger?.error("RPC request has a replyTo the worker does not allow; dead-lettering it", {
      rpcName,
      queueName,
      replyTo,
    });
    return Err(
      new NonRetryableError(
        `RPC "${rpcName}" received a message with a disallowed replyTo "${replyTo}"; not replying`,
      ),
    );
  }
  if (typeof correlationId !== "string" || correlationId.length === 0) {
    // Without a correlationId the client cannot match the reply to its
    // pending call — publishing anyway would guarantee a client-side timeout.
    ctx.logger?.error(
      "RPC handler returned a reply but the incoming message has no correlationId",
      { rpcName, queueName, replyTo },
    );
    return Err(
      new NonRetryableError(
        `RPC "${rpcName}" received a message without correlationId; cannot deliver response`,
      ),
    );
  }
  return Ok({ replyTo, correlationId });
}

/**
 * Validate a reply payload (RPC response or RPC error data) against its
 * schema. Validation failures are NonRetryableError — the handler produced
 * the wrong shape; retrying the same input will not fix it.
 */
export function validateReplyPayload(
  schema: StandardSchemaV1,
  value: unknown,
  description: string,
  source: string,
): AsyncResult<unknown, HandlerError> {
  // `fromSchemaAsync` owns the validation boundary: schema issues surface as
  // the modeled error, and a validator that throws synchronously or rejects
  // becomes a Defect — it can never crash the consume callback. Both are
  // recovered into NonRetryableError here because the reply-side policy is
  // the same either way: the handler (or its schema) produced something
  // unusable, and retrying the same input will not fix it — DLQ.
  return fromSchemaAsync(schema)(value)
    .mapErrCases((matcher) =>
      matcher.with(
        // oxlint-disable-next-line unthrown/no-catch-all-pattern -- SchemaIssues is a single non-union error type
        P._,
        (issues): HandlerError =>
          new NonRetryableError(
            `${description} failed schema validation`,
            new MessageValidationError(source, issues),
          ),
      ),
    )
    .recoverDefect((cause) =>
      Err<HandlerError>(new NonRetryableError(`${description} schema validation threw`, cause)),
    );
}

/**
 * Publish a validated reply body to the caller's reply queue via the AMQP
 * default exchange.
 *
 * Reply-side failures are not retryable from the inbox: by the time the
 * broker can't deliver the reply, the caller's RPC future has already (or
 * will shortly) time out. Retrying the original message re-runs the handler
 * against a stale caller. Send to DLQ instead so the failure is visible
 * without churning the queue.
 */
function publishReply(
  ctx: ReplyContext,
  body: unknown,
  replyTo: string,
  options: { correlationId: string; contentType: string; headers?: Record<string, unknown> },
): AsyncResult<void, HandlerError> {
  // The documented semantics of this path are "reply failure →
  // NonRetryableError → DLQ" (see publishRpcResponse), because retrying the
  // original message re-runs the handler against a caller that has already
  // timed out: a modeled PublishError and an unclassifiable failure (defect)
  // alike become that NonRetryableError.
  return ctx.amqpClient
    .publish({ exchange: "", routingKey: replyTo }, body, options)
    .mapErrCases((matcher) =>
      matcher.with(
        P.tag(PublishError.tag),
        (error): HandlerError => new NonRetryableError("Failed to publish RPC reply", error),
      ),
    )
    .recoverDefect((cause) =>
      Err<HandlerError>(new NonRetryableError("Failed to publish RPC reply", cause)),
    );
}
