import type {
  ConsumerDefinition,
  ConsumerEntry,
  ContractDefinition,
  InferConsumerNames,
  InferRpcNames,
  MessageDefinition,
  QueueEntry,
  RpcDefinition,
  RpcErrorMap,
} from "@amqp-contract/contract";
import type { RpcError } from "@amqp-contract/core";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { ConsumeMessage } from "amqplib";
import type { AsyncResult } from "unthrown";
import type { HandlerError } from "./errors.js";
import { ConsumerOptions } from "./worker.js";

/**
 * Infer the output type from a schema (used by consumers after validation)
 */
type InferSchemaOutput<TSchema extends StandardSchemaV1> =
  TSchema extends StandardSchemaV1<infer _TInput, infer TOutput> ? TOutput : never;

/**
 * Infer the input type from a schema (used for RPC error data — the handler
 * supplies the pre-validation shape; the worker validates before replying).
 */
type InferSchemaInput<TSchema extends StandardSchemaV1> =
  TSchema extends StandardSchemaV1<infer TInput> ? TInput : never;

/**
 * Extract the ConsumerDefinition from any consumer entry type.
 * Handles ConsumerDefinition, EventConsumerResult, and CommandConsumerConfig.
 */
type ExtractConsumerDefinition<T extends ConsumerEntry> = T extends ConsumerDefinition
  ? T
  : T extends { consumer: ConsumerDefinition }
    ? T["consumer"]
    : never;

/**
 * Infer consumer message payload output type.
 * Works with any consumer entry type by first extracting the ConsumerDefinition.
 */
type ConsumerInferPayloadOutput<TConsumer extends ConsumerEntry> =
  ExtractConsumerDefinition<TConsumer> extends ConsumerDefinition
    ? InferSchemaOutput<ExtractConsumerDefinition<TConsumer>["message"]["payload"]>
    : never;

/**
 * Infer consumer message headers output type.
 * Returns undefined if no headers schema is defined.
 */
type ConsumerInferHeadersOutput<TConsumer extends ConsumerEntry> =
  ExtractConsumerDefinition<TConsumer> extends ConsumerDefinition
    ? ExtractConsumerDefinition<TConsumer>["message"] extends MessageDefinition<
        infer _TPayload,
        infer THeaders
      >
      ? THeaders extends StandardSchemaV1<Record<string, unknown>>
        ? InferSchemaOutput<THeaders>
        : undefined
      : undefined
    : undefined;

// =============================================================================
// Per-name lookups
// =============================================================================

type InferConsumers<TContract extends ContractDefinition> = NonNullable<TContract["consumers"]>;
type InferConsumer<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
> = InferConsumers<TContract>[TName];

type InferRpcs<TContract extends ContractDefinition> = NonNullable<TContract["rpcs"]>;
type InferRpc<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> = InferRpcs<TContract>[TName];

/**
 * Infer the payload type for a regular consumer (validated against the message schema).
 */
type WorkerInferConsumerPayload<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
> = ConsumerInferPayloadOutput<InferConsumer<TContract, TName>>;

/**
 * Infer the headers type for a regular consumer.
 * Returns undefined if no headers schema is defined.
 */
export type WorkerInferConsumerHeaders<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
> = ConsumerInferHeadersOutput<InferConsumer<TContract, TName>>;

/**
 * Infer the request payload type for an RPC.
 */
export type WorkerInferRpcRequest<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> =
  InferRpc<TContract, TName> extends RpcDefinition<infer TRequest, MessageDefinition>
    ? TRequest extends MessageDefinition
      ? InferSchemaOutput<TRequest["payload"]>
      : never
    : never;

/**
 * Infer the request headers type for an RPC. Returns undefined unless the RPC's
 * request `MessageDefinition` declares a headers schema.
 */
export type WorkerInferRpcHeaders<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> =
  InferRpc<TContract, TName> extends RpcDefinition<infer TRequest, MessageDefinition>
    ? TRequest extends MessageDefinition<infer _TPayload, infer THeaders>
      ? THeaders extends StandardSchemaV1<Record<string, unknown>>
        ? InferSchemaOutput<THeaders>
        : undefined
      : undefined
    : undefined;

/**
 * Infer the typed error union for an RPC handler — one `RpcError<code, data>`
 * member per entry in the RPC's `errors` map, with `data` typed as the
 * declared schema's *input* (the worker validates before replying). Resolves
 * to `never` when the RPC declares no errors, leaving the handler's error
 * channel as plain `HandlerError`.
 */
export type WorkerInferRpcErrors<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> =
  InferRpc<TContract, TName> extends RpcDefinition<
    MessageDefinition,
    MessageDefinition,
    QueueEntry,
    infer TErrors
  >
    ? TErrors extends RpcErrorMap
      ? {
          [K in keyof TErrors & string]: RpcError<K, InferSchemaInput<TErrors[K]["payload"]>>;
        }[keyof TErrors & string]
      : never
    : never;

/**
 * Infer the response payload type for an RPC. The handler must return a
 * `AsyncResult<TResponse, HandlerError>` matching this shape.
 */
export type WorkerInferRpcResponse<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> =
  InferRpc<TContract, TName> extends RpcDefinition<MessageDefinition, infer TResponse>
    ? TResponse extends MessageDefinition
      ? InferSchemaOutput<TResponse["payload"]>
      : never
    : never;

// =============================================================================
// Consumed message envelopes
// =============================================================================

/**
 * A consumed message containing parsed payload and headers.
 *
 * This type represents the first argument passed to consumer handlers.
 * It contains the validated payload and (if defined in the message schema) the validated headers.
 *
 * @template TPayload - The inferred payload type from the message schema
 * @template THeaders - The inferred headers type from the message schema (undefined if not defined)
 *
 * @example
 * ```typescript
 * const handler = defineHandler(contract, 'processOrder', (message, rawMessage) => {
 *   console.log(message.payload.orderId);  // Typed payload
 *   console.log(message.headers?.priority); // Typed headers (if defined)
 *   console.log(rawMessage.fields.deliveryTag); // Raw AMQP message
 *   return Ok(undefined).toAsync();
 * });
 * ```
 */
export type WorkerConsumedMessage<TPayload, THeaders = undefined> = {
  /** The validated message payload */
  payload: TPayload;
  /** The validated message headers (present only when headers schema is defined) */
  headers: THeaders extends undefined ? undefined : THeaders;
};

/**
 * Infer the full consumed message type for a regular consumer.
 */
export type WorkerInferConsumedMessage<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
> = WorkerConsumedMessage<
  WorkerInferConsumerPayload<TContract, TName>,
  WorkerInferConsumerHeaders<TContract, TName>
>;

/**
 * Infer the consumed message type for an RPC handler — payload + headers from
 * the request side of the RPC.
 */
export type WorkerInferRpcConsumedMessage<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> = WorkerConsumedMessage<
  WorkerInferRpcRequest<TContract, TName>,
  WorkerInferRpcHeaders<TContract, TName>
>;

// =============================================================================
// Handler Types
// =============================================================================
// All handlers return `AsyncResult<TResponse, HandlerError>` for explicit
// error handling. Regular consumers return `void`; RPC handlers return the
// response payload. RetryableError → exponential backoff retry; NonRetryableError → DLQ.

/**
 * Handler signature for a regular consumer (event/command). Returns
 * `AsyncResult<void, HandlerError>` — there is no response message.
 */
export type WorkerInferConsumerHandler<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
> = (
  message: WorkerInferConsumedMessage<TContract, TName>,
  rawMessage: ConsumeMessage,
) => AsyncResult<void, HandlerError>;

/**
 * Handler signature for an RPC. Returns
 * `AsyncResult<TResponse, HandlerError | RpcError>` where `TResponse` is the
 * inferred response payload and the `RpcError` members come from the RPC's
 * declared `errors` map (absent when none are declared). The worker validates
 * the response against the RPC's response schema and publishes it back to
 * `msg.properties.replyTo` with the same `correlationId`; a declared
 * `RpcError` is validated, published as an error reply, and the request is
 * acked (business errors are not retried).
 */
export type WorkerInferRpcHandler<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> = (
  message: WorkerInferRpcConsumedMessage<TContract, TName>,
  rawMessage: ConsumeMessage,
) => AsyncResult<
  WorkerInferRpcResponse<TContract, TName>,
  HandlerError | WorkerInferRpcErrors<TContract, TName>
>;

/**
 * Handler entry for a regular consumer — function or `[handler, options]`.
 */
export type WorkerInferConsumerHandlerEntry<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
> =
  | WorkerInferConsumerHandler<TContract, TName>
  | readonly [WorkerInferConsumerHandler<TContract, TName>, ConsumerOptions];

/**
 * Handler entry for an RPC — function or `[handler, options]`.
 */
export type WorkerInferRpcHandlerEntry<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> =
  | WorkerInferRpcHandler<TContract, TName>
  | readonly [WorkerInferRpcHandler<TContract, TName>, ConsumerOptions];

/**
 * All handlers for a contract: one entry per `consumers` key plus one entry
 * per `rpcs` key. The two name spaces are disjoint so the resulting object
 * type is unambiguous.
 *
 * @example
 * ```typescript
 * const handlers: WorkerInferHandlers<typeof contract> = {
 *   processOrder: ({ payload }) =>
 *     fromPromise(
 *       processPayment(payload),
 *       (error) => new RetryableError('Payment failed', error),
 *     ).map(() => undefined),
 *   calculate: ({ payload }) => Ok({ sum: payload.a + payload.b }).toAsync(),
 * };
 * ```
 */
export type WorkerInferHandlers<TContract extends ContractDefinition> = ([
  InferConsumerNames<TContract>,
] extends [never]
  ? {}
  : { [K in InferConsumerNames<TContract>]: WorkerInferConsumerHandlerEntry<TContract, K> }) &
  ([InferRpcNames<TContract>] extends [never]
    ? {}
    : { [K in InferRpcNames<TContract>]: WorkerInferRpcHandlerEntry<TContract, K> });
