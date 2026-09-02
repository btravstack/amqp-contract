import type {
  ConsumerDefinition,
  ConsumerEntry,
  ContractDefinition,
  InferConsumerNames,
  InferRpcNames,
  InferSchemaInput,
  InferSchemaOutput,
  MessageDefinition,
  QueueDefinition,
  RpcDefinition,
  RpcErrorMap,
} from "@amqp-contract/contract";
import type { RpcError } from "@amqp-contract/core";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { ConsumeMessage } from "amqplib";
import type { AsyncResult } from "unthrown";

import type { HandlerError, NonRetryableError, RetryableError } from "./errors.js";
import type { EmptyContext } from "./middleware.js";
import { type ConsumerOptions } from "./worker.js";

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
 * Typed constructors for an RPC's declared errors, handed to the handler via
 * its helpers argument: `errors.ORDER_NOT_FOUND({ orderId })` builds the
 * `RpcError` with per-code data inference and autocomplete — the
 * constructor-bag form of the free `rpcError(code, data)` factory (org DNA,
 * mirroring temporal-contract's `helpers.errors`).
 */
export type WorkerInferRpcErrorConstructors<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> =
  InferRpc<TContract, TName> extends RpcDefinition<
    MessageDefinition,
    MessageDefinition,
    QueueDefinition,
    infer TErrors
  >
    ? TErrors extends RpcErrorMap
      ? {
          [K in keyof TErrors & string]: (
            data: InferSchemaInput<TErrors[K]["data"]>,
            message?: string,
          ) => RpcError<K, InferSchemaInput<TErrors[K]["data"]>>;
        }
      : EmptyContext
    : EmptyContext;

/**
 * The helpers record every handler receives as its FIRST argument —
 * everything the delivery carries, the validated message included, with that
 * message repeated as the second parameter.
 *
 * That is oRPC's shape, and the one this family converged on:
 * `ProcedureHandlerOptions` carries `input` and the handler still takes it
 * positionally, so `({ errors, message }) => ...` and
 * `({ errors }, message) => ...` are the same call. `raw` rides here rather
 * than in a third parameter for the same reason — one record for everything
 * ambient, and the message where a reader wants it.
 */
export type WorkerHandlerHelpers<
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
  TErrors = EmptyContext,
  TMessage = unknown,
> = {
  /**
   * The validated message — the SAME value the second parameter carries. It is
   * on the record so a whole handler can be written from one destructuring,
   * which is oRPC's own shape: `ProcedureHandlerOptions` carries `input` and
   * the handler still takes it positionally. Take it whichever way reads better
   * at the call.
   */
  readonly message: TMessage;
  /** Context produced by `createContext` and the middleware chain. */
  readonly context: TContext;
  /** Typed constructors for the contract-declared errors (empty for consumers). */
  readonly errors: TErrors;
  /** The raw AMQP delivery — `fields`, `properties`, and the untouched `content`. */
  readonly raw: ConsumeMessage;
  /**
   * "Infrastructure comes back" — the failure the retry schedule is for,
   * handed over rather than imported and constructed. `ErrAsync(retryable(...))`
   * is `ErrAsync(new RetryableError(...))` without the import.
   */
  readonly retryable: (message: string, cause?: unknown) => RetryableError;
  /**
   * "This will never work" — straight to the dead-letter queue, no retry
   * budget spent. The permanent twin of {@link WorkerHandlerHelpers.retryable}.
   */
  readonly nonRetryable: (message: string, cause?: unknown) => NonRetryableError;
};

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
    QueueDefinition,
    infer TErrors
  >
    ? TErrors extends RpcErrorMap
      ? {
          [K in keyof TErrors & string]: RpcError<K, InferSchemaInput<TErrors[K]["data"]>>;
        }[keyof TErrors & string]
      : never
    : never;

/**
 * Infer the response payload type for an RPC. The handler must return an
 * `AsyncResult<TResponse, HandlerError>` matching this shape.
 *
 * Typed as the schema's *input* — the handler supplies the pre-validation
 * shape (defaults optional, transforms not yet applied); the worker validates
 * against the response schema before publishing the reply. Same convention as
 * RPC error data.
 */
export type WorkerInferRpcResponse<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> =
  InferRpc<TContract, TName> extends RpcDefinition<MessageDefinition, infer TResponse>
    ? TResponse extends MessageDefinition
      ? InferSchemaInput<TResponse["payload"]>
      : never
    : never;

// =============================================================================
// Consumed message envelopes
// =============================================================================

/**
 * A consumed message containing parsed payload and headers.
 *
 * This type represents the second argument passed to consumer handlers — the
 * helpers record comes first. It contains the validated payload and (if
 * defined in the message schema) the validated headers.
 *
 * @template TPayload - The inferred payload type from the message schema
 * @template THeaders - The inferred headers type from the message schema (undefined if not defined)
 *
 * @example
 * ```typescript
 * const handler = declareHandler(contract, 'processOrder', ({ raw }, message) => {
 *   console.log(message.payload.orderId);  // Typed payload
 *   console.log(message.headers?.priority); // Typed headers (if defined)
 *   console.log(raw.fields.deliveryTag); // Raw AMQP delivery
 *   return OkAsync(undefined);
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
//
// Every handler takes the `helpers` record FIRST and the validated message
// second — oRPC's shape, which this family converged on, down to the message
// being on the record as well as in the second parameter. `helpers` is
// `{ message, context, errors, raw, retryable, nonRetryable }`: `context` is
// produced by `createContext` and the middleware chain (an empty object when
// neither is configured), `errors` carries typed constructors for the RPC's
// declared errors (empty for consumers), `raw` is the AMQP delivery, and the
// two factories are the modeled failures. So `({ errors, message }) => ...`
// and `({ errors }, message) => ...` are the same call, and a handler that
// needs none of them is `(_, { payload }) => ...`.

/**
 * Handler signature for a regular consumer (event/command). Returns
 * `AsyncResult<void, HandlerError>` — there is no response message.
 */
export type WorkerInferConsumerHandler<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> = (
  helpers: WorkerHandlerHelpers<
    TContext,
    EmptyContext,
    WorkerInferConsumedMessage<TContract, TName>
  >,
  message: WorkerInferConsumedMessage<TContract, TName>,
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
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> = (
  helpers: WorkerHandlerHelpers<
    TContext,
    WorkerInferRpcErrorConstructors<TContract, TName>,
    WorkerInferRpcConsumedMessage<TContract, TName>
  >,
  message: WorkerInferRpcConsumedMessage<TContract, TName>,
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
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> =
  | WorkerInferConsumerHandler<TContract, TName, TContext>
  | readonly [WorkerInferConsumerHandler<TContract, TName, TContext>, ConsumerOptions];

/**
 * Handler entry for an RPC — function or `[handler, options]`.
 */
export type WorkerInferRpcHandlerEntry<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> =
  | WorkerInferRpcHandler<TContract, TName, TContext>
  | readonly [WorkerInferRpcHandler<TContract, TName, TContext>, ConsumerOptions];

/**
 * All handlers for a contract: one entry per `consumers` key plus one entry
 * per `rpcs` key. The two name spaces are disjoint so the resulting object
 * type is unambiguous.
 *
 * `TContext` is the context produced by the worker's middleware chain; the
 * third handler argument is typed with it.
 *
 * @example
 * ```typescript
 * const handlers: WorkerInferHandlers<typeof contract> = {
 *   processOrder: (_, { payload }) =>
 *     fromPromise(
 *       processPayment(payload),
 *       (error) => new RetryableError('Payment failed', error),
 *     ).map(() => undefined),
 *   calculate: (_, { payload }) => OkAsync({ sum: payload.a + payload.b }),
 * };
 * ```
 */
export type WorkerInferHandlers<
  TContract extends ContractDefinition,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> = ([InferConsumerNames<TContract>] extends [never]
  ? {}
  : {
      [K in InferConsumerNames<TContract>]: WorkerInferConsumerHandlerEntry<TContract, K, TContext>;
    }) &
  ([InferRpcNames<TContract>] extends [never]
    ? {}
    : { [K in InferRpcNames<TContract>]: WorkerInferRpcHandlerEntry<TContract, K, TContext> });
