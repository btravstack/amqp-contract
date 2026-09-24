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
 * positionally, so `({ errors, input }) => ...` and
 * `({ errors }, message) => ...` are the same call. `raw` rides here rather
 * than in a third parameter for the same reason — one record for everything
 * the delivery carries.
 */
export type WorkerHandlerHelpers<
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
  TErrors = EmptyContext,
  TMessage = unknown,
> = {
  /**
   * The validated message — the SAME value the second parameter carries. It is
   * on the record so a whole handler is one destructuring, which is oRPC's own
   * shape and its own word for it: `ProcedureHandlerOptions` carries `input`,
   * and the handler still takes it positionally. One name across the three
   * transports is the point — a developer moving between them destructures
   * `input` in each.
   */
  readonly input: TMessage;
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
 * What a handler receives as `input` on its record, and again as the second
 * positional argument. It contains the validated payload and (if defined in
 * the message schema) the validated headers.
 *
 * @template TPayload - The inferred payload type from the message schema
 * @template THeaders - The inferred headers type from the message schema (undefined if not defined)
 *
 * @example
 * ```typescript
 * const handler = declareHandler(contract, 'processOrder', ({ raw, input }) => {
 *   console.log(input.payload.orderId);  // Typed payload
 *   console.log(input.headers?.priority); // Typed headers (if defined)
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
// `{ input, context, errors, raw, retryable, nonRetryable }`: `context` is
// produced by `createContext` and the middleware chain (an empty object when
// neither is configured), `errors` carries typed constructors for the RPC's
// declared errors (empty for consumers), `raw` is the AMQP delivery, and the
// two factories are the modeled failures. So `({ errors, input }) => ...` and
// `({ errors }, message) => ...` are the same call — oRPC offers both — and a
// handler that wants only its message is `({ input: { payload } }) => ...`,
// with no placeholder to spell.

/**
 * A consumer handler for messages whose validated payload is `TPayload` (and
 * headers `THeaders`): the helpers record first, the message second, an
 * `AsyncResult<void, HandlerError>` back. This short alias — over the
 * RESOLVED payload — is what a mistake in a handler reports
 * (`ConsumerHandler<{ to: string; }, …>`), never the whole contract type.
 */
export type ConsumerHandler<
  TPayload,
  THeaders = undefined,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> = (
  helpers: WorkerHandlerHelpers<TContext, EmptyContext, WorkerConsumedMessage<TPayload, THeaders>>,
  message: WorkerConsumedMessage<TPayload, THeaders>,
) => AsyncResult<void, HandlerError>;

/** A {@link ConsumerHandler}, or a `[handler, consumerOptions]` tuple. */
export type ConsumerHandlerEntry<
  TPayload,
  THeaders = undefined,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> =
  | ConsumerHandler<TPayload, THeaders, TContext>
  | readonly [ConsumerHandler<TPayload, THeaders, TContext>, ConsumerOptions];

/**
 * An RPC handler for requests whose validated payload is `TRequest`,
 * answering `TResponse` — or one of the declared errors `TErrors` (an
 * `RpcError` union), whose constructors it receives as `helpers.errors`.
 * Like {@link ConsumerHandler}, a short alias over resolved types.
 */
export type RpcHandler<
  TRequest,
  TResponse,
  TErrors extends RpcError = never,
  THeaders = undefined,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> = (
  helpers: WorkerHandlerHelpers<
    TContext,
    RpcErrorConstructors<TErrors>,
    WorkerConsumedMessage<TRequest, THeaders>
  >,
  message: WorkerConsumedMessage<TRequest, THeaders>,
) => AsyncResult<TResponse, HandlerError | TErrors>;

/** A {@link RpcHandler}, or a `[handler, consumerOptions]` tuple. */
export type RpcHandlerEntry<
  TRequest,
  TResponse,
  TErrors extends RpcError = never,
  THeaders = undefined,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> =
  | RpcHandler<TRequest, TResponse, TErrors, THeaders, TContext>
  | readonly [RpcHandler<TRequest, TResponse, TErrors, THeaders, TContext>, ConsumerOptions];

/** `helpers.errors` for an RPC declaring the errors `TErrors`: one constructor per code. */
type RpcErrorConstructors<TErrors extends RpcError> = [TErrors] extends [never]
  ? EmptyContext
  : {
      [E in TErrors as E["code"]]: (data: E["data"], message?: string) => E;
    };

// The contract-driven names below resolve the payload types FIRST (the
// `extends [infer …]` step), then instantiate the short alias with them — so
// the compiler reports `ConsumerHandler<{ … }>`, not `…<ContractOutput<…>>`.

/**
 * Handler type for the consumer `TName` of `TContract`: a
 * {@link ConsumerHandler} over that consumer's resolved payload and headers.
 */
export type WorkerInferConsumerHandler<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> = [
  WorkerInferConsumerPayload<TContract, TName>,
  WorkerInferConsumerHeaders<TContract, TName>,
] extends [infer TPayload, infer THeaders]
  ? ConsumerHandler<TPayload, THeaders, TContext>
  : never;

/**
 * Handler type for the RPC `TName` of `TContract`: a {@link RpcHandler} over
 * its resolved request, response, declared errors and headers.
 */
export type WorkerInferRpcHandler<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> = [
  WorkerInferRpcRequest<TContract, TName>,
  WorkerInferRpcResponse<TContract, TName>,
  WorkerInferRpcErrors<TContract, TName>,
  WorkerInferRpcHeaders<TContract, TName>,
] extends [infer TRequest, infer TResponse, infer TErrors extends RpcError, infer THeaders]
  ? RpcHandler<TRequest, TResponse, TErrors, THeaders, TContext>
  : never;

/** A {@link WorkerInferConsumerHandler}, or a `[handler, consumerOptions]` tuple. */
export type WorkerInferConsumerHandlerEntry<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> = [
  WorkerInferConsumerPayload<TContract, TName>,
  WorkerInferConsumerHeaders<TContract, TName>,
] extends [infer TPayload, infer THeaders]
  ? ConsumerHandlerEntry<TPayload, THeaders, TContext>
  : never;

/** A {@link WorkerInferRpcHandler}, or a `[handler, consumerOptions]` tuple. */
export type WorkerInferRpcHandlerEntry<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
> = [
  WorkerInferRpcRequest<TContract, TName>,
  WorkerInferRpcResponse<TContract, TName>,
  WorkerInferRpcErrors<TContract, TName>,
  WorkerInferRpcHeaders<TContract, TName>,
] extends [infer TRequest, infer TResponse, infer TErrors extends RpcError, infer THeaders]
  ? RpcHandlerEntry<TRequest, TResponse, TErrors, THeaders, TContext>
  : never;

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
