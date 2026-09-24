import {
  type ConsumerDefinition,
  type ContractDefinition,
  type InferConsumerNames,
  type InferRpcNames,
  type RpcErrorMap,
  extractConsumer,
} from "@amqp-contract/contract";
import { _internal_queueHasDeadLetterExchange } from "@amqp-contract/contract/internal";
import {
  AmqpClient,
  type AmqpConsumeOptions,
  type ConnectionError,
  type Logger,
  RpcError,
  TechnicalError,
  type TelemetryProvider,
  defaultTelemetryProvider,
  isRpcError,
} from "@amqp-contract/core";
import {
  decodeMessage,
  startConsumeSpan,
  startOrClose,
  technicalDefect,
} from "@amqp-contract/core/internal";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import { fromSchemaAsync } from "@unthrown/standard-schema";
import type { AmqpConnectionManagerOptions, ConnectionUrl } from "amqp-connection-manager";
import type { ConsumeMessage } from "amqplib";
import {
  allAsync,
  Err,
  fromPromise,
  fromSafePromise,
  Ok,
  OkAsync,
  P,
  type AsyncResult,
} from "unthrown";

import type { HandlerError } from "./errors.js";
import { MessageValidationError, NonRetryableError, RetryableError } from "./errors.js";
import {
  availableHandlerNames,
  invalidHandlerNames,
  missingHandlerNames,
  unknownHandlerNames,
} from "./handlers.js";
import {
  composeMiddleware,
  type AnyWorkerMiddleware,
  type EmptyContext,
  type WorkerMiddleware,
} from "./middleware.js";
import {
  ACKED,
  asError,
  type DeadLettered,
  type Outcome,
  recordOutcome,
  settle,
} from "./outcome.js";
import { handleError, readCount } from "./retry.js";
import {
  isDirectReplyTo,
  publishRpcErrorReply,
  publishRpcResponse,
  type ReplyContext,
  validateReplyPayload,
} from "./rpc-reply.js";
import type { WorkerInferHandlers } from "./types.js";

/**
 * Either a regular consumer name or an RPC name from the contract.
 */
type HandlerName<TContract extends ContractDefinition> =
  | InferConsumerNames<TContract>
  | InferRpcNames<TContract>;

/**
 * Resolved handler entry stored on the worker, regardless of whether the
 * source is a `consumers` or `rpcs` slot. The handler signature is widened
 * here because both kinds share the same dispatch loop; specific call sites
 * cast back to the correct typed handler. RPC handlers may additionally fail
 * with a contract-declared `RpcError`, which the dispatch path publishes back
 * to the caller instead of routing to retry/DLQ.
 */
/**
 * The two modeled failures, as factories the handler is handed. Stateless, so
 * they are built once for the module rather than per delivery.
 */
const retryableFactory = (message: string, cause?: unknown): RetryableError =>
  new RetryableError(message, cause);
const nonRetryableFactory = (message: string, cause?: unknown): NonRetryableError =>
  new NonRetryableError(message, cause);

type StoredHandler = (
  helpers: {
    input: { payload: unknown; headers: unknown };
    context: Record<string, unknown>;
    errors: Record<string, (data: unknown, message?: string) => RpcError>;
    raw: ConsumeMessage;
    retryable: (message: string, cause?: unknown) => RetryableError;
    nonRetryable: (message: string, cause?: unknown) => NonRetryableError;
  },
  message: { payload: unknown; headers: unknown },
) => AsyncResult<unknown, HandlerError | RpcError>;

/** A message whose payload (and headers, when declared) passed their schemas. */
type ValidatedMessage = { payload: unknown; headers: unknown };

/**
 * Per-message information handed to the `createContext` factory — enough to
 * derive request-scoped dependencies (correlation-id loggers, per-message
 * transactions) without closing over the dispatch loop.
 */
export type WorkerCreateContextInfo = {
  /** The `consumers` / `rpcs` key being dispatched. */
  handlerName: string;
  /** True when the handler is an RPC server. */
  isRpc: boolean;
  /** The validated message (payload and headers already schema-checked). */
  message: { payload: unknown; headers: unknown };
  /** The raw amqplib message. */
  rawMessage: ConsumeMessage;
};

/**
 * `ConsumerDefinition`-shaped view over a `consumers` or `rpcs` entry, as
 * produced by `resolveConsumerView`. `isRpc` (with `responseSchema` and
 * `errorSchemas`) tells the dispatch path whether to validate the handler
 * return value and publish a reply.
 */
type ConsumerView = {
  consumer: ConsumerDefinition;
  isRpc: boolean;
  responseSchema?: StandardSchemaV1 | undefined;
  errorSchemas?: RpcErrorMap | undefined;
  /** Typed error constructors handed to RPC handlers via `helpers.errors`. */
  errorConstructors?: Record<string, (data: unknown, message?: string) => RpcError> | undefined;
};

/**
 * Consumer options a worker handler may configure — a curated subset of the
 * AMQP consume options. `noAck` and `noLocal` are deliberately excluded:
 * `noAck: true` would silently break the worker's ack-exactly-once and
 * retry/DLQ invariants (deliveries would be considered settled on send), and
 * `noLocal` is not supported by RabbitMQ.
 */
export type ConsumerOptions = Pick<
  AmqpConsumeOptions,
  "prefetch" | "priority" | "arguments" | "consumerTag" | "exclusive"
>;

/**
 * Default time `close()` waits for in-flight handlers before tearing the
 * channel down anyway. Finite by default so a hung handler cannot wedge
 * shutdown — the un-acked deliveries are redelivered by the broker
 * (at-least-once semantics). Pass `drainTimeoutMs: null` to wait forever.
 */
export const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;

/**
 * Type guard to check if a handler entry is a tuple format [handler, options].
 */
function isHandlerTuple(entry: unknown): entry is [unknown, ConsumerOptions] {
  return Array.isArray(entry) && entry.length === 2;
}

/**
 * Options for creating a type-safe AMQP worker.
 *
 * @typeParam TContract - The contract definition type
 *
 * @example
 * ```typescript
 * const options: CreateWorkerOptions<typeof contract> = {
 *   contract: myContract,
 *   handlers: {
 *     // Simple handler
 *     processOrder: ({ input: { payload } }) => {
 *       console.log('Processing order:', payload.orderId);
 *       return OkAsync(undefined);
 *     },
 *     // Handler with prefetch configuration
 *     processPayment: [
 *       ({ input: { payload } }) => {
 *         console.log('Processing payment:', payload.paymentId);
 *         return OkAsync(undefined);
 *       },
 *       { prefetch: 10 }
 *     ]
 *   },
 *   urls: ['amqp://localhost'],
 *   defaultConsumerOptions: {
 *     prefetch: 5,
 *   },
 *   connectionOptions: {
 *     heartbeatIntervalInSeconds: 30
 *   },
 *   logger: myLogger
 * };
 * ```
 *
 * Note: Retry configuration is defined at the queue level in the contract,
 * not at the handler level. See `QueueDefinition.retry` for configuration options.
 */
export type CreateWorkerOptions<
  TContract extends ContractDefinition,
  TCreated extends Record<string, unknown> | EmptyContext = EmptyContext,
  TContext extends TCreated = TCreated,
> = {
  /** The AMQP contract definition specifying consumers and their message schemas */
  contract: TContract;
  /**
   * Handlers for each `consumers` and `rpcs` entry in the contract.
   *
   * - Regular consumers return `AsyncResult<void, HandlerError>`.
   * - RPC handlers return `AsyncResult<TResponse, HandlerError>` where
   *   `TResponse` is inferred from the RPC's response message schema. When
   *   the RPC declares an `errors` map, the error channel additionally
   *   accepts the declared `RpcError<code, data>` members (otherwise it
   *   stays plain `HandlerError`).
   *
   * Handlers receive one record FIRST —
   * `{ input, context, errors, raw, retryable, nonRetryable }`, where `input`
   * is the validated message — and that message again as the second
   * parameter. `context` is an empty object when no `createContext` and no
   * `middleware` are configured.
   *
   * Use `declareHandler` / `declareHandlers` to create handlers with full type
   * inference.
   */
  handlers: WorkerInferHandlers<TContract, TContext>;
  /**
   * Build the per-message dependency context — the *seed* of the middleware
   * chain (and, without middleware, the context handlers receive directly in
   * `helpers.context`). Invoked once per message after validation, so it can
   * produce request-scoped values (correlation-id loggers, per-message
   * transactions); close over singletons for per-worker dependencies. A
   * rejection/throw routes the message to the DLQ as a NonRetryableError.
   *
   * demesne's `Layer.forkScope` is the recommended implementation for
   * DI-managed graphs.
   */
  createContext?: ((info: WorkerCreateContextInfo) => TCreated | Promise<TCreated>) | undefined;
  /**
   * Optional middleware wrapping every handler invocation (consumers and
   * RPCs), applied after message validation. The chain is seeded with the
   * `createContext` result (an empty object when none is configured).
   *
   * Accepts either a single middleware or an array (first entry = outermost,
   * mirroring the client's interceptor arrays). The array form composes at
   * runtime exactly like `composeMiddleware(...)`, but cannot thread the
   * stepwise context types across entries — when middleware accumulate typed
   * context for the handlers, pre-compose with
   * `composeMiddleware(outermost, ..., innermost)` so the chain's final
   * context type is inferred into `helpers.context`.
   *
   * A middleware can short-circuit by returning without calling `next`:
   * handler-style errors route through retry/DLQ (or a typed RPC error
   * reply), and an `Ok(value)` skips the handler entirely. `next({ payload })`
   * substitutes the message payload, re-validated before the handler runs.
   */
  middleware?: WorkerMiddleware<TCreated, TContext> | readonly AnyWorkerMiddleware[] | undefined;
  /** AMQP broker URL(s). Multiple URLs provide failover support */
  urls: ConnectionUrl[];
  /** Optional connection configuration (heartbeat, reconnect settings, etc.) */
  connectionOptions?: AmqpConnectionManagerOptions | undefined;
  /** Optional logger for logging message consumption and errors */
  logger?: Logger | undefined;
  /**
   * Optional telemetry provider for tracing and metrics.
   * If not provided, uses the default provider which attempts to load OpenTelemetry.
   * OpenTelemetry instrumentation is automatically enabled if @opentelemetry/api is installed.
   */
  telemetry?: TelemetryProvider | undefined;
  /**
   * Optional default consumer options applied to all consumer handlers.
   * Handler-specific options provided in tuple form override these defaults.
   */
  defaultConsumerOptions?: ConsumerOptions | undefined;
  /**
   * Maximum time in ms to wait for the AMQP connection to become ready before
   * `create()` answers `Err(ConnectionError)`. Defaults to 30s
   * (the {@link AmqpClient}'s `DEFAULT_CONNECT_TIMEOUT_MS`). Pass `null` to
   * disable the timeout and let amqp-connection-manager retry indefinitely.
   */
  connectTimeoutMs?: number | null | undefined;
  /**
   * Maximum time in ms a worker-side publish (retry republish, RPC reply) may
   * sit buffered waiting for the broker before its promise settles with a
   * timeout failure (surfaced as a `Defect`). Maps to
   * amqp-connection-manager's channel-level `publishTimeout`. Defaults to 30s
   * (the {@link AmqpClient}'s `DEFAULT_PUBLISH_TIMEOUT_MS`). Pass `null` to
   * disable, restoring unbounded buffering — a publish issued during an
   * outage then never settles.
   */
  publishTimeoutMs?: number | null | undefined;
  /**
   * Cap on the decompressed size (bytes) of a single inbound message. Guards
   * against a decompression bomb — a few-KB payload that expands to gigabytes
   * before schema validation runs. Over-cap messages follow the poison-message
   * DLQ path. Also caps uncompressed bodies. Defaults to core's
   * `DEFAULT_MAX_MESSAGE_BYTES` (16 MiB).
   */
  maxDecompressedBytes?: number | undefined;
  /** RPC server options. */
  rpc?:
    | {
        /**
         * Which `replyTo` addresses the worker may publish a reply to. By
         * default only RabbitMQ direct reply-to (`amq.rabbitmq.reply-to`,
         * delivered as `amq.rabbitmq.reply-to.<token>`), which is what
         * `client.call()` uses. A request whose `replyTo` is refused is
         * dead-lettered with the reason logged — never replied to — so a
         * forged request cannot make the worker publish into an arbitrary
         * queue. Pass a predicate to allow other reply queues.
         */
        allowReplyTo?: ((replyTo: string) => boolean) | undefined;
      }
    | undefined;
};

/**
 * Type-safe AMQP worker for consuming messages from RabbitMQ.
 *
 * This class provides automatic message validation, connection management,
 * and error handling for consuming messages based on a contract definition.
 *
 * @typeParam TContract - The contract definition type
 *
 * @example
 * ```typescript
 * import { TypedAmqpWorker } from '@amqp-contract/worker';
 * import { defineQueue, defineMessage, defineContract, defineConsumer } from '@amqp-contract/contract';
 * import { OkAsync } from 'unthrown';
 * import { z } from 'zod';
 *
 * const orderQueue = defineQueue('order-processing');
 * const orderMessage = defineMessage(z.object({
 *   orderId: z.string(),
 *   amount: z.number()
 * }));
 *
 * const contract = defineContract({
 *   consumers: {
 *     processOrder: defineConsumer(orderQueue, orderMessage)
 *   }
 * });
 *
 * const worker = await TypedAmqpWorker.create({
 *   contract,
 *   handlers: {
 *     processOrder: ({ input: { payload } }) => {
 *       console.log('Processing order', payload.orderId);
 *       return OkAsync(undefined);
 *     },
 *   },
 *   urls: ['amqp://localhost'],
 * }).getOrThrow();
 *
 * // Close when done (drains in-flight handlers first)
 * await worker.close().get();
 * ```
 */
export class TypedAmqpWorker<TContract extends ContractDefinition> {
  /**
   * Internal handler storage. Keyed by handler name (consumer or RPC); the
   * stored function signature is widened so the dispatch loop can call it
   * uniformly. The actual handler is type-checked at the worker's public API
   * boundary via `WorkerInferHandlers<TContract>`.
   */
  private readonly actualHandlers: Partial<Record<HandlerName<TContract>, StoredHandler>>;
  private readonly consumerOptions: Partial<Record<HandlerName<TContract>, ConsumerOptions>>;
  private readonly consumerTags: Set<string> = new Set();

  /**
   * Messages currently being processed. {@link close} drains this set after
   * cancelling the consumers so in-flight handlers finish (and their acks
   * land) before the channel goes away.
   */
  private readonly inFlight: Set<Promise<void>> = new Set();
  private readonly telemetry: TelemetryProvider;
  private readonly replyContext: ReplyContext;

  private constructor(
    private readonly contract: TContract,
    private readonly amqpClient: AmqpClient,
    handlers: WorkerInferHandlers<TContract>,
    private readonly defaultConsumerOptions: ConsumerOptions,
    private readonly logger?: Logger,
    telemetry?: TelemetryProvider,
    private readonly middleware?: AnyWorkerMiddleware,
    private readonly createContext?: (
      info: WorkerCreateContextInfo,
    ) => Record<string, unknown> | Promise<Record<string, unknown>>,
    private readonly maxDecompressedBytes?: number,
    allowReplyTo: (replyTo: string) => boolean = isDirectReplyTo,
  ) {
    this.replyContext = { amqpClient, logger, allowReplyTo };
    this.telemetry = telemetry ?? defaultTelemetryProvider;

    this.actualHandlers = {};
    this.consumerOptions = {};

    const handlersRecord = handlers as Record<string, unknown>;

    for (const handlerName of Object.keys(handlersRecord)) {
      const handlerEntry = handlersRecord[handlerName];
      const typedName = handlerName as HandlerName<TContract>;

      if (isHandlerTuple(handlerEntry)) {
        const [handler, options] = handlerEntry;
        this.actualHandlers[typedName] = handler as StoredHandler;
        this.consumerOptions[typedName] = {
          ...this.defaultConsumerOptions,
          ...options,
        };
      } else {
        this.actualHandlers[typedName] = handlerEntry as StoredHandler;
        this.consumerOptions[typedName] = this.defaultConsumerOptions;
      }
    }
  }

  /**
   * Build a `ConsumerDefinition`-shaped view for a handler name, regardless
   * of whether it came from `contract.consumers` or `contract.rpcs`. The
   * dispatch path treats both uniformly; the returned `isRpc` flag (and the
   * accompanying `responseSchema`) tells `processMessage` whether to validate
   * the handler return value and publish a reply.
   */
  private resolveConsumerView(name: HandlerName<TContract>): ConsumerView {
    // Use `Object.hasOwn` rather than `key in rpcs` so prototype properties
    // (e.g. "toString") on a plain object are not misclassified as RPC names.
    const rpcs = this.contract.rpcs;
    if (rpcs && Object.hasOwn(rpcs, name as string)) {
      const rpc = rpcs[name as string]!;
      const errorConstructors = rpc.errors
        ? Object.fromEntries(
            Object.entries(rpc.errors).map(([code, entry]) => [
              code,
              // The contract's declared default message fills in when the
              // handler constructs the error without one.
              (data: unknown, message?: string) =>
                new RpcError(code, data, message ?? entry.message),
            ]),
          )
        : undefined;
      return {
        consumer: { queue: rpc.queue, message: rpc.request },
        isRpc: true,
        responseSchema: rpc.response.payload,
        errorSchemas: rpc.errors,
        errorConstructors,
      };
    }
    const consumerEntry = this.contract.consumers![name as string]!;
    return {
      consumer: extractConsumer(consumerEntry),
      isRpc: false,
    };
  }

  /**
   * Create a type-safe AMQP worker from a contract.
   *
   * Connection management (including automatic reconnection) is handled internally
   * by amqp-connection-manager via the {@link AmqpClient}. The worker will set up
   * consumers for all contract-defined handlers asynchronously in the background
   * once the underlying connection and channels are ready.
   *
   * Connections are automatically shared across clients and workers with the same
   * URLs and connection options, following RabbitMQ best practices.
   *
   * @returns An AsyncResult that resolves to the worker. An unreachable broker
   *   is a modeled `Err({@link ConnectionError})` — the anticipated failure of
   *   dialing one, and the case a start-up path wants to branch on. Everything
   *   else that can go wrong here (a bad option, a topology assert the broker
   *   refuses, a bug in a provider) stays on the `Defect` channel with a
   *   `TechnicalError` cause.
   *
   * @example
   * ```typescript
   * const result = await TypedAmqpWorker.create({
   *   contract: myContract,
   *   handlers: {
   *     processOrder: ({ input: { payload } }) => OkAsync(undefined),
   *   },
   *   urls: ['amqp://localhost'],
   * });
   * ```
   */
  static create<
    TContract extends ContractDefinition,
    TCreated extends Record<string, unknown> | EmptyContext = EmptyContext,
    TContext extends TCreated = TCreated,
  >({
    contract,
    handlers,
    createContext,
    middleware,
    urls,
    connectionOptions,
    defaultConsumerOptions,
    logger,
    telemetry,
    connectTimeoutMs,
    publishTimeoutMs,
    maxDecompressedBytes,
    rpc,
  }: CreateWorkerOptions<TContract, TCreated, TContext>): AsyncResult<
    TypedAmqpWorker<TContract>,
    ConnectionError
  > {
    // Fail fast on missing or incomplete handlers — the type system enforces
    // this at the public API boundary, but a JavaScript caller or a cast can
    // bypass it, and the dispatch loop would otherwise crash with an opaque
    // TypeError on the first delivery. Checked before constructing the
    // AmqpClient so no pooled connection reference is acquired on this error
    // path. The nullish/shape guard keeps create() throw-free even when
    // `handlers` is absent entirely.
    if (handlers === null || typeof handlers !== "object") {
      return technicalDefect(
        new TechnicalError(
          "TypedAmqpWorker.create requires a `handlers` object with one handler per `consumers` and `rpcs` entry",
        ),
      ).toAsync();
    }
    const unknown = unknownHandlerNames(contract, handlers);
    if (unknown.length > 0) {
      return technicalDefect(
        new TechnicalError(
          `Unknown handler keys with no matching contract entry: ${unknown.join(", ")}. ` +
            `Declared consumers and RPCs: ${availableHandlerNames(contract).join(", ") || "(none)"}.`,
        ),
      ).toAsync();
    }
    const missing = missingHandlerNames(contract, handlers);
    if (missing.length > 0) {
      return technicalDefect(
        new TechnicalError(
          `Missing handlers for contract entries: ${missing.join(", ")}. ` +
            "Every `consumers` and `rpcs` key requires a handler.",
        ),
      ).toAsync();
    }
    const invalid = invalidHandlerNames(contract, handlers);
    if (invalid.length > 0) {
      return technicalDefect(
        new TechnicalError(
          `Handlers for contract entries are not functions: ${invalid.join(", ")}. ` +
            "Each handler must be a function or a [handler, options] tuple.",
        ),
      ).toAsync();
    }

    return startOrClose(
      () =>
        new TypedAmqpWorker(
          contract,
          new AmqpClient(contract, {
            urls,
            connectionOptions,
            // A pool of its own: never share a TCP connection with a client.
            connectionPool: "worker",
            connectTimeoutMs,
            publishTimeoutMs,
            logger,
          }),
          // Context types are erased at the dispatch boundary: handlers receive
          // whatever the (type-checked) middleware chain produced at runtime.
          handlers as WorkerInferHandlers<TContract>,
          defaultConsumerOptions ?? {},
          logger,
          telemetry,
          // The array form (first = outermost) composes exactly like an explicit
          // composeMiddleware(...) call; an empty array means "no middleware".
          // The cast reaches past the fixed-arity typed overloads to the variadic
          // implementation signature.
          (Array.isArray(middleware)
            ? middleware.length === 0
              ? undefined
              : (
                  composeMiddleware as (...m: readonly AnyWorkerMiddleware[]) => AnyWorkerMiddleware
                )(...(middleware as AnyWorkerMiddleware[]))
            : middleware) as AnyWorkerMiddleware | undefined,
          createContext as
            | ((
                info: WorkerCreateContextInfo,
              ) => Record<string, unknown> | Promise<Record<string, unknown>>)
            | undefined,
          maxDecompressedBytes,
          rpc?.allowReplyTo,
        ),
      // Wait queues are declared by core's setupAmqpTopology (ttl-backoff).
      (worker) => worker.amqpClient.waitForConnect().flatMap(() => worker.consumeAll()),
      { name: "worker", logger },
    );
  }

  /**
   * Close the AMQP channel and connection.
   *
   * Graceful shutdown in three steps: cancel every consumer (no new
   * deliveries), drain in-flight handlers so their acks/nacks land on the
   * still-open channel, then close the channel and release the connection.
   *
   * The drain waits up to `drainTimeoutMs` (default
   * {@link DEFAULT_DRAIN_TIMEOUT_MS}) for in-flight handlers. On timeout the
   * teardown proceeds anyway — the un-acked deliveries are redelivered by the
   * broker, preserving at-least-once semantics — and a warning is logged.
   * Pass `null` to wait indefinitely.
   *
   * @example
   * ```typescript
   * await worker.close().get();
   * ```
   */
  close(options?: { drainTimeoutMs?: number | null }): AsyncResult<void, never> {
    const cancellations = Array.from(this.consumerTags).map((consumerTag) =>
      // Swallow per-consumer cancel failures during close — they are best-effort
      // cleanup and we still want to release the underlying connection. A cancel
      // failure surfaces as a `Defect` now, so recover it back to `Ok`.
      this.amqpClient.cancel(consumerTag).recoverDefect((cause) => {
        this.logger?.warn("Failed to cancel consumer during close", { consumerTag, error: cause });
        return Ok(undefined);
      }),
    );

    return allAsync(cancellations)
      .tap(() => {
        this.consumerTags.clear();
      })
      .flatMap(() =>
        // Drain AFTER the cancels resolved: `basic.cancel-ok` guarantees no
        // further deliveries, so the snapshot cannot miss a late arrival.
        fromSafePromise(this.drainInFlight(options?.drainTimeoutMs)),
      )
      .flatMap(() => this.amqpClient.close())
      .map(() => undefined);
  }

  /**
   * Wait for in-flight handlers, bounded by the drain timeout. The tracked
   * promises never reject (the consume callback catches), but `allSettled`
   * keeps the drain immune to that assumption changing.
   */
  private async drainInFlight(drainTimeoutMs: number | null | undefined): Promise<void> {
    if (this.inFlight.size === 0) return;

    const drained = Promise.allSettled(this.inFlight).then(() => true as const);
    const timeoutMs = drainTimeoutMs === null ? null : (drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS);
    if (timeoutMs === null) {
      await drained;
      return;
    }

    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<false>((resolve) => {
      timer = setTimeout(() => resolve(false), timeoutMs);
    });
    const completed = await Promise.race([drained, timedOut]);
    clearTimeout(timer);
    if (!completed) {
      this.logger?.warn(
        "Drain timeout elapsed with handlers still in flight; closing anyway (broker will redeliver their messages)",
        { inFlight: this.inFlight.size, drainTimeoutMs: timeoutMs },
      );
    }
  }

  /**
   * Start consuming for every entry in `contract.consumers` and `contract.rpcs`.
   */
  private consumeAll(): AsyncResult<void, never> {
    const consumerNames = Object.keys(
      this.contract.consumers ?? {},
    ) as InferConsumerNames<TContract>[];
    const rpcNames = Object.keys(this.contract.rpcs ?? {}) as InferRpcNames<TContract>[];
    const allNames = [...consumerNames, ...rpcNames] as HandlerName<TContract>[];

    return allAsync(allNames.map((name) => this.consume(name))).map(() => undefined);
  }

  /**
   * Validate data against a Standard Schema. Schema issues are the modeled
   * `MessageValidationError` — the same error the client reports for an
   * invalid outgoing message; a validator that throws is a Defect.
   */
  private validateSchema(
    schema: StandardSchemaV1,
    data: unknown,
    consumerName: string,
  ): AsyncResult<unknown, MessageValidationError> {
    return fromSchemaAsync(schema)(data).mapErrCases((matcher) =>
      matcher.with(
        // oxlint-disable-next-line unthrown/no-catch-all-pattern -- SchemaIssues is a single non-union error type
        P._,
        (issues) => new MessageValidationError(consumerName, issues),
      ),
    );
  }

  /**
   * Decode and validate a message from AMQP: the payload (decompression, size
   * cap and JSON parse through the core codec, then its schema) and, when the
   * message declares them, the headers.
   */
  private parseAndValidateMessage(
    msg: ConsumeMessage,
    consumer: ConsumerDefinition,
    consumerName: string,
  ): AsyncResult<ValidatedMessage, MessageValidationError> {
    const parsePayload = decodeMessage(msg.content, msg.properties.contentEncoding, {
      maxBytes: this.maxDecompressedBytes,
    }).flatMap((parsed) =>
      this.validateSchema(consumer.message.payload as StandardSchemaV1, parsed, consumerName),
    );

    const parseHeaders: AsyncResult<unknown, MessageValidationError> = consumer.message.headers
      ? this.validateSchema(
          consumer.message.headers as StandardSchemaV1,
          msg.properties.headers ?? {},
          consumerName,
        )
      : OkAsync(undefined);

    return allAsync([parsePayload, parseHeaders]).map(([payload, headers]) => ({
      payload,
      headers,
    }));
  }

  /**
   * Parse and validate the message; a message that cannot be decoded or fails
   * its schema is poison and becomes a `dead-lettered` outcome at once. It
   * never enters the retry pipeline — a malformed payload is deterministic,
   * and retrying it would burn the queue's retry budget on a guaranteed
   * failure.
   *
   * Decode faults (unknown encoding, corrupt stream, over the size cap,
   * invalid JSON) and a throwing validator arrive as defects; at this
   * boundary every one of them means "these bytes cannot become a valid
   * message", so they are triaged into the same poison outcome.
   */
  private parseOrPoison(
    msg: ConsumeMessage,
    consumer: ConsumerDefinition,
    consumerName: string,
  ): AsyncResult<ValidatedMessage, DeadLettered> {
    const poison = (error: Error, reason: string): DeadLettered => {
      const fields = { consumerName, queueName: consumer.queue.name };
      this.logger?.error("Failed to parse/validate message; sending to DLQ", { ...fields, error });
      // A poison message on a queue with no DLX is discarded by the nack.
      // Mirrors the retry path's dead-letter logging: a declared drop is a
      // fact at `info`; a queue carrying neither a DLX nor the declaration is
      // only reachable via a hand-built ContractDefinition that bypassed
      // defineContract, and keeps the warning. "Is there a DLX?" is the
      // guard's own question, asked through the shared predicate so a queue
      // dead-lettering via the raw `arguments` passthrough stays silent here.
      if (!_internal_queueHasDeadLetterExchange(consumer.queue)) {
        if (consumer.queue.onPoison === "drop") {
          this.logger?.info(
            'Discarding poison message: queue is declared onPoison: "drop" and has no DLX',
            fields,
          );
        } else {
          this.logger?.warn(
            "Queue has no dead-letter exchange and no onPoison declaration - poison message will be lost on nack",
            fields,
          );
        }
      }
      return { kind: "dead-lettered", error, reason };
    };

    return this.parseAndValidateMessage(msg, consumer, consumerName)
      .mapErrCases((matcher) =>
        matcher.with(P.tag(MessageValidationError.tag), (error) =>
          poison(error, "invalid message"),
        ),
      )
      .recoverDefect((cause) => Err(poison(asError(cause), "undecodable message")));
  }

  /**
   * Invoke the handler — through the middleware chain when one is configured.
   * Returns the handler's response (RPC) or `undefined` (regular consumer).
   * Errors propagate as `HandlerError` for downstream RPC reply publishing or
   * routing via {@link handleError}.
   *
   * The middleware chain wraps only the handler, not validation or ack/nack:
   * a middleware that never calls `next` short-circuits the handler, and its
   * returned result flows through the exact same reply/retry/DLQ routing a
   * handler result would.
   */
  private runHandler(
    handler: StoredHandler,
    validatedMessage: { payload: unknown; headers: unknown },
    msg: ConsumeMessage,
    name: HandlerName<TContract>,
    view: ConsumerView,
  ): AsyncResult<unknown, HandlerError | RpcError> {
    const errors = view.errorConstructors ?? {};

    // Seed the context: createContext when configured, empty otherwise. A
    // rejection/throw in the factory is a permanent failure — retrying the
    // message cannot fix a broken dependency factory. The factory is invoked
    // *inside* the promise chain so a synchronous throw becomes a rejection
    // that `fromPromise` qualifies — invoking it eagerly as an argument would
    // let sync throws escape to the defect channel instead of the DLQ.
    const createContext = this.createContext;
    const seed: AsyncResult<Record<string, unknown>, HandlerError> = createContext
      ? fromPromise(
          Promise.resolve().then(() =>
            createContext({
              handlerName: String(name),
              isRpc: view.isRpc,
              message: validatedMessage,
              rawMessage: msg,
            }),
          ),
          (cause): HandlerError => new NonRetryableError("createContext failed", cause),
        )
      : OkAsync({});

    return seed.flatMap((seedContext) => {
      const terminal = (opts?: {
        context?: Record<string, unknown>;
        payload?: unknown;
      }): AsyncResult<unknown, HandlerError | RpcError> => {
        // Merge over the seed rather than replace it: `composeMiddleware`
        // already merges internally, so this keeps the bare `middleware: mw`
        // form and the array form observably identical (and is a no-op for
        // the composed chain, whose context already contains the seed).
        // The message is on the record as `input` AND in the second parameter,
        // oRPC's own shape — so the record is built per invocation rather than
        // once: a middleware that substituted the payload must not leave it
        // showing the value the handler did not receive.
        const ambient = {
          context: { ...seedContext, ...opts?.context },
          errors,
          raw: msg,
          retryable: retryableFactory,
          nonRetryable: nonRetryableFactory,
        };
        // Presence, not value: `next({ payload: undefined })` is a
        // substitution, and what happens to it is the payload schema's
        // decision; `next({})` is not one.
        if (!opts || !Object.hasOwn(opts, "payload")) {
          return handler({ ...ambient, input: validatedMessage }, validatedMessage);
        }
        // A middleware substituted the payload — re-validate against the
        // consumer's schema before the handler sees it, so middleware cannot
        // smuggle unvalidated data past the contract boundary. A validation
        // failure is a permanent, modeled NonRetryableError (routed to the DLQ),
        // not a defect: `validateReplyPayload` produces exactly that.
        return validateReplyPayload(
          view.consumer.message.payload as StandardSchemaV1,
          opts.payload,
          "Middleware-substituted payload",
          String(name),
        ).flatMap((validatedPayload) => {
          const substituted = { ...validatedMessage, payload: validatedPayload };
          return handler({ ...ambient, input: substituted }, substituted);
        });
      };

      if (!this.middleware) {
        return terminal();
      }
      return this.middleware(
        {
          message: validatedMessage,
          rawMessage: msg,
          handlerName: String(name),
          isRpc: view.isRpc,
          context: seedContext,
        },
        terminal,
      );
    });
  }

  /**
   * For RPC handlers, validate and publish the reply on the caller's
   * `replyTo` / `correlationId`. For non-RPC consumers, this is a no-op that
   * resolves to `OkAsync(undefined)`.
   */
  private publishReplyIfRpc(
    msg: ConsumeMessage,
    view: ConsumerView,
    name: HandlerName<TContract>,
    handlerResponse: unknown,
  ): AsyncResult<void, HandlerError> {
    if (!view.isRpc || !view.responseSchema) {
      return OkAsync(undefined);
    }
    return publishRpcResponse(
      this.replyContext,
      msg,
      view.consumer.queue.name,
      String(name),
      view.responseSchema,
      handlerResponse,
    );
  }

  /**
   * Process a single consumed message — validate, invoke the handler, publish
   * the RPC reply, route failures — down to the {@link Outcome} it must be
   * settled with. Nothing here acks or nacks: {@link dispatchMessage} settles
   * once, from the outcome.
   *
   * A Defect reaching the end (a handler or middleware that threw, a bug) is
   * dead-lettered rather than left un-acked — stuck until the channel closes,
   * then redelivered, which would re-run the failing code forever.
   */
  private processMessage(
    msg: ConsumeMessage,
    view: ConsumerView,
    name: HandlerName<TContract>,
    handler: StoredHandler,
  ): AsyncResult<Outcome, never> {
    const { consumer } = view;
    const fields = { consumerName: String(name), queueName: consumer.queue.name };

    return this.parseOrPoison(msg, consumer, String(name))
      .flatMap((validatedMessage): AsyncResult<Outcome, never> =>
        this.runHandler(handler, validatedMessage, msg, name, view)
          .flatMap((handlerResponse) =>
            this.publishReplyIfRpc(msg, view, name, handlerResponse).map((): Outcome => {
              this.logger?.info("Message consumed successfully", fields);
              return ACKED;
            }),
          )
          .flatMapErrCases((matcher) =>
            matcher.with(
              P.tag("@amqp-contract/RetryableError"),
              P.tag("@amqp-contract/NonRetryableError"),
              P.tag("@amqp-contract/RpcError"),
              (handlerError) => {
                // A contract-declared RpcError is the RPC's business-failure
                // channel, not a processing failure: publish it back to the
                // caller and ack the request. Only if the error reply itself
                // cannot be produced (undeclared code, schema mismatch, publish
                // failure) does the failure fall through to retry/DLQ routing.
                if (isRpcError(handlerError) && view.isRpc) {
                  return publishRpcErrorReply(
                    this.replyContext,
                    msg,
                    consumer.queue.name,
                    view.errorSchemas,
                    String(name),
                    handlerError,
                  )
                    .map((): Outcome => {
                      this.logger?.info("RPC handler replied with a typed error", {
                        ...fields,
                        errorCode: handlerError.code,
                      });
                      return ACKED;
                    })
                    .flatMapErrCases((replyMatcher) =>
                      replyMatcher.with(
                        P.tag("@amqp-contract/RetryableError"),
                        P.tag("@amqp-contract/NonRetryableError"),
                        (replyError: HandlerError) =>
                          this.routeHandlerError(replyError, msg, name, view),
                      ),
                    );
                }
                // An RpcError from a non-RPC consumer is type-impossible but
                // runtime-reachable through casts; treat it as a permanent
                // failure rather than crashing the dispatch loop.
                const routableError: HandlerError = isRpcError(handlerError)
                  ? new NonRetryableError(
                      `Consumer "${String(name)}" returned an RpcError but is not an RPC`,
                      handlerError,
                    )
                  : handlerError;
                return this.routeHandlerError(routableError, msg, name, view);
              },
            ),
          ),
      )
      .recoverErrCases((matcher) =>
        matcher.with({ kind: "dead-lettered" }, (poisoned): Outcome => poisoned),
      )
      .recoverDefect((cause) => {
        this.logger?.error("Message processing failed with a defect; nacking message", {
          ...fields,
          error: cause,
        });
        return Ok<Outcome>({ kind: "dead-lettered", error: asError(cause), reason: "defect" });
      });
  }

  /**
   * Route a handler failure to retry / DLQ via {@link handleError}, which
   * answers the outcome to settle with.
   */
  private routeHandlerError(
    handlerError: HandlerError,
    msg: ConsumeMessage,
    name: HandlerName<TContract>,
    view: ConsumerView,
  ): AsyncResult<Outcome, never> {
    const headers = msg.properties.headers;
    this.logger?.error("Error processing message", {
      consumerName: String(name),
      queueName: view.consumer.queue.name,
      errorType: handlerError.name,
      retryCount: readCount(headers, "x-delivery-count") || readCount(headers, "x-retry-count"),
      error: handlerError.message,
    });

    return handleError(
      { amqpClient: this.amqpClient, logger: this.logger },
      handlerError,
      msg,
      String(name),
      view.consumer,
      { isRpc: view.isRpc },
    );
  }

  /**
   * Process one delivery end to end, then settle it exactly once from its
   * {@link Outcome} and record telemetry from the same outcome. Never rejects.
   */
  private async dispatchMessage(
    msg: ConsumeMessage,
    view: ConsumerView,
    name: HandlerName<TContract>,
    handler: StoredHandler,
    queueName: string,
    deliveryEpoch: number,
  ): Promise<void> {
    const consumerName = String(name);
    const startTime = Date.now();
    const span = startConsumeSpan(this.telemetry, queueName, consumerName, {
      "messaging.rabbitmq.message.delivery_tag": msg.fields.deliveryTag,
    });

    let outcome: Outcome;
    try {
      outcome = await this.processMessage(msg, view, name, handler).get();
    } catch (error: unknown) {
      // Only reachable if the defect recovery itself threw (e.g. a throwing
      // logger) — still settle the delivery rather than leave it stuck.
      outcome = { kind: "dead-lettered", error: asError(error), reason: "defect" };
    }
    settle(this.amqpClient, msg, outcome, deliveryEpoch, this.logger);
    recordOutcome(this.telemetry, span, outcome, queueName, consumerName, startTime);
  }

  /**
   * Start consuming messages for a specific handler — either a `consumers`
   * entry (regular event/command consumer) or an `rpcs` entry (RPC server).
   */
  private consume(name: HandlerName<TContract>): AsyncResult<void, never> {
    const view = this.resolveConsumerView(name);
    // Non-null assertion safe: `WorkerInferHandlers<TContract>` requires every
    // consumers / rpcs key to have a handler, so by the time we reach this
    // dispatch path the entry exists in `actualHandlers`. Enforced by the type
    // system at the public API boundary, not by a runtime check.
    const handler = this.actualHandlers[name]!;
    const queueName = view.consumer.queue.name;

    return this.amqpClient
      .consume(
        queueName,
        (msg) => {
          if (msg === null) {
            this.logger?.warn("Consumer cancelled by server", {
              consumerName: String(name),
              queueName,
            });
            return;
          }
          // Track the processing promise so `close()` can drain in-flight
          // messages before tearing down the channel. The promise never
          // rejects — `dispatchMessage` catches everything — so the returned
          // callback promise cannot surface an unhandled rejection either.
          // Stamp the delivery with the current channel epoch so every settle
          // for this message can be refused if a reconnect happens first.
          const deliveryEpoch = this.amqpClient.currentChannelEpoch;
          const processing = this.dispatchMessage(
            msg,
            view,
            name,
            handler,
            queueName,
            deliveryEpoch,
          ).finally(() => {
            this.inFlight.delete(processing);
          });
          this.inFlight.add(processing);
          return processing;
        },
        this.consumerOptions[name],
      )
      .tap((consumerTag) => {
        this.consumerTags.add(consumerTag);
      })
      .map(() => undefined);
  }
}
