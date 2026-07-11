import {
  extractQueue,
  type CompressionAlgorithm,
  type ContractDefinition,
  type InferPublisherNames,
  type InferRpcNames,
  type RpcDefinition,
  type RpcErrorMap,
} from "@amqp-contract/contract";
import {
  AmqpClient,
  PublishOptions as AmqpClientPublishOptions,
  type Logger,
  MessagingSemanticConventions,
  RPC_ERROR_CODE_HEADER,
  RpcError,
  TechnicalError,
  type TelemetryProvider,
  defaultTelemetryProvider,
  endSpanError,
  endSpanSuccess,
  recordLateRpcReply,
  recordPublishMetric,
  safeJsonParse,
  startPublishSpan,
} from "@amqp-contract/core";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { AmqpConnectionManagerOptions, ConnectionUrl } from "amqp-connection-manager";
import { Err, fromPromise, fromSafePromise, Ok, type AsyncResult, type Result } from "unthrown";
import { randomUUID } from "node:crypto";
import { compressBuffer } from "./compression.js";
import { MessageValidationError, RpcCancelledError, RpcTimeoutError } from "./errors.js";
import { chainInterceptors } from "./interceptors.js";
import type {
  CallError as InterceptorCallError,
  CallInterceptor,
  CallInterceptorArgs,
  PublishError,
  PublishInterceptor,
  PublishInterceptorArgs,
} from "./interceptors.js";
import type {
  ClientInferPublisherInput,
  ClientInferRpcErrors,
  ClientInferRpcRequestInput,
  ClientInferRpcResponseOutput,
} from "./types.js";

/**
 * The RabbitMQ direct-reply-to pseudo-queue. Publishing with `replyTo` set to
 * this value tells the server to deliver the response back to the consumer
 * subscribed on this queue on the same channel — no real queue is created and
 * no setup is required beyond consuming from it once with `noAck: true`.
 *
 * @see https://www.rabbitmq.com/docs/direct-reply-to
 */
const DIRECT_REPLY_TO = "amq.rabbitmq.reply-to";

/**
 * In-flight RPC call tracked by `TypedAmqpClient`. The reply consumer
 * looks up entries by `correlationId` when responses arrive.
 */
type PendingCall = {
  rpcName: string;
  responseSchema: StandardSchemaV1;
  /**
   * The RPC's declared `errors` map (code → message definition), used to
   * validate the `data` of a typed error reply. Undefined when the RPC
   * declares no errors — any error reply then resolves to a TechnicalError.
   */
  rpcErrorSchemas?: RpcErrorMap | undefined;
  resolve: (
    result: Result<
      unknown,
      TechnicalError | MessageValidationError | RpcTimeoutError | RpcCancelledError | RpcError
    >,
  ) => void;
  timer: ReturnType<typeof setTimeout>;
};

/**
 * Publish options that extend amqp-client's PublishOptions with optional compression support.
 */
export type PublishOptions = AmqpClientPublishOptions & {
  /**
   * Optional compression algorithm to use for the message payload.
   * When specified, the message will be compressed using the chosen algorithm
   * and the contentEncoding header will be set automatically.
   */
  compression?: CompressionAlgorithm | undefined;
};

/**
 * Options for creating a client
 */
export type CreateClientOptions<TContract extends ContractDefinition> = {
  contract: TContract;
  urls: ConnectionUrl[];
  connectionOptions?: AmqpConnectionManagerOptions | undefined;
  logger?: Logger | undefined;
  /**
   * Optional telemetry provider for tracing and metrics.
   * If not provided, uses the default provider which attempts to load OpenTelemetry.
   * OpenTelemetry instrumentation is automatically enabled if @opentelemetry/api is installed.
   */
  telemetry?: TelemetryProvider | undefined;
  /**
   * Default publish options that will be applied to all publish operations.
   * These can be overridden by options passed to the publish method.
   * By default, persistent is set to true for message durability.
   */
  defaultPublishOptions?: PublishOptions | undefined;
  /**
   * Maximum time in ms to wait for the AMQP connection to become ready before
   * `create()` resolves to an `Err(TechnicalError)`. Defaults to 30s
   * (the {@link AmqpClient}'s `DEFAULT_CONNECT_TIMEOUT_MS`). Pass `null` to
   * disable the timeout and let amqp-connection-manager retry indefinitely.
   */
  connectTimeoutMs?: number | null | undefined;
  /**
   * Interceptors wrapping every `publish(...)`: the first entry is the
   * outermost. Each can patch the message/options, observe the outcome,
   * retry by calling `next` again, or short-circuit. A patched message is
   * validated exactly like the caller's original.
   */
  publishInterceptors?: readonly PublishInterceptor[] | undefined;
  /**
   * Interceptors wrapping every `call(...)` round trip (request validation,
   * publish, reply await): the first entry is the outermost.
   */
  callInterceptors?: readonly CallInterceptor[] | undefined;
};

/**
 * Per-call options for `client.call()`.
 */
export type CallOptions = {
  /**
   * Maximum time in ms to wait for an RPC reply. If exceeded, the call resolves
   * to `Err(RpcTimeoutError)` and the in-memory correlation entry is cleared.
   * A late reply arriving after the timeout is silently dropped.
   *
   * Required: RPC without a timeout is a footgun.
   */
  timeoutMs: number;

  /**
   * Optional AMQP message properties to merge into the request. `replyTo` and
   * `correlationId` are managed by the client and cannot be overridden.
   */
  publishOptions?: Omit<AmqpClientPublishOptions, "replyTo" | "correlationId">;
};

/**
 * Type-safe AMQP client for publishing messages
 */
export class TypedAmqpClient<TContract extends ContractDefinition> {
  /**
   * In-flight RPC calls keyed by `correlationId`. Cleared when a reply is
   * received, when the call times out, or when the client is closed.
   */
  private readonly pendingCalls = new Map<string, PendingCall>();

  /**
   * Consumer tag of the reply consumer subscribed on `amq.rabbitmq.reply-to`.
   * Set when the contract has at least one entry in `rpcs`; undefined otherwise.
   */
  private replyConsumerTag?: string;

  private constructor(
    private readonly contract: TContract,
    private readonly amqpClient: AmqpClient,
    private readonly defaultPublishOptions: PublishOptions,
    private readonly logger?: Logger,
    private readonly telemetry: TelemetryProvider = defaultTelemetryProvider,
    private readonly publishInterceptors: readonly PublishInterceptor[] = [],
    private readonly callInterceptors: readonly CallInterceptor[] = [],
  ) {}

  /**
   * Create a type-safe AMQP client from a contract.
   *
   * Connection management (including automatic reconnection) is handled internally
   * by amqp-connection-manager via the {@link AmqpClient}. The client establishes
   * infrastructure asynchronously in the background once the connection is ready.
   *
   * Connections are automatically shared across clients with the same URLs and
   * connection options, following RabbitMQ best practices.
   */
  static create<TContract extends ContractDefinition>({
    contract,
    urls,
    connectionOptions,
    defaultPublishOptions,
    logger,
    telemetry,
    connectTimeoutMs,
    publishInterceptors,
    callInterceptors,
  }: CreateClientOptions<TContract>): AsyncResult<TypedAmqpClient<TContract>, TechnicalError> {
    const client = new TypedAmqpClient(
      contract,
      new AmqpClient(contract, { urls, connectionOptions, connectTimeoutMs }),
      { persistent: true, ...defaultPublishOptions },
      logger,
      telemetry ?? defaultTelemetryProvider,
      publishInterceptors ?? [],
      callInterceptors ?? [],
    );

    const setup = client
      .waitForConnectionReady()
      .flatMap(() => client.setupReplyConsumerIfNeeded());

    const inner = (async (): Promise<Result<TypedAmqpClient<TContract>, TechnicalError>> => {
      const setupResult = await setup;
      if (!setupResult.isOk()) {
        const closeResult = await client.close();
        if (closeResult.isErr()) {
          logger?.warn("Failed to close client after connection failure", {
            error: closeResult.error,
          });
        }
      }
      // `map` runs only on Ok; an Err/Defect passes through with its value type
      // re-shaped to the client, so the failure surfaces unchanged.
      return setupResult.map(() => client);
    })();

    return fromSafePromise(inner).flatMap((result) => result);
  }

  /**
   * If the contract has any RPC entry, subscribe to `amq.rabbitmq.reply-to`
   * once. Replies for every in-flight call arrive on this single consumer and
   * are demultiplexed by `correlationId`.
   */
  private setupReplyConsumerIfNeeded(): AsyncResult<void, TechnicalError> {
    const rpcs = this.contract.rpcs ?? {};
    if (Object.keys(rpcs).length === 0) {
      return Ok(undefined).toAsync();
    }

    return this.amqpClient
      .consume(DIRECT_REPLY_TO, (msg) => this.handleRpcReply(msg), { noAck: true })
      .tap((tag) => {
        this.replyConsumerTag = tag;
      })
      .map(() => undefined);
  }

  /**
   * Demultiplex an RPC reply by `correlationId`, validate the body against the
   * call's response schema, and resolve the awaiting caller. Replies with no
   * matching pending call (the call already timed out, was cancelled, or the
   * correlationId is unknown) are logged at warn — a non-zero rate of these
   * usually indicates a tuning problem (handler latency exceeds caller
   * timeout). The `messaging.rpc.late_reply` counter lets dashboards alert on
   * sustained drift without parsing logs.
   */
  private handleRpcReply(msg: Parameters<Parameters<AmqpClient["consume"]>[1]>[0]): void {
    if (!msg) return;
    const correlationId = msg.properties.correlationId;
    if (typeof correlationId !== "string") {
      this.logger?.warn("Received RPC reply without correlationId; dropping", {
        deliveryTag: msg.fields.deliveryTag,
      });
      recordLateRpcReply(this.telemetry, "missing-correlation-id");
      return;
    }
    const pending = this.pendingCalls.get(correlationId);
    if (!pending) {
      this.logger?.warn(
        "Received RPC reply for unknown correlationId (caller already timed out or cancelled)",
        { correlationId },
      );
      recordLateRpcReply(this.telemetry, "unknown-correlation-id");
      return;
    }
    this.pendingCalls.delete(correlationId);
    clearTimeout(pending.timer);

    const parseResult = safeJsonParse(
      msg.content,
      (error) =>
        new TechnicalError(`Failed to parse RPC reply JSON for "${pending.rpcName}"`, error),
    );
    if (!parseResult.isOk()) {
      pending.resolve(
        Err(
          parseResult.isErr()
            ? parseResult.error
            : new TechnicalError(
                `Failed to parse RPC reply JSON for "${pending.rpcName}"`,
                parseResult.cause,
              ),
        ),
      );
      return;
    }
    const parsed = parseResult.value;

    // A reply carrying the error-code header is a typed error reply: its body
    // is `{ message, data }` rather than the response payload. Resolve it
    // through the RPC's declared error schemas instead of the response schema.
    const errorCode = msg.properties.headers?.[RPC_ERROR_CODE_HEADER];
    if (typeof errorCode === "string") {
      this.resolveRpcErrorReply(pending, errorCode, parsed);
      return;
    }

    // Wrap the validate call itself — a Standard Schema implementation may
    // throw synchronously, and the throw would otherwise escape the consume
    // callback and could crash the reply consumer.
    let rawValidation: ReturnType<StandardSchemaV1["~standard"]["validate"]>;
    try {
      rawValidation = pending.responseSchema["~standard"].validate(parsed);
    } catch (error: unknown) {
      pending.resolve(
        Err(new TechnicalError(`RPC reply validation threw for "${pending.rpcName}"`, error)),
      );
      return;
    }
    const validationPromise =
      rawValidation instanceof Promise ? rawValidation : Promise.resolve(rawValidation);

    validationPromise.then(
      (validation) => {
        if (validation.issues) {
          pending.resolve(Err(new MessageValidationError(pending.rpcName, validation.issues)));
          return;
        }
        pending.resolve(Ok(validation.value));
      },
      (error: unknown) => {
        pending.resolve(
          Err(new TechnicalError(`RPC reply validation threw for "${pending.rpcName}"`, error)),
        );
      },
    );
  }

  /**
   * Resolve a pending call with a typed error reply: look up the error's
   * schema in the RPC's declared `errors` map, validate the reply's `data`
   * against it, and resolve the caller with `Err(RpcError<code, data>)`.
   *
   * An error code the contract does not declare resolves to a TechnicalError —
   * it means the two sides run different contract versions (or the worker
   * bypassed the type system), and the client has no schema to give the data
   * a type under.
   */
  private resolveRpcErrorReply(pending: PendingCall, errorCode: string, parsed: unknown): void {
    // `Object.hasOwn` rather than plain indexing so prototype properties
    // (e.g. "toString") are not misclassified as declared error codes.
    const errorSchema =
      pending.rpcErrorSchemas && Object.hasOwn(pending.rpcErrorSchemas, errorCode)
        ? pending.rpcErrorSchemas[errorCode]
        : undefined;
    if (!errorSchema) {
      pending.resolve(
        Err(
          new TechnicalError(
            `RPC "${pending.rpcName}" replied with undeclared error code "${errorCode}"`,
          ),
        ),
      );
      return;
    }

    const body =
      typeof parsed === "object" && parsed !== null
        ? (parsed as { message?: unknown; data?: unknown })
        : {};
    const message = typeof body.message === "string" ? body.message : undefined;

    // Wrap the validate call itself — a Standard Schema implementation may
    // throw synchronously, and the throw would otherwise escape the consume
    // callback and could crash the reply consumer.
    let rawValidation: ReturnType<StandardSchemaV1["~standard"]["validate"]>;
    try {
      rawValidation = errorSchema.payload["~standard"].validate(body.data);
    } catch (error: unknown) {
      pending.resolve(
        Err(new TechnicalError(`RPC error data validation threw for "${pending.rpcName}"`, error)),
      );
      return;
    }
    const validationPromise =
      rawValidation instanceof Promise ? rawValidation : Promise.resolve(rawValidation);

    validationPromise.then(
      (validation) => {
        if (validation.issues) {
          pending.resolve(Err(new MessageValidationError(pending.rpcName, validation.issues)));
          return;
        }
        pending.resolve(Err(new RpcError(errorCode, validation.value, message)));
      },
      (error: unknown) => {
        pending.resolve(
          Err(
            new TechnicalError(`RPC error data validation threw for "${pending.rpcName}"`, error),
          ),
        );
      },
    );
  }

  /**
   * Publish a message using a defined publisher.
   *
   * @param publisherName - The name of the publisher to use
   * @param message - The message to publish
   * @param options - Optional publish options including compression, headers, priority, etc.
   *
   * @remarks
   * If `options.compression` is specified, the message will be compressed before publishing
   * and the `contentEncoding` property will be set automatically. Any `contentEncoding`
   * value already in options will be overwritten by the compression algorithm.
   */
  publish<TName extends InferPublisherNames<TContract>>(
    publisherName: TName,
    message: ClientInferPublisherInput<TContract, TName>,
    options?: PublishOptions,
  ): AsyncResult<void, TechnicalError | MessageValidationError> {
    const startTime = Date.now();
    // Non-null assertions safe: TypeScript guarantees these exist for valid TName
    const publisher = this.contract.publishers![publisherName as string]!;
    const { exchange, routingKey } = publisher;

    // Start telemetry span
    const span = startPublishSpan(this.telemetry, exchange.name, routingKey, {
      [MessagingSemanticConventions.AMQP_PUBLISHER_NAME]: String(publisherName),
    });

    const validateMessage = (
      rawMessage: unknown,
    ): AsyncResult<unknown, TechnicalError | MessageValidationError> => {
      const validationResult = publisher.message.payload["~standard"].validate(rawMessage);
      const promise =
        validationResult instanceof Promise ? validationResult : Promise.resolve(validationResult);
      return fromPromise(
        promise,
        (error): TechnicalError | MessageValidationError =>
          new TechnicalError("Validation failed", error),
      ).flatMap((validation) => {
        if (validation.issues) {
          return Err<TechnicalError | MessageValidationError>(
            new MessageValidationError(String(publisherName), validation.issues),
          );
        }
        return Ok(validation.value);
      });
    };

    const publishMessage = (
      validatedMessage: unknown,
      callOptions: PublishOptions,
    ): AsyncResult<void, TechnicalError> => {
      // Merge default options with provided options
      const mergedOptions = { ...this.defaultPublishOptions, ...callOptions };

      // Extract compression from merged options and create publish options without it
      const { compression, ...restOptions } = mergedOptions;
      const publishOptions: AmqpClientPublishOptions = { ...restOptions };

      // Prepare payload and options based on compression configuration
      const preparePayload = (): AsyncResult<Buffer | unknown, TechnicalError> => {
        if (compression) {
          // Compress the message payload
          const messageBuffer = Buffer.from(JSON.stringify(validatedMessage));
          publishOptions.contentEncoding = compression;
          return compressBuffer(messageBuffer, compression);
        }

        // No compression: use the channel's built-in JSON serialization
        return Ok(validatedMessage).toAsync();
      };

      return preparePayload().flatMap((payload) =>
        this.amqpClient
          .publish(publisher.exchange.name, publisher.routingKey ?? "", payload, publishOptions)
          .flatMap((published) => {
            if (!published) {
              return Err<TechnicalError>(
                new TechnicalError(
                  `Failed to publish message for publisher "${String(publisherName)}": Channel rejected the message (buffer full or other channel issue)`,
                ),
              );
            }

            this.logger?.info("Message published successfully", {
              publisherName: String(publisherName),
              exchange: publisher.exchange.name,
              routingKey: publisher.routingKey,
              compressed: !!compression,
            });

            return Ok(undefined);
          }),
      );
    };

    // Interceptors wrap validation + publish; telemetry stays outermost so
    // the span covers interceptor work too.
    const terminal = (args: PublishInterceptorArgs): AsyncResult<void, PublishError> =>
      validateMessage(args.message).flatMap((validatedMessage) =>
        publishMessage(validatedMessage, args.options),
      );

    // Explicit type arguments: TArgs must be the wire-level interceptor shape
    // (message: unknown), not the narrower type inferred from this literal.
    return chainInterceptors<
      PublishInterceptorArgs,
      { message?: unknown; options?: PublishOptions },
      void,
      PublishError
    >(
      this.publishInterceptors,
      { publisherName: String(publisherName), message, options: options ?? {} },
      terminal,
    )
      .tap(() => {
        const durationMs = Date.now() - startTime;
        endSpanSuccess(span);
        recordPublishMetric(this.telemetry, exchange.name, routingKey, true, durationMs);
      })
      .tapErr((error) => {
        const durationMs = Date.now() - startTime;
        endSpanError(span, error);
        recordPublishMetric(this.telemetry, exchange.name, routingKey, false, durationMs);
      });
  }

  /**
   * Invoke an RPC defined via `defineRpc` and await the typed response.
   *
   * The request payload is validated against the RPC's request schema, then
   * published to the AMQP default exchange with the server's queue name as
   * routing key, `replyTo` set to `amq.rabbitmq.reply-to`, and a fresh UUID
   * `correlationId`. The returned AsyncResult resolves once a matching reply
   * arrives and validates against the response schema, or once `timeoutMs`
   * elapses (whichever comes first).
   *
   * @example
   * ```typescript
   * const result = await client.call('calculate', { a: 1, b: 2 }, { timeoutMs: 5_000 });
   * result.match({
   *   ok: (value) => console.log(value.sum), // 3
   *   err: (error) => console.error(error),
   *   defect: (cause) => console.error(cause),
   * });
   * ```
   */
  call<TName extends InferRpcNames<TContract>>(
    rpcName: TName,
    request: ClientInferRpcRequestInput<TContract, TName>,
    options: CallOptions,
  ): AsyncResult<
    ClientInferRpcResponseOutput<TContract, TName>,
    | TechnicalError
    | MessageValidationError
    | RpcTimeoutError
    | RpcCancelledError
    | ClientInferRpcErrors<TContract, TName>
  > {
    type ResponseType = ClientInferRpcResponseOutput<TContract, TName>;
    type CallError =
      | TechnicalError
      | MessageValidationError
      | RpcTimeoutError
      | RpcCancelledError
      | ClientInferRpcErrors<TContract, TName>;

    const startTime = Date.now();
    // Non-null assertion safe: TName is constrained to RPC names in the contract.
    const rpc = this.contract.rpcs![rpcName as string]!;
    const queueName = extractQueue(rpc.queue).name;

    // RPC publishes to the default exchange with the queue name as routing key.
    const span = startPublishSpan(this.telemetry, "", queueName, {
      [MessagingSemanticConventions.AMQP_PUBLISHER_NAME]: String(rpcName),
    });

    // Interceptors wrap the full round trip (request validation, publish,
    // reply await) so they can adjust timeouts, stamp headers, or retry by
    // calling `next` again; telemetry stays outermost. Error/value types are
    // erased through the chain and restored at this public boundary; TArgs is
    // pinned to the wire-level interceptor shape (request: unknown).
    const chained = chainInterceptors<
      CallInterceptorArgs,
      { request?: unknown; options?: CallOptions },
      unknown,
      InterceptorCallError
    >(this.callInterceptors, { rpcName: String(rpcName), request, options }, (args) =>
      this.executeCall(String(rpcName), rpc, args.request, args.options),
    );

    const instrumented = chained
      .tap(() => {
        const durationMs = Date.now() - startTime;
        endSpanSuccess(span);
        recordPublishMetric(this.telemetry, "", queueName, true, durationMs);
      })
      .tapErr((error) => {
        const durationMs = Date.now() - startTime;
        endSpanError(span, error);
        recordPublishMetric(this.telemetry, "", queueName, false, durationMs);
      });

    // Safe: executeCall resolves with the schema-validated response, and its
    // wire-level error union is the widened form of CallError.
    return instrumented as AsyncResult<ResponseType, CallError>;
  }

  /**
   * The uninstrumented RPC round trip: timeout validation, correlation
   * bookkeeping, request validation, publish, and reply await. Runs as the
   * terminal of the call-interceptor chain — `call()` owns telemetry and the
   * typed public signature.
   */
  private executeCall(
    rpcName: string,
    rpc: RpcDefinition,
    request: unknown,
    options: CallOptions,
  ): AsyncResult<unknown, InterceptorCallError> {
    type CallResult = Result<unknown, InterceptorCallError>;

    // setTimeout truncates fractional ms and clamps anything outside the
    // 32-bit signed integer range (~24.8 days) to 1ms, so reject those up
    // front as user errors rather than producing surprising behavior.
    const TIMEOUT_MAX_MS = 2_147_483_647;
    if (
      typeof options.timeoutMs !== "number" ||
      !Number.isFinite(options.timeoutMs) ||
      options.timeoutMs <= 0 ||
      options.timeoutMs > TIMEOUT_MAX_MS
    ) {
      return Err<InterceptorCallError>(
        new TechnicalError(
          `Invalid timeoutMs for RPC call to "${rpcName}": expected a finite positive number ≤ ${TIMEOUT_MAX_MS}, got ${String(options.timeoutMs)}`,
        ),
      ).toAsync();
    }

    const requestSchema = rpc.request.payload;
    const responseSchema = rpc.response.payload;
    const queueName = extractQueue(rpc.queue).name;

    const correlationId = randomUUID();

    // Set up the reply future + pending entry up front so a reply that arrives
    // racing the publish round-trip can find a slot. Cleanup on preflight
    // failure happens in the `.flatMapErr` below.
    let resolveCall!: (result: CallResult) => void;
    const callPromise = new Promise<CallResult>((res) => {
      resolveCall = res;
    });
    // `callPromise` resolves to a `Result` (never rejects), so lift it with
    // `fromSafePromise` and collapse the nested `Result` back into the channel.
    const callResultAsync: AsyncResult<unknown, InterceptorCallError> = fromSafePromise(
      callPromise,
    ).flatMap((result) => result);

    const timer = setTimeout(() => {
      if (!this.pendingCalls.has(correlationId)) return;
      this.pendingCalls.delete(correlationId);
      resolveCall(Err(new RpcTimeoutError(rpcName, options.timeoutMs)));
    }, options.timeoutMs);

    this.pendingCalls.set(correlationId, {
      rpcName,
      responseSchema,
      rpcErrorSchemas: rpc.errors,
      resolve: resolveCall as PendingCall["resolve"],
      timer,
    });

    const validateRequest = (): AsyncResult<unknown, TechnicalError | MessageValidationError> => {
      // Wrap the validate call — a Standard Schema implementation may throw
      // synchronously, and that throw would otherwise escape the chain and
      // leave the pending-call entry/timer dangling until timeout.
      let rawValidation: ReturnType<StandardSchemaV1["~standard"]["validate"]>;
      try {
        rawValidation = requestSchema["~standard"].validate(request);
      } catch (error: unknown) {
        return Err<TechnicalError | MessageValidationError>(
          new TechnicalError("RPC request validation threw", error),
        ).toAsync();
      }
      const validationPromise =
        rawValidation instanceof Promise ? rawValidation : Promise.resolve(rawValidation);
      return fromPromise(
        validationPromise,
        (error): TechnicalError | MessageValidationError =>
          new TechnicalError("RPC request validation threw", error),
      ).flatMap((validation) =>
        validation.issues
          ? Err<TechnicalError | MessageValidationError>(
              new MessageValidationError(rpcName, validation.issues),
            )
          : Ok(validation.value),
      );
    };

    const publishRequest = (validatedRequest: unknown): AsyncResult<void, TechnicalError> => {
      // Merge `defaultPublishOptions` (persistent, priority, headers, …) with
      // the per-call options, then layer the RPC-managed fields on top so they
      // cannot be overridden. `compression` is intentionally dropped: RPC v1
      // does not implement reply-side decompression, so request-side
      // compression would break the round-trip.
      const { compression: _ignoredCompression, ...defaultsWithoutCompression } =
        this.defaultPublishOptions;
      const publishOptions: AmqpClientPublishOptions = {
        ...defaultsWithoutCompression,
        ...options.publishOptions,
        replyTo: DIRECT_REPLY_TO,
        correlationId,
        contentType: "application/json",
      };
      return this.amqpClient
        .publish("", queueName, validatedRequest, publishOptions)
        .flatMap((published) =>
          published
            ? Ok(undefined)
            : Err<TechnicalError>(
                new TechnicalError(
                  `Failed to publish RPC request for "${rpcName}": channel buffer full`,
                ),
              ),
        );
    };

    return validateRequest()
      .flatMap((validated) => publishRequest(validated))
      .flatMap(() => callResultAsync)
      .flatMapErr((error: InterceptorCallError) => {
        // If preflight failed (validate or publish), the pending entry still
        // exists and the timer is alive. Clean both up so the call doesn't
        // leak. Timer-fired errors and reply-resolved errors have already
        // cleaned the entry, so the .has() check guards against double cleanup.
        if (this.pendingCalls.has(correlationId)) {
          clearTimeout(timer);
          this.pendingCalls.delete(correlationId);
        }
        return Err(error).toAsync();
      });
  }

  /**
   * Close the channel and connection. Cancels the reply consumer (if any) and
   * rejects every in-flight RPC call with `RpcCancelledError`.
   */
  close(): AsyncResult<void, TechnicalError> {
    // Reject pending calls first — once close() runs, no reply will arrive.
    for (const [, pending] of this.pendingCalls) {
      clearTimeout(pending.timer);
      pending.resolve(Err(new RpcCancelledError(pending.rpcName)));
    }
    this.pendingCalls.clear();

    const cancelReply: AsyncResult<void, TechnicalError> = this.replyConsumerTag
      ? this.amqpClient.cancel(this.replyConsumerTag).flatMapErr((error) => {
          this.logger?.warn("Failed to cancel RPC reply consumer during close", { error });
          return Ok(undefined);
        })
      : Ok(undefined).toAsync();

    return cancelReply.flatMap(() => this.amqpClient.close());
  }

  private waitForConnectionReady(): AsyncResult<void, TechnicalError> {
    return this.amqpClient.waitForConnect();
  }
}
