import {
  type ConsumerDefinition,
  type ContractDefinition,
  type InferConsumerNames,
  type InferRpcNames,
  extractConsumer,
  extractQueue,
} from "@amqp-contract/contract";
import {
  AmqpClient,
  ConsumerOptions as AmqpClientConsumerOptions,
  type Logger,
  TechnicalError,
  type TelemetryProvider,
  defaultTelemetryProvider,
  endSpanError,
  endSpanSuccess,
  recordConsumeMetric,
  safeJsonParse,
  startConsumeSpan,
} from "@amqp-contract/core";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { AmqpConnectionManagerOptions, ConnectionUrl } from "amqp-connection-manager";
import type { ConsumeMessage } from "amqplib";
import {
  allAsync,
  Err,
  fromPromise,
  fromSafePromise,
  Ok,
  type AsyncResult,
  type Result,
} from "unthrown";
import { decompressBuffer } from "./decompression.js";
import type { HandlerError } from "./errors.js";
import { MessageValidationError, NonRetryableError } from "./errors.js";
import { handleError } from "./retry.js";
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
 * cast back to the correct typed handler.
 */
type StoredHandler = (
  message: { payload: unknown; headers: unknown },
  rawMessage: ConsumeMessage,
) => AsyncResult<unknown, HandlerError>;

export type ConsumerOptions = AmqpClientConsumerOptions;

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
 *     processOrder: ({ payload }) => {
 *       console.log('Processing order:', payload.orderId);
 *       return Ok(undefined).toAsync();
 *     },
 *     // Handler with prefetch configuration
 *     processPayment: [
 *       ({ payload }) => {
 *         console.log('Processing payment:', payload.paymentId);
 *         return Ok(undefined).toAsync();
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
export type CreateWorkerOptions<TContract extends ContractDefinition> = {
  /** The AMQP contract definition specifying consumers and their message schemas */
  contract: TContract;
  /**
   * Handlers for each `consumers` and `rpcs` entry in the contract.
   *
   * - Regular consumers return `AsyncResult<void, HandlerError>`.
   * - RPC handlers return `AsyncResult<TResponse, HandlerError>` where
   *   `TResponse` is inferred from the RPC's response message schema.
   *
   * Use `defineHandler` / `defineHandlers` to create handlers with full type
   * inference.
   */
  handlers: WorkerInferHandlers<TContract>;
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
   * `create()` resolves to an `Err(TechnicalError)`. Defaults to 30s
   * (the {@link AmqpClient}'s `DEFAULT_CONNECT_TIMEOUT_MS`). Pass `null` to
   * disable the timeout and let amqp-connection-manager retry indefinitely.
   */
  connectTimeoutMs?: number | null | undefined;
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
 * import { Ok } from 'unthrown';
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
 * const result = await TypedAmqpWorker.create({
 *   contract,
 *   handlers: {
 *     processOrder: ({ payload }) => {
 *       console.log('Processing order', payload.orderId);
 *       return Ok(undefined).toAsync();
 *     },
 *   },
 *   urls: ['amqp://localhost'],
 * });
 *
 * const worker = result.unwrap();
 *
 * // Close when done
 * await worker.close();
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
  private readonly telemetry: TelemetryProvider;

  private constructor(
    private readonly contract: TContract,
    private readonly amqpClient: AmqpClient,
    handlers: WorkerInferHandlers<TContract>,
    private readonly defaultConsumerOptions: ConsumerOptions,
    private readonly logger?: Logger,
    telemetry?: TelemetryProvider,
  ) {
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
  private resolveConsumerView(name: HandlerName<TContract>): {
    consumer: ConsumerDefinition;
    isRpc: boolean;
    responseSchema?: StandardSchemaV1;
  } {
    // Use `Object.hasOwn` rather than `key in rpcs` so prototype properties
    // (e.g. "toString") on a plain object are not misclassified as RPC names.
    const rpcs = this.contract.rpcs;
    if (rpcs && Object.hasOwn(rpcs, name as string)) {
      const rpc = rpcs[name as string]!;
      return {
        consumer: { queue: rpc.queue, message: rpc.request },
        isRpc: true,
        responseSchema: rpc.response.payload,
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
   * @returns A AsyncResult that resolves to the worker or a TechnicalError.
   *
   * @example
   * ```typescript
   * const result = await TypedAmqpWorker.create({
   *   contract: myContract,
   *   handlers: {
   *     processOrder: ({ payload }) => Ok(undefined).toAsync(),
   *   },
   *   urls: ['amqp://localhost'],
   * });
   * ```
   */
  static create<TContract extends ContractDefinition>({
    contract,
    handlers,
    urls,
    connectionOptions,
    defaultConsumerOptions,
    logger,
    telemetry,
    connectTimeoutMs,
  }: CreateWorkerOptions<TContract>): AsyncResult<TypedAmqpWorker<TContract>, TechnicalError> {
    const worker = new TypedAmqpWorker(
      contract,
      new AmqpClient(contract, {
        urls,
        connectionOptions,
        connectTimeoutMs,
      }),
      handlers,
      defaultConsumerOptions ?? {},
      logger,
      telemetry,
    );

    // Note: Wait queues are now created by the core package in setupAmqpTopology
    // when the queue's retry mode is "ttl-backoff"
    const setup = worker.waitForConnectionReady().flatMap(() => worker.consumeAll());

    // If setup fails, release the AmqpClient's connection ref-count and cancel
    // any consumers that registered before the failure, so a failed create()
    // does not leak.
    const inner = (async (): Promise<Result<TypedAmqpWorker<TContract>, TechnicalError>> => {
      const setupResult = await setup;
      if (!setupResult.isOk()) {
        const closeResult = await worker.close();
        if (closeResult.isErr()) {
          logger?.warn("Failed to close worker after setup failure", {
            error: closeResult.error,
          });
        }
      }
      // `map` runs only on Ok; an Err/Defect passes through with its value type
      // re-shaped to the worker, so the failure surfaces unchanged.
      return setupResult.map(() => worker);
    })();

    return fromSafePromise(inner).flatMap((result) => result);
  }

  /**
   * Close the AMQP channel and connection.
   *
   * This gracefully closes the connection to the AMQP broker,
   * stopping all message consumption and cleaning up resources.
   *
   * @example
   * ```typescript
   * const closeResult = await worker.close();
   * if (closeResult.isOk()) {
   *   console.log('Worker closed successfully');
   * }
   * ```
   */
  close(): AsyncResult<void, TechnicalError> {
    const cancellations = Array.from(this.consumerTags).map((consumerTag) =>
      // Swallow per-consumer cancel errors during close — they are best-effort
      // cleanup and we still want to release the underlying connection.
      this.amqpClient.cancel(consumerTag).orElse((error) => {
        this.logger?.warn("Failed to cancel consumer during close", { consumerTag, error });
        return Ok(undefined);
      }),
    );

    return allAsync(cancellations)
      .tap(() => {
        this.consumerTags.clear();
      })
      .flatMap(() => this.amqpClient.close())
      .map(() => undefined);
  }

  /**
   * Start consuming for every entry in `contract.consumers` and `contract.rpcs`.
   */
  private consumeAll(): AsyncResult<void, TechnicalError> {
    const consumerNames = Object.keys(
      this.contract.consumers ?? {},
    ) as InferConsumerNames<TContract>[];
    const rpcNames = Object.keys(this.contract.rpcs ?? {}) as InferRpcNames<TContract>[];
    const allNames = [...consumerNames, ...rpcNames] as HandlerName<TContract>[];

    return allAsync(allNames.map((name) => this.consume(name))).map(() => undefined);
  }

  private waitForConnectionReady(): AsyncResult<void, TechnicalError> {
    return this.amqpClient.waitForConnect();
  }

  /**
   * Start consuming messages for a specific handler — either a `consumers`
   * entry (regular event/command consumer) or an `rpcs` entry (RPC server).
   */
  private consume(name: HandlerName<TContract>): AsyncResult<void, TechnicalError> {
    const view = this.resolveConsumerView(name);
    // Non-null assertion safe: `WorkerInferHandlers<TContract>` requires every
    // consumers / rpcs key to have a handler, so by the time we reach this
    // dispatch path the entry exists in `actualHandlers`. Enforced by the type
    // system at the public API boundary, not by a runtime check.
    const handler = this.actualHandlers[name]!;

    return this.consumeSingle(name, view, handler);
  }

  /**
   * Validate data against a Standard Schema. No side effects; the caller is
   * responsible for ack/nack based on the Result.
   */
  private validateSchema(
    schema: StandardSchemaV1,
    data: unknown,
    context: { consumerName: string; field: string },
  ): AsyncResult<unknown, TechnicalError> {
    const rawValidation = schema["~standard"].validate(data);
    const validationPromise =
      rawValidation instanceof Promise ? rawValidation : Promise.resolve(rawValidation);

    return fromPromise(
      validationPromise,
      (error) => new TechnicalError(`Error validating ${context.field}`, error),
    ).flatMap((result) => {
      if (result.issues) {
        return Err(
          new TechnicalError(
            `${context.field} validation failed`,
            new MessageValidationError(context.consumerName, result.issues),
          ),
        );
      }
      return Ok(result.value);
    });
  }

  /**
   * Parse and validate a message from AMQP. Pure: returns the validated payload
   * and headers, or an error. The dispatch path in {@link processMessage} routes
   * validation/parse errors directly to the DLQ (single nack) — they never enter
   * the retry pipeline because retrying an unparseable or schema-violating
   * payload cannot succeed.
   */
  private parseAndValidateMessage(
    msg: ConsumeMessage,
    consumer: ConsumerDefinition,
    consumerName: HandlerName<TContract>,
  ): AsyncResult<{ payload: unknown; headers: unknown }, TechnicalError> {
    const context = { consumerName: String(consumerName) };

    const parsePayload = decompressBuffer(msg.content, msg.properties.contentEncoding)
      .flatMap((buffer) =>
        safeJsonParse(buffer, (error) => new TechnicalError("Failed to parse JSON", error)),
      )
      .flatMap((parsed) =>
        this.validateSchema(consumer.message.payload as StandardSchemaV1, parsed, {
          ...context,
          field: "payload",
        }),
      );

    const parseHeaders: AsyncResult<unknown, TechnicalError> = consumer.message.headers
      ? this.validateSchema(
          consumer.message.headers as StandardSchemaV1,
          msg.properties.headers ?? {},
          {
            ...context,
            field: "headers",
          },
        )
      : Ok(undefined).toAsync();

    return allAsync([parsePayload, parseHeaders]).map(([payload, headers]) => ({
      payload,
      headers,
    }));
  }

  /**
   * Validate an RPC handler's response and publish it back to the caller's reply
   * queue with the same `correlationId`. Published via the AMQP default exchange
   * with `routingKey = msg.properties.replyTo`, which works for both
   * `amq.rabbitmq.reply-to` and any anonymous queue declared by the caller.
   *
   * Failure semantics:
   * - **Missing replyTo / correlationId**: NonRetryableError. The caller is
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
  private publishRpcResponse(
    msg: ConsumeMessage,
    queueName: string,
    rpcName: HandlerName<TContract>,
    responseSchema: StandardSchemaV1,
    response: unknown,
  ): AsyncResult<void, HandlerError> {
    const replyTo = msg.properties.replyTo;
    const correlationId = msg.properties.correlationId;
    if (typeof replyTo !== "string" || replyTo.length === 0) {
      this.logger?.error(
        "RPC handler returned a response but the incoming message has no replyTo",
        { rpcName: String(rpcName), queueName },
      );
      return Err(
        new NonRetryableError(
          `RPC "${String(rpcName)}" received a message without replyTo; cannot deliver response`,
        ),
      ).toAsync();
    }
    if (typeof correlationId !== "string" || correlationId.length === 0) {
      // Without a correlationId the client cannot match the reply to its
      // pending call — publishing anyway would guarantee a client-side timeout.
      this.logger?.error(
        "RPC handler returned a response but the incoming message has no correlationId",
        { rpcName: String(rpcName), queueName, replyTo },
      );
      return Err(
        new NonRetryableError(
          `RPC "${String(rpcName)}" received a message without correlationId; cannot deliver response`,
        ),
      ).toAsync();
    }

    // Wrap the call to `validate` itself in try/catch — a Standard Schema
    // implementation may throw synchronously (not via a rejected Promise), and
    // we don't want that to crash the consume callback.
    let rawValidation: ReturnType<StandardSchemaV1["~standard"]["validate"]>;
    try {
      rawValidation = responseSchema["~standard"].validate(response);
    } catch (error: unknown) {
      return Err(new NonRetryableError("RPC response schema validation threw", error)).toAsync();
    }
    const validationPromise =
      rawValidation instanceof Promise ? rawValidation : Promise.resolve(rawValidation);

    return fromPromise(
      validationPromise,
      (error: unknown) =>
        new NonRetryableError("RPC response schema validation threw", error) as HandlerError,
    )
      .flatMap((validation) => {
        if (validation.issues) {
          return Err<HandlerError>(
            new NonRetryableError(
              `RPC response for "${String(rpcName)}" failed schema validation`,
              new MessageValidationError(String(rpcName), validation.issues),
            ),
          );
        }
        return Ok(validation.value);
      })
      .flatMap((validatedResponse) =>
        this.amqpClient
          .publish("", replyTo, validatedResponse, {
            correlationId,
            contentType: "application/json",
          })
          // Reply-side failures are not retryable from the inbox: by the time
          // the broker can't deliver the reply, the caller's RPC future has
          // already (or will soon) time out. Retrying the original message
          // re-runs the handler against a stale caller. Send to DLQ instead so
          // the failure is visible without churning the queue.
          .mapErr(
            (error: TechnicalError): HandlerError =>
              new NonRetryableError("Failed to publish RPC response", error),
          )
          .flatMap((published) =>
            published
              ? Ok(undefined)
              : Err<HandlerError>(
                  new NonRetryableError("Failed to publish RPC response: channel buffer full"),
                ),
          ),
      );
  }

  /**
   * Parse and validate the message; on failure, nack(requeue=false) so the
   * queue's DLX (if configured) receives the poison message and bypass the
   * retry pipeline — a malformed payload is deterministic and retrying it
   * would burn the queue's retry budget on a guaranteed failure.
   */
  private parseAndValidateOrNack(
    msg: ConsumeMessage,
    consumer: ConsumerDefinition,
    name: HandlerName<TContract>,
  ): AsyncResult<{ payload: unknown; headers: unknown }, TechnicalError> {
    return this.parseAndValidateMessage(msg, consumer, name).orElse((parseError) => {
      this.amqpClient.nack(msg, false, false);
      return Err(parseError).toAsync();
    });
  }

  /**
   * Invoke the handler and ack the message on success. Returns the handler's
   * response (RPC) or `undefined` (regular consumer). Errors propagate as
   * `HandlerError` for downstream RPC reply publishing or routing via
   * {@link handleError}.
   */
  private runHandler(
    handler: StoredHandler,
    validatedMessage: { payload: unknown; headers: unknown },
    msg: ConsumeMessage,
  ): AsyncResult<unknown, HandlerError> {
    return handler(validatedMessage, msg);
  }

  /**
   * For RPC handlers, validate and publish the reply on the caller's
   * `replyTo` / `correlationId`. For non-RPC consumers, this is a no-op that
   * resolves to `Ok(undefined).toAsync()`.
   */
  private publishReplyIfRpc(
    msg: ConsumeMessage,
    view: { consumer: ConsumerDefinition; isRpc: boolean; responseSchema?: StandardSchemaV1 },
    name: HandlerName<TContract>,
    handlerResponse: unknown,
  ): AsyncResult<void, HandlerError> {
    if (!view.isRpc || !view.responseSchema) {
      return Ok(undefined).toAsync();
    }
    const queueName = extractQueue(view.consumer.queue).name;
    return this.publishRpcResponse(msg, queueName, name, view.responseSchema, handlerResponse);
  }

  /**
   * Process a single consumed message: validate, invoke handler, optionally
   * publish the RPC response, record telemetry, and route errors.
   *
   * The caller-supplied `state` is mutated as the message is ack'd/nack'd so
   * the consume callback's catch-all guard can tell whether a defensive nack
   * is still needed (see {@link consumeSingle}).
   *
   * Success-vs-failure telemetry is data-driven: the chain resolves to
   * `Ok(undefined)` only on handler success (and reply-publish success for
   * RPC). Handler failures — even when {@link handleError} routes them
   * successfully to retry/DLQ — are classified as failures for metrics by
   * re-failing the chain with a `TechnicalError` whose `cause` is the
   * original `HandlerError`. The terminal `orTee` unwraps the cause before
   * recording the span exception so traces keep the original
   * `RetryableError` / `NonRetryableError` class as the exception type.
   */
  private processMessage(
    msg: ConsumeMessage,
    view: { consumer: ConsumerDefinition; isRpc: boolean; responseSchema?: StandardSchemaV1 },
    name: HandlerName<TContract>,
    handler: StoredHandler,
    state: { messageHandled: boolean },
  ): AsyncResult<void, TechnicalError> {
    const { consumer } = view;
    const queueName = extractQueue(consumer.queue).name;
    const startTime = Date.now();
    const span = startConsumeSpan(this.telemetry, queueName, String(name), {
      "messaging.rabbitmq.message.delivery_tag": msg.fields.deliveryTag,
    });

    return this.parseAndValidateOrNack(msg, consumer, name)
      .tapErr((parseError) => {
        this.logger?.error("Failed to parse/validate message; sending to DLQ", {
          consumerName: String(name),
          queueName,
          error: parseError,
        });
        // parseAndValidateOrNack already nacked; mark handled so the
        // catch-all in consumeSingle does not double-act.
        state.messageHandled = true;
      })
      .flatMap<void, TechnicalError>((validatedMessage) =>
        this.runHandler(handler, validatedMessage, msg)
          .flatMap((handlerResponse) =>
            this.publishReplyIfRpc(msg, view, name, handlerResponse).tap(() => {
              this.logger?.info("Message consumed successfully", {
                consumerName: String(name),
                queueName,
              });
              this.amqpClient.ack(msg);
              state.messageHandled = true;
            }),
          )
          .orElse((handlerError: HandlerError) => {
            this.logger?.error("Error processing message", {
              consumerName: String(name),
              queueName,
              errorType: handlerError.name,
              retryCount:
                (msg.properties.headers?.["x-delivery-count"] as number | undefined) ??
                (msg.properties.headers?.["x-retry-count"] as number | undefined) ??
                0,
              error: handlerError.message,
            });

            // Route the failure to retry / DLQ via handleError. On its
            // success paths (retry republish, immediate-requeue nack, DLQ
            // nack) the message has been ack'd or nack'd, so mark it
            // handled. On its failure paths (e.g. TTL-backoff misconfig)
            // no ack/nack happens and the message will be redelivered —
            // leave messageHandled false so the consume catch-all can
            // defensive-nack if needed.
            //
            // Either way, re-fail the chain with the original handlerError
            // as `cause` so the failure-telemetry path fires; routing-
            // internal errors (TechnicalError) take precedence and surface
            // as the chain's error directly.
            return handleError(
              { amqpClient: this.amqpClient, logger: this.logger },
              handlerError,
              msg,
              String(name),
              consumer,
            )
              .tap(() => {
                state.messageHandled = true;
              })
              .flatMap(() =>
                Err(
                  new TechnicalError(
                    `Handler "${String(name)}" failed: ${handlerError.message}`,
                    handlerError,
                  ),
                ).toAsync(),
              );
          }),
      )
      .tap(() => {
        // Telemetry must never throw out of the consume loop — wrap each
        // call so an instrumentation bug cannot poison the dispatch path
        // (which would land us in the catch-all in consumeSingle, racing
        // with the ack we already issued above).
        try {
          endSpanSuccess(span);
          recordConsumeMetric(
            this.telemetry,
            queueName,
            String(name),
            true,
            Date.now() - startTime,
          );
        } catch (telemetryError: unknown) {
          this.logger?.warn("Telemetry recording threw; ignoring", {
            consumerName: String(name),
            queueName,
            error: telemetryError,
          });
        }
      })
      .tapErr((error) => {
        // Routed handler failures arrive here wrapped in a `TechnicalError`
        // with the original `HandlerError` carried via `cause`. Surface the
        // original to the span so the recorded `exception.type` is the
        // discriminating subclass (`RetryableError` / `NonRetryableError`)
        // rather than the wrapper.
        const reportedError = error.cause instanceof Error ? error.cause : error;
        try {
          endSpanError(span, reportedError);
          recordConsumeMetric(
            this.telemetry,
            queueName,
            String(name),
            false,
            Date.now() - startTime,
          );
        } catch (telemetryError: unknown) {
          this.logger?.warn("Telemetry recording threw; ignoring", {
            consumerName: String(name),
            queueName,
            error: telemetryError,
          });
        }
      });
  }

  /**
   * Consume messages one at a time.
   */
  private consumeSingle(
    name: HandlerName<TContract>,
    view: { consumer: ConsumerDefinition; isRpc: boolean; responseSchema?: StandardSchemaV1 },
    handler: StoredHandler,
  ): AsyncResult<void, TechnicalError> {
    const queueName = extractQueue(view.consumer.queue).name;

    return this.amqpClient
      .consume(
        queueName,
        async (msg) => {
          if (msg === null) {
            this.logger?.warn("Consumer cancelled by server", {
              consumerName: String(name),
              queueName,
            });
            return;
          }
          // The dispatch path is built on `AsyncResult` so handler failures
          // are values, not exceptions. Defensively guard the boundary anyway:
          // a handler that violates the contract by throwing synchronously (or
          // any unexpected fault inside processMessage) would otherwise leave
          // the message neither acked nor nacked, and amqp-connection-manager
          // would not redeliver it until the channel closes. nack(requeue=false)
          // routes it via DLX if configured.
          //
          // The `state.messageHandled` flag guards the catch-block nack: if
          // an exception is thrown *after* the message was already ack'd or
          // nack'd (e.g. from the telemetry chain in processMessage's tail),
          // a second nack would target the same delivery tag and close the
          // channel with 406 PRECONDITION_FAILED.
          const state = { messageHandled: false };
          try {
            await this.processMessage(msg, view, name, handler, state);
          } catch (error: unknown) {
            if (state.messageHandled) {
              this.logger?.error(
                "Uncaught error in consume callback after message was already handled; not nacking",
                {
                  consumerName: String(name),
                  queueName,
                  error,
                },
              );
              return;
            }
            this.logger?.error("Uncaught error in consume callback; nacking message", {
              consumerName: String(name),
              queueName,
              error,
            });
            this.amqpClient.nack(msg, false, false);
          }
        },
        this.consumerOptions[name],
      )
      .tap((consumerTag) => {
        this.consumerTags.add(consumerTag);
      })
      .map(() => undefined)
      .mapErr(
        (error) => new TechnicalError(`Failed to start consuming for "${String(name)}"`, error),
      );
  }
}
