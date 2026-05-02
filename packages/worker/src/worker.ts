import {
  type ConsumerDefinition,
  type ContractDefinition,
  type InferConsumerNames,
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
  startConsumeSpan,
} from "@amqp-contract/core";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import { Future, Result } from "@swan-io/boxed";
import type { AmqpConnectionManagerOptions, ConnectionUrl } from "amqp-connection-manager";
import type { ConsumeMessage } from "amqplib";
import { decompressBuffer } from "./decompression.js";
import type { HandlerError } from "./errors.js";
import { MessageValidationError } from "./errors.js";
import { handleError } from "./retry.js";
import type {
  WorkerInferConsumedMessage,
  WorkerInferConsumerHandler,
  WorkerInferConsumerHandlers,
} from "./types.js";

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
 *       return Future.value(Result.Ok(undefined));
 *     },
 *     // Handler with prefetch configuration
 *     processPayment: [
 *       ({ payload }) => {
 *         console.log('Processing payment:', payload.paymentId);
 *         return Future.value(Result.Ok(undefined));
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
   * Handlers for each consumer defined in the contract.
   * Handlers must return `Future<Result<void, HandlerError>>` for explicit error handling.
   * Use defineHandler() to create handlers.
   */
  handlers: WorkerInferConsumerHandlers<TContract>;
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
 *     processOrder: async (message) => {
 *       console.log('Processing order', message.orderId);
 *       // Process the order...
 *     }
 *   },
 *   urls: ['amqp://localhost']
 * }).resultToPromise();
 *
 * // Close when done
 * await worker.close().resultToPromise();
 * ```
 */
export class TypedAmqpWorker<TContract extends ContractDefinition> {
  /**
   * Internal handler storage - handlers returning `Future<Result>`.
   */
  private readonly actualHandlers: Partial<
    Record<
      InferConsumerNames<TContract>,
      WorkerInferConsumerHandler<TContract, InferConsumerNames<TContract>>
    >
  >;
  private readonly consumerOptions: Partial<Record<InferConsumerNames<TContract>, ConsumerOptions>>;
  private readonly consumerTags: Set<string> = new Set();
  private readonly telemetry: TelemetryProvider;

  private constructor(
    private readonly contract: TContract,
    private readonly amqpClient: AmqpClient,
    handlers: WorkerInferConsumerHandlers<TContract>,
    private readonly defaultConsumerOptions: ConsumerOptions,
    private readonly logger?: Logger,
    telemetry?: TelemetryProvider,
  ) {
    this.telemetry = telemetry ?? defaultTelemetryProvider;

    // Extract handlers and options from the handlers object
    this.actualHandlers = {};
    this.consumerOptions = {};

    // Cast handlers to a generic record for iteration
    const handlersRecord = handlers as Record<string, unknown>;

    for (const consumerName of Object.keys(handlersRecord)) {
      const handlerEntry = handlersRecord[consumerName];
      const typedConsumerName = consumerName as InferConsumerNames<TContract>;

      if (isHandlerTuple(handlerEntry)) {
        // Tuple format: [handler, options]
        const [handler, options] = handlerEntry;
        this.actualHandlers[typedConsumerName] = handler as WorkerInferConsumerHandler<
          TContract,
          InferConsumerNames<TContract>
        >;
        this.consumerOptions[typedConsumerName] = {
          ...this.defaultConsumerOptions,
          ...options,
        };
      } else {
        // Direct function format
        this.actualHandlers[typedConsumerName] = handlerEntry as WorkerInferConsumerHandler<
          TContract,
          InferConsumerNames<TContract>
        >;
        this.consumerOptions[typedConsumerName] = this.defaultConsumerOptions;
      }
    }
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
   * @param options - Configuration options for the worker
   * @returns A Future that resolves to a Result containing the worker or an error
   *
   * @example
   * ```typescript
   * const worker = await TypedAmqpWorker.create({
   *   contract: myContract,
   *   handlers: {
   *     processOrder: async ({ payload }) => console.log('Order:', payload.orderId)
   *   },
   *   urls: ['amqp://localhost']
   * }).resultToPromise();
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
  }: CreateWorkerOptions<TContract>): Future<Result<TypedAmqpWorker<TContract>, TechnicalError>> {
    const worker = new TypedAmqpWorker(
      contract,
      new AmqpClient(contract, {
        urls,
        connectionOptions,
      }),
      handlers,
      defaultConsumerOptions ?? {},
      logger,
      telemetry,
    );

    // Note: Wait queues are now created by the core package in setupAmqpTopology
    // when the queue's retry mode is "ttl-backoff"
    return worker
      .waitForConnectionReady()
      .flatMapOk(() => worker.consumeAll())
      .flatMap((result) =>
        result.match({
          Ok: () => Future.value(Result.Ok<TypedAmqpWorker<TContract>, TechnicalError>(worker)),
          // Release the AmqpClient's connection ref-count and cancel any consumers
          // that registered before the failure, so a failed create() does not leak.
          Error: (error) =>
            worker
              .close()
              .tapError((closeError) => {
                logger?.warn("Failed to close worker after setup failure", {
                  error: closeError,
                });
              })
              .map(() => Result.Error<TypedAmqpWorker<TContract>, TechnicalError>(error)),
        }),
      );
  }

  /**
   * Close the AMQP channel and connection.
   *
   * This gracefully closes the connection to the AMQP broker,
   * stopping all message consumption and cleaning up resources.
   *
   * @returns A Future that resolves to a Result indicating success or failure
   *
   * @example
   * ```typescript
   * const closeResult = await worker.close().resultToPromise();
   * if (closeResult.isOk()) {
   *   console.log('Worker closed successfully');
   * }
   * ```
   */
  close(): Future<Result<void, TechnicalError>> {
    return Future.all(
      Array.from(this.consumerTags).map((consumerTag) =>
        this.amqpClient.cancel(consumerTag).mapErrorToResult((error) => {
          this.logger?.warn("Failed to cancel consumer during close", { consumerTag, error });
          return Result.Ok(undefined);
        }),
      ),
    )
      .map(Result.all)
      .tapOk(() => {
        // Clear consumer tags after successful cancellation
        this.consumerTags.clear();
      })
      .flatMapOk(() => this.amqpClient.close())
      .mapOk(() => undefined);
  }

  /**
   * Start consuming messages for all consumers.
   * TypeScript guarantees consumers exist (handlers require matching consumers).
   */
  private consumeAll(): Future<Result<void, TechnicalError>> {
    // Non-null assertion safe: TypeScript guarantees consumers exist (handlers require matching consumers)
    const consumers = this.contract.consumers!;
    const consumerNames = Object.keys(consumers) as InferConsumerNames<TContract>[];

    return Future.all(consumerNames.map((name) => this.consume(name)))
      .map(Result.all)
      .mapOk(() => undefined);
  }

  private waitForConnectionReady(): Future<Result<void, TechnicalError>> {
    return this.amqpClient.waitForConnect();
  }

  /**
   * Start consuming messages for a specific consumer.
   * TypeScript guarantees consumer and handler exist for valid consumer names.
   */
  private consume<TName extends InferConsumerNames<TContract>>(
    consumerName: TName,
  ): Future<Result<void, TechnicalError>> {
    // Non-null assertions safe: TypeScript guarantees these exist for valid TName
    const consumerEntry = this.contract.consumers![consumerName as string]!;
    const consumer = extractConsumer(consumerEntry);
    // Non-null assertion safe: constructor validates handlers match consumer names
    const handler = this.actualHandlers[consumerName]!;

    return this.consumeSingle(
      consumerName,
      consumer,
      handler as Parameters<typeof this.consumeSingle<TName>>[2],
    );
  }

  /**
   * Validate data against a Standard Schema and handle errors.
   */
  private validateSchema(
    schema: StandardSchemaV1,
    data: unknown,
    context: { consumerName: string; queueName: string; field: string },
    msg: ConsumeMessage,
  ): Future<Result<unknown, TechnicalError>> {
    const rawValidation = schema["~standard"].validate(data);
    const validationPromise =
      rawValidation instanceof Promise ? rawValidation : Promise.resolve(rawValidation);

    return Future.fromPromise(validationPromise)
      .mapError((error) => new TechnicalError(`Error validating ${context.field}`, error))
      .mapOkToResult((result) => {
        if (result.issues) {
          return Result.Error(
            new TechnicalError(
              `${context.field} validation failed`,
              new MessageValidationError(context.consumerName, result.issues),
            ),
          );
        }
        return Result.Ok(result.value);
      })
      .tapError((error) => {
        this.logger?.error(`${context.field} validation failed`, {
          consumerName: context.consumerName,
          queueName: context.queueName,
          error,
        });
        this.amqpClient.nack(msg, false, false);
      });
  }

  /**
   * Parse and validate a message from AMQP.
   * @returns Ok with validated message (payload + headers), or Error (message already nacked)
   */
  private parseAndValidateMessage<TName extends InferConsumerNames<TContract>>(
    msg: ConsumeMessage,
    consumer: ConsumerDefinition,
    consumerName: TName,
  ): Future<Result<WorkerInferConsumedMessage<TContract, TName>, TechnicalError>> {
    const queue = extractQueue(consumer.queue);
    const context = {
      consumerName: String(consumerName),
      queueName: queue.name,
    };

    const nackAndError = (message: string, error?: unknown): TechnicalError => {
      this.logger?.error(message, { ...context, error });
      this.amqpClient.nack(msg, false, false);
      return new TechnicalError(message, error);
    };

    // Decompress → Parse JSON → Validate payload
    const parsePayload = decompressBuffer(msg.content, msg.properties.contentEncoding)
      .tapError((error) => {
        this.logger?.error("Failed to decompress message", { ...context, error });
        this.amqpClient.nack(msg, false, false);
      })
      .mapOkToResult((buffer) =>
        Result.fromExecution(() => JSON.parse(buffer.toString()) as unknown).mapError((error) =>
          nackAndError("Failed to parse JSON", error),
        ),
      )
      .flatMapOk((parsed) =>
        this.validateSchema(
          consumer.message.payload as StandardSchemaV1,
          parsed,
          { ...context, field: "payload" },
          msg,
        ),
      );

    // Validate headers (if schema defined)
    const parseHeaders = consumer.message.headers
      ? this.validateSchema(
          consumer.message.headers as StandardSchemaV1,
          msg.properties.headers ?? {},
          { ...context, field: "headers" },
          msg,
        )
      : Future.value(Result.Ok<unknown, TechnicalError>(undefined));

    return Future.allFromDict({ payload: parsePayload, headers: parseHeaders }).map(
      Result.allFromDict,
    ) as Future<Result<WorkerInferConsumedMessage<TContract, TName>, TechnicalError>>;
  }

  /**
   * Process a single consumed message: validate, invoke handler, record telemetry, and handle errors.
   */
  private processMessage<TName extends InferConsumerNames<TContract>>(
    msg: ConsumeMessage,
    consumer: ConsumerDefinition,
    consumerName: TName,
    handler: (
      message: WorkerInferConsumedMessage<TContract, TName>,
      rawMessage: ConsumeMessage,
    ) => Future<Result<void, HandlerError>>,
  ): Future<Result<void, TechnicalError>> {
    const queueName = extractQueue(consumer.queue).name;
    const startTime = Date.now();
    const span = startConsumeSpan(this.telemetry, queueName, String(consumerName), {
      "messaging.rabbitmq.message.delivery_tag": msg.fields.deliveryTag,
    });

    let messageHandled = false;
    let firstError: Error | undefined;

    return this.parseAndValidateMessage(msg, consumer, consumerName)
      .flatMapOk((validatedMessage) =>
        handler(validatedMessage, msg)
          .flatMapOk(() => {
            this.logger?.info("Message consumed successfully", {
              consumerName: String(consumerName),
              queueName,
            });
            this.amqpClient.ack(msg);
            messageHandled = true;

            return Future.value(Result.Ok<void, HandlerError>(undefined));
          })
          .flatMapError((handlerError: HandlerError) => {
            this.logger?.error("Error processing message", {
              consumerName: String(consumerName),
              queueName,
              errorType: handlerError.name,
              error: handlerError.message,
            });
            firstError = handlerError;

            return handleError(
              { amqpClient: this.amqpClient, logger: this.logger },
              handlerError,
              msg,
              String(consumerName),
              consumer,
            );
          }),
      )
      .map((result) => {
        const durationMs = Date.now() - startTime;
        if (messageHandled) {
          endSpanSuccess(span);
          recordConsumeMetric(this.telemetry, queueName, String(consumerName), true, durationMs);
        } else {
          const error = result.isError()
            ? result.error
            : (firstError ?? new Error("Unknown error"));
          endSpanError(span, error);
          recordConsumeMetric(this.telemetry, queueName, String(consumerName), false, durationMs);
        }
        return result;
      });
  }

  /**
   * Consume messages one at a time.
   */
  private consumeSingle<TName extends InferConsumerNames<TContract>>(
    consumerName: TName,
    consumer: ConsumerDefinition,
    handler: (
      message: WorkerInferConsumedMessage<TContract, TName>,
      rawMessage: ConsumeMessage,
    ) => Future<Result<void, HandlerError>>,
  ): Future<Result<void, TechnicalError>> {
    const queueName = extractQueue(consumer.queue).name;

    return this.amqpClient
      .consume(
        queueName,
        async (msg) => {
          if (msg === null) {
            this.logger?.warn("Consumer cancelled by server", {
              consumerName: String(consumerName),
              queueName,
            });
            return;
          }
          await this.processMessage(msg, consumer, consumerName, handler).toPromise();
        },
        this.consumerOptions[consumerName],
      )
      .tapOk((consumerTag) => {
        this.consumerTags.add(consumerTag);
      })
      .mapError(
        (error) =>
          new TechnicalError(`Failed to start consuming for "${String(consumerName)}"`, error),
      )
      .mapOk(() => undefined);
  }
}
