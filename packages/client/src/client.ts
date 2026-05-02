import type {
  CompressionAlgorithm,
  ContractDefinition,
  InferPublisherNames,
} from "@amqp-contract/contract";
import {
  AmqpClient,
  PublishOptions as AmqpClientPublishOptions,
  type Logger,
  MessagingSemanticConventions,
  TechnicalError,
  type TelemetryProvider,
  defaultTelemetryProvider,
  endSpanError,
  endSpanSuccess,
  recordPublishMetric,
  startPublishSpan,
} from "@amqp-contract/core";
import { Future, Result } from "@swan-io/boxed";
import type { AmqpConnectionManagerOptions, ConnectionUrl } from "amqp-connection-manager";
import { compressBuffer } from "./compression.js";
import { MessageValidationError } from "./errors.js";
import type { ClientInferPublisherInput } from "./types.js";

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
};

/**
 * Type-safe AMQP client for publishing messages
 */
export class TypedAmqpClient<TContract extends ContractDefinition> {
  private constructor(
    private readonly contract: TContract,
    private readonly amqpClient: AmqpClient,
    private readonly defaultPublishOptions: PublishOptions,
    private readonly logger?: Logger,
    private readonly telemetry: TelemetryProvider = defaultTelemetryProvider,
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
  }: CreateClientOptions<TContract>): Future<Result<TypedAmqpClient<TContract>, TechnicalError>> {
    const client = new TypedAmqpClient(
      contract,
      new AmqpClient(contract, { urls, connectionOptions }),
      { persistent: true, ...defaultPublishOptions },
      logger,
      telemetry ?? defaultTelemetryProvider,
    );

    return client.waitForConnectionReady().flatMap((result) =>
      result.match({
        Ok: () => Future.value(Result.Ok<TypedAmqpClient<TContract>, TechnicalError>(client)),
        // Release the AmqpClient's connection ref-count so a failed create() does not leak.
        Error: (error) =>
          client
            .close()
            .tapError((closeError) => {
              logger?.warn("Failed to close client after connection failure", {
                error: closeError,
              });
            })
            .map(() => Result.Error<TypedAmqpClient<TContract>, TechnicalError>(error)),
      }),
    );
  }

  /**
   * Publish a message using a defined publisher
   *
   * @param publisherName - The name of the publisher to use
   * @param message - The message to publish
   * @param options - Optional publish options including compression, headers, priority, etc.
   *
   * @remarks
   * If `options.compression` is specified, the message will be compressed before publishing
   * and the `contentEncoding` property will be set automatically. Any `contentEncoding`
   * value already in options will be overwritten by the compression algorithm.
   *
   * @returns Result.Ok(void) on success, or Result.Error with specific error on failure
   */
  /**
   * Publish a message using a defined publisher.
   * TypeScript guarantees publisher exists for valid publisher names.
   */
  publish<TName extends InferPublisherNames<TContract>>(
    publisherName: TName,
    message: ClientInferPublisherInput<TContract, TName>,
    options?: PublishOptions,
  ): Future<Result<void, TechnicalError | MessageValidationError>> {
    const startTime = Date.now();
    // Non-null assertions safe: TypeScript guarantees these exist for valid TName
    const publisher = this.contract.publishers![publisherName as string]!;
    const { exchange, routingKey } = publisher;

    // Start telemetry span
    const span = startPublishSpan(this.telemetry, exchange.name, routingKey, {
      [MessagingSemanticConventions.AMQP_PUBLISHER_NAME]: String(publisherName),
    });

    const validateMessage = () => {
      const validationResult = publisher.message.payload["~standard"].validate(message);
      return Future.fromPromise(
        validationResult instanceof Promise ? validationResult : Promise.resolve(validationResult),
      )
        .mapError((error) => new TechnicalError(`Validation failed`, error))
        .mapOkToResult((validation) => {
          if (validation.issues) {
            return Result.Error(
              new MessageValidationError(String(publisherName), validation.issues),
            );
          }

          return Result.Ok(validation.value);
        });
    };

    const publishMessage = (validatedMessage: unknown): Future<Result<void, TechnicalError>> => {
      // Merge default options with provided options
      const mergedOptions = { ...this.defaultPublishOptions, ...options };

      // Extract compression from merged options and create publish options without it
      const { compression, ...restOptions } = mergedOptions;
      const publishOptions: AmqpClientPublishOptions = { ...restOptions };

      // Prepare payload and options based on compression configuration
      const preparePayload = (): Future<Result<Buffer | unknown, TechnicalError>> => {
        if (compression) {
          // Compress the message payload
          const messageBuffer = Buffer.from(JSON.stringify(validatedMessage));
          publishOptions.contentEncoding = compression;

          return compressBuffer(messageBuffer, compression);
        }

        // No compression: use the channel's built-in JSON serialization
        return Future.value(Result.Ok(validatedMessage));
      };

      // Publish the prepared payload
      return preparePayload().flatMapOk((payload) =>
        this.amqpClient
          .publish(publisher.exchange.name, publisher.routingKey ?? "", payload, publishOptions)
          .mapOkToResult((published) => {
            if (!published) {
              return Result.Error(
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

            return Result.Ok(undefined);
          }),
      );
    };

    // Validate message using schema
    return validateMessage()
      .flatMapOk((validatedMessage) => publishMessage(validatedMessage))
      .tapOk(() => {
        const durationMs = Date.now() - startTime;
        endSpanSuccess(span);
        recordPublishMetric(this.telemetry, exchange.name, routingKey, true, durationMs);
      })
      .tapError((error) => {
        const durationMs = Date.now() - startTime;
        endSpanError(span, error);
        recordPublishMetric(this.telemetry, exchange.name, routingKey, false, durationMs);
      });
  }

  /**
   * Close the channel and connection
   */
  close(): Future<Result<void, TechnicalError>> {
    return this.amqpClient.close().mapOk(() => undefined);
  }

  private waitForConnectionReady(): Future<Result<void, TechnicalError>> {
    return this.amqpClient.waitForConnect();
  }
}
