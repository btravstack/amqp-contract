import type { ConsumeMessage } from "amqplib";
import type { AsyncResult } from "unthrown";
import type {
  AmqpClient,
  ConsumeCallback,
  ConsumerOptions,
  PublishOptions,
} from "./amqp-client.js";
import type { TechnicalError } from "./errors.js";

/**
 * The transport surface `TypedAmqpClient` / `TypedAmqpWorker` operate
 * against — exactly the subset of {@link AmqpClient} they call.
 *
 * The default implementation is {@link AmqpClient} (a real AMQP connection
 * via amqp-connection-manager). Alternative implementations can be injected
 * through the `transport` option of `TypedAmqpClient.create` /
 * `TypedAmqpWorker.create` — most notably `@amqp-contract/testing`'s
 * `InMemoryAmqpBroker`, which runs the full contract pipeline (validation,
 * middleware, RPC correlation, retry routing) without a broker.
 */
export type AmqpTransport = {
  /** Resolve once the transport is ready to publish/consume. */
  waitForConnect(): AsyncResult<void, TechnicalError>;
  /**
   * Publish a message. Resolves `true` when accepted, `false` when the
   * write buffer is full (backpressure).
   */
  publish(
    exchange: string,
    routingKey: string,
    content: Buffer | unknown,
    options?: PublishOptions,
  ): AsyncResult<boolean, TechnicalError>;
  /** Start consuming a queue; resolves to the consumer tag. */
  consume(
    queue: string,
    callback: ConsumeCallback,
    options?: ConsumerOptions,
  ): AsyncResult<string, TechnicalError>;
  /** Cancel a consumer by tag. */
  cancel(consumerTag: string): AsyncResult<void, TechnicalError>;
  /** Acknowledge a delivered message. */
  ack(msg: ConsumeMessage, allUpTo?: boolean): void;
  /** Reject a delivered message; `requeue: false` routes to the DLX if configured. */
  nack(msg: ConsumeMessage, allUpTo?: boolean, requeue?: boolean): void;
  /** Release the transport (consumers, channel, connection reference). */
  close(): AsyncResult<void, TechnicalError>;
};

/**
 * Compile-time proof that {@link AmqpClient} satisfies {@link AmqpTransport};
 * fails to typecheck if the surfaces drift apart.
 */
export type _AmqpClientIsTransport = AmqpClient extends AmqpTransport ? true : never;
