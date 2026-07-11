import type { ContractDefinition } from "@amqp-contract/contract";
import type {
  AmqpConnectionManager,
  AmqpConnectionManagerOptions,
  ChannelWrapper,
  ConnectionUrl,
  CreateChannelOpts,
} from "amqp-connection-manager";
import type { Channel, ConsumeMessage, Options } from "amqplib";
import {
  Err,
  ErrAsync,
  fromPromise,
  fromSafePromise,
  Ok,
  type AsyncResult,
  type Result,
} from "unthrown";
import { ConnectionManagerSingleton } from "./connection-manager.js";
import { TechnicalError } from "./errors.js";
import { setupAmqpTopology } from "./setup.js";

/**
 * Invoke a SetupFunc, handling both callback-based and promise-based signatures.
 * Uses Function.length to distinguish (same approach as promise-breaker).
 * @internal
 */
function callSetupFunc(
  setup: NonNullable<CreateChannelOpts["setup"]>,
  channel: Channel,
): Promise<void> {
  if (setup.length >= 2) {
    return new Promise<void>((resolve, reject) => {
      (setup as (channel: Channel, callback: (error?: Error) => void) => void)(
        channel,
        (error?: Error) => {
          if (error) reject(error);
          else resolve();
        },
      );
    });
  }
  return (setup as (channel: Channel) => Promise<void>)(channel);
}

/**
 * Default time `waitForConnect` will wait for the broker before erroring out.
 * Defaulting to a finite value (rather than waiting forever) means a fail-fast
 * developer experience: a misconfigured URL, a down broker, or wrong
 * credentials surface as an `err` within 30 seconds. Pass `null`
 * explicitly to disable the timeout — `Infinity` and other non-finite values
 * are also coerced to "no timeout" because Node's `setTimeout` clamps large
 * delays to ~24.8 days and silently fires near-immediately on `Infinity`.
 */
export const DEFAULT_CONNECT_TIMEOUT_MS = 30_000;

/**
 * Normalise the user-supplied connect timeout to either a positive finite
 * number of milliseconds, or `null` (no timeout). `Infinity`, `NaN`, and
 * non-positive values all map to `null` rather than being passed to
 * `setTimeout` — see {@link DEFAULT_CONNECT_TIMEOUT_MS}.
 */
function resolveConnectTimeoutMs(input: number | null | undefined): number | null {
  if (input === null) return null;
  if (input === undefined) return DEFAULT_CONNECT_TIMEOUT_MS;
  if (!Number.isFinite(input) || input <= 0) return null;
  return input;
}

/**
 * Options for creating an AMQP client.
 *
 * @property urls - AMQP broker URL(s). Multiple URLs provide failover support.
 * @property connectionOptions - Optional connection configuration (heartbeat, reconnect settings, etc.).
 * @property channelOptions - Optional channel configuration options.
 * @property connectTimeoutMs - Maximum time in ms to wait for the channel to
 *   become ready in `waitForConnect`. Defaults to {@link DEFAULT_CONNECT_TIMEOUT_MS}.
 *   Pass `null` to disable the timeout entirely (amqp-connection-manager will
 *   retry indefinitely).
 */
export type AmqpClientOptions = {
  urls: ConnectionUrl[];
  connectionOptions?: AmqpConnectionManagerOptions | undefined;
  channelOptions?: Partial<CreateChannelOpts> | undefined;
  connectTimeoutMs?: number | null | undefined;
};

/**
 * Callback type for consuming messages.
 */
export type ConsumeCallback = (msg: ConsumeMessage | null) => void | Promise<void>;

/**
 * Publish options for `AmqpClient.publish` / `AmqpClient.sendToQueue`.
 *
 * Currently a re-export of amqplib's `Options.Publish`. A previous version of
 * this type also exposed a `timeout` field, but that field never had a
 * meaningful AMQP-level effect in this codebase and has been removed to avoid
 * suggesting behaviour we do not provide. (`amqp-connection-manager`'s own
 * `publishTimeout` channel option is unrelated and is configured at channel
 * creation, not per-publish.)
 */
export type PublishOptions = Options.Publish;

/**
 * Consume options that extend amqplib's `Options.Consume` with an optional
 * per-consumer prefetch count.
 *
 * `prefetch` is intercepted by {@link AmqpClient.consume}: it is stripped from
 * the options handed to the underlying `channelWrapper.consume(...)` call
 * (since amqplib's `Options.Consume` does not include it) and applied via
 * `channel.prefetch(count, false)` registered through `addSetup` *before* the
 * consume so the value is in effect when the consumer starts and is reapplied
 * automatically on channel reconnect.
 */
export type ConsumerOptions = Options.Consume & {
  /** Per-consumer prefetch count. Applied before `channel.consume(...)`. */
  prefetch?: number;
};

/**
 * AMQP client that manages connections and channels with automatic topology setup.
 *
 * This class handles:
 * - Connection management with automatic reconnection via amqp-connection-manager
 * - Connection pooling and sharing across instances with the same URLs
 * - Automatic AMQP topology setup (exchanges, queues, bindings) from contract
 * - Channel creation with JSON serialization enabled by default
 *
 * All operations return `AsyncResult<T, TechnicalError>` for consistent error handling.
 *
 * @example
 * ```typescript
 * const client = new AmqpClient(contract, {
 *   urls: ['amqp://localhost'],
 *   connectionOptions: { heartbeatIntervalInSeconds: 30 }
 * });
 *
 * // Wait for connection (AsyncResult is thenable)
 * await client.waitForConnect();
 *
 * // Publish a message
 * const result = await client.publish('exchange', 'routingKey', { data: 'value' });
 *
 * // Close when done
 * await client.close();
 * ```
 */
export class AmqpClient {
  private readonly connection: AmqpConnectionManager;
  private readonly channelWrapper: ChannelWrapper;
  private readonly urls: ConnectionUrl[];
  private readonly connectionOptions?: AmqpConnectionManagerOptions;
  /** Resolved timeout in ms; `null` means "wait forever". */
  private readonly connectTimeoutMs: number | null;
  /**
   * Per-consumer prefetch setup functions registered via `addSetup` so they
   * can be removed in {@link cancel} once the consumer is gone — otherwise
   * the channel wrapper would replay the cancelled consumer's QoS on every
   * reconnect and silently apply it to subsequent consumers.
   *
   * @internal
   */
  private readonly prefetchSetups: Map<string, (channel: Channel) => Promise<void>> = new Map();

  /**
   * Create a new AMQP client instance.
   *
   * The client will automatically:
   * - Get or create a shared connection using the singleton pattern
   * - Set up AMQP topology (exchanges, queues, bindings) from the contract
   * - Create a channel with JSON serialization enabled
   *
   * @param contract - The contract definition specifying the AMQP topology
   * @param options - Client configuration options
   */
  constructor(
    private readonly contract: ContractDefinition,
    options: AmqpClientOptions,
  ) {
    // Store for cleanup
    this.urls = options.urls;
    if (options.connectionOptions !== undefined) {
      this.connectionOptions = options.connectionOptions;
    }
    // Resolve connect timeout: explicit null disables it; undefined (the common
    // case) gets the fail-fast default. Finite positive numbers pass through;
    // any other numeric value (Infinity, NaN, ≤ 0) is coerced to null because
    // Node's `setTimeout` clamps large delays to ~24.8 days and silently fires
    // near-immediately on Infinity — neither is what a caller asking for "no
    // timeout" expects.
    this.connectTimeoutMs = resolveConnectTimeoutMs(options.connectTimeoutMs);

    // Always use singleton to get/create connection
    const singleton = ConnectionManagerSingleton.getInstance();
    this.connection = singleton.getConnection(options.urls, options.connectionOptions);

    // Create default setup function that calls setupAmqpTopology
    const defaultSetup = (channel: Channel) => setupAmqpTopology(channel, this.contract);

    // Destructure setup from channelOptions to handle it separately
    const { setup: userSetup, ...otherChannelOptions } = options.channelOptions ?? {};

    // Merge user-provided channel options with defaults
    const channelOpts: CreateChannelOpts = {
      confirm: true,
      json: true,
      setup: defaultSetup,
      ...otherChannelOptions,
    };

    // If user provided a custom setup, wrap it to call both
    if (userSetup) {
      channelOpts.setup = async (channel: Channel) => {
        await defaultSetup(channel);
        await callSetupFunc(userSetup, channel);
      };
    }

    this.channelWrapper = this.connection.createChannel(channelOpts);
  }

  /**
   * Get the underlying connection manager
   *
   * This method exposes the AmqpConnectionManager instance that this client uses.
   * The connection is automatically shared across all AmqpClient instances that
   * use the same URLs and connection options.
   *
   * @returns The AmqpConnectionManager instance used by this client
   */
  getConnection(): AmqpConnectionManager {
    return this.connection;
  }

  /**
   * Wait for the channel to be connected and ready.
   *
   * If `connectTimeoutMs` was provided in the constructor options, the returned
   * AsyncResult resolves to `Err(TechnicalError)` once the timeout elapses.
   * Without a timeout, this waits forever — amqp-connection-manager retries
   * connections indefinitely and never errors on its own.
   *
   * NOTE: When using `AmqpClient` directly (not via `TypedAmqpClient` /
   * `TypedAmqpWorker`), the constructor has already incremented the pooled
   * connection's reference count. Callers must invoke `close()` on the error
   * path to release the connection — `waitForConnect` does not do this
   * automatically. The typed factories handle this cleanup for you.
   */
  waitForConnect(): AsyncResult<void, TechnicalError> {
    const connectPromise = this.channelWrapper.waitForConnect();
    const timeoutMs = this.connectTimeoutMs;

    const racedPromise =
      timeoutMs === null
        ? connectPromise
        : new Promise<void>((resolve, reject) => {
            const handle = setTimeout(() => {
              reject(new Error(`Timed out waiting for AMQP connection after ${timeoutMs}ms`));
            }, timeoutMs);
            connectPromise.then(
              () => {
                clearTimeout(handle);
                resolve();
              },
              (error: unknown) => {
                clearTimeout(handle);
                reject(error);
              },
            );
          });

    return fromPromise(
      racedPromise,
      (error: unknown) =>
        new TechnicalError(
          "Failed to connect to AMQP broker — verify the broker is running and reachable at the configured `urls`",
          error,
        ),
    );
  }

  /**
   * Publish a message to an exchange.
   *
   * @returns AsyncResult resolving to `true` if the message was sent, `false` if the channel buffer is full.
   */
  publish(
    exchange: string,
    routingKey: string,
    content: Buffer | unknown,
    options?: PublishOptions,
  ): AsyncResult<boolean, TechnicalError> {
    return fromPromise(
      this.channelWrapper.publish(exchange, routingKey, content, options),
      (error: unknown) =>
        new TechnicalError(
          `Failed to publish message to exchange "${exchange}" (routing key "${routingKey}")`,
          error,
        ),
    );
  }

  /**
   * Publish a message directly to a queue.
   *
   * @returns AsyncResult resolving to `true` if the message was sent, `false` if the channel buffer is full.
   */
  sendToQueue(
    queue: string,
    content: Buffer | unknown,
    options?: PublishOptions,
  ): AsyncResult<boolean, TechnicalError> {
    return fromPromise(
      this.channelWrapper.sendToQueue(queue, content, options),
      (error: unknown) =>
        new TechnicalError(`Failed to publish message to queue "${queue}"`, error),
    );
  }

  /**
   * Start consuming messages from a queue.
   *
   * If `options.prefetch` is set, a per-consumer prefetch count is applied via
   * `channel.prefetch(count, false)` registered as a setup function on the
   * channel wrapper *before* the underlying `consume` call. Registering it via
   * `addSetup` ensures the prefetch is reapplied automatically on channel
   * reconnect; using `global=false` scopes it to subsequent consumers on the
   * channel (RabbitMQ semantics — opposite of intuition: `false` is per-
   * consumer, `true` is channel-wide).
   *
   * `prefetch` is stripped from the options handed to `channelWrapper.consume`
   * because it is not a valid `amqplib` `Options.Consume` field — leaving it
   * in would just travel as a no-op key-value pair on the consume frame.
   *
   * @returns AsyncResult resolving to the consumer tag.
   */
  consume(
    queue: string,
    callback: ConsumeCallback,
    options?: ConsumerOptions,
  ): AsyncResult<string, TechnicalError> {
    // Split prefetch out of the options that go to consume(...).
    const { prefetch, ...consumeOptions } = options ?? {};

    // Validate the prefetch value before forwarding to RabbitMQ. AMQP
    // basic.qos prefetch-count is an unsigned 16-bit short (0–65535); 0
    // means unlimited. NaN, negatives, fractions, and out-of-range numbers
    // were silently dropped by the previous implementation — now they'd
    // travel to the broker, which either rejects or interprets unexpectedly.
    if (prefetch !== undefined) {
      if (!Number.isInteger(prefetch) || prefetch < 0 || prefetch > 65_535) {
        return ErrAsync(
          new TechnicalError(
            `Invalid prefetch: expected a non-negative integer ≤ 65535, got ${String(prefetch)}`,
          ),
        );
      }
    }

    // Capture the prefetch setup function so it can be removed when the
    // consumer is cancelled. Otherwise the channel wrapper would replay
    // it on every reconnect, applying the cancelled consumer's QoS to
    // subsequent consumers (RabbitMQ's `basic.qos(global=false)` semantics
    // affect every later consumer on the channel until another `qos`).
    const prefetchSetup =
      typeof prefetch === "number"
        ? async (channel: Channel) => {
            await channel.prefetch(prefetch, false);
          }
        : undefined;

    const consumePromise = (async () => {
      if (prefetchSetup) {
        // Register prefetch as a channel setup so it is (re)applied on every
        // reconnect, then start consuming. addSetup() also runs the function
        // immediately if a channel is already up, so the prefetch is in
        // effect by the time consume() starts the new consumer.
        await this.channelWrapper.addSetup(prefetchSetup);
      }
      let reply: { consumerTag: string };
      try {
        reply = await this.channelWrapper.consume(queue, callback, consumeOptions);
      } catch (error) {
        // Roll back the prefetch setup. If consume failed (e.g. queue is
        // gone), the setup is registered but tied to no consumer; without
        // this rollback every reconnect would replay it, silently changing
        // QoS for unrelated consumers on the channel.
        if (prefetchSetup) {
          await this.channelWrapper.removeSetup(prefetchSetup).catch(() => {
            // Best-effort cleanup; swallow so we propagate the original
            // consume error instead of masking it.
          });
        }
        throw error;
      }
      if (prefetchSetup) {
        this.prefetchSetups.set(reply.consumerTag, prefetchSetup);
      }
      return reply;
    })();

    return fromPromise(
      consumePromise,
      (error: unknown) => new TechnicalError("Failed to start consuming messages", error),
    ).map((reply: { consumerTag: string }) => reply.consumerTag);
  }

  /**
   * Cancel a consumer by its consumer tag.
   */
  cancel(consumerTag: string): AsyncResult<void, TechnicalError> {
    return fromPromise(
      (async () => {
        // Drop the prefetch setup whether or not the cancel itself succeeds.
        // If `cancel` rejects (consumer already gone, tag unknown), keeping
        // the setup registered means every reconnect replays a stale
        // `basic.qos`, silently changing QoS for unrelated consumers on the
        // channel. Best-effort cleanup runs in `finally`.
        const setup = this.prefetchSetups.get(consumerTag);
        this.prefetchSetups.delete(consumerTag);
        try {
          await this.channelWrapper.cancel(consumerTag);
        } finally {
          if (setup !== undefined) {
            await this.channelWrapper.removeSetup(setup).catch(() => {
              // Best-effort cleanup; swallow so the original cancel error
              // (if any) propagates unchanged.
            });
          }
        }
      })(),
      (error: unknown) => new TechnicalError("Failed to cancel consumer", error),
    ).map(() => undefined);
  }

  /**
   * Acknowledge a message.
   *
   * @param msg - The message to acknowledge
   * @param allUpTo - If true, acknowledge all messages up to and including this one
   */
  ack(msg: ConsumeMessage, allUpTo = false): void {
    this.channelWrapper.ack(msg, allUpTo);
  }

  /**
   * Negative acknowledge a message.
   *
   * @param msg - The message to nack
   * @param allUpTo - If true, nack all messages up to and including this one
   * @param requeue - If true, requeue the message(s)
   */
  nack(msg: ConsumeMessage, allUpTo = false, requeue = true): void {
    this.channelWrapper.nack(msg, allUpTo, requeue);
  }

  /**
   * Add a setup function to be called when the channel is created or reconnected.
   *
   * This is useful for setting up channel-level configuration like prefetch.
   *
   * @param setup - The setup function to add
   */
  addSetup(setup: (channel: Channel) => void | Promise<void>): void {
    this.channelWrapper.addSetup(setup);
  }

  /**
   * Register an event listener on the channel wrapper.
   *
   * Available events:
   * - 'connect': Emitted when the channel is (re)connected
   * - 'close': Emitted when the channel is closed
   * - 'error': Emitted when an error occurs
   *
   * @param event - The event name
   * @param listener - The event listener
   */
  on(event: string, listener: (...args: unknown[]) => void): void {
    this.channelWrapper.on(event, listener);
  }

  /**
   * Close the channel and release the connection reference.
   *
   * This will:
   * - Close the channel wrapper
   * - Decrease the reference count on the shared connection
   * - Close the connection if this was the last client using it
   *
   * Both steps run regardless of each other's outcome; if both fail, the
   * errors are wrapped in an AggregateError.
   */
  close(): AsyncResult<void, TechnicalError> {
    const inner = (async (): Promise<Result<void, TechnicalError>> => {
      const channelResult = await fromPromise(
        this.channelWrapper.close(),
        (error: unknown) => new TechnicalError("Failed to close channel", error),
      );
      const releaseResult = await fromPromise(
        ConnectionManagerSingleton.getInstance().releaseConnection(
          this.urls,
          this.connectionOptions,
        ),
        (error: unknown) => new TechnicalError("Failed to release connection", error),
      );

      if (channelResult.isErr() && releaseResult.isErr()) {
        return Err(
          new TechnicalError(
            "Failed to close channel and release connection",
            new AggregateError(
              [channelResult.error, releaseResult.error],
              "Failed to close channel and release connection",
            ),
          ),
        );
      }

      if (channelResult.isErr()) return channelResult;
      if (releaseResult.isErr()) return releaseResult;
      return Ok(undefined);
    })();

    // `inner` is structured to never reject, so lift it with `fromSafePromise`
    // and collapse the nested `Result` it resolves to back into the channel.
    return fromSafePromise(inner).flatMap((result) => result);
  }

  /**
   * Reset connection singleton cache (for testing only)
   * @internal
   */
  static async _resetConnectionCacheForTesting(): Promise<void> {
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  }
}
