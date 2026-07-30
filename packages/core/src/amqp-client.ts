import type { ContractDefinition } from "@amqp-contract/contract";
import type {
  AmqpConnectionManager,
  AmqpConnectionManagerOptions,
  ChannelWrapper,
  ConnectionUrl,
  CreateChannelOpts,
} from "amqp-connection-manager";
import type { Channel, ConsumeMessage, Options } from "amqplib";
import { fromPromise, fromSafePromise, fromSafeThrowable, Ok, type AsyncResult } from "unthrown";

import { ConnectionManagerSingleton, type ConnectionLease } from "./connection-manager.js";
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
 * explicitly to disable the timeout.
 */
export const DEFAULT_CONNECT_TIMEOUT_MS = 30_000;

/**
 * Normalise the user-supplied connect timeout to either a positive finite
 * number of milliseconds, or `null` (no timeout). An invalid numeric value
 * (`NaN`, `Infinity`, zero, negative) throws a {@link TechnicalError} rather
 * than silently disabling the timeout — a typo'd value must not turn a
 * 30-second fail-fast into an indefinite wait. The throw surfaces as a
 * `Defect` from the typed `create()` factories.
 */
function resolveConnectTimeoutMs(input: number | null | undefined): number | null {
  if (input === null) return null;
  if (input === undefined) return DEFAULT_CONNECT_TIMEOUT_MS;
  if (!Number.isFinite(input) || input <= 0) {
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast config error; surfaces as a Defect from the typed create() factories (see doc above)
    throw new TechnicalError(
      `Invalid connectTimeoutMs: expected a positive finite number of milliseconds, got ${String(input)}. Pass null to disable the timeout.`,
    );
  }
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
 * `prefetch` maps to amqp-connection-manager's native per-consumer prefetch:
 * it is applied via `basic.qos(count, global=false)` immediately before this
 * consumer's `basic.consume` — and re-applied the same way when the consumer
 * is re-established after a reconnect — so the value never bleeds onto other
 * consumers sharing the channel.
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
 * - Content encoding: non-Buffer payloads are JSON-encoded at publish time,
 *   Buffers go on the wire byte-for-byte
 *
 * All operations return `AsyncResult<T, never>`: infrastructure failures are
 * **unexpected**, so they surface through the `Defect` channel (with a
 * {@link TechnicalError} as the defect's `cause` for logging), never as a
 * modeled `Err`.
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
  /** Lease on the pooled connection; released (idempotently) by {@link close}. */
  private readonly connectionLease: ConnectionLease;
  /** Resolved timeout in ms; `null` means "wait forever". */
  private readonly connectTimeoutMs: number | null;
  /**
   * Memoized close result: `close()` is idempotent, so a double close (e.g. a
   * `finally` block plus an error path) performs the teardown once and both
   * callers observe the same outcome.
   */
  private closing?: AsyncResult<void, never>;

  /**
   * Create a new AMQP client instance.
   *
   * The client will automatically:
   * - Get or create a shared connection using the singleton pattern
   * - Set up AMQP topology (exchanges, queues, bindings) from the contract
   * - Create a confirm channel that encodes content at publish time (JSON for
   *   plain values, byte-for-byte for Buffers)
   *
   * @param contract - The contract definition specifying the AMQP topology
   * @param options - Client configuration options
   */
  constructor(
    private readonly contract: ContractDefinition,
    options: AmqpClientOptions,
  ) {
    // Resolve connect timeout: explicit null disables it; undefined (the
    // common case) gets the fail-fast default; an invalid numeric value
    // throws (routed to the defect channel by the typed create() factories).
    this.connectTimeoutMs = resolveConnectTimeoutMs(options.connectTimeoutMs);

    // Always use singleton to get/create connection
    const singleton = ConnectionManagerSingleton.getInstance();
    this.connectionLease = singleton.acquire(options.urls, options.connectionOptions);
    this.connection = this.connectionLease.connection;

    // Create default setup function that calls setupAmqpTopology
    const defaultSetup = (channel: Channel) => setupAmqpTopology(channel, this.contract);

    // Destructure setup from channelOptions to handle it separately
    const { setup: userSetup, ...otherChannelOptions } = options.channelOptions ?? {};

    // Merge user-provided channel options with defaults. `json` is forced off
    // (after the spread, so user options cannot re-enable it):
    // amqp-connection-manager's JSON mode stringifies EVERY payload —
    // including Buffers, which reach the broker as the literal text
    // '{"type":"Buffer","data":[...]}' and corrupt compressed messages and
    // retry republishing. This client encodes content itself in
    // {@link publish} / {@link sendToQueue}: Buffers pass through untouched,
    // everything else is JSON-encoded.
    const channelOpts: CreateChannelOpts = {
      confirm: true,
      setup: defaultSetup,
      ...otherChannelOptions,
      json: false,
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
   * AsyncResult resolves to a `Defect` (a {@link TechnicalError} cause) once the
   * timeout elapses. Without a timeout, this waits forever —
   * amqp-connection-manager retries connections indefinitely and never errors on
   * its own.
   *
   * NOTE: When using `AmqpClient` directly (not via `TypedAmqpClient` /
   * `TypedAmqpWorker`), the constructor has already incremented the pooled
   * connection's reference count. Callers must invoke `close()` on the failure
   * path to release the connection — `waitForConnect` does not do this
   * automatically. The typed factories handle this cleanup for you.
   */
  waitForConnect(): AsyncResult<void, never> {
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

    return fromPromise(racedPromise, (error: unknown, defect) =>
      defect(
        new TechnicalError(
          "Failed to connect to AMQP broker — verify the broker is running and reachable at the configured `urls`",
          error,
        ),
      ),
    );
  }

  /**
   * Encode publishable content into the exact bytes that go on the wire:
   * Buffers pass through untouched (compressed payloads, retry republishing);
   * everything else is JSON-encoded. A non-serializable value (circular
   * references, BigInt, `undefined`) is a programming fault — the throw is
   * routed to the defect channel by the `fromSafeThrowable` boundary at the
   * call sites.
   */
  private static encodeContent(content: Buffer | unknown): Buffer {
    if (Buffer.isBuffer(content)) return content;
    try {
      return Buffer.from(JSON.stringify(content));
    } catch (error) {
      // oxlint-disable-next-line unthrown/no-throw -- known-technical precondition throw in a plain helper, adopted by the fromSafeThrowable boundary at the call sites
      throw new TechnicalError("Failed to JSON-encode message content", error);
    }
  }

  /**
   * Publish a message to an exchange.
   *
   * Non-Buffer content is JSON-encoded; Buffers are published byte-for-byte.
   *
   * @returns AsyncResult resolving to `true` if the message was sent, `false` if the channel buffer is full.
   */
  publish(
    exchange: string,
    routingKey: string,
    content: Buffer | unknown,
    options?: PublishOptions,
  ): AsyncResult<boolean, never> {
    return fromSafeThrowable(() => AmqpClient.encodeContent(content))()
      .toAsync()
      .flatMap((encoded) =>
        fromPromise(
          this.channelWrapper.publish(exchange, routingKey, encoded, options),
          (error: unknown, defect) =>
            defect(
              new TechnicalError(
                `Failed to publish message to exchange "${exchange}" (routing key "${routingKey}")`,
                error,
              ),
            ),
        ),
      );
  }

  /**
   * Publish a message directly to a queue.
   *
   * Non-Buffer content is JSON-encoded; Buffers are published byte-for-byte.
   *
   * @returns AsyncResult resolving to `true` if the message was sent, `false` if the channel buffer is full.
   */
  sendToQueue(
    queue: string,
    content: Buffer | unknown,
    options?: PublishOptions,
  ): AsyncResult<boolean, never> {
    return fromSafeThrowable(() => AmqpClient.encodeContent(content))()
      .toAsync()
      .flatMap((encoded) =>
        fromPromise(
          this.channelWrapper.sendToQueue(queue, encoded, options),
          (error: unknown, defect) =>
            defect(new TechnicalError(`Failed to publish message to queue "${queue}"`, error)),
        ),
      );
  }

  /**
   * Start consuming messages from a queue.
   *
   * `options.prefetch` maps to amqp-connection-manager's native per-consumer
   * prefetch: applied via `basic.qos(count, global=false)` immediately before
   * this consumer's `basic.consume`, and re-applied the same way when the
   * consumer is re-established after a reconnect — so the value never bleeds
   * onto other consumers sharing the channel.
   *
   * @returns AsyncResult resolving to the consumer tag.
   */
  consume(
    queue: string,
    callback: ConsumeCallback,
    options?: ConsumerOptions,
  ): AsyncResult<string, never> {
    // Validate the prefetch value before forwarding to RabbitMQ. AMQP
    // basic.qos prefetch-count is an unsigned 16-bit short (0–65535); 0
    // means unlimited. NaN, negatives, fractions, and out-of-range numbers
    // would travel to the broker, which either rejects or interprets them
    // unexpectedly.
    const prefetch = options?.prefetch;
    if (prefetch !== undefined) {
      if (!Number.isInteger(prefetch) || prefetch < 0 || prefetch > 65_535) {
        // A misconfigured prefetch is a programming fault, not a modeled
        // failure — surface it through the defect channel.
        return fromSafeThrowable((): string => {
          // oxlint-disable-next-line unthrown/no-throw -- deliberate defect-channel routing inside the fromSafeThrowable thunk
          throw new TechnicalError(
            `Invalid prefetch: expected a non-negative integer ≤ 65535, got ${String(prefetch)}`,
          );
        })().toAsync();
      }
    }

    return fromPromise(
      this.channelWrapper.consume(queue, callback, options),
      (error: unknown, defect) =>
        defect(new TechnicalError("Failed to start consuming messages", error)),
    ).map((reply: { consumerTag: string }) => reply.consumerTag);
  }

  /**
   * Cancel a consumer by its consumer tag.
   */
  cancel(consumerTag: string): AsyncResult<void, never> {
    return fromPromise(this.channelWrapper.cancel(consumerTag), (error: unknown, defect) =>
      defect(new TechnicalError("Failed to cancel consumer", error)),
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
   * Close the channel and release the connection lease.
   *
   * This will:
   * - Close the channel wrapper
   * - Release this client's lease on the shared connection
   * - Close the connection if this was the last client using it
   *
   * Idempotent: a second `close()` returns the same in-flight (or settled)
   * result instead of double-releasing the shared connection.
   *
   * Both steps run regardless of each other's outcome; if both fail, the
   * errors are wrapped in an AggregateError.
   */
  close(): AsyncResult<void, never> {
    if (this.closing) return this.closing;

    const inner = (async () => {
      const channelResult = await fromPromise(
        this.channelWrapper.close(),
        (error: unknown, defect) => defect(new TechnicalError("Failed to close channel", error)),
      );
      const releaseResult = await fromPromise(
        this.connectionLease.release(),
        (error: unknown, defect) =>
          defect(new TechnicalError("Failed to release connection", error)),
      );

      if (channelResult.isDefect() && releaseResult.isDefect()) {
        // Both steps failed — combine their defect causes. The throw is
        // adopted by `fromSafePromise` below and re-emerges as a single Defect.
        // oxlint-disable-next-line unthrown/no-throw -- deliberate defect-channel routing — adopted by the fromSafePromise boundary below
        throw new TechnicalError(
          "Failed to close channel and release connection",
          new AggregateError(
            [channelResult.cause, releaseResult.cause],
            "Failed to close channel and release connection",
          ),
        );
      }

      if (channelResult.isDefect()) return channelResult;
      if (releaseResult.isDefect()) return releaseResult;
      return Ok(undefined);
    })();

    // `inner` is structured to never reject, so lift it with `fromSafePromise`
    // and collapse the nested `Result` it resolves to back into the channel.
    this.closing = fromSafePromise(inner).flatMap((result) => result);
    return this.closing;
  }

  /**
   * Reset connection singleton cache (for testing only)
   * @internal
   */
  static async _resetConnectionCacheForTesting(): Promise<void> {
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  }
}
