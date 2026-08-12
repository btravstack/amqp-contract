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
  fromPromise,
  fromSafePromise,
  fromSafeThrowable,
  Ok,
  type AsyncResult,
  type Result,
} from "unthrown";

import { ConnectionManagerSingleton, type ConnectionLease } from "./connection-manager.js";
import { TechnicalError } from "./errors.js";
import type { Logger } from "./logger.js";
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
 * Collapse the channel wrapper's boolean send confirmation into the void
 * result channel. `false` means the channel's write buffer is full — an
 * infrastructure condition callers cannot meaningfully branch on — so it
 * becomes a Defect (with a {@link TechnicalError} cause) HERE, at the single
 * decision point, instead of leaking a boolean that every downstream layer
 * re-triages its own way.
 */
function absorbWriteBufferConfirmation(published: boolean, target: string): Result<void, never> {
  if (!published) {
    // oxlint-disable-next-line unthrown/no-throw -- deliberate defect-channel routing — the combinator adopts the throw as a Defect
    throw new TechnicalError(`Failed to publish message to ${target}: channel write buffer full`);
  }
  return Ok(undefined);
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
 * Default per-consumer prefetch.
 *
 * Bounds in-flight messages per consumer, which bounds both memory and the
 * redelivery burst when a worker crashes. Throughput-bound consumers raise it
 * explicitly; `"unbounded"` restores AMQP's unlimited behavior.
 */
export const DEFAULT_PREFETCH = 10;

/**
 * Default `publishTimeout` for the channel, in milliseconds.
 *
 * Without a bound, publishes issued while the broker is unreachable buffer
 * indefinitely and their promises never settle — a caller awaiting one waits
 * forever. 30s is long enough that a brief reconnect does not fail healthy
 * publishes, short enough that a real outage surfaces as an error.
 *
 * Pass `publishTimeoutMs: null` to disable, matching the `connectTimeoutMs`
 * convention.
 */
export const DEFAULT_PUBLISH_TIMEOUT_MS = 30_000;

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
 * @property publishTimeoutMs - Maximum time in ms a publish may sit buffered
 *   waiting for the broker before its promise settles with a failure.
 *   Defaults to {@link DEFAULT_PUBLISH_TIMEOUT_MS}. Pass `null` to disable,
 *   restoring unbounded buffering. See the field's own doc comment for the
 *   precedence against `channelOptions.publishTimeout`.
 * @property logger - Optional logger. Channel-level `'error'` events (topology
 *   setup failures on connect/reconnect, publish-worker faults) are routed
 *   here — they are recoverable-by-reconnect conditions, never thrown.
 */
export type AmqpClientOptions = {
  urls: ConnectionUrl[];
  connectionOptions?: AmqpConnectionManagerOptions | undefined;
  channelOptions?: Partial<CreateChannelOpts> | undefined;
  connectTimeoutMs?: number | null | undefined;
  /**
   * Maximum time in ms a publish may sit buffered waiting for the broker
   * before its promise settles with a failure. Defaults to
   * {@link DEFAULT_PUBLISH_TIMEOUT_MS}. Pass `null` to disable, restoring
   * unbounded buffering — a publish issued during an outage then never
   * settles. Same convention as `connectTimeoutMs`.
   *
   * Precedence against `channelOptions.publishTimeout` (most specific wins):
   * 1. This option, when explicitly set to a number or `null` — always
   *    authoritative.
   * 2. Otherwise `channelOptions.publishTimeout`, if the caller set one on
   *    the passthrough bag — respected, not silently overwritten.
   * 3. Otherwise {@link DEFAULT_PUBLISH_TIMEOUT_MS}.
   *
   * `null` here disables the timeout outright, even when
   * `channelOptions.publishTimeout` is also set — it is the more specific
   * expression of intent.
   */
  publishTimeoutMs?: number | null | undefined;
  logger?: Logger | undefined;
};

/**
 * Callback type for consuming messages.
 */
export type ConsumeCallback = (msg: ConsumeMessage | null) => void | Promise<void>;

/**
 * Publish options for `AmqpClient.publish` / `AmqpClient.sendToQueue`.
 *
 * Named `AmqpPublishOptions` (not `PublishOptions`) so it never collides with
 * the user-facing `PublishOptions` of `@amqp-contract/client`.
 *
 * Currently a re-export of amqplib's `Options.Publish`. A previous version of
 * this type also exposed a `timeout` field, but that field never had a
 * meaningful AMQP-level effect in this codebase and has been removed to avoid
 * suggesting behaviour we do not provide. (`amqp-connection-manager`'s own
 * `publishTimeout` channel option is unrelated and is configured at channel
 * creation, not per-publish.)
 */
export type AmqpPublishOptions = Options.Publish;

/**
 * Consume options that extend amqplib's `Options.Consume` with an optional
 * per-consumer prefetch count.
 *
 * Named `AmqpConsumeOptions` (not `ConsumerOptions`) so it never collides with
 * the user-facing `ConsumerOptions` of `@amqp-contract/worker`.
 *
 * `prefetch` maps to amqp-connection-manager's native per-consumer prefetch:
 * it is applied via `basic.qos(count, global=false)` immediately before this
 * consumer's `basic.consume` — and re-applied the same way when the consumer
 * is re-established after a reconnect — so the value never bleeds onto other
 * consumers sharing the channel.
 */
export type AmqpConsumeOptions = Omit<Options.Consume, "prefetch"> & {
  /**
   * Per-consumer prefetch count, applied before `channel.consume(...)`.
   *
   * Defaults to {@link DEFAULT_PREFETCH}. Pass `"unbounded"` to opt out and
   * let the broker push the entire ready backlog — AMQP's original default,
   * and a memory hazard on any queue that can build a backlog.
   *
   * `"unbounded"` rather than `0` because AMQP's `0` means *unlimited*, which
   * reads at a call site as its opposite.
   */
  prefetch?: number | "unbounded";
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
 * const result = await client.publish(
 *   { exchange: 'exchange', routingKey: 'routingKey' },
 *   { data: 'value' },
 * );
 *
 * // Close when done
 * await client.close().get();
 * ```
 */
export class AmqpClient {
  private readonly connection: AmqpConnectionManager;
  private readonly channelWrapper: ChannelWrapper;
  /** Lease on the pooled connection; released (idempotently) by {@link close}. */
  private readonly connectionLease: ConnectionLease;
  /** Resolved timeout in ms; `null` means "wait forever". */
  private readonly connectTimeoutMs: number | null;
  private readonly logger?: Logger | undefined;
  /**
   * Memoized close result: `close()` is idempotent, so a double close (e.g. a
   * `finally` block plus an error path) performs the teardown once and both
   * callers observe the same outcome.
   */
  private closing?: AsyncResult<void, never>;
  /**
   * Bumped on every channel `'connect'` (initial connect included). Delivery
   * tags are per-channel, so a settle stamped with an older epoch targets a
   * channel that no longer exists — see {@link ack} / {@link nack}.
   */
  private channelEpoch = 0;

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

    // Resolved here, at the single point where every caller's channel is
    // created, so client, worker, and direct core users all inherit it.
    // Precedence (most specific wins):
    //   1. `publishTimeoutMs` explicitly set (number or `null`) — the
    //      library-owned option is always authoritative.
    //   2. otherwise `channelOptions.publishTimeout`, if the caller set one
    //      via the passthrough bag — respected, not silently overwritten.
    //   3. otherwise {@link DEFAULT_PUBLISH_TIMEOUT_MS}.
    // `null` means "no timeout at all" and wins even over an explicit
    // `channelOptions.publishTimeout` — it is the more specific expression of
    // intent (see connectTimeoutMs for the same null-means-disabled convention).
    if (options.publishTimeoutMs === null) {
      delete channelOpts.publishTimeout;
    } else if (options.publishTimeoutMs !== undefined) {
      channelOpts.publishTimeout = options.publishTimeoutMs;
    } else if (channelOpts.publishTimeout === undefined) {
      channelOpts.publishTimeout = DEFAULT_PUBLISH_TIMEOUT_MS;
    }

    this.channelWrapper = this.connection.createChannel(channelOpts);

    // amqp-connection-manager's ChannelWrapper emits plain 'error' events for
    // conditions it recovers from by reconnecting (topology setup failure on
    // connect/reconnect, publish-worker faults, consumer re-establishment
    // failures). A Node 'error' emit with zero listeners throws
    // ERR_UNHANDLED_ERROR — inside the manager's async connect listener that
    // becomes an unhandled rejection and crashes the process. This listener
    // must therefore always exist: it degrades the event to a log line while
    // still letting callers attach their own observers via `on('error', …)`.
    this.channelWrapper.on("connect", () => {
      this.channelEpoch += 1;
    });

    const logger = options.logger;
    this.logger = logger;
    this.channelWrapper.on("error", (error: unknown, info?: { name?: string }) => {
      logger?.error("AMQP channel error", {
        error: error instanceof Error ? error.message : String(error),
        cause: error,
        channel: info?.name,
      });
    });
  }

  /**
   * The underlying connection manager.
   *
   * The connection is shared across every `AmqpClient` created with the same
   * URLs and connection options, so two clients that pool together return the
   * identical instance — which is how the connection-sharing suite asserts
   * pooling.
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

    // `AbortSignal.timeout` rather than a hand-rolled `setTimeout` race: its
    // timer does not hold the event loop open, so a settled connect needs no
    // paired `clearTimeout` to avoid keeping the process alive.
    const racedPromise =
      timeoutMs === null
        ? connectPromise
        : Promise.race([
            connectPromise,
            new Promise<never>((_resolve, reject) => {
              AbortSignal.timeout(timeoutMs).addEventListener("abort", () => {
                reject(new Error(`Timed out waiting for AMQP connection after ${timeoutMs}ms`));
              });
            }),
          ]);

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
   * A full channel write buffer (the wrapper's boolean `false` confirmation)
   * surfaces as a Defect with a {@link TechnicalError} cause — like every
   * other publish-side infrastructure failure. Callers never see the boolean.
   *
   * @param target - The exchange and routing key to publish to
   * @param content - The message payload
   * @param options - AMQP publish options
   */
  publish(
    target: { exchange: string; routingKey: string },
    content: Buffer | unknown,
    options?: AmqpPublishOptions,
  ): AsyncResult<void, never> {
    const { exchange, routingKey } = target;
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
      )
      .flatMap((published) =>
        absorbWriteBufferConfirmation(
          published,
          `exchange "${exchange}" (routing key "${routingKey}")`,
        ),
      );
  }

  /**
   * Publish a message directly to a queue.
   *
   * Non-Buffer content is JSON-encoded; Buffers are published byte-for-byte.
   *
   * A full channel write buffer surfaces as a Defect with a
   * {@link TechnicalError} cause — see {@link publish}.
   */
  sendToQueue(
    queue: string,
    content: Buffer | unknown,
    options?: AmqpPublishOptions,
  ): AsyncResult<void, never> {
    return fromSafeThrowable(() => AmqpClient.encodeContent(content))()
      .toAsync()
      .flatMap((encoded) =>
        fromPromise(
          this.channelWrapper.sendToQueue(queue, encoded, options),
          (error: unknown, defect) =>
            defect(new TechnicalError(`Failed to publish message to queue "${queue}"`, error)),
        ),
      )
      .flatMap((published) => absorbWriteBufferConfirmation(published, `queue "${queue}"`));
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
    options?: AmqpConsumeOptions,
  ): AsyncResult<string, never> {
    // Validate the prefetch value before forwarding to RabbitMQ. AMQP
    // basic.qos prefetch-count is an unsigned 16-bit short (0–65535); 0
    // means unlimited. NaN, negatives, fractions, and out-of-range numbers
    // would travel to the broker, which either rejects or interprets them
    // unexpectedly.
    //
    // Resolve before validating so the range check sees the real value that
    // will reach the broker. `"unbounded"` maps to AMQP's 0 (unlimited).
    const requested = options?.prefetch;
    const prefetch =
      requested === undefined ? DEFAULT_PREFETCH : requested === "unbounded" ? 0 : requested;

    if (!Number.isInteger(prefetch) || prefetch < 0 || prefetch > 65_535) {
      // A misconfigured prefetch is a programming fault, not a modeled
      // failure — surface it through the defect channel.
      return fromSafeThrowable((): string => {
        // oxlint-disable-next-line unthrown/no-throw -- deliberate defect-channel routing inside the fromSafeThrowable thunk
        throw new TechnicalError(
          `Invalid prefetch: expected a non-negative integer ≤ 65535 or "unbounded", got ${String(requested)}`,
        );
      })().toAsync();
    }

    return fromPromise(
      this.channelWrapper.consume(queue, callback, { ...options, prefetch }),
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
   * The current channel epoch — bumped on every channel `'connect'` (initial
   * connect included). Consumers stamp deliveries with this value and pass it
   * back via {@link ack} / {@link nack} so a settle can never target a tag
   * from a previous channel incarnation.
   */
  get currentChannelEpoch(): number {
    return this.channelEpoch;
  }

  /**
   * Refuse a settle whose delivery predates the current channel: delivery tags
   * are per-channel, so forwarding it would either close the channel with 406
   * PRECONDITION_FAILED or — worse — settle an unrelated in-flight delivery on
   * the new channel. Skipping is safe: the broker already redelivered the
   * message when the old channel died (at-least-once holds).
   */
  private isStaleDelivery(
    operation: "ack" | "nack",
    msg: ConsumeMessage,
    deliveryEpoch: number | undefined,
  ): boolean {
    if (deliveryEpoch === undefined || deliveryEpoch === this.channelEpoch) {
      return false;
    }
    this.logger?.warn(`Skipping ${operation} for a delivery from a previous channel incarnation`, {
      deliveryTag: msg.fields.deliveryTag,
      deliveryEpoch,
      currentEpoch: this.channelEpoch,
    });
    return true;
  }

  /**
   * Acknowledge a message.
   *
   * @param msg - The message to acknowledge
   * @param options - Settle options:
   *   - `allUpTo` — if true, acknowledge all messages up to and including
   *     this one (defaults to false).
   *   - `deliveryEpoch` — pass the epoch captured when the message was
   *     delivered ({@link currentChannelEpoch}) to make the ack
   *     reconnect-safe: a stale epoch skips the ack (logged) instead of
   *     settling a foreign tag.
   */
  ack(
    msg: ConsumeMessage,
    options?: { allUpTo?: boolean | undefined; deliveryEpoch?: number | undefined },
  ): void {
    if (this.isStaleDelivery("ack", msg, options?.deliveryEpoch)) {
      return;
    }
    this.channelWrapper.ack(msg, options?.allUpTo ?? false);
  }

  /**
   * Negative acknowledge a message.
   *
   * @param msg - The message to nack
   * @param options - Settle options:
   *   - `allUpTo` — if true, nack all messages up to and including this one
   *     (defaults to false).
   *   - `requeue` — if true, requeue the message(s) (defaults to true).
   *   - `deliveryEpoch` — pass the epoch captured at delivery time to make
   *     the nack reconnect-safe (see {@link ack}).
   */
  nack(
    msg: ConsumeMessage,
    options?: {
      allUpTo?: boolean | undefined;
      requeue?: boolean | undefined;
      deliveryEpoch?: number | undefined;
    },
  ): void {
    if (this.isStaleDelivery("nack", msg, options?.deliveryEpoch)) {
      return;
    }
    this.channelWrapper.nack(msg, options?.allUpTo ?? false, options?.requeue ?? true);
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
}
