import type { ContractDefinition } from "@amqp-contract/contract";
import type { AsyncResult } from "unthrown";

import {
  AmqpClient,
  type AmqpClientOptions,
  type AmqpConsumeOptions,
  type AmqpPublishOptions,
  type ConsumeCallback,
} from "./amqp-client.js";
import type { ConnectionError } from "./errors.js";
import { TechnicalError } from "./errors.js";

/**
 * The transport surface `TypedAmqpClient` and `TypedAmqpWorker` actually use.
 *
 * {@link AmqpClient} is the one that speaks to a broker, and this is the
 * subset of it the typed facades depend on — eight members out of its full
 * surface. Naming that subset is what lets a test substitute an in-memory
 * broker for a real one without either facade knowing.
 *
 * It is deliberately **structural and small**. `sendToQueue`, `addSetup`,
 * `on` and `getConnection` are absent because no facade calls them; an
 * implementation is free to have them, and a future facade that reaches for
 * one has to widen this type first, which is the point.
 *
 * The compile-time assertion below is what keeps it honest: `AmqpClient` must
 * satisfy it, so a signature change there is a type error here rather than a
 * substitution that silently stops matching.
 */
export type AmqpTransport = {
  /**
   * Resolve once the transport is ready to carry messages. The one member
   * with a modeled error: a broker that cannot be dialled is an anticipated
   * outcome, where every other failure here is infrastructure and rides the
   * defect channel.
   */
  waitForConnect(): AsyncResult<void, ConnectionError>;
  publish(
    target: { exchange: string; routingKey: string },
    content: Buffer | unknown,
    options?: AmqpPublishOptions,
  ): AsyncResult<void, never>;
  consume(
    queue: string,
    callback: ConsumeCallback,
    options?: AmqpConsumeOptions,
  ): AsyncResult<string, never>;
  cancel(consumerTag: string): AsyncResult<void, never>;
  ack(msg: ConsumeMessageLike, options?: AckOptions): void;
  nack(msg: ConsumeMessageLike, options?: NackOptions): void;
  close(): AsyncResult<void, never>;
  /**
   * Bumped whenever the underlying channel is re-established. A delivery
   * carries the epoch it arrived on, and `ack`/`nack` drop a delivery whose
   * epoch has moved — the broker has already requeued it, so acknowledging
   * would settle a different message.
   */
  readonly currentChannelEpoch: number;
};

/** What `ack` / `nack` accept — amqplib's `ConsumeMessage`, structurally. */
type ConsumeMessageLike = Parameters<AmqpClient["ack"]>[0];
type AckOptions = NonNullable<Parameters<AmqpClient["ack"]>[1]>;
type NackOptions = NonNullable<Parameters<AmqpClient["nack"]>[1]>;

// The seam is only worth having if the real client still fits through it.
// A signature change in `AmqpClient` fails here rather than silently
// diverging from every substitute.
type AssertAmqpClientIsATransport = AmqpClient extends AmqpTransport ? true : never;
const _assertAmqpClientIsATransport: AssertAmqpClientIsATransport = true;
void _assertAmqpClientIsATransport;

/** What a facade was handed as its connection source. */
export type TransportSource = Omit<AmqpClientOptions, "urls"> & {
  urls?: AmqpClientOptions["urls"] | undefined;
  transport?: AmqpTransport | undefined;
};

/**
 * Pick the transport a facade will run on: the supplied one, or a real
 * {@link AmqpClient} dialled from `urls`.
 *
 * **Exactly one source**, and neither degenerate case is allowed to pass
 * quietly. With neither there is nothing to dial and every later call would
 * fail one at a time; with both, preferring one silently would mean a test
 * that passes a transport AND inherits a `urls` default connects to a real
 * broker while believing it did not — the failure this option exists to
 * prevent, arriving in disguise.
 *
 * @throws TechnicalError when the sources are not exactly one. Both facades
 * call this inside their `OkAsync(...).flatMap(...)` safety net, so it
 * surfaces as a Defect from `create()` rather than as a raw throw.
 */
export const resolveTransport = (
  contract: ContractDefinition,
  { urls, transport, ...options }: TransportSource,
): AmqpTransport => {
  if (transport !== undefined && urls !== undefined) {
    // oxlint-disable-next-line unthrown/no-throw -- a synchronous misuse at the create() boundary; both facades adopt it as a Defect through their OkAsync safety net (documented @throws)
    throw new TechnicalError(
      "Both `urls` and `transport` were supplied. Pass exactly one connection source: `urls` to dial a broker, or `transport` to run on a supplied one.",
    );
  }
  if (transport !== undefined) return transport;
  if (urls === undefined) {
    // oxlint-disable-next-line unthrown/no-throw -- same boundary, same adoption
    throw new TechnicalError(
      "No connection source. Pass `urls` to dial a broker, or `transport` to run on a supplied one.",
    );
  }
  return new AmqpClient(contract, { ...options, urls });
};
