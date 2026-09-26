import {
  type ContractDefinition,
  deriveTtlBackoffInfrastructure,
  type QueueDefinition,
} from "@amqp-contract/contract";
import type {
  AmqpConsumeOptions,
  AmqpPublishOptions,
  AmqpTransport,
  ConnectionError,
  ConsumeCallback,
} from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { type AsyncResult, OkAsync } from "unthrown";

/**
 * The `replyTo` value RabbitMQ treats as direct reply-to. A publisher naming
 * it is asking for a per-channel pseudo-queue rather than a declared one.
 */
const DIRECT_REPLY_TO = "amq.rabbitmq.reply-to";

type Binding = {
  readonly queue: string;
  readonly routingKey: string;
  readonly arguments?: Record<string, unknown> | undefined;
};

type Exchange = {
  readonly name: string;
  readonly type: "topic" | "direct" | "fanout" | "headers";
  readonly bindings: Binding[];
};

type Queue = {
  readonly name: string;
  readonly arguments: Record<string, unknown>;
  /** Ready messages, oldest first. */
  readonly ready: Delivery[];
  /** Consumers, in the order they subscribed — deliveries round-robin. */
  readonly consumers: Consumer[];
};

type Consumer = {
  readonly tag: string;
  readonly callback: ConsumeCallback;
  readonly noAck: boolean;
  /** The transport that opened it, so a reply can find its way home. */
  readonly owner: InMemoryTransport;
};

type Delivery = {
  readonly content: Buffer;
  readonly properties: ConsumeMessage["properties"];
  readonly routingKey: string;
  readonly exchange: string;
  redelivered: boolean;
  deliveryCount: number;
};

/** Deliveries are handed out asynchronously, as a broker's would be. */
const soon = (run: () => void): void => {
  queueMicrotask(run);
};

/**
 * Topic matching: `*` is exactly one word, `#` is zero or more. Built as a
 * regular expression over the dot-separated key, which is what RabbitMQ's
 * trie computes the same answer for.
 */
const topicMatches = (pattern: string, routingKey: string): boolean => {
  const source = pattern
    .split(".")
    .map((word) => (word === "*" ? "[^.]+" : word === "#" ? ".*" : escapeWord(word)))
    .join("\\.")
    // `#` spans separators, so a `#` segment must be able to eat the dot
    // beside it — `a.#` matches `a`, not just `a.something`.
    .replace(/\\\.\.\*/g, "(?:\\..*)?")
    .replace(/^\.\*\\\./, "(?:.*\\.)?");
  return new RegExp(`^${source}$`).test(routingKey);
};

const escapeWord = (word: string): string => word.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * Headers matching, per AMQP's `x-match`: `all` requires every declared
 * header, `any` requires one. Keys beginning `x-` are the matcher's own
 * configuration and never participate.
 */
const headersMatch = (
  binding: Record<string, unknown>,
  message: Record<string, unknown>,
): boolean => {
  const mode = binding["x-match"] === "any" ? "any" : "all";
  const required = Object.entries(binding).filter(([key]) => !key.startsWith("x-"));
  if (required.length === 0) return mode === "all";
  const matched = required.filter(([key, value]) => message[key] === value);
  return mode === "any" ? matched.length > 0 : matched.length === required.length;
};

/**
 * An in-memory AMQP broker: enough of one to run a contract end to end
 * without Docker.
 *
 * It is shared state plus a routing table, and every transport it hands out
 * talks to the same state — so a client and a worker built from one broker
 * see each other exactly as they would across a real connection. What runs
 * for real is everything above the wire: serialization, both validation
 * passes, interceptors and middleware, RPC correlation, retry routing and
 * dead-lettering.
 *
 * What it is NOT is a RabbitMQ. It models the routing and settlement rules
 * the contract pipeline depends on; it models no cluster, no flow control,
 * no channel errors, and no persistence. Topology *refusals* — the 406 a
 * real broker answers when a queue is redeclared with different arguments —
 * are a broker behaviour and stay the integration suite's job.
 *
 * @example
 * ```ts
 * const broker = new InMemoryAmqpBroker();
 * const worker = await TypedAmqpWorker.create({
 *   contract,
 *   handlers,
 *   transport: broker.createTransport(contract),
 * });
 * const client = await TypedAmqpClient.create({
 *   contract,
 *   transport: broker.createTransport(contract),
 * });
 * ```
 */
export class InMemoryAmqpBroker {
  private readonly exchanges = new Map<string, Exchange>();
  private readonly queues = new Map<string, Queue>();
  /** Direct reply-to pseudo-queues, one per transport that consumes one. */
  private readonly replyQueues = new Map<string, InMemoryTransport>();
  private nextTag = 0;

  /**
   * Register a contract's topology and hand back a transport bound to it.
   *
   * Declaring is idempotent and additive, exactly as `assertExchange` /
   * `assertQueue` are: two transports built from the same contract converge
   * on one set of exchanges, queues and bindings rather than two.
   */
  createTransport(contract: ContractDefinition): AmqpTransport {
    this.declare(contract);
    return new InMemoryTransport(this);
  }

  /** Every queue currently declared, for a spec that wants to look inside. */
  queueNames(): readonly string[] {
    return [...this.queues.keys()].sort();
  }

  /** The messages sitting unconsumed on a queue — a DLQ, most usefully. */
  peek(queue: string): readonly ConsumeMessage[] {
    return (this.queues.get(queue)?.ready ?? []).map((delivery, index) =>
      this.toConsumeMessage(delivery, index + 1),
    );
  }

  /** Register exchanges, queues, TTL-backoff wait queues and bindings. */
  private declare(contract: ContractDefinition): void {
    for (const exchange of Object.values(contract.exchanges ?? {})) {
      // The default exchange is implicit and never declared, here or on a
      // real broker; `route` handles it directly.
      if (exchange.name === "") continue;
      if (!this.exchanges.has(exchange.name)) {
        this.exchanges.set(exchange.name, {
          name: exchange.name,
          type: exchange.type,
          bindings: [],
        });
      }
    }

    for (const queue of Object.values(contract.queues ?? {})) {
      this.declareQueue(queue.name, queueArgumentsOf(queue));
      for (const wait of deriveTtlBackoffInfrastructure(queue)?.waitQueues ?? []) {
        this.declareQueue(wait.name, {
          "x-message-ttl": wait.messageTtlMs,
          "x-dead-letter-exchange": "",
          "x-dead-letter-routing-key": queue.name,
        });
      }
    }

    for (const binding of Object.values(contract.bindings ?? {})) {
      // Exchange-to-exchange bindings are declarable but carry no traffic
      // this fake routes: every publish here names a leaf exchange or the
      // default one. A spec that needs the hop is a broker-behaviour test.
      if (binding.type !== "queue") continue;
      const exchange = this.exchanges.get(binding.exchange.name);
      if (!exchange) continue;
      const routingKey = binding.routingKey ?? "";
      const already = exchange.bindings.some(
        (b) => b.queue === binding.queue.name && b.routingKey === routingKey,
      );
      if (!already) {
        exchange.bindings.push({
          queue: binding.queue.name,
          routingKey,
          arguments: binding.arguments,
        });
      }
    }
  }

  private declareQueue(name: string, args: Record<string, unknown>): void {
    if (!this.queues.has(name)) {
      this.queues.set(name, { name, arguments: args, ready: [], consumers: [] });
    }
  }

  /**
   * Route one publish to every matching queue.
   *
   * An unroutable publish is **dropped and confirmed**, which is AMQP's own
   * behaviour without a mandatory flag or an alternate exchange — and the
   * hazard the contract's define-time guard exists to catch, so the fake must
   * not be kinder than the broker.
   */
  publish(
    from: InMemoryTransport,
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: AmqpPublishOptions | undefined,
  ): void {
    const properties = {
      ...options,
      headers: { ...(options?.headers as Record<string, unknown> | undefined) },
    } as ConsumeMessage["properties"];

    // Direct reply-to: the publisher's `replyTo` names a pseudo-queue rather
    // than a declared one, and a real broker rewrites it per channel. Doing
    // the same here is what lets a reply find the transport that asked.
    if (properties.replyTo === DIRECT_REPLY_TO) {
      properties.replyTo = from.replyQueueName;
      this.replyQueues.set(from.replyQueueName, from);
    }

    for (const queue of this.match(exchange, routingKey, properties, content)) {
      this.enqueue(queue, {
        content,
        properties,
        routingKey,
        exchange,
        redelivered: false,
        deliveryCount: 0,
      });
    }
  }

  /** Which queues a message reaches, by exchange type. */
  private match(
    exchange: string,
    routingKey: string,
    properties: ConsumeMessage["properties"],
    content: Buffer = Buffer.alloc(0),
  ): Queue[] {
    // The default exchange routes by queue name, and is what every RPC
    // request and every retry republish travels through.
    if (exchange === "") {
      const direct = this.queues.get(routingKey);
      if (direct) return [direct];
      // A direct reply-to pseudo-queue is not a queue: it has no backlog and
      // no settlement, and a reply arriving after its consumer is gone is
      // dropped rather than parked — which is the late-reply case the client
      // records a metric for.
      this.replyQueues.get(routingKey)?.deliverReply(content, properties, routingKey);
      return [];
    }

    const declared = this.exchanges.get(exchange);
    if (!declared) return [];

    const headers = (properties.headers ?? {}) as Record<string, unknown>;
    const names = declared.bindings
      .filter((binding) => {
        switch (declared.type) {
          case "fanout":
            return true;
          case "direct":
            return binding.routingKey === routingKey;
          case "topic":
            return topicMatches(binding.routingKey, routingKey);
          case "headers":
            return headersMatch(binding.arguments ?? {}, headers);
        }
      })
      .map((binding) => binding.queue);

    // A queue bound twice to one exchange receives one copy, as on a broker.
    return [...new Set(names)].flatMap((name) => {
      const queue = this.queues.get(name);
      return queue ? [queue] : [];
    });
  }

  /**
   * Put a message on a queue, honouring its TTL, then try to deliver.
   *
   * The effective TTL is the smaller of the per-message `expiration` and the
   * queue's `x-message-ttl` — which is what makes TTL-backoff retry work:
   * the republish carries `expiration`, the wait queue carries the ceiling,
   * and whichever is shorter decides when it dead-letters back.
   */
  private enqueue(queue: Queue, delivery: Delivery): void {
    const ttl = effectiveTtl(queue, delivery);
    if (ttl !== undefined) {
      const timer = setTimeout(() => {
        const index = queue.ready.indexOf(delivery);
        if (index === -1) return;
        queue.ready.splice(index, 1);
        this.deadLetter(queue, delivery);
      }, ttl);
      // A pending TTL must never hold a test process open.
      timer.unref?.();
    }
    queue.ready.push(delivery);
    soon(() => {
      this.drain(queue);
    });
  }

  /** Hand ready messages to consumers, round-robin. */
  private drain(queue: Queue): void {
    while (queue.ready.length > 0 && queue.consumers.length > 0) {
      const delivery = queue.ready.shift();
      if (!delivery) return;
      const consumer = queue.consumers[this.nextTag++ % queue.consumers.length];
      if (!consumer) return;
      const message = this.toConsumeMessage(delivery, this.nextTag);
      consumer.owner.track(message, queue, delivery, consumer.noAck);
      void consumer.callback(message);
    }
  }

  /**
   * Route a message to the queue's dead-letter exchange, or drop it.
   *
   * A DLX naming an exchange nothing is bound to loses the message, exactly
   * as a real broker does — the hazard `dlx-routability` exists to prove, and
   * one this fake must reproduce rather than paper over.
   */
  private deadLetter(queue: Queue, delivery: Delivery): void {
    const exchange = queue.arguments["x-dead-letter-exchange"];
    if (typeof exchange !== "string") return;
    const routingKey =
      typeof queue.arguments["x-dead-letter-routing-key"] === "string"
        ? (queue.arguments["x-dead-letter-routing-key"] as string)
        : delivery.routingKey;
    for (const target of this.match(exchange, routingKey, delivery.properties, delivery.content)) {
      this.enqueue(target, { ...delivery, redelivered: false });
    }
  }

  /** Settle a delivery: ack drops it, nack requeues or dead-letters. */
  settle(queue: Queue, delivery: Delivery, requeue: boolean | undefined): void {
    if (requeue !== true) {
      this.deadLetter(queue, delivery);
      return;
    }
    // Quorum semantics: a redelivery is marked, and its delivery count is
    // what the worker's immediate-requeue retry budget counts.
    delivery.redelivered = true;
    delivery.deliveryCount += 1;
    delivery.properties.headers = {
      ...delivery.properties.headers,
      "x-delivery-count": delivery.deliveryCount,
    };
    queue.ready.unshift(delivery);
    soon(() => {
      this.drain(queue);
    });
  }

  register(queue: string, consumer: Consumer): string | undefined {
    const declared = this.queues.get(queue);
    if (!declared) return undefined;
    declared.consumers.push(consumer);
    soon(() => {
      this.drain(declared);
    });
    return consumer.tag;
  }

  unregister(tag: string): void {
    for (const queue of this.queues.values()) {
      const index = queue.consumers.findIndex((consumer) => consumer.tag === tag);
      if (index !== -1) queue.consumers.splice(index, 1);
    }
  }

  mintTag(): string {
    this.nextTag += 1;
    return `in-memory-ctag-${this.nextTag}`;
  }

  private toConsumeMessage(delivery: Delivery, deliveryTag: number): ConsumeMessage {
    return {
      content: delivery.content,
      fields: {
        consumerTag: "",
        deliveryTag,
        redelivered: delivery.redelivered,
        exchange: delivery.exchange,
        routingKey: delivery.routingKey,
      },
      properties: delivery.properties,
    } as ConsumeMessage;
  }
}

/** The queue arguments `setupAmqpTopology` would have asserted. */
const queueArgumentsOf = (queue: QueueDefinition): Record<string, unknown> => ({
  ...queue.arguments,
  "x-queue-type": queue.type,
  ...(queue.deadLetter && {
    "x-dead-letter-exchange": queue.deadLetter.exchange.name,
    ...(queue.deadLetter.routingKey !== undefined && {
      "x-dead-letter-routing-key": queue.deadLetter.routingKey,
    }),
  }),
});

/** The smaller of the per-message `expiration` and the queue's ceiling. */
const effectiveTtl = (queue: Queue, delivery: Delivery): number | undefined => {
  const perMessage = Number(delivery.properties.expiration);
  const perQueue = Number(queue.arguments["x-message-ttl"]);
  const candidates = [perMessage, perQueue].filter((value) => Number.isFinite(value) && value >= 0);
  return candidates.length === 0 ? undefined : Math.min(...candidates);
};

/**
 * One "connection" onto an {@link InMemoryAmqpBroker}, satisfying
 * `AmqpTransport`.
 *
 * A transport per facade, as a real deployment has a connection per facade:
 * that is what gives direct reply-to somewhere to route back to, and what
 * lets `close()` retire one side without disturbing the other.
 */
class InMemoryTransport implements AmqpTransport {
  /** The pseudo-queue a direct reply-to publish is rewritten to. */
  readonly replyQueueName = `amq.rabbitmq.reply-to.${Math.random().toString(36).slice(2)}`;
  private readonly outstanding = new Map<number, { queue: Queue; delivery: Delivery }>();
  private replyConsumer: ConsumeCallback | undefined;
  private closed = false;

  constructor(private readonly broker: InMemoryAmqpBroker) {}

  /**
   * Always zero: nothing here reconnects, so no delivery is ever stale. The
   * epoch guard in `ack`/`nack` therefore never fires, which is correct — a
   * reconnect is exactly the broker behaviour this fake does not model, and
   * pretending otherwise would make the guard untestable in both directions.
   */
  readonly currentChannelEpoch = 0;

  waitForConnect(): AsyncResult<void, ConnectionError> {
    return OkAsync();
  }

  publish(
    target: { exchange: string; routingKey: string },
    content: Buffer | unknown,
    options?: AmqpPublishOptions,
  ): AsyncResult<void, never> {
    // Byte-for-byte what `AmqpClient.encodeContent` does, so a compressed
    // payload survives the round trip unchanged and the decompression path
    // runs for real.
    const encoded = Buffer.isBuffer(content) ? content : Buffer.from(JSON.stringify(content));
    this.broker.publish(this, target.exchange, target.routingKey, encoded, options);
    return OkAsync();
  }

  consume(
    queue: string,
    callback: ConsumeCallback,
    options?: AmqpConsumeOptions,
  ): AsyncResult<string, never> {
    if (queue === DIRECT_REPLY_TO) {
      this.replyConsumer = callback;
      return OkAsync(this.broker.mintTag());
    }
    const tag = this.broker.mintTag();
    const registered = this.broker.register(queue, {
      tag,
      callback,
      noAck: options?.noAck === true,
      owner: this,
    });
    // Consuming a queue the contract never declared is a broker error on the
    // real thing; here it is a tag that receives nothing, which is the
    // closest honest analogue without inventing a channel-error path.
    return OkAsync(registered ?? tag);
  }

  cancel(consumerTag: string): AsyncResult<void, never> {
    this.broker.unregister(consumerTag);
    return OkAsync();
  }

  ack(msg: Parameters<AmqpTransport["ack"]>[0]) {
    this.outstanding.delete(msg.fields.deliveryTag);
  }

  nack(msg: Parameters<AmqpTransport["nack"]>[0], options?: { requeue?: boolean | undefined }) {
    const held = this.outstanding.get(msg.fields.deliveryTag);
    if (!held) return;
    this.outstanding.delete(msg.fields.deliveryTag);
    this.broker.settle(held.queue, held.delivery, options?.requeue);
  }

  close(): AsyncResult<void, never> {
    this.closed = true;
    this.replyConsumer = undefined;
    return OkAsync();
  }

  /** Remember an unsettled delivery so `nack` can put it back. */
  track(message: ConsumeMessage, queue: Queue, delivery: Delivery, noAck: boolean): void {
    if (noAck) return;
    this.outstanding.set(message.fields.deliveryTag, { queue, delivery });
  }

  /** A direct reply-to message coming home to this transport's consumer. */
  deliverReply(
    content: Buffer,
    properties: ConsumeMessage["properties"],
    routingKey: string,
  ): void {
    const consumer = this.replyConsumer;
    if (this.closed || !consumer) return;
    const message = {
      content,
      fields: {
        consumerTag: "",
        deliveryTag: 0,
        redelivered: false,
        exchange: "",
        routingKey,
      },
      properties,
    } as ConsumeMessage;
    soon(() => {
      void consumer(message);
    });
  }
}
