import { AsyncLocalStorage } from "node:async_hooks";
import type { EventEmitter } from "node:events";

import {
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import {
  _internal_resetConnections,
  _internal_resetTelemetryCache,
} from "@amqp-contract/core/internal";
import {
  type Context,
  type ContextManager,
  context,
  ROOT_CONTEXT,
  type Span,
  trace,
  TraceFlags,
} from "@opentelemetry/api";
import type { AmqpConnectionManager } from "amqp-connection-manager";
import type { ConsumeMessage } from "amqplib";
import { ErrAsync, OkAsync } from "unthrown";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { RetryableError } from "./errors.js";
import { TypedAmqpWorker } from "./worker.js";

/**
 * The dispatch path through the worker's real consume callback: every
 * delivery ends in one modeled outcome, settled exactly once, and telemetry
 * reads the same outcome. Invariant 1 lives here too: the original delivery
 * is settled according to what the retry publish actually did on the broker.
 */

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
  addSetup: ReturnType<typeof vi.fn>;
  consume: ReturnType<typeof vi.fn>;
  cancel: ReturnType<typeof vi.fn>;
  publish: ReturnType<typeof vi.fn>;
  ack: ReturnType<typeof vi.fn>;
  nack: ReturnType<typeof vi.fn>;
};
const fakes = vi.hoisted(() => ({ wrapper: undefined as unknown }));
const wrapper = (): FakeWrapper => fakes.wrapper as FakeWrapper;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.setMaxListeners(50);
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
  w.addSetup = vi.fn(() => Promise.resolve());
  w.consume = vi.fn(() => Promise.resolve({ consumerTag: "tag-1" }));
  w.cancel = vi.fn(() => Promise.resolve());
  w.publish = vi.fn();
  w.ack = vi.fn();
  w.nack = vi.fn();
  fakes.wrapper = w;
  return {
    default: {
      connect: vi.fn(() => ({
        createChannel: vi.fn(() => w),
        on: vi.fn(),
        removeListener: vi.fn(),
        close: vi.fn(() => Promise.resolve()),
      })),
    },
  };
});

const dlx = defineExchange("orders-dlx");
const dlq = defineQueue("orders-dlq");
const queue = defineQueue("orders", {
  deadLetter: { exchange: dlx },
  retry: { mode: "ttl-backoff", maxRetries: 3 },
});
const contract = defineContract({
  consumers: { processOrder: defineConsumer(queue, defineMessage(z.object({ id: z.string() }))) },
  queues: { dlq },
  bindings: { dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "#" }) },
});

function delivery(): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify({ id: "1" })),
    fields: {
      consumerTag: "tag-1",
      deliveryTag: 1,
      redelivered: false,
      exchange: "orders",
      routingKey: "orders",
    },
    properties: { contentType: "application/json", headers: {} },
  } as unknown as ConsumeMessage;
}

type Handler = Parameters<
  typeof TypedAmqpWorker.create<typeof contract>
>[0]["handlers"]["processOrder"];

function spies() {
  return {
    logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    counter: { add: vi.fn() },
  };
}

async function deliver(
  handler: Handler = () => ErrAsync(new RetryableError("transient")),
  message: ConsumeMessage = delivery(),
  { logger, counter }: ReturnType<typeof spies> = spies(),
): Promise<void> {
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers: { processOrder: handler },
    urls: ["amqp://localhost"],
    logger,
    telemetry: {
      getTracer: () => undefined,
      getPublishCounter: () => undefined,
      getConsumeCounter: () => counter as never,
      getPublishLatencyHistogram: () => undefined,
      getConsumeLatencyHistogram: () => undefined,
      getLateRpcReplyCounter: () => undefined,
    },
  }).getOrThrow();
  const consumeCallback = wrapper().consume.mock.calls[0]?.[1] as (
    msg: ConsumeMessage | null,
  ) => Promise<void>;
  await consumeCallback(message);
  await worker.close().get();
}

const deliverFailingMessage = () => deliver();

/** [acks, nacks] — and the nack arguments, when any. */
const settles = () => [
  wrapper().ack.mock.calls.length,
  wrapper().nack.mock.calls.map((c) => c.slice(1)),
];
const consumeSuccess = (counter: { add: ReturnType<typeof vi.fn> }) =>
  counter.add.mock.calls.map((call) => (call[1] as { success: boolean }).success);

describe("dispatch outcomes", () => {
  beforeEach(async () => {
    wrapper().publish.mockReset();
    wrapper().consume.mockClear();
    wrapper().ack.mockClear();
    wrapper().nack.mockClear();
    await _internal_resetConnections();
  });

  it("INVARIANT: acks the original after a confirmed retry publish even when the write buffer is full", async () => {
    // amqp-connection-manager resolves a confirm-channel publish with `false`
    // only AFTER the broker's ack: the retry copy is on the broker.
    wrapper().publish.mockResolvedValue(false);

    await deliverFailingMessage();

    expect(wrapper().publish).toHaveBeenCalledTimes(1);
    expect(wrapper().ack).toHaveBeenCalledTimes(1);
    expect(wrapper().nack).not.toHaveBeenCalled();
  });

  it.for(["timeout", "message nacked", "Channel closed"])(
    "INVARIANT: a retry publish that fails with '%s' requeues the original (nack requeue=true), never dead-letters it",
    async (rejection) => {
      wrapper().publish.mockRejectedValue(new Error(rejection));

      await deliverFailingMessage();

      expect(wrapper().publish).toHaveBeenCalledTimes(1);
      expect(wrapper().nack).toHaveBeenCalledTimes(1);
      expect(wrapper().nack).toHaveBeenCalledWith(expect.anything(), false, true);
      expect(wrapper().ack).not.toHaveBeenCalled();
    },
  );

  it("INVARIANT: the original is acked only AFTER the retry copy's publish is confirmed", async () => {
    const order: string[] = [];
    wrapper().publish.mockImplementation(() => {
      order.push("publish");
      return Promise.resolve(true);
    });
    wrapper().ack.mockImplementationOnce(() => order.push("ack"));

    await deliverFailingMessage();

    expect(order).toEqual(["publish", "ack"]);
  });

  it("a handler success acks once and records a successful consume", async () => {
    const probes = spies();

    await deliver(() => OkAsync(undefined), delivery(), probes);

    expect([settles(), consumeSuccess(probes.counter)]).toEqual([[1, []], [true]]);
  });

  it("a routed handler failure is a modeled outcome: failure telemetry, no defect", async () => {
    wrapper().publish.mockResolvedValue(true);
    const probes = spies();

    await deliver(undefined, delivery(), probes);

    expect([settles(), consumeSuccess(probes.counter)]).toEqual([[1, []], [false]]);
    expect(probes.logger.error).not.toHaveBeenCalledWith(
      "Message processing failed with a defect; nacking message",
      expect.anything(),
    );
  });

  it("an invalid payload is a modeled MessageValidationError dead-letter, not a defect", async () => {
    const probes = spies();
    const handler = vi.fn(() => OkAsync(undefined));
    const invalid = { ...delivery(), content: Buffer.from(JSON.stringify({ id: 42 })) };

    await deliver(handler, invalid as ConsumeMessage, probes);

    expect([settles(), consumeSuccess(probes.counter), handler.mock.calls.length]).toEqual([
      [0, [[false, false]]],
      [false],
      0,
    ]);
    expect(probes.logger.error).toHaveBeenCalledWith(
      "Failed to parse/validate message; sending to DLQ",
      expect.objectContaining({
        error: expect.objectContaining({ name: "MessageValidationError" }),
      }),
    );
    expect(probes.logger.error).not.toHaveBeenCalledWith(
      "Message processing failed with a defect; nacking message",
      expect.anything(),
    );
  });

  it("a genuine bug (a handler that throws) is a defect, dead-lettered exactly once", async () => {
    const probes = spies();

    await deliver(
      () => {
        throw new TypeError("bug");
      },
      delivery(),
      probes,
    );

    expect([settles(), consumeSuccess(probes.counter)]).toEqual([[0, [[false, false]]], [false]]);
    expect(probes.logger.error).toHaveBeenCalledWith(
      "Message processing failed with a defect; nacking message",
      expect.anything(),
    );
  });
});

describe("message size cap", () => {
  beforeEach(async () => {
    wrapper().consume.mockClear();
    wrapper().ack.mockClear();
    wrapper().nack.mockClear();
    await _internal_resetConnections();
  });

  it.for([{ maxMessageBytes: 4 }, { maxDecompressedBytes: 4 }])(
    "a body over %o is dead-lettered as poison (the old option name still works)",
    async (cap) => {
      const worker = await TypedAmqpWorker.create({
        contract,
        handlers: { processOrder: () => OkAsync(undefined) },
        urls: ["amqp://localhost"],
        ...cap,
      }).getOrThrow();
      const consumeCallback = wrapper().consume.mock.calls[0]?.[1] as (
        msg: ConsumeMessage | null,
      ) => Promise<void>;

      await consumeCallback(delivery());

      expect(settles()).toEqual([0, [[false, false]]]);
      await worker.close().get();
    },
  );
});

describe("connection, health and topology", () => {
  beforeEach(async () => {
    await _internal_resetConnections();
  });

  function ownedConnection(isUp: () => boolean) {
    const createChannel = vi.fn(() => wrapper());
    return {
      connection: {
        createChannel,
        on: vi.fn(),
        removeListener: vi.fn(),
        isConnected: isUp,
        close: vi.fn(() => Promise.resolve()),
      } as unknown as AmqpConnectionManager,
      createChannel,
    };
  }

  it("isConnected() reports the state of an explicit, caller-owned connection, which close() leaves open", async () => {
    let up = true;
    const { connection } = ownedConnection(() => up);
    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: { processOrder: () => OkAsync(undefined) },
      connection,
    }).getOrThrow();

    const whileUp = worker.isConnected();
    up = false;

    expect([whileUp, worker.isConnected()]).toEqual([true, false]);
    await worker.close().get();
    expect(connection.close).not.toHaveBeenCalled();
  });

  it.for(["passive", "none"] as const)(
    "honours topology: %s — the channel setup declares nothing",
    async (topology) => {
      const { connection, createChannel } = ownedConnection(() => true);
      const worker = await TypedAmqpWorker.create({
        contract,
        handlers: { processOrder: () => OkAsync(undefined) },
        connection,
        topology,
      }).getOrThrow();
      const setup = (
        createChannel.mock.calls[0] as unknown as [{ setup: (ch: unknown) => Promise<void> }]
      )[0].setup;
      const declare = vi.fn(() => Promise.resolve({}));
      const check = vi.fn(() => Promise.resolve({}));
      const channel = {
        assertExchange: declare,
        assertQueue: declare,
        bindQueue: declare,
        bindExchange: declare,
        checkExchange: check,
        checkQueue: check,
      };

      await setup(channel);

      expect([declare.mock.calls.length, check.mock.calls.length > 0]).toEqual([
        0,
        topology === "passive",
      ]);
      await worker.close().get();
    },
  );
});

/** AsyncLocalStorage-backed context manager — what an OTel Node SDK installs. */
class AsyncContextManager implements ContextManager {
  private readonly storage = new AsyncLocalStorage<Context>();
  active(): Context {
    return this.storage.getStore() ?? ROOT_CONTEXT;
  }
  with<A extends unknown[], F extends (...args: A) => ReturnType<F>>(
    ctx: Context,
    fn: F,
    thisArg?: ThisParameterType<F>,
    ...args: A
  ): ReturnType<F> {
    return this.storage.run(ctx, () => fn.call(thisArg, ...args));
  }
  bind<T>(_ctx: Context, target: T): T {
    return target;
  }
  enable(): this {
    return this;
  }
  disable(): this {
    this.storage.disable();
    return this;
  }
}

describe("trace context", () => {
  beforeEach(async () => {
    wrapper().consume.mockClear();
    _internal_resetTelemetryCache();
    await _internal_resetConnections();
  });

  afterEach(() => {
    context.disable();
  });

  it("the consume span is the active span inside the handler", async () => {
    context.setGlobalContextManager(new AsyncContextManager());
    const consumeSpan = trace.wrapSpanContext({
      traceId: "0af7651916cd43dd8448eb211c80319c",
      spanId: "00f067aa0ba902b7",
      traceFlags: TraceFlags.SAMPLED,
    });
    let seen: Span | undefined;
    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: {
        processOrder: () => {
          seen = trace.getSpan(context.active());
          return OkAsync(undefined);
        },
      },
      urls: ["amqp://localhost"],
      telemetry: {
        getTracer: () => ({ startSpan: () => consumeSpan }) as never,
        getPublishCounter: () => undefined,
        getConsumeCounter: () => undefined,
        getPublishLatencyHistogram: () => undefined,
        getConsumeLatencyHistogram: () => undefined,
        getLateRpcReplyCounter: () => undefined,
      },
    }).getOrThrow();
    const consumeCallback = wrapper().consume.mock.calls[0]?.[1] as (
      msg: ConsumeMessage | null,
    ) => Promise<void>;

    await consumeCallback(delivery());

    expect(seen).toBe(consumeSpan);
    await worker.close().get();
  });
});
