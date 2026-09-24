import type { EventEmitter } from "node:events";

import {
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { _internal_resetConnections } from "@amqp-contract/core/internal";
import type { ConsumeMessage } from "amqplib";
import { ErrAsync, OkAsync } from "unthrown";
import { beforeEach, describe, expect, it, vi } from "vitest";
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
