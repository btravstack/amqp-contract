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
import { ErrAsync } from "unthrown";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { RetryableError } from "./errors.js";
import { TypedAmqpWorker } from "./worker.js";

/**
 * Invariant 1 through the worker's real consume path (not just
 * `publishForRetry`): the original delivery is settled according to what the
 * retry publish actually did on the broker.
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

async function deliverFailingMessage(): Promise<void> {
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers: { processOrder: () => ErrAsync(new RetryableError("transient")) },
    urls: ["amqp://localhost"],
  }).getOrThrow();
  const consumeCallback = wrapper().consume.mock.calls[0]?.[1] as (
    msg: ConsumeMessage | null,
  ) => Promise<void>;
  await consumeCallback(delivery());
  await worker.close().get();
}

describe("retry publish settles the original delivery (through dispatch)", () => {
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
});
