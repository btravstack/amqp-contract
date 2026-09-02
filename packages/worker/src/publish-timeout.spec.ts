import type { EventEmitter } from "node:events";

import {
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { DEFAULT_PUBLISH_TIMEOUT_MS } from "@amqp-contract/core";
import { _internal_resetConnections } from "@amqp-contract/core/internal";
import { OkAsync } from "unthrown";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { TypedAmqpWorker } from "./worker.js";

/**
 * Guard for the audit's unbounded-publish-buffer finding, worker side: retry
 * republishes and RPC replies issued during a broker outage buffer unboundedly
 * in the ChannelWrapper unless the channel was created with `publishTimeout`.
 * `publishTimeoutMs` must reach `createChannel`'s options.
 */

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
  consume: ReturnType<typeof vi.fn>;
  cancel: ReturnType<typeof vi.fn>;
};
const fakes = vi.hoisted(() => ({
  wrapper: undefined as unknown,
  createChannel: undefined as unknown,
}));
const createChannelMock = (): ReturnType<typeof vi.fn> =>
  fakes.createChannel as ReturnType<typeof vi.fn>;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
  w.consume = vi.fn(() => Promise.resolve({ consumerTag: "tag-1" }));
  w.cancel = vi.fn(() => Promise.resolve());
  fakes.wrapper = w;
  const createChannel = vi.fn(() => w);
  fakes.createChannel = createChannel;
  return {
    default: {
      connect: vi.fn(() => ({
        createChannel,
        close: vi.fn(() => Promise.resolve()),
      })),
    },
  };
});

const ordersDlx = defineExchange("orders-dlx");
// `orders-dlx` is topic and the queue sets no dead-letter routing key, so `#`
// catches whatever key the rejected message arrived with.
const ordersDlq = defineQueue("orders-dlq", { type: "classic", durable: false });

const contract = defineContract({
  consumers: {
    processOrder: defineConsumer(
      defineQueue("orders", {
        type: "classic",
        durable: false,
        deadLetter: { exchange: ordersDlx },
      }),
      defineMessage(z.object({ orderId: z.string() })),
    ),
  },
  queues: { ordersDlq },
  bindings: { ordersDlqBinding: defineQueueBinding(ordersDlq, ordersDlx, { routingKey: "#" }) },
});

describe("publishTimeoutMs threading (worker)", () => {
  beforeEach(async () => {
    (fakes.wrapper as FakeWrapper).removeAllListeners();
    createChannelMock().mockClear();
    await _internal_resetConnections();
  });

  it("passes publishTimeoutMs to the channel wrapper as publishTimeout", async () => {
    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: { processOrder: () => OkAsync(undefined) },
      urls: ["amqp://localhost"],
      publishTimeoutMs: 2_000,
    }).getOrThrow();

    expect(createChannelMock()).toHaveBeenCalledTimes(1);
    expect(createChannelMock()).toHaveBeenCalledWith(
      expect.objectContaining({ publishTimeout: 2_000 }),
    );

    await worker.close().get();
  });

  it("creates the channel with the core default publishTimeout when the option is omitted", async () => {
    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: { processOrder: () => OkAsync(undefined) },
      urls: ["amqp://localhost"],
    }).getOrThrow();

    expect(createChannelMock()).toHaveBeenCalledWith(
      expect.objectContaining({ publishTimeout: DEFAULT_PUBLISH_TIMEOUT_MS }),
    );

    await worker.close().get();
  });

  it("omits publishTimeout entirely when explicitly disabled with null", async () => {
    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: { processOrder: () => OkAsync(undefined) },
      urls: ["amqp://localhost"],
      publishTimeoutMs: null,
    }).getOrThrow();

    const opts = createChannelMock().mock.calls[0]?.[0] as Record<string, unknown>;
    expect(opts).not.toHaveProperty("publishTimeout");

    await worker.close().get();
  });
});
