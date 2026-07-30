import type { EventEmitter } from "node:events";

import {
  defineConsumer,
  defineContract,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
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

const contract = defineContract({
  consumers: {
    processOrder: defineConsumer(
      defineQueue("orders", { type: "classic", durable: false }),
      defineMessage(z.object({ orderId: z.string() })),
    ),
  },
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
    }).get();

    expect(createChannelMock()).toHaveBeenCalledTimes(1);
    expect(createChannelMock()).toHaveBeenCalledWith(
      expect.objectContaining({ publishTimeout: 2_000 }),
    );

    await worker.close().get();
  });

  it("creates the channel without a publishTimeout when the option is omitted", async () => {
    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: { processOrder: () => OkAsync(undefined) },
      urls: ["amqp://localhost"],
    }).get();

    const opts = createChannelMock().mock.calls[0]?.[0] as Record<string, unknown>;
    expect(opts).not.toHaveProperty("publishTimeout");

    await worker.close().get();
  });
});
