import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import { _internal_resetConnections } from "@amqp-contract/core/internal";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { TypedAmqpClient } from "./client.js";

/**
 * Guard for the audit's unbounded-publish-buffer finding: during a broker
 * outage, amqp-connection-manager's ChannelWrapper buffers publishes and their
 * promises never settle unless the channel was created with `publishTimeout`.
 * Core has always accepted `channelOptions.publishTimeout`, but the typed
 * client never passed it. `publishTimeoutMs` must reach `createChannel`'s
 * options so a buffered publish settles (as a Defect) within the bound.
 */

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
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

const contract: ContractDefinition = {};

describe("publishTimeoutMs threading", () => {
  beforeEach(async () => {
    (fakes.wrapper as FakeWrapper).removeAllListeners();
    createChannelMock().mockClear();
    await _internal_resetConnections();
  });

  it("passes publishTimeoutMs to the channel wrapper as publishTimeout", async () => {
    const client = await TypedAmqpClient.create({
      contract,
      urls: ["amqp://localhost"],
      publishTimeoutMs: 1_500,
    }).get();

    expect(createChannelMock()).toHaveBeenCalledTimes(1);
    expect(createChannelMock()).toHaveBeenCalledWith(
      expect.objectContaining({ publishTimeout: 1_500 }),
    );

    await client.close().get();
  });

  it("creates the channel without a publishTimeout when the option is omitted (buffer-forever default preserved)", async () => {
    const client = await TypedAmqpClient.create({
      contract,
      urls: ["amqp://localhost"],
    }).get();

    expect(createChannelMock()).toHaveBeenCalledTimes(1);
    const opts = createChannelMock().mock.calls[0]?.[0] as Record<string, unknown>;
    expect(opts).not.toHaveProperty("publishTimeout");

    await client.close().get();
  });
});
