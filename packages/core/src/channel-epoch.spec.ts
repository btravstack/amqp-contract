import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import type { ConsumeMessage } from "amqplib";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AmqpClient } from "./amqp-client.js";
import { ConnectionManagerSingleton } from "./connection-manager.js";

/**
 * Regression guard for the audit's stale-delivery-tag finding: delivery tags
 * are per-channel, but ChannelWrapper buffers publishes across disconnects and
 * blindly forwards ack/nack to whatever channel is current. A retry publish or
 * RPC reply that completes on a NEW channel must not ack the OLD channel's
 * delivery tag — at best the broker closes the channel with 406, at worst the
 * tag settles an unrelated in-flight delivery (whose own DLQ nack is then
 * silently lost). The client tracks a channel epoch (bumped on every
 * 'connect') and refuses an ack/nack stamped with a stale epoch — the broker
 * redelivers, preserving at-least-once.
 */

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
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
  w.ack = vi.fn();
  w.nack = vi.fn();
  fakes.wrapper = w;
  return {
    default: {
      connect: vi.fn(() => ({
        createChannel: vi.fn(() => w),
        close: vi.fn(() => Promise.resolve()),
      })),
    },
  };
});

const contract = {} as ContractDefinition;
const msg = { fields: { deliveryTag: 5 } } as unknown as ConsumeMessage;

describe("AmqpClient channel epoch", () => {
  beforeEach(async () => {
    wrapper().removeAllListeners();
    wrapper().ack.mockClear();
    wrapper().nack.mockClear();
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  it("bumps the epoch on every 'connect' (initial connect included)", () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    expect(client.currentChannelEpoch).toBe(0);
    wrapper().emit("connect");
    expect(client.currentChannelEpoch).toBe(1);
    wrapper().emit("connect");
    expect(client.currentChannelEpoch).toBe(2);

    void client.close();
  });

  it("INVARIANT: an ack stamped with a stale epoch is skipped (broker redelivery beats settling a foreign tag)", () => {
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"], logger });

    wrapper().emit("connect"); // channel A: epoch 1
    const deliveryEpoch = client.currentChannelEpoch;
    wrapper().emit("connect"); // reconnect → channel B: epoch 2

    client.ack(msg, { deliveryEpoch });

    expect(wrapper().ack).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledTimes(1);

    void client.close();
  });

  it("INVARIANT: a nack stamped with a stale epoch is skipped", () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    wrapper().emit("connect");
    const deliveryEpoch = client.currentChannelEpoch;
    wrapper().emit("connect");

    client.nack(msg, { requeue: false, deliveryEpoch });

    expect(wrapper().nack).not.toHaveBeenCalled();

    void client.close();
  });

  it("acks and nacks normally when the epoch still matches (and when no epoch is stamped)", () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    wrapper().emit("connect");
    client.ack(msg, { deliveryEpoch: client.currentChannelEpoch });
    client.nack(msg, { requeue: true, deliveryEpoch: client.currentChannelEpoch });
    client.ack(msg); // unstamped: legacy behavior, always forwarded
    expect(wrapper().ack).toHaveBeenCalledTimes(2);
    expect(wrapper().nack).toHaveBeenCalledTimes(1);
    expect(wrapper().nack).toHaveBeenCalledWith(msg, false, true);

    void client.close();
  });
});
