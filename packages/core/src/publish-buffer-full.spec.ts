import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AmqpClient } from "./amqp-client.js";
import { ConnectionManagerSingleton } from "./connection-manager.js";
import { TechnicalError } from "./errors.js";

/**
 * Guard for the audit's B3 finding: the channel wrapper reports a full write
 * buffer as a boolean `false`, and downstream layers used to re-triage that
 * boolean four different ways. The decision now lives in ONE place —
 * `AmqpClient.publish` / `sendToQueue` absorb the boolean and surface a full
 * buffer as a Defect (TechnicalError cause), so every caller sees the same
 * infrastructure failure through the same channel.
 */

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
  publish: ReturnType<typeof vi.fn>;
  sendToQueue: ReturnType<typeof vi.fn>;
};
const fakes = vi.hoisted(() => ({ wrapper: undefined as unknown }));
const wrapper = (): FakeWrapper => fakes.wrapper as FakeWrapper;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
  w.publish = vi.fn();
  w.sendToQueue = vi.fn();
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

describe("AmqpClient write-buffer-full unification", () => {
  beforeEach(async () => {
    wrapper().publish.mockReset();
    wrapper().sendToQueue.mockReset();
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  it("publish resolves to Ok(void) when the channel accepts the message", async () => {
    wrapper().publish.mockResolvedValue(true);
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    const result = await client.publish(
      { exchange: "orders", routingKey: "order.created" },
      {
        id: "1",
      },
    );

    expect(result).toBeOkWith(undefined);

    void client.close();
  });

  it("INVARIANT: publish surfaces a full write buffer as a Defect with a TechnicalError cause, at the core layer", async () => {
    wrapper().publish.mockResolvedValue(false);
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    const result = await client.publish(
      { exchange: "orders", routingKey: "order.created" },
      {
        id: "1",
      },
    );

    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(result.cause).toBeInstanceOf(TechnicalError);
      expect((result.cause as TechnicalError).message).toContain("channel write buffer full");
    }

    void client.close();
  });

  it("INVARIANT: sendToQueue surfaces a full write buffer as the same Defect", async () => {
    wrapper().sendToQueue.mockResolvedValue(false);
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    const result = await client.sendToQueue("replies", { id: "1" });

    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(result.cause).toBeInstanceOf(TechnicalError);
      expect((result.cause as TechnicalError).message).toContain("channel write buffer full");
    }

    void client.close();
  });
});
