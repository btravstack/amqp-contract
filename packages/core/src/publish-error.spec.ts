import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AmqpClient } from "./amqp-client.js";
import { ConnectionManagerSingleton } from "./connection-manager.js";
import { PublishError, TechnicalError } from "./errors.js";

/**
 * A broker-side publish failure is an operational condition a publisher must
 * be able to branch on, so `AmqpClient.publish` / `sendToQueue` report it as
 * the modeled `PublishError` — classified ONCE, here, from what the channel
 * wrapper settles with (a `false` confirmation, or its timeout / nack /
 * channel-closed rejections). Anything core cannot name stays a Defect.
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
        on: vi.fn(),
        removeListener: vi.fn(),
        close: vi.fn(() => Promise.resolve()),
      })),
    },
  };
});

const contract = {} as ContractDefinition;

describe("AmqpClient publish failures", () => {
  beforeEach(async () => {
    wrapper().publish.mockReset();
    wrapper().sendToQueue.mockReset();
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  const target = { exchange: "orders", routingKey: "order.created" };

  it("publish resolves to Ok(void) when the channel accepts the message", async () => {
    wrapper().publish.mockResolvedValue(true);
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    expect(await client.publish(target, { id: "1" })).toBeOkWith(undefined);

    void client.close();
  });

  it("INVARIANT: publish reports a full write buffer as Err(PublishError buffer-full), at the core layer", async () => {
    wrapper().publish.mockResolvedValue(false);
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    const result = await client.publish(target, { id: "1" });

    expect(result).toBeErrWith(
      expect.objectContaining({ constructor: PublishError, reason: "buffer-full" }),
    );
    if (result.isErr()) expect(result.error.message).toContain("channel write buffer full");

    void client.close();
  });

  it.for([
    { rejection: "timeout", reason: "timeout" },
    { rejection: "message nacked", reason: "nacked" },
    { rejection: "Channel closed", reason: "channel-closed" },
  ] as const)(
    "INVARIANT: a '$rejection' rejection is Err(PublishError $reason), never a defect",
    async ({ rejection, reason }) => {
      const cause = new Error(rejection);
      wrapper().publish.mockRejectedValue(cause);
      const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

      const result = await client.publish(target, { id: "1" });

      expect(result).toBeErrWith(
        expect.objectContaining({ constructor: PublishError, reason, cause }),
      );

      void client.close();
    },
  );

  it("keeps an unrecognised rejection on the defect channel (TechnicalError cause)", async () => {
    wrapper().publish.mockRejectedValue(new Error("something nobody anticipated"));
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    const result = await client.publish(target, { id: "1" });

    expect(result).toBeDefectWith(expect.objectContaining({ constructor: TechnicalError }));

    void client.close();
  });

  it("INVARIANT: sendToQueue reports a full write buffer as the same PublishError", async () => {
    wrapper().sendToQueue.mockResolvedValue(false);
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    const result = await client.sendToQueue("replies", { id: "1" });

    expect(result).toBeErrWith(
      expect.objectContaining({ constructor: PublishError, reason: "buffer-full" }),
    );

    void client.close();
  });
});
