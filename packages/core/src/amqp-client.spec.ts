import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AmqpClient, DEFAULT_PREFETCH } from "./amqp-client.js";
import { ConnectionManagerSingleton } from "./connection-manager.js";

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
  consume: ReturnType<typeof vi.fn>;
};
const fakes = vi.hoisted(() => ({ wrapper: undefined as unknown }));
const wrapper = (): FakeWrapper => fakes.wrapper as FakeWrapper;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
  w.consume = vi.fn(() => Promise.resolve({ consumerTag: "tag-1" }));
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

describe("AmqpClient.consume prefetch default", () => {
  beforeEach(async () => {
    wrapper().consume.mockClear();
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  it(`defaults an unset prefetch to ${String(DEFAULT_PREFETCH)} rather than AMQP's unlimited`, async () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    await client.consume("orders", () => {});

    expect(wrapper().consume).toHaveBeenCalledWith(
      "orders",
      expect.any(Function),
      expect.objectContaining({ prefetch: DEFAULT_PREFETCH }),
    );

    void client.close();
  });

  it("honors an explicit numeric prefetch", async () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    await client.consume("orders", () => {}, { prefetch: 42 });

    expect(wrapper().consume).toHaveBeenCalledWith(
      "orders",
      expect.any(Function),
      expect.objectContaining({ prefetch: 42 }),
    );

    void client.close();
  });

  it('maps the "unbounded" opt-out to AMQP 0 (unlimited)', async () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    await client.consume("orders", () => {}, { prefetch: "unbounded" });

    expect(wrapper().consume).toHaveBeenCalledWith(
      "orders",
      expect.any(Function),
      expect.objectContaining({ prefetch: 0 }),
    );

    void client.close();
  });

  it("still rejects an out-of-range prefetch as a defect", async () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    const result = await client.consume("orders", () => {}, { prefetch: 70_000 });

    expect(result).toBeDefect();

    void client.close();
  });
});
