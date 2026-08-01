import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AmqpClient, DEFAULT_PUBLISH_TIMEOUT_MS } from "./amqp-client.js";
import { ConnectionManagerSingleton } from "./connection-manager.js";

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
};
const fakes = vi.hoisted(() => ({ createChannel: undefined as unknown }));
const createChannel = (): ReturnType<typeof vi.fn> =>
  fakes.createChannel as ReturnType<typeof vi.fn>;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
  const cc = vi.fn(() => w);
  fakes.createChannel = cc;
  return {
    default: {
      connect: vi.fn(() => ({ createChannel: cc, close: vi.fn(() => Promise.resolve()) })),
    },
  };
});

const contract = {} as ContractDefinition;

describe("publishTimeout default", () => {
  beforeEach(async () => {
    createChannel().mockClear();
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  it(`defaults publishTimeout to ${String(DEFAULT_PUBLISH_TIMEOUT_MS)}ms so a buffered publish always settles`, () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    expect(createChannel()).toHaveBeenCalledWith(
      expect.objectContaining({ publishTimeout: DEFAULT_PUBLISH_TIMEOUT_MS }),
    );

    void client.close();
  });

  it("honors an explicit publishTimeoutMs", () => {
    const client = new AmqpClient(contract, {
      urls: ["amqp://localhost"],
      publishTimeoutMs: 5_000,
    });

    expect(createChannel()).toHaveBeenCalledWith(
      expect.objectContaining({ publishTimeout: 5_000 }),
    );

    void client.close();
  });

  it("omits publishTimeout entirely when explicitly disabled with null", () => {
    const client = new AmqpClient(contract, {
      urls: ["amqp://localhost"],
      publishTimeoutMs: null,
    });

    const opts = createChannel().mock.calls[0]?.[0] as Record<string, unknown>;
    expect(opts).not.toHaveProperty("publishTimeout");

    void client.close();
  });
});
