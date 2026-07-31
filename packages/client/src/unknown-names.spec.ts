import type { EventEmitter } from "node:events";

import {
  defineContract,
  defineEventPublisher,
  defineExchange,
  defineMessage,
} from "@amqp-contract/contract";
import { TechnicalError } from "@amqp-contract/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { TypedAmqpClient } from "./client.js";

/**
 * Regression guard: `publish()` / `call()` with a name the contract does not
 * declare (a JS caller, a stale name after a contract change, a cast) used to
 * dereference `contract.publishers![name]!` synchronously — the destructuring
 * threw a raw TypeError, violating the client's documented "nothing in the
 * public API throws" contract. Unknown names must surface as a Defect carrying
 * a TechnicalError that names the culprit and lists the declared names.
 */

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
};
const fakes = vi.hoisted(() => ({ wrapper: undefined as unknown }));

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
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

const exchange = defineExchange("orders-x", { durable: false });
const orderCreated = defineEventPublisher(exchange, defineMessage(z.object({ id: z.string() })), {
  routingKey: "order.created",
  // Publish-only fixture: this test only exercises the client's publish/call
  // surface, so no consumer is declared and none is needed.
  externalConsumers: true,
});
const contract = defineContract({
  publishers: { orderCreated },
});

async function createClient() {
  const { _internal_resetConnections } = await import("@amqp-contract/core/internal");
  await _internal_resetConnections();
  return TypedAmqpClient.create({ contract, urls: ["amqp://localhost"] }).get();
}

describe("unknown publisher / RPC names", () => {
  beforeEach(() => {
    (fakes.wrapper as FakeWrapper).removeAllListeners();
  });

  it("publish() with an unknown publisher name resolves to a Defect instead of throwing synchronously", async () => {
    const client = await createClient();

    // The buggy version threw a TypeError right here, before any combinator.
    const pending = client.publish("nonexistent" as never, { id: "1" } as never);

    const result = await pending;
    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(result.cause).toBeInstanceOf(TechnicalError);
      const message = (result.cause as TechnicalError).message;
      expect(message).toContain("nonexistent");
      expect(message).toContain("orderCreated");
    }

    await client.close().get();
  });

  it("call() with an unknown RPC name resolves to a Defect instead of throwing synchronously", async () => {
    const client = await createClient();

    const pending = client.call(
      "nonexistent" as never,
      {} as never,
      {
        timeoutMs: 100,
      } as never,
    );

    const result = await pending;
    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(result.cause).toBeInstanceOf(TechnicalError);
      expect((result.cause as TechnicalError).message).toContain("nonexistent");
    }

    await client.close().get();
  });
});
