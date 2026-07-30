import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AmqpClient } from "./amqp-client.js";
import { ConnectionManagerSingleton } from "./connection-manager.js";

/**
 * Regression guard for the audit's crash finding: amqp-connection-manager's
 * ChannelWrapper emits plain `'error'` events (setup failure on
 * connect/reconnect, publish-worker faults, consumer-reconnect failures). A
 * Node EventEmitter `'error'` emit with zero listeners throws
 * ERR_UNHANDLED_ERROR — inside the manager's async `_onConnect` listener that
 * becomes an unhandled rejection and takes the whole process down. The client
 * must always attach a listener that degrades the event to a log line.
 */

// A real EventEmitter stands in for the ChannelWrapper so the unhandled-
// 'error' crash semantics are authentic, not simulated. The holder is hoisted
// (vi.mock factories run before imports); the wrapper itself is built inside
// the factory, where imports are available.
type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
};
const fakes = vi.hoisted(() => ({ wrapper: undefined as unknown }));
const wrapper = (): FakeWrapper => fakes.wrapper as FakeWrapper;

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

const contract = {} as ContractDefinition;

describe("AmqpClient channel error events", () => {
  beforeEach(async () => {
    wrapper().removeAllListeners();
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  it("INVARIANT: a ChannelWrapper 'error' event never crashes the process — it degrades to a logger.error line", () => {
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const client = new AmqpClient(contract, {
      urls: ["amqp://localhost"],
      logger,
    });

    const cause = new Error("simulated setup failure (406 PRECONDITION_FAILED)");
    // Without a constructor-attached listener this emit throws
    // ERR_UNHANDLED_ERROR — the unit-level equivalent of the process crash.
    expect(() => wrapper().emit("error", cause, { name: "test-channel" })).not.toThrow();

    expect(logger.error).toHaveBeenCalledTimes(1);
    const [message, context] = logger.error.mock.calls[0] as [string, Record<string, unknown>];
    expect(message).toMatch(/channel error/i);
    expect(context).toMatchObject({ error: cause.message });

    void client.close();
  });

  it("survives a channel 'error' event even when no logger is configured", () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    expect(() => wrapper().emit("error", new Error("boom"), { name: "c" })).not.toThrow();

    void client.close();
  });

  it("still delivers the event to user listeners attached via client.on('error')", () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });
    const seen: unknown[] = [];
    client.on("error", (err) => seen.push(err));

    const cause = new Error("observable");
    wrapper().emit("error", cause, { name: "c" });

    expect(seen).toEqual([cause]);

    void client.close();
  });
});
