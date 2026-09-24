import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AmqpClient } from "./amqp-client.js";
import { ConnectionManagerSingleton } from "./connection-manager.js";
import { ConnectionError } from "./errors.js";

/**
 * A connect timeout used to report only "Timed out waiting for AMQP
 * connection" — the manager's `connectFailed` events, which carry the actual
 * reason (ECONNREFUSED, ACCESS_REFUSED, …), were never observed.
 */

const fakes = vi.hoisted(() => ({ connection: undefined as unknown }));
const connection = (): EventEmitter => fakes.connection as EventEmitter;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  return {
    default: {
      connect: vi.fn(() => {
        const wrapper = Object.assign(new Emitter(), {
          // Never connects: the broker is unreachable.
          waitForConnect: () => new Promise(() => {}),
          close: () => Promise.resolve(),
        });
        const conn = Object.assign(new Emitter(), {
          createChannel: () => wrapper,
          close: () => Promise.resolve(),
        });
        fakes.connection = conn;
        return conn;
      }),
    },
  };
});

const contract = {} as ContractDefinition;

describe("AmqpClient connect diagnostics", () => {
  beforeEach(async () => {
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  it("reports the last connectFailed error as the ConnectionError cause, and warns once on the first failure", async () => {
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const client = new AmqpClient(contract, {
      urls: ["amqp://localhost"],
      connectTimeoutMs: 20,
      logger,
    });
    const refused = new Error("connect ECONNREFUSED 127.0.0.1:5672");
    connection().emit("connectFailed", { err: new Error("first attempt"), url: "amqp://x" });
    connection().emit("connectFailed", { err: refused, url: "amqp://x" });

    const result = await client.waitForConnect();

    expect(result).toBeErrWith(expect.objectContaining({ constructor: ConnectionError }));
    if (result.isErr()) {
      expect(result.error.cause).toBe(refused);
      expect(result.error.message).toContain("ECONNREFUSED");
    }
    expect(logger.warn).toHaveBeenCalledTimes(1);
    expect(logger.warn).toHaveBeenCalledWith(
      "AMQP connection attempt failed; retrying",
      expect.objectContaining({ error: "first attempt" }),
    );

    await client.close();
    expect(connection().listenerCount("connectFailed")).toBe(0);
  });
});
