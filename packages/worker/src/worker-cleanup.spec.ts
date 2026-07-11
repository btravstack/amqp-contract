import {
  defineConsumer,
  defineContract,
  defineMessage,
  defineQueue,
  type ContractDefinition,
} from "@amqp-contract/contract";
import { _getConnectionCountForTesting, _resetConnectionsForTesting } from "@amqp-contract/core";
import { beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { TypedAmqpWorker } from "./worker.js";

describe("TypedAmqpWorker.create cleanup", () => {
  beforeEach(async () => {
    await _resetConnectionsForTesting();
  });

  it("releases the pooled connection when waitForConnect times out", async () => {
    const contract: ContractDefinition = {};

    // Port 1 is closed on every reasonable host; amqp-connection-manager will
    // retry forever, so the timeout is what forces create() to fail.
    const result = await TypedAmqpWorker.create({
      contract,
      handlers: {},
      urls: ["amqp://localhost:1"],
      connectTimeoutMs: 200,
    });

    expect(result).toBeErr();
    expect(_getConnectionCountForTesting()).toBe(0);
  });

  it("fails fast without acquiring a connection when handlers are missing", async () => {
    const contract = defineContract({
      consumers: {
        processOrder: defineConsumer(
          defineQueue("orders", { type: "classic", durable: false }),
          defineMessage(z.object({ orderId: z.string() })),
        ),
      },
    });

    // Cast to bypass the type-level completeness requirement; the runtime
    // guard is what's under test. The URL is never dialed — the guard runs
    // before the AmqpClient is constructed.
    const result = await TypedAmqpWorker.create({
      contract,
      handlers: {} as never,
      urls: ["amqp://localhost:1"],
    });

    expect(result).toBeErr();
    if (result.isErr()) {
      expect(result.error.message).toBe(
        "Missing handlers for contract entries: processOrder. " +
          "Every `consumers` and `rpcs` key requires a handler.",
      );
    }
    expect(_getConnectionCountForTesting()).toBe(0);
  });

  it("returns Err (does not throw) when handlers is missing entirely", async () => {
    const contract: ContractDefinition = {};

    // Cast to bypass the type system — a JavaScript caller can omit handlers,
    // and create() must stay throw-free per the error model.
    const result = await TypedAmqpWorker.create({
      contract,
      handlers: undefined as never,
      urls: ["amqp://localhost:1"],
    });

    expect(result).toBeErr();
    if (result.isErr()) {
      expect(result.error.message).toBe(
        "TypedAmqpWorker.create requires a `handlers` object with one handler per `consumers` and `rpcs` entry",
      );
    }
    expect(_getConnectionCountForTesting()).toBe(0);
  });
});
