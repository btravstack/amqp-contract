import type { ContractDefinition } from "@amqp-contract/contract";
import { _getConnectionCountForTesting, _resetConnectionsForTesting } from "@amqp-contract/core";
import { beforeEach, describe, expect, it } from "vitest";
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
});
