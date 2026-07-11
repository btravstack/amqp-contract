import type { ContractDefinition } from "@amqp-contract/contract";
import { _internal_getConnectionCount, _internal_resetConnections } from "@amqp-contract/core";
import { beforeEach, describe, expect, it } from "vitest";
import { TypedAmqpClient } from "./client.js";

describe("TypedAmqpClient.create cleanup", () => {
  beforeEach(async () => {
    await _internal_resetConnections();
  });

  it("releases the pooled connection when waitForConnect times out", async () => {
    const contract: ContractDefinition = {};

    // Port 1 is closed on every reasonable host; amqp-connection-manager will
    // retry forever, so the timeout is what forces create() to fail.
    const result = await TypedAmqpClient.create({
      contract,
      urls: ["amqp://localhost:1"],
      connectTimeoutMs: 200,
    });

    expect(result).toBeErr();
    expect(_internal_getConnectionCount()).toBe(0);
  });
});
