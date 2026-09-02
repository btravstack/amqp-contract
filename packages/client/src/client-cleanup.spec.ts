import type { ContractDefinition } from "@amqp-contract/contract";
import {
  _internal_getConnectionCount,
  _internal_resetConnections,
} from "@amqp-contract/core/internal";
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

    // A connect timeout is the MODELED failure of dialing a broker — an
    // operator's business, not a bug — so it lands on the `Err` channel, and
    // the pooled connection is released either way.
    expect(result).toBeErrTagged("@amqp-contract/ConnectionError");
    expect(_internal_getConnectionCount()).toBe(0);
  });
});
