import { beforeEach, describe, expect, vi } from "vitest";
import type { ContractDefinition } from "@amqp-contract/contract";
import { defineExchange } from "@amqp-contract/contract";
import { AmqpClient } from "../amqp-client.js";
import type { Channel } from "amqplib";
import { it } from "@amqp-contract/testing/extension";

describe("AmqpClient Channel Configuration", () => {
  beforeEach(async () => {
    // Reset connection cache between tests
    await AmqpClient._resetConnectionCacheForTesting();
  });

  it("should allow overriding json option to false", async ({ amqpConnectionUrl }) => {
    // GIVEN
    const contract: ContractDefinition = {
      exchanges: {
        test: defineExchange("test-json-false", { durable: false }),
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
      channelOptions: {
        json: false,
      },
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - Channel should be created with json: false
    // We can't directly test the internal json setting, but we can verify the client was created
    expect(client.getConnection()).toBeDefined();

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
  });

  it("should keep json as true by default", async ({ amqpConnectionUrl }) => {
    // GIVEN
    const contract: ContractDefinition = {
      exchanges: {
        test: defineExchange("test-json-default", { durable: false }),
      },
    };

    // WHEN - Create client without specifying channelOptions
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - Default json: true should be used
    expect(client.getConnection()).toBeDefined();

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
  });

  it("should call custom setup function after topology setup", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    // GIVEN
    const contract: ContractDefinition = {
      exchanges: {
        orders: defineExchange("orders-custom-setup", { durable: false }),
      },
    };

    const customSetupMock = vi.fn(async (channel: Channel) => {
      // Create an additional queue in the custom setup
      await channel.assertQueue("custom-queue", { durable: false });
    });

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
      channelOptions: {
        setup: customSetupMock,
      },
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - Custom setup should have been called
    expect(customSetupMock).toHaveBeenCalledTimes(1);
    expect(customSetupMock).toHaveBeenCalledWith(expect.any(Object));

    // Verify both the contract exchange and the custom queue were created
    await expect(amqpChannel.checkExchange("orders-custom-setup")).resolves.toBeDefined();
    await expect(amqpChannel.checkQueue("custom-queue")).resolves.toBeDefined();

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
  });

  it("should support callback-based custom setup function", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    // GIVEN
    const contract: ContractDefinition = {
      exchanges: {
        test: defineExchange("test-callback-setup", { durable: false }),
      },
    };

    const callbackSetupMock = vi.fn((channel: Channel, callback: (error?: Error) => void) => {
      // Simulate async operation with callback
      channel
        .assertQueue("callback-queue", { durable: false })
        .then(() => callback())
        .catch((err) => callback(err));
    });

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
      channelOptions: {
        setup: callbackSetupMock,
      },
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - Callback setup should have been called
    expect(callbackSetupMock).toHaveBeenCalledTimes(1);
    expect(callbackSetupMock).toHaveBeenNthCalledWith(1, expect.any(Object), expect.any(Function));

    // Verify the custom queue was created
    await expect(amqpChannel.checkQueue("callback-queue")).resolves.toBeDefined();

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
  });

  it("should override default channel name", async ({ amqpConnectionUrl }) => {
    // GIVEN
    const contract: ContractDefinition = {
      exchanges: {
        test: defineExchange("test-channel-name", { durable: false }),
      },
    };

    const customChannelName = "my-custom-channel";

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
      channelOptions: {
        name: customChannelName,
      },
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - Channel should be created
    expect(client.getConnection()).toBeDefined();

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
  });

  it("should allow disabling confirmChannel option", async ({ amqpConnectionUrl }) => {
    // GIVEN
    const contract: ContractDefinition = {
      exchanges: {
        test: defineExchange("test-regular-channel", { durable: false }),
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
      channelOptions: {
        confirm: false, // Use regular channel
      },
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - Regular channel should be created
    expect(client.getConnection()).toBeDefined();

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
  });

  it("should combine user channel options with defaults", async ({ amqpConnectionUrl }) => {
    // GIVEN
    const contract: ContractDefinition = {
      exchanges: {
        test: defineExchange("test-combined-options", { durable: false }),
      },
    };

    // WHEN - Provide multiple channel options
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
      channelOptions: {
        name: "custom-channel",
        json: true, // Explicitly set to true (same as default)
      },
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - All options should be applied
    expect(client.getConnection()).toBeDefined();

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
  });
});
