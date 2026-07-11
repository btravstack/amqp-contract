import { beforeEach, describe, expect } from "vitest";
import type { ContractDefinition } from "@amqp-contract/contract";
import { defineExchange, defineQueue } from "@amqp-contract/contract";
import { AmqpClient } from "../amqp-client.js";
import { it } from "@amqp-contract/testing/extension";

describe("Dead Letter Exchange Support", () => {
  beforeEach(async () => {
    // Reset connection cache between tests
    await AmqpClient._resetConnectionCacheForTesting();
  });

  it("should setup queue with dead letter exchange", async ({ amqpConnectionUrl, amqpChannel }) => {
    // GIVEN
    const dlx = defineExchange("test-dlx", { durable: false });
    const queue = defineQueue("test-queue-with-dlx", {
      type: "classic",
      durable: false,
      deadLetter: {
        exchange: dlx,
        routingKey: "failed",
      },
    });

    const contract: ContractDefinition = {
      exchanges: {
        dlx,
      },
      queues: {
        testQueue: queue,
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - Check that the queue was created with dead letter configuration
    const queueInfo = await amqpChannel.checkQueue("test-queue-with-dlx");
    expect(queueInfo).toEqual(
      expect.objectContaining({
        queue: "test-queue-with-dlx",
        messageCount: 0,
      }),
    );

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
    await amqpChannel.deleteQueue("test-queue-with-dlx");
    await amqpChannel.deleteExchange("test-dlx");
  });

  it("should setup queue with dead letter exchange without routing key", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    // GIVEN
    const dlx = defineExchange("test-dlx-no-key", { type: "fanout", durable: false });
    const queue = defineQueue("test-queue-dlx-no-key", {
      type: "classic",
      durable: false,
      deadLetter: {
        exchange: dlx,
      },
    });

    const contract: ContractDefinition = {
      exchanges: {
        dlx,
      },
      queues: {
        testQueue: queue,
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - Check that the queue was created
    const queueInfo = await amqpChannel.checkQueue("test-queue-dlx-no-key");
    expect(queueInfo).toEqual(
      expect.objectContaining({
        queue: "test-queue-dlx-no-key",
      }),
    );

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
    await amqpChannel.deleteQueue("test-queue-dlx-no-key");
    await amqpChannel.deleteExchange("test-dlx-no-key");
  });

  it("should setup queue without dead letter exchange", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    // GIVEN
    const queue = defineQueue("test-queue-no-dlx", {
      type: "classic",
      durable: false,
    });

    const contract: ContractDefinition = {
      queues: {
        testQueue: queue,
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - Check that the queue was created normally
    const queueInfo = await amqpChannel.checkQueue("test-queue-no-dlx");
    expect(queueInfo).toEqual(
      expect.objectContaining({
        queue: "test-queue-no-dlx",
      }),
    );

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
    await amqpChannel.deleteQueue("test-queue-no-dlx");
  });

  it("should setup complete dead letter exchange pattern", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    // GIVEN - A complete DLX setup with main exchange, main queue, DLX, and DLX queue
    const mainExchange = defineExchange("test-main-exchange", { durable: false });
    const dlx = defineExchange("test-complete-dlx", { durable: false });
    const dlxQueue = defineQueue("test-dlx-queue", { type: "classic", durable: false });
    const mainQueue = defineQueue("test-main-queue", {
      type: "classic",
      durable: false,
      deadLetter: {
        exchange: dlx,
        routingKey: "failed",
      },
    });

    const contract: ContractDefinition = {
      exchanges: {
        main: mainExchange,
        dlx,
      },
      queues: {
        mainQueue,
        dlxQueue,
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrElse((e) => {
      throw e;
    });

    // THEN - All resources should be created with correct structure
    const mainQueueInfo = await amqpChannel.checkQueue("test-main-queue");
    expect(mainQueueInfo).toEqual(
      expect.objectContaining({
        queue: "test-main-queue",
        messageCount: 0,
        consumerCount: 0,
      }),
    );

    const dlxQueueInfo = await amqpChannel.checkQueue("test-dlx-queue");
    expect(dlxQueueInfo).toEqual(
      expect.objectContaining({
        queue: "test-dlx-queue",
        messageCount: 0,
        consumerCount: 0,
      }),
    );

    // Verify exchanges exist
    const mainExchangeInfo = await amqpChannel.checkExchange("test-main-exchange");
    expect(mainExchangeInfo).toBeDefined();

    const dlxInfo = await amqpChannel.checkExchange("test-complete-dlx");
    expect(dlxInfo).toBeDefined();

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
    await amqpChannel.deleteQueue("test-main-queue");
    await amqpChannel.deleteQueue("test-dlx-queue");
    await amqpChannel.deleteExchange("test-main-exchange");
    await amqpChannel.deleteExchange("test-complete-dlx");
  });

  it("should throw error when dead letter exchange is not in contract", async ({
    amqpConnectionUrl,
  }) => {
    // GIVEN - A queue with DLX reference but DLX not in contract
    const dlx = defineExchange("test-missing-dlx", { durable: false });
    const queue = defineQueue("test-queue-bad-dlx", {
      type: "classic",
      durable: false,
      deadLetter: {
        exchange: dlx,
        routingKey: "failed",
      },
    });

    // Contract doesn't include the DLX in exchanges
    const contract: ContractDefinition = {
      queues: {
        testQueue: queue,
      },
    };

    // WHEN - Creating client with invalid contract
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    // THEN - Should throw error when channel setup tries to create the queue
    // Listen for error event on the channel
    const errorPromise = new Promise((resolve, reject) => {
      client.on("error", (error) => {
        resolve(error);
      });
      // Also listen for close in case that fires instead
      client.on("close", () => {
        reject(new Error("Channel closed without error event"));
      });
    });

    await expect(errorPromise).resolves.toMatchObject({
      message: expect.stringContaining(
        'Queue "test-queue-bad-dlx" references dead letter exchange "test-missing-dlx" which is not declared in the contract',
      ),
    });

    // CLEANUP
    await client.close().getOrElse((e) => {
      throw e;
    });
  });
});
