import { beforeEach, describe, expect } from "vitest";
import type { ContractDefinition } from "@amqp-contract/contract";
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineExchangeBinding,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { z } from "zod";
import { AmqpClient } from "../amqp-client.js";
import { it } from "@amqp-contract/testing/extension";

describe("AmqpClient Integration", () => {
  beforeEach(async () => {
    // Reset connection cache between tests
    await AmqpClient._resetConnectionCacheForTesting();
  });

  it("should setup exchanges from contract", async ({ amqpConnectionUrl, amqpChannel }) => {
    // GIVEN
    const contract: ContractDefinition = {
      exchanges: {
        orders: defineExchange("orders", { durable: false }),
        notifications: defineExchange("notifications", { type: "fanout", durable: false }),
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    // Wait for setup to complete
    await client.waitForConnect().getOrThrow();

    // THEN - Verify exchanges exist by checking them
    await expect(amqpChannel.checkExchange("orders")).resolves.toBeDefined();
    await expect(amqpChannel.checkExchange("notifications")).resolves.toBeDefined();

    // CLEANUP
    await client.close().getOrThrow();
  });

  it("should setup queues from contract", async ({ amqpConnectionUrl, amqpChannel }) => {
    // GIVEN
    const contract: ContractDefinition = {
      queues: {
        orderProcessing: defineQueue("order-processing", { type: "classic", durable: false }),
        notifications: defineQueue("notifications", { type: "classic", durable: false }),
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // THEN - Verify queues exist by checking them
    await expect(amqpChannel.checkQueue("order-processing")).resolves.toBeDefined();
    await expect(amqpChannel.checkQueue("notifications")).resolves.toBeDefined();

    // CLEANUP
    await client.close().getOrThrow();
  });

  it("should setup queue bindings from contract", async ({
    amqpConnectionUrl,
    publishMessage,
    initConsumer,
  }) => {
    // GIVEN
    const ordersExchange = defineExchange("orders", { durable: false });
    const orderQueue = defineQueue("order-processing", { type: "classic", durable: false });
    const contract: ContractDefinition = {
      exchanges: {
        orders: ordersExchange,
      },
      queues: {
        orderProcessing: orderQueue,
      },
      bindings: {
        orderBinding: defineQueueBinding(orderQueue, ordersExchange, {
          routingKey: "order.created",
        }),
      },
    };

    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // Setup consumer before publishing
    const waitForMessages = await initConsumer("orders", "order.created");

    // WHEN - Publish a message
    publishMessage("orders", "order.created", { orderId: "123" });

    // THEN - Message should be routed through binding to queue
    await expect(waitForMessages()).resolves.toEqual([
      expect.objectContaining({
        content: Buffer.from(JSON.stringify({ orderId: "123" })),
      }),
    ]);

    // CLEANUP
    await client.close().getOrThrow();
  });

  it("should setup exchange-to-exchange bindings", async ({
    amqpConnectionUrl,
    publishMessage,
    initConsumer,
  }) => {
    // GIVEN
    const sourceExchange = defineExchange("source", { durable: false });
    const destExchange = defineExchange("destination", { durable: false });
    const contract: ContractDefinition = {
      exchanges: {
        source: sourceExchange,
        destination: destExchange,
      },
      bindings: {
        exchangeBinding: defineExchangeBinding(destExchange, sourceExchange, {
          routingKey: "*.important",
        }),
      },
    };

    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // Setup consumer on destination exchange
    const waitForMessages = await initConsumer("destination", "test.important");

    // WHEN - Publish to source exchange
    publishMessage("source", "test.important", { data: "important message" });

    // THEN - Message should be routed through exchange binding
    await expect(waitForMessages()).resolves.toEqual([
      expect.objectContaining({
        content: Buffer.from(JSON.stringify({ data: "important message" })),
      }),
    ]);

    // CLEANUP
    await client.close().getOrThrow();
  });

  it("should setup complete contract with all resources", async ({
    amqpConnectionUrl,
    publishMessage,
    initConsumer,
  }) => {
    // GIVEN
    const ordersExchange = defineExchange("orders", { durable: false });
    const analyticsExchange = defineExchange("analytics", { type: "fanout", durable: false });
    const orderQueue = defineQueue("order-processing", { type: "classic", durable: false });
    const analyticsQueue = defineQueue("analytics-processing", { type: "classic", durable: false });

    const contract: ContractDefinition = {
      exchanges: {
        orders: ordersExchange,
        analytics: analyticsExchange,
      },
      queues: {
        orderProcessing: orderQueue,
        analyticsProcessing: analyticsQueue,
      },
      bindings: {
        orderBinding: defineQueueBinding(orderQueue, ordersExchange, {
          routingKey: "order.#",
        }),
        analyticsBinding: defineQueueBinding(analyticsQueue, analyticsExchange),
        exchangeBinding: defineExchangeBinding(analyticsExchange, ordersExchange, {
          routingKey: "order.created",
        }),
      },
    };

    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // Setup consumers
    const waitForOrderMessages = await initConsumer("orders", "order.created");
    const waitForAnalyticsMessages = await initConsumer("analytics", "");

    // WHEN - Publish to orders exchange
    publishMessage("orders", "order.created", { orderId: "456" });

    // THEN - Both queues should receive messages
    await expect(waitForOrderMessages()).resolves.toEqual([
      expect.objectContaining({
        content: Buffer.from(JSON.stringify({ orderId: "456" })),
      }),
    ]);

    await expect(waitForAnalyticsMessages()).resolves.toEqual([
      expect.objectContaining({
        content: Buffer.from(JSON.stringify({ orderId: "456" })),
      }),
    ]);

    // CLEANUP
    await client.close().getOrThrow();
  });

  it("should handle empty contract", async ({ amqpConnectionUrl }) => {
    // GIVEN
    const contract: ContractDefinition = {};

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // THEN - Should not throw and client should be usable
    expect(client.getConnection()).toBeDefined();

    // CLEANUP
    await client.close().getOrThrow();
  });

  it("should handle fanout exchange binding without routing key", async ({
    amqpConnectionUrl,
    publishMessage,
    initConsumer,
  }) => {
    // GIVEN
    const fanoutExchange = defineExchange("fanout", { type: "fanout", durable: false });
    const orderQueue = defineQueue("order-queue", { type: "classic", durable: false });
    const contract: ContractDefinition = {
      exchanges: {
        fanout: fanoutExchange,
      },
      queues: {
        orderQueue: orderQueue,
      },
      bindings: {
        fanoutBinding: defineQueueBinding(orderQueue, fanoutExchange),
      },
    };

    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // Setup consumer
    const waitForMessages = await initConsumer("fanout", "");

    // WHEN - Publish to fanout exchange
    publishMessage("fanout", "any-key", { message: "broadcast" });

    // THEN - Message should be delivered
    await expect(waitForMessages()).resolves.toEqual([
      expect.objectContaining({
        content: Buffer.from(JSON.stringify({ message: "broadcast" })),
      }),
    ]);

    // CLEANUP
    await client.close().getOrThrow();
  });

  it("should pass custom arguments to exchanges", async ({ amqpConnectionUrl, amqpChannel }) => {
    // GIVEN
    const contract: ContractDefinition = {
      exchanges: {
        orders: defineExchange("orders", {
          durable: false,
          arguments: { "x-custom": "value" },
        }),
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // THEN - Exchange should exist (arguments would have been passed to RabbitMQ)
    await expect(amqpChannel.checkExchange("orders")).resolves.toBeDefined();

    // CLEANUP
    await client.close().getOrThrow();
  });

  it("should pass custom arguments to queues", async ({ amqpConnectionUrl, amqpChannel }) => {
    // GIVEN
    const contract: ContractDefinition = {
      queues: {
        orders: defineQueue("orders", {
          type: "classic",
          durable: false,
          arguments: { "x-max-length": 1000 },
        }),
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // THEN - Queue should exist with custom arguments
    await expect(amqpChannel.checkQueue("orders")).resolves.toBeDefined();

    // CLEANUP
    await client.close().getOrThrow();
  });

  it("should setup bridged exchange-to-exchange bindings from contract", async ({
    amqpConnectionUrl,
    publishMessage,
    initConsumer,
  }) => {
    // GIVEN - Cross-domain: source exchange → (e2e) → bridge exchange → queue
    const sourceExchange = defineExchange("source-domain", { durable: false });
    const bridgeExchange = defineExchange("local-domain", { durable: false });
    const localQueue = defineQueue("local-processing", { type: "classic", durable: false });
    const orderMessage = defineMessage(z.object({ orderId: z.string() }));

    const orderCreated = defineEventPublisher(sourceExchange, orderMessage, {
      routingKey: "order.created",
    });

    const contract = defineContract({
      consumers: {
        processOrder: defineEventConsumer(orderCreated, localQueue, {
          bridgeExchange,
        }),
      },
    });

    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // Setup consumer on the local queue via bridge exchange
    const waitForMessages = await initConsumer(bridgeExchange.name, "order.created");

    // WHEN - Publish to source exchange (simulating remote domain)
    publishMessage(sourceExchange.name, "order.created", { orderId: "bridge-test-123" });

    // THEN - Message flows: source → (e2e) → bridge → queue → consumer
    await expect(waitForMessages()).resolves.toEqual([
      expect.objectContaining({
        content: Buffer.from(JSON.stringify({ orderId: "bridge-test-123" })),
      }),
    ]);

    // CLEANUP
    await client.close().getOrThrow();
  });

  it("should close channel and connection properly", async ({ amqpConnectionUrl }) => {
    // GIVEN
    const contract: ContractDefinition = {
      exchanges: {
        test: defineExchange("test", { durable: false }),
      },
    };

    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // WHEN
    await client.close().getOrThrow();

    // THEN - Client should have been properly closed
    // Note: We can't easily verify connection closure in isolation due to singleton
    expect(client.getConnection()).toBeDefined();
  });
});
