import { Ok } from "unthrown";
import { TypedAmqpWorker, defineHandlers } from "@amqp-contract/worker";
import { describe, expect, vi } from "vitest";
import { it } from "@amqp-contract/testing/extension";
import { orderContract } from "@amqp-contract-examples/basic-order-processing-contract";

describe("Basic Order Processing Worker Integration", () => {
  it("should process new orders from order.created queue", async ({
    amqpConnectionUrl,
    publishMessage,
  }) => {
    // GIVEN
    const processedOrders: Array<unknown> = [];
    const worker = await TypedAmqpWorker.create({
      contract: orderContract,
      handlers: defineHandlers(orderContract, {
        processOrder: ({ payload }) => {
          processedOrders.push(payload);
          return Ok(undefined).toAsync();
        },
        notifyOrder: () => Ok(undefined).toAsync(),
        shipOrder: () => Ok(undefined).toAsync(),
        handleUrgentOrder: () => Ok(undefined).toAsync(),

        handleFailedOrders: () => Ok(undefined).toAsync(),
        fulfillOrder: () => Ok(undefined).toAsync(),
      }),
      urls: [amqpConnectionUrl],
    }).getOrThrow();

    try {
      const newOrder = {
        orderId: "TEST-001",
        customerId: "CUST-123",
        items: [{ productId: "PROD-A", quantity: 2, price: 29.99 }],
        totalAmount: 59.98,
        createdAt: new Date().toISOString(),
      };

      // WHEN
      publishMessage(
        orderContract.publishers.orderCreated.exchange.name,
        orderContract.publishers.orderCreated.routingKey,
        newOrder,
      );

      // THEN
      await vi.waitFor(() => {
        if (processedOrders.length < 1) {
          throw new Error("Order not yet processed");
        }
      });
      expect(processedOrders).toEqual([newOrder]);
    } finally {
      await worker.close().getOrThrow();
    }
  });

  it("should receive notifications for all order events", async ({
    amqpConnectionUrl,
    publishMessage,
  }) => {
    // GIVEN
    const notifications: Array<unknown> = [];
    const workerResult = await TypedAmqpWorker.create({
      contract: orderContract,
      handlers: defineHandlers(orderContract, {
        processOrder: () => Ok(undefined).toAsync(),
        notifyOrder: ({ payload }) => {
          notifications.push(payload);
          return Ok(undefined).toAsync();
        },
        shipOrder: () => Ok(undefined).toAsync(),
        handleUrgentOrder: () => Ok(undefined).toAsync(),

        handleFailedOrders: () => Ok(undefined).toAsync(),
        fulfillOrder: () => Ok(undefined).toAsync(),
      }),
      urls: [amqpConnectionUrl],
    });
    const worker = workerResult.getOrThrow();

    try {
      // WHEN
      const newOrder = {
        orderId: "TEST-002",
        customerId: "CUST-456",
        items: [{ productId: "PROD-B", quantity: 1, price: 49.99 }],
        totalAmount: 49.99,
        createdAt: new Date().toISOString(),
      };

      const orderUpdate = {
        orderId: "TEST-002",
        status: "processing" as const,
        updatedAt: new Date().toISOString(),
      };

      publishMessage(
        orderContract.publishers.orderCreated.exchange.name,
        orderContract.publishers.orderCreated.routingKey,
        newOrder,
      );
      publishMessage(
        orderContract.publishers.orderUpdated.exchange.name,
        orderContract.publishers.orderUpdated.routingKey,
        orderUpdate,
      );

      // THEN
      await vi.waitFor(() => {
        if (notifications.length < 2) {
          throw new Error("Notifications not yet received");
        }
      });
      expect(notifications.length).toBeGreaterThanOrEqual(2);
    } finally {
      await worker.close().getOrThrow();
    }
  });

  it("should start all consumers with consumeAll", async ({
    amqpConnectionUrl,
    publishMessage,
  }) => {
    // GIVEN
    const processedOrders: Array<unknown> = [];
    const notifications: Array<unknown> = [];
    const workerResult = await TypedAmqpWorker.create({
      contract: orderContract,
      handlers: defineHandlers(orderContract, {
        processOrder: ({ payload }) => {
          processedOrders.push(payload);
          return Ok(undefined).toAsync();
        },
        notifyOrder: ({ payload }) => {
          notifications.push(payload);
          return Ok(undefined).toAsync();
        },
        shipOrder: () => Ok(undefined).toAsync(),
        handleUrgentOrder: () => Ok(undefined).toAsync(),

        handleFailedOrders: () => Ok(undefined).toAsync(),
        fulfillOrder: () => Ok(undefined).toAsync(),
      }),
      urls: [amqpConnectionUrl],
    });
    const worker = workerResult.getOrThrow();

    try {
      const newOrder = {
        orderId: "TEST-003",
        customerId: "CUST-789",
        items: [{ productId: "PROD-C", quantity: 1, price: 19.99 }],
        totalAmount: 19.99,
        createdAt: new Date().toISOString(),
      };

      // WHEN
      publishMessage(
        orderContract.publishers.orderCreated.exchange.name,
        orderContract.publishers.orderCreated.routingKey,
        newOrder,
      );

      // THEN
      await vi.waitFor(() => {
        if (processedOrders.length < 1 || notifications.length < 1) {
          throw new Error("Messages not yet processed");
        }
      });
      expect(processedOrders.length).toBeGreaterThanOrEqual(1);
      expect(notifications.length).toBeGreaterThan(0); // Receives all events
    } finally {
      await worker.close().getOrThrow();
    }
  });

  it("should deliver a fulfillment command to the single owning consumer", async ({
    amqpConnectionUrl,
    publishMessage,
  }) => {
    // GIVEN
    const fulfilled: Array<unknown> = [];
    const worker = await TypedAmqpWorker.create({
      contract: orderContract,
      handlers: defineHandlers(orderContract, {
        processOrder: () => Ok(undefined).toAsync(),
        notifyOrder: () => Ok(undefined).toAsync(),
        shipOrder: () => Ok(undefined).toAsync(),
        handleUrgentOrder: () => Ok(undefined).toAsync(),
        handleFailedOrders: () => Ok(undefined).toAsync(),
        fulfillOrder: ({ payload }) => {
          fulfilled.push(payload);
          return Ok(undefined).toAsync();
        },
      }),
      urls: [amqpConnectionUrl],
    }).getOrThrow();

    try {
      const command = {
        orderId: "TEST-004",
        warehouseId: "WH-EU-1",
        priority: "express" as const,
      };

      // WHEN — the command is addressed to the fulfillment queue's exchange + key
      publishMessage(
        orderContract.publishers.requestFulfillment.exchange.name,
        orderContract.publishers.requestFulfillment.routingKey,
        command,
      );

      // THEN
      await vi.waitFor(() => {
        if (fulfilled.length < 1) {
          throw new Error("Fulfillment command not yet handled");
        }
      });
      expect(fulfilled).toEqual([command]);
    } finally {
      await worker.close().getOrThrow();
    }
  });
});
