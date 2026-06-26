import { describe, expect } from "vitest";
import { ok } from "unthrown";
import { TypedAmqpClient } from "@amqp-contract/client";
import { it } from "@amqp-contract/testing/extension";
import { orderContract } from "@amqp-contract-examples/basic-order-processing-contract";

describe("Basic Order Processing Client Integration", () => {
  it("should publish a new order successfully", async ({ amqpConnectionUrl }) => {
    // GIVEN
    const client = (
      await TypedAmqpClient.create({
        contract: orderContract,
        urls: [amqpConnectionUrl],
      })
    ).unwrap();

    const newOrder = {
      orderId: "TEST-001",
      customerId: "CUST-123",
      items: [
        { productId: "PROD-A", quantity: 2, price: 29.99 },
        { productId: "PROD-B", quantity: 1, price: 49.99 },
      ],
      totalAmount: 109.97,
      createdAt: new Date().toISOString(),
    };

    // WHEN
    const result = await client.publish("orderCreated", newOrder);

    // THEN
    expect(result).toEqual(ok(undefined));

    // CLEANUP
    (await client.close()).unwrap();
  });

  it("should publish order status updates", async ({ amqpConnectionUrl }) => {
    // GIVEN
    const client = (
      await TypedAmqpClient.create({
        contract: orderContract,
        urls: [amqpConnectionUrl],
      })
    ).unwrap();

    const orderUpdate = {
      orderId: "TEST-001",
      status: "processing" as const,
      updatedAt: new Date().toISOString(),
    };

    // WHEN
    const result = await client.publish("orderUpdated", orderUpdate);

    // THEN
    expect(result).toEqual(ok(undefined));

    // CLEANUP
    (await client.close()).unwrap();
  });

  it("should validate order schema before publishing", async ({ amqpConnectionUrl }) => {
    // GIVEN
    const client = (
      await TypedAmqpClient.create({
        contract: orderContract,
        urls: [amqpConnectionUrl],
      })
    ).unwrap();

    const invalidOrder = {
      orderId: "TEST-001",
      customerId: "CUST-123",
      items: [
        { productId: "PROD-A", quantity: -1, price: 29.99 }, // Invalid: negative quantity
      ],
      totalAmount: 29.99,
      createdAt: new Date().toISOString(),
    };

    // WHEN
    const result = await client.publish("orderCreated", invalidOrder);

    // THEN
    expect(result.isErr()).toBe(true);

    // CLEANUP
    (await client.close()).unwrap();
  });
});
