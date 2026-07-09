import { orderContract } from "@amqp-contract-examples/basic-order-processing-contract";
import { PublishOptions, TypedAmqpClient } from "@amqp-contract/client";
import pino from "pino";
import { z } from "zod";

const env = z
  .object({
    AMQP_URL: z.string().url().default("amqp://localhost:5672"),
    LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace"]).default("info"),
  })
  .parse(process.env);

const logger = pino({
  level: env.LOG_LEVEL,
  transport: {
    target: "pino-pretty",
    options: {
      colorize: true,
    },
  },
});

async function main() {
  // Create type-safe client
  const client = (
    await TypedAmqpClient.create({
      contract: orderContract,
      urls: [env.AMQP_URL],
    }).tapErr((error) => logger.error({ error }, "Failed to create client"))
  ).unwrap();

  logger.info("Client ready");
  logger.info("=".repeat(60));
  logger.info("Publishing orders to demonstrate RabbitMQ topic pattern");
  logger.info("=".repeat(60));

  // Helper function to publish and handle errors
  // In production code, you might want to return the Result to the caller
  // instead of throwing, but for this demo we throw to simplify the flow
  const publishWithLog = async <T extends Parameters<typeof client.publish>[0]>(
    publisherName: T,
    message: Parameters<typeof client.publish<T>>[1],
    options?: PublishOptions,
  ): Promise<void> => {
    (
      await client
        .publish(publisherName, message, options)
        .tapErr((error) => logger.error({ error }, `Failed to publish: ${publisherName}`))
        .tap(() => logger.debug(`Successfully published to ${publisherName}`))
    ).unwrap();
  };

  // 1. Publish a new order (routing key: order.created)
  logger.info("1️⃣ Publishing NEW ORDER (order.created)");
  const newOrder = {
    orderId: "ORD-001",
    customerId: "CUST-123",
    items: [
      { productId: "PROD-A", quantity: 2, price: 29.99 },
      { productId: "PROD-B", quantity: 1, price: 49.99 },
    ],
    totalAmount: 109.97,
  };
  await publishWithLog("orderCreated", newOrder);
  logger.info(`   ✓ Published order ${newOrder.orderId}`);
  logger.info(`   → Will be received by: processing & notifications queues`);
  await new Promise((resolve) => setTimeout(resolve, 1500));

  // 2. Publish a regular order update (routing key: order.updated)
  logger.info("2️⃣ Publishing ORDER UPDATE (order.updated)");
  const orderUpdate = {
    orderId: "ORD-001",
    status: "processing" as const,
  };
  await publishWithLog("orderUpdated", orderUpdate);
  logger.info(`   ✓ Published update for ${orderUpdate.orderId}`);
  logger.info(`   → Will be received by: notifications queue only`);
  await new Promise((resolve) => setTimeout(resolve, 1500));

  // 3. Publish a shipped order (routing key: order.shipped)
  logger.info("3️⃣ Publishing ORDER SHIPPED (order.shipped)");
  const shippedOrder = {
    orderId: "ORD-001",
    status: "shipped" as const,
  };
  await publishWithLog("orderShipped", shippedOrder);
  logger.info(`   ✓ Published shipment for ${shippedOrder.orderId}`);
  logger.info(`   → Will be received by: notifications & shipping queues`);
  await new Promise((resolve) => setTimeout(resolve, 1500));

  // 4. Publish another new order
  logger.info("4️⃣ Publishing ANOTHER NEW ORDER (order.created)");
  const newOrder2 = {
    orderId: "ORD-002",
    customerId: "CUST-456",
    items: [{ productId: "PROD-C", quantity: 3, price: 15.99 }],
    totalAmount: 47.97,
  };
  const newOrderHeaders2 = {
    eventSource: "new-order-service",
    eventVersion: 2,
  };
  await publishWithLog("orderCreated", newOrder2, { headers: newOrderHeaders2 });
  logger.info(`   ✓ Published order ${newOrder2.orderId}`);
  logger.info(`   → Will be received by: processing & notifications queues`);
  await new Promise((resolve) => setTimeout(resolve, 1500));

  // 5. Publish an urgent order update (routing key: order.updated.urgent)
  logger.info("5️⃣ Publishing URGENT ORDER UPDATE (order.updated.urgent)");
  const urgentUpdate = {
    orderId: "ORD-002",
    status: "cancelled" as const,
  };
  await publishWithLog("orderUrgentUpdate", urgentUpdate);
  logger.info(`   ✓ Published urgent update for ${urgentUpdate.orderId}`);
  logger.info(`   → Will be received by: notifications & urgent queues`);
  await new Promise((resolve) => setTimeout(resolve, 1500));

  // 6. Send a FULFILLMENT COMMAND (routing key: order.fulfill)
  //    This is a command, not an event: it is addressed to the single
  //    fulfillment worker (a task queue), not broadcast to subscribers.
  logger.info("6️⃣ Sending FULFILLMENT COMMAND (order.fulfill)");
  const fulfillment = {
    orderId: "ORD-001",
    warehouseId: "WH-EU-1",
    priority: "express" as const,
  };
  await publishWithLog("requestFulfillment", fulfillment);
  logger.info(`   ✓ Sent fulfillment command for ${fulfillment.orderId}`);
  logger.info(`   → Will be received by: the single fulfillment worker (task queue)`);

  logger.info("=".repeat(60));
  logger.info("All orders published!");
  logger.info("=".repeat(60));

  // Keep the connection open for a bit
  await new Promise((resolve) => setTimeout(resolve, 2000));

  // Clean up
  (await client.close()).unwrap();
  logger.info("Publisher stopped");
  process.exit(0);
}

main().catch((error) => {
  logger.error({ error }, "Publisher error");
  process.exit(1);
});
