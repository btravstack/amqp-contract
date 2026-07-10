import { orderContract } from "@amqp-contract-examples/basic-order-processing-contract";
import { RetryableError, TypedAmqpWorker, defineHandlers } from "@amqp-contract/worker";
import { fromPromise } from "unthrown";
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
  // Create type-safe worker with handlers for each consumer
  const workerResult = TypedAmqpWorker.create({
    contract: orderContract,
    handlers: defineHandlers(orderContract, {
      // Handler for processing NEW orders (order.created)
      processOrder: ({ payload, headers }) => {
        logger.info(
          {
            orderId: payload.orderId,
            customerId: payload.customerId,
            items: payload.items,
            total: payload.totalAmount,
            createdAt: payload.createdAt,
            headers: {
              eventSource: headers.eventSource,
              eventVersion: headers.eventVersion,
            },
          },
          "[PROCESSING] New order received",
        );

        return fromPromise(
          new Promise<void>((resolve) => setTimeout(resolve, 500)),
          (e) => new RetryableError("Processing failed", e),
        ).map(() => {
          logger.info({ orderId: payload.orderId }, "Order processed successfully");
        });
      },

      // Handler for ALL order notifications (order.#)
      notifyOrder: ({ payload }) => {
        // Check if it's a new order or a status update
        if ("items" in payload) {
          // It's a full order
          logger.info(
            {
              type: "new_order",
              orderId: payload.orderId,
              customerId: payload.customerId,
              createdAt: payload.createdAt,
            },
            "[NOTIFICATIONS] Event received",
          );
        } else {
          // It's a status update
          logger.info(
            {
              type: "status_update",
              orderId: payload.orderId,
              status: payload.status,
              updatedAt: payload.updatedAt,
            },
            "[NOTIFICATIONS] Event received",
          );
        }

        return fromPromise(
          new Promise<void>((resolve) => setTimeout(resolve, 300)),
          (e) => new RetryableError("Notification failed", e),
        ).map(() => {
          logger.info("Notification sent");
        });
      },

      // Handler for SHIPPED orders (order.shipped)
      shipOrder: ({ payload }) => {
        logger.info(
          {
            orderId: payload.orderId,
            status: payload.status,
            updatedAt: payload.updatedAt,
          },
          "[SHIPPING] Shipment notification received",
        );

        return fromPromise(
          new Promise<void>((resolve) => setTimeout(resolve, 400)),
          (e) => new RetryableError("Shipping failed", e),
        ).map(() => {
          logger.info({ orderId: payload.orderId }, "Shipping label prepared");
        });
      },

      // Handler for URGENT orders (order.*.urgent)
      handleUrgentOrder: ({ payload }) => {
        logger.warn(
          {
            orderId: payload.orderId,
            status: payload.status,
            updatedAt: payload.updatedAt,
          },
          "[URGENT] Priority order update received!",
        );

        return fromPromise(
          new Promise<void>((resolve) => setTimeout(resolve, 200)),
          (e) => new RetryableError("Urgent handling failed", e),
        ).map(() => {
          logger.warn({ orderId: payload.orderId }, "Urgent update handled");
        });
      },

      // Command handler (task queue): the fulfillment worker owns this queue.
      // Unlike the event handlers above, this command reaches exactly one worker.
      fulfillOrder: ({ payload }) => {
        logger.info(
          {
            orderId: payload.orderId,
            warehouseId: payload.warehouseId,
            priority: payload.priority,
          },
          "[FULFILLMENT] Fulfillment command received",
        );

        return fromPromise(
          new Promise<void>((resolve) => setTimeout(resolve, 400)),
          (e) => new RetryableError("Fulfillment failed", e),
        ).map(() => {
          logger.info({ orderId: payload.orderId }, "Order handed to the warehouse");
        });
      },

      // Handler for FAILED orders (from dead letter exchange)
      handleFailedOrders: ({ payload }) => {
        logger.error(
          {
            orderId: payload.orderId,
            customerId: payload.customerId,
            totalAmount: payload.totalAmount,
            createdAt: payload.createdAt,
          },
          "[DLX] Failed order received from dead letter exchange",
        );

        return fromPromise(
          new Promise<void>((resolve) => setTimeout(resolve, 200)),
          (e) => new RetryableError("Failed order handling failed", e),
        ).map(() => {
          logger.error({ orderId: payload.orderId }, "Failed order logged for investigation");
        });
      },
    }),
    urls: [env.AMQP_URL],
  }).tapErr((error) => logger.error({ error }, "Failed to create worker"));
  const worker = await workerResult.unwrapOrElse((error) => {
    throw error;
  });

  logger.info("Worker ready, waiting for messages...");
  logger.info("=".repeat(60));
  logger.info("Subscribed to:");
  logger.info("  • order.created     → processOrder handler (event)");
  logger.info("  • order.#           → notifyOrder handler (all events)");
  logger.info("  • order.shipped     → shipOrder handler (event)");
  logger.info("  • order.*.urgent    → handleUrgentOrder handler (event)");
  logger.info("  • order.fulfill     → fulfillOrder handler (command / task queue)");
  logger.info("  • order.failed (via DLX) → handleFailedOrders handler");
  logger.info("=".repeat(60));
  logger.info("Dead Letter Exchange:");
  logger.info("  order-processing → orders-dlx (routing: order.failed)");
  logger.info("=".repeat(60));

  // Handle graceful shutdown
  process.on("SIGINT", async () => {
    logger.info("Shutting down worker...");
    await worker.close().unwrapOrElse((error) => {
      throw error;
    });
    process.exit(0);
  });
}

main().catch((error) => {
  logger.error({ error }, "Worker error");
  process.exit(1);
});
