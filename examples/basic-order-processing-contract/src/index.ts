import {
  defineCommandConsumer,
  defineCommandPublisher,
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  definePublisher,
  defineQueue,
} from "@amqp-contract/contract";
import { z } from "zod";

/**
 * Message schema for order events
 */
const orderSchema = z.object({
  orderId: z.string(),
  customerId: z.string(),
  items: z.array(
    z.object({
      productId: z.string(),
      quantity: z.number().int().positive(),
      price: z.number().positive(),
    }),
  ),
  totalAmount: z.number().positive(),
  createdAt: z
    .string()
    .datetime()
    .default(() => new Date().toISOString()),
});

/**
 * Message schema for order status updates
 */
const orderStatusSchema = z.object({
  orderId: z.string(),
  status: z.enum(["processing", "shipped", "delivered", "cancelled"]),
  updatedAt: z
    .string()
    .datetime()
    .default(() => new Date().toISOString()),
});

/**
 * Message schema for the fulfillment command.
 *
 * A command is an instruction to do work ("fulfill this order"), not a fact
 * that happened ("this order was created"). It is addressed to one owner.
 */
const fulfillmentSchema = z.object({
  orderId: z.string(),
  warehouseId: z.string(),
  priority: z.enum(["standard", "express"]).default("standard"),
});

/**
 * Message headers schema for order events
 */
const orderHeadersSchema = z.object({
  eventSource: z.string().default("order-service"),
  eventVersion: z.number().default(1),
});

// Define exchanges
const ordersExchange = defineExchange("orders");

// Define dead letter exchange for failed messages
const ordersDlx = defineExchange("orders-dlx");

// Direct exchange dedicated to the fulfillment command (task queue). A command
// targets a single owner, so a direct exchange routing on an exact key fits.
const fulfillmentExchange = defineExchange("fulfillment", { type: "direct" });

// Define queues
const orderProcessingQueue = defineQueue("order-processing", {
  deadLetter: {
    exchange: ordersDlx,
    routingKey: "order.failed",
  },
  arguments: {
    "x-message-ttl": 86400000, // 24 hours
  },
});
const orderNotificationsQueue = defineQueue("order-notifications");
const orderShippingQueue = defineQueue("order-shipping");
const orderUrgentQueue = defineQueue("order-urgent");

// The fulfillment worker owns this queue — commands sent to it are processed
// by exactly one consumer (a task queue), not broadcast.
const orderFulfillmentQueue = defineQueue("order-fulfillment");

// Dead letter queue to collect failed messages
const ordersDlxQueue = defineQueue("orders-dlx-queue");

// Define messages with metadata
const orderMessage = defineMessage(orderSchema, {
  headers: orderHeadersSchema,
  summary: "Order created event",
  description: "Emitted when a new order is created in the system",
});

const orderStatusMessage = defineMessage(orderStatusSchema, {
  summary: "Order status update event",
  description: "Emitted when an order status changes",
});

const orderUnionMessage = defineMessage(z.union([orderSchema, orderStatusSchema]));

const fulfillmentMessage = defineMessage(fulfillmentSchema, {
  summary: "Order fulfillment command",
  description: "Instructs the fulfillment service to pick, pack, and ship an order",
});

/**
 * Event publishers for each event type.
 *
 * Each publisher broadcasts a specific event to the orders exchange.
 * Consumers subscribe using defineEventConsumer with optional routing key overrides.
 */
const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

const orderShippedEvent = defineEventPublisher(ordersExchange, orderStatusMessage, {
  routingKey: "order.shipped",
});

/**
 * Virtual event publisher for the notifications consumer.
 *
 * This is not added to the publishers section since it's only used to define
 * the consumer's message type (union of all order event schemas) and binding
 * with a wildcard routing key (order.#).
 */
const allOrderEvents = defineEventPublisher(ordersExchange, orderUnionMessage, {
  routingKey: "order.created",
});

/**
 * Virtual event publisher for the urgent orders consumer.
 *
 * Used to define the binding with the wildcard pattern order.*.urgent.
 */
const urgentOrderEvents = defineEventPublisher(ordersExchange, orderStatusMessage, {
  routingKey: "order.updated.urgent",
});

/**
 * Virtual event publisher for the DLX consumer.
 *
 * Used to bind the DLX queue to the dead letter exchange.
 */
const failedOrderEvent = defineEventPublisher(ordersDlx, orderMessage, {
  routingKey: "order.failed",
});

/**
 * Command consumer for the fulfillment task queue.
 *
 * The command pattern is the inverse of the event pattern: instead of one
 * publisher broadcasting to many consumers, many publishers send commands to a
 * single consumer that owns the queue. Here the fulfillment worker owns
 * `order-fulfillment` and exposes one command.
 *
 * `defineCommandPublisher` derives the publisher's message type and routing key
 * from this consumer, so callers cannot drift from the owner's contract.
 */
const fulfillOrder = defineCommandConsumer(
  orderFulfillmentQueue,
  fulfillmentExchange,
  fulfillmentMessage,
  { routingKey: "order.fulfill" },
);

const requestFulfillment = defineCommandPublisher(fulfillOrder);

/**
 * Order processing contract demonstrating both the event and command patterns.
 *
 * This contract demonstrates:
 * 1. Event Pattern: publishers broadcast events, consumers subscribe with routing key overrides
 * 2. Command Pattern: many publishers send a command to one owner (the fulfillment task queue)
 * 3. Dead Letter Exchange: Failed messages from orderProcessingQueue are routed to DLX
 * 4. Topic Exchange Wildcards: Consumers use patterns like order.# and order.*.urgent
 *
 * Exchanges, queues, and bindings are automatically extracted from publishers and consumers.
 */
export const orderContract = defineContract({
  publishers: {
    orderCreated: orderCreatedEvent,
    orderShipped: orderShippedEvent,
    orderUpdated: definePublisher(ordersExchange, orderStatusMessage, {
      routingKey: "order.updated",
    }),
    orderUrgentUpdate: definePublisher(ordersExchange, orderStatusMessage, {
      routingKey: "order.updated.urgent",
    }),

    // Command publisher: sends a fulfillment command to the single owner.
    // Its message type is derived from `fulfillOrder`, not restated here.
    requestFulfillment,
  },
  consumers: {
    // Event consumer: subscribes to order.created events
    processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue),

    // Event consumer with routing key override: subscribes to ALL order events
    notifyOrder: defineEventConsumer(allOrderEvents, orderNotificationsQueue, {
      routingKey: "order.#",
    }),

    // Event consumer: subscribes to order.shipped events
    shipOrder: defineEventConsumer(orderShippedEvent, orderShippingQueue),

    // Event consumer with routing key override: subscribes to urgent events
    handleUrgentOrder: defineEventConsumer(urgentOrderEvents, orderUrgentQueue, {
      routingKey: "order.*.urgent",
    }),

    // DLX consumer: receives failed messages
    handleFailedOrders: defineEventConsumer(failedOrderEvent, ordersDlxQueue),

    // Command consumer: the fulfillment worker owns this task queue
    fulfillOrder,
  },
});
