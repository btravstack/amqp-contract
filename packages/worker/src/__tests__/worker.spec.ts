import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  extractQueue,
} from "@amqp-contract/contract";
import { Err, Ok } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";
import { RetryableError } from "../errors.js";
import { defineHandler, defineHandlers } from "../handlers.js";
import { TypedAmqpWorker } from "../worker.js";
import { it } from "./fixture.js";

describe("AmqpWorker Integration", () => {
  it("should consume messages from a real RabbitMQ instance", async ({
    workerFactory,
    publishMessage,
  }) => {
    // GIVEN
    const TestMessage = z.object({
      id: z.string(),
      message: z.string(),
    });

    const exchange = defineExchange("worker-test-exchange", { durable: false });
    const queue = defineQueue("worker-test-queue", { type: "classic", durable: false });
    const testMessage = defineMessage(TestMessage);
    const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

    const contract = defineContract({
      publishers: { testPublisher: testEvent },
      consumers: {
        testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
      },
    });

    const messages: Array<{ id: string; message: string }> = [];
    await workerFactory(
      contract,
      defineHandlers(contract, {
        testConsumer: ({ payload }) => {
          messages.push(payload);
          return Ok(undefined).toAsync();
        },
      }),
    );

    // WHEN
    publishMessage(exchange.name, "test.message", {
      id: "123",
      message: "Hello from integration test!",
    });

    // THEN
    await vi.waitFor(() => {
      if (messages.length < 1) {
        throw new Error("Message not yet consumed");
      }
    });

    expect(messages).toEqual([
      {
        id: "123",
        message: "Hello from integration test!",
      },
    ]);
  });

  it("should consume messages with default values", async ({ workerFactory, publishMessage }) => {
    // GIVEN
    const TestMessage = z.object({
      id: z.string(),
      count: z.number().default(1),
    });

    const exchange = defineExchange("worker-test-exchange", { durable: false });
    const queue = defineQueue("worker-test-queue", { type: "classic", durable: false });
    const testMessage = defineMessage(TestMessage);
    const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

    const contract = defineContract({
      publishers: { testPublisher: testEvent },
      consumers: {
        testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
      },
    });

    const messages: Array<{ id: string; count: number }> = [];
    await workerFactory(
      contract,
      defineHandlers(contract, {
        testConsumer: ({ payload }) => {
          messages.push(payload);
          return Ok(undefined).toAsync();
        },
      }),
    );

    // WHEN
    publishMessage(exchange.name, "test.message", {
      id: "123",
      // count is omitted, should use default value of 1
    });

    // THEN
    await vi.waitFor(() => {
      if (messages.length < 1) {
        throw new Error("Message not yet consumed");
      }
    });

    expect(messages).toEqual([
      {
        id: "123",
        count: 1, // Default value applied
      },
    ]);
  });

  it("should consume messages with headers", async ({ workerFactory, publishMessage }) => {
    // GIVEN
    const TestMessage = z.object({
      id: z.string(),
      count: z.number().default(1),
    });
    const TestHeaders = z.object({
      "x-test-header": z.string(),
      "x-default-header": z.string().default("default-header-value"),
    });

    const exchange = defineExchange("worker-test-exchange", { durable: false });
    const queue = defineQueue("worker-test-queue", { type: "classic", durable: false });
    const testMessage = defineMessage(TestMessage, { headers: TestHeaders });
    const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

    const contract = defineContract({
      publishers: { testPublisher: testEvent },
      consumers: {
        testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
      },
    });

    const messages: Array<{ id: string; count: number }> = [];
    const messageHeaders: Array<{ "x-test-header": string }> = [];
    await workerFactory(
      contract,
      defineHandlers(contract, {
        testConsumer: ({ payload, headers }) => {
          messages.push(payload);
          messageHeaders.push(headers);
          return Ok(undefined).toAsync();
        },
      }),
    );

    // WHEN
    publishMessage(
      exchange.name,
      "test.message",
      {
        id: "123",
        // count is omitted, should use default value of 1
      },
      {
        headers: {
          "x-test-header": "test-header-value",
          // "x-default-header" is omitted, should use default value of "default-header-value"
        },
      },
    );

    // THEN
    await vi.waitFor(() => {
      if (messages.length < 1) {
        throw new Error("Message not yet consumed");
      }
    });

    expect(messages).toEqual([
      {
        id: "123",
        count: 1, // Default value applied
      },
    ]);
    expect(messageHeaders).toEqual([
      {
        "x-test-header": "test-header-value",
        "x-default-header": "default-header-value", // Default value applied
      },
    ]);
  });

  it("should handle multiple messages", async ({ workerFactory, publishMessage }) => {
    // GIVEN
    const TestMessage = z.object({
      id: z.string(),
      count: z.number(),
    });

    const exchange = defineExchange("worker-multi-exchange", { durable: false });
    const queue = defineQueue("worker-multi-queue", { type: "classic", durable: false });
    const testMessage = defineMessage(TestMessage);
    const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "multi.test" });

    const contract = defineContract({
      publishers: { testPublisher: testEvent },
      consumers: {
        testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "multi.#" }),
      },
    });

    const messages: Array<{ id: string; count: number }> = [];
    await workerFactory(
      contract,
      defineHandlers(contract, {
        testConsumer: ({ payload }) => {
          messages.push(payload);
          return Ok(undefined).toAsync();
        },
      }),
    );

    // WHEN
    publishMessage(exchange.name, "multi.test", { id: "1", count: 1 });
    publishMessage(exchange.name, "multi.test", { id: "2", count: 2 });
    publishMessage(exchange.name, "multi.test", { id: "3", count: 3 });

    // THEN - Wait for all messages to be consumed
    await vi.waitFor(() => {
      if (messages.length < 3) {
        throw new Error("Message not yet consumed");
      }
    });

    expect(messages).toEqual([
      { id: "1", count: 1 },
      { id: "2", count: 2 },
      { id: "3", count: 3 },
    ]);
  });

  it("should consume all consumers with consumeAll", async ({ workerFactory, publishMessage }) => {
    // GIVEN
    const TestMessage = z.object({ id: z.string() });

    const exchange = defineExchange("worker-all-exchange", { durable: false });
    const queue1 = defineQueue("worker-all-queue1", { type: "classic", durable: false });
    const queue2 = defineQueue("worker-all-queue2", { type: "classic", durable: false });
    const testMessage = defineMessage(TestMessage);
    const event1 = defineEventPublisher(exchange, testMessage, { routingKey: "all.one" });
    const event2 = defineEventPublisher(exchange, testMessage, { routingKey: "all.two" });

    const contract = defineContract({
      publishers: { pub1: event1, pub2: event2 },
      consumers: {
        consumer1: defineEventConsumer(event1, queue1),
        consumer2: defineEventConsumer(event2, queue2),
      },
    });

    const messages1: Array<{ id: string }> = [];
    const messages2: Array<{ id: string }> = [];

    await workerFactory(
      contract,
      defineHandlers(contract, {
        consumer1: ({ payload }) => {
          messages1.push(payload);
          return Ok(undefined).toAsync();
        },
        consumer2: ({ payload }) => {
          messages2.push(payload);
          return Ok(undefined).toAsync();
        },
      }),
    );

    // WHEN
    publishMessage(exchange.name, "all.one", { id: "msg1" });
    publishMessage(exchange.name, "all.two", { id: "msg2" });

    // THEN
    await vi.waitFor(() => {
      if (messages1.length + messages2.length < 2) {
        throw new Error("Message not yet consumed");
      }
    });

    expect(messages1).toEqual([{ id: "msg1" }]);
    expect(messages2).toEqual([{ id: "msg2" }]);
  });

  it("should handle validation errors and nack messages", async ({
    workerFactory,
    publishMessage,
  }) => {
    // GIVEN
    const TestMessage = z.object({
      id: z.string(),
      count: z.number().positive(),
    });

    const exchange = defineExchange("worker-validation-exchange", { durable: false });
    const queue = defineQueue("worker-validation-queue", { type: "classic", durable: false });
    const testMessage = defineMessage(TestMessage);
    const testEvent = defineEventPublisher(exchange, testMessage, {
      routingKey: "validation.message",
    });

    const contract = defineContract({
      publishers: { testPublisher: testEvent },
      consumers: {
        testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "validation.#" }),
      },
    });

    const messages: Array<{ id: string; count: number }> = [];
    await workerFactory(
      contract,
      defineHandlers(contract, {
        testConsumer: ({ payload }) => {
          messages.push(payload);
          return Ok(undefined).toAsync();
        },
      }),
    );

    // WHEN - Publish invalid message
    publishMessage(exchange.name, "validation.message", {
      id: "invalid",
      count: "not-a-number", // Invalid type
    });

    // THEN - Message should not be processed (validation failed)
    // Wait a moment to ensure message would have been processed if valid
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(messages).toHaveLength(0);
  });

  it("should handle handler errors and requeue messages", async ({
    workerFactory,
    publishMessage,
  }) => {
    // GIVEN
    const TestMessage = z.object({ id: z.string(), shouldFail: z.boolean() });

    const exchange = defineExchange("worker-error-exchange", { durable: false });
    const queue = defineQueue("worker-error-queue", {
      type: "quorum",
      retry: { mode: "immediate-requeue", maxRetries: 3 },
    });
    const testMessage = defineMessage(TestMessage);
    const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "error.test" });

    const contract = defineContract({
      publishers: { testPublisher: testEvent },
      consumers: {
        testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "error.#" }),
      },
    });

    let attemptCount = 0;
    const messages: Array<{ id: string; shouldFail: boolean }> = [];
    await workerFactory(
      contract,
      defineHandlers(contract, {
        testConsumer: ({ payload }) => {
          attemptCount++;
          if (payload.shouldFail && attemptCount === 1) {
            return Err(new RetryableError("Handler error on first attempt")).toAsync();
          }
          messages.push(payload);
          return Ok(undefined).toAsync();
        },
      }),
    );

    // WHEN - Publish message that will fail first time
    publishMessage(exchange.name, "error.test", { id: "retry-test", shouldFail: true });

    // THEN - Message should be reprocessed and eventually succeed
    await vi.waitFor(() => {
      if (messages.length < 1) {
        throw new Error("Message not yet consumed successfully");
      }
    });

    expect(messages).toEqual([{ id: "retry-test", shouldFail: true }]);
    expect(attemptCount).toBeGreaterThanOrEqual(2); // At least 2 attempts
  });

  it("should handle exchange-to-exchange bindings", async ({
    workerFactory,
    publishMessage,
    amqpChannel,
  }) => {
    // GIVEN
    const TestMessage = z.object({ msg: z.string() });

    const sourceExchange = defineExchange("worker-source-exchange", { durable: false });
    const destExchange = defineExchange("worker-dest-exchange", { durable: false });
    const queue = defineQueue("worker-dest-queue", { type: "classic", durable: false });
    const testMessage = defineMessage(TestMessage);
    const destEvent = defineEventPublisher(destExchange, testMessage, {
      routingKey: "test.important",
    });

    const contract = defineContract({
      publishers: { destPublisher: destEvent },
      consumers: {
        destConsumer: defineEventConsumer(destEvent, queue),
      },
    });

    const messages: Array<{ msg: string }> = [];
    await workerFactory(
      contract,
      defineHandlers(contract, {
        destConsumer: ({ payload }) => {
          messages.push(payload);
          return Ok(undefined).toAsync();
        },
      }),
    );

    // Set up source exchange and exchange-to-exchange binding manually
    // (exchange-to-exchange bindings are not part of the new contract input API)
    await amqpChannel.assertExchange(sourceExchange.name, sourceExchange.type, { durable: false });
    await amqpChannel.bindExchange(destExchange.name, sourceExchange.name, "*.important");

    // WHEN - Publish to source exchange
    publishMessage(sourceExchange.name, "test.important", { msg: "routed through exchange" });

    // THEN
    await vi.waitFor(() => {
      if (messages.length < 1) {
        throw new Error("Message not yet consumed");
      }
    });

    expect(messages).toEqual([{ msg: "routed through exchange" }]);
  });

  it("should close cleanly and stop consuming", async ({ workerFactory, publishMessage }) => {
    // GIVEN
    const TestMessage = z.object({ id: z.string() });

    const exchange = defineExchange("worker-close-exchange", { durable: false });
    const queue = defineQueue("worker-close-queue", { type: "classic", durable: false });
    const testMessage = defineMessage(TestMessage);
    const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "close.test" });

    const contract = defineContract({
      publishers: { testPublisher: testEvent },
      consumers: {
        testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "close.#" }),
      },
    });

    const messages: Array<{ id: string }> = [];
    const worker = await workerFactory(
      contract,
      defineHandlers(contract, {
        testConsumer: ({ payload }) => {
          messages.push(payload);
          return Ok(undefined).toAsync();
        },
      }),
    );

    // Consume first message
    publishMessage(exchange.name, "close.test", { id: "before-close" });
    await vi.waitFor(() => {
      if (messages.length < 1) {
        throw new Error("Message not yet consumed");
      }
    });

    // WHEN - Close worker
    const closeResult = await worker.close();

    // Publish message after close
    publishMessage(exchange.name, "close.test", { id: "after-close" });

    // THEN
    expect(closeResult).toBeOk();
    expect(messages).toHaveLength(1);
    expect(messages[0]).toEqual({ id: "before-close" });

    // Message published after close should not be consumed
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(messages).toHaveLength(1);
  });

  it("should handle multiple consumers with different message types", async ({
    workerFactory,
    publishMessage,
  }) => {
    // GIVEN
    const OrderMessage = z.object({ orderId: z.string(), amount: z.number() });
    const NotificationMessage = z.object({ userId: z.string(), message: z.string() });

    const exchange = defineExchange("worker-multi-type-exchange", { durable: false });
    const orderQueue = defineQueue("worker-multi-type-orders", { type: "classic", durable: false });
    const notifQueue = defineQueue("worker-multi-type-notifs", { type: "classic", durable: false });
    const orderMessage = defineMessage(OrderMessage);
    const notifMessage = defineMessage(NotificationMessage);
    const orderEvent = defineEventPublisher(exchange, orderMessage, {
      routingKey: "order.created",
    });
    const notifEvent = defineEventPublisher(exchange, notifMessage, {
      routingKey: "notification.email",
    });

    const contract = defineContract({
      publishers: { orderPub: orderEvent, notifPub: notifEvent },
      consumers: {
        orderConsumer: defineEventConsumer(orderEvent, orderQueue, { routingKey: "order.#" }),
        notificationConsumer: defineEventConsumer(notifEvent, notifQueue, {
          routingKey: "notification.#",
        }),
      },
    });

    const orders: Array<{ orderId: string; amount: number }> = [];
    const notifications: Array<{ userId: string; message: string }> = [];

    await workerFactory(
      contract,
      defineHandlers(contract, {
        orderConsumer: ({ payload }) => {
          orders.push(payload);
          return Ok(undefined).toAsync();
        },
        notificationConsumer: ({ payload }) => {
          notifications.push(payload);
          return Ok(undefined).toAsync();
        },
      }),
    );

    // WHEN
    publishMessage(exchange.name, "order.created", { orderId: "123", amount: 99.99 });
    publishMessage(exchange.name, "notification.email", {
      userId: "user1",
      message: "Order created",
    });

    // THEN
    await vi.waitFor(() => {
      if (orders.length + notifications.length < 2) {
        throw new Error("Messages not yet consumed");
      }
    });

    expect(orders).toEqual([{ orderId: "123", amount: 99.99 }]);
    expect(notifications).toEqual([{ userId: "user1", message: "Order created" }]);
  });

  it("should handle consumer cancellation by RabbitMQ (null message)", async ({
    amqpConnection,
  }) => {
    // GIVEN
    const exchange = defineExchange("worker-cancel-exchange", { durable: false });
    const queue = defineQueue("worker-cancel-queue", { type: "classic", durable: false });

    // Setup exchange and queue manually using an admin channel
    const adminChannel = await amqpConnection.createChannel();
    await adminChannel.assertExchange(exchange.name, exchange.type, { durable: false });
    await adminChannel.assertQueue(extractQueue(queue).name, { durable: false });
    await adminChannel.bindQueue(extractQueue(queue).name, exchange.name, "cancel.#");

    // Create a mock handler to track messages received
    const messageHandler = vi.fn();

    // Create a consumer directly using amqplib to test null message handling
    const consumerChannel = await amqpConnection.createChannel();
    await consumerChannel.consume(extractQueue(queue).name, messageHandler, {
      noAck: true,
    });

    // Wait for consumer to be set up
    const CONSUMER_SETUP_WAIT_MS = 500;
    await new Promise((resolve) => setTimeout(resolve, CONSUMER_SETUP_WAIT_MS));

    // WHEN - Delete the queue, which causes RabbitMQ
    // to cancel the consumer and send a null message to the consumer callback
    await adminChannel.deleteQueue(extractQueue(queue).name);

    // THEN - Wait for the null message to be received
    await vi.waitFor(
      () => {
        const nullMessageReceived = messageHandler.mock.calls.some((call) => call[0] === null);
        if (!nullMessageReceived) {
          throw new Error("Null message not yet received");
        }
      },
      { timeout: 2000 },
    );

    expect(messageHandler).toHaveBeenCalledWith(null);

    // Clean up
    await adminChannel.close();
    await consumerChannel.close();
  });

  it("should create worker with proper null message handling infrastructure", async ({
    amqpConnectionUrl,
    publishMessage,
  }) => {
    // GIVEN
    const TestMessage = z.object({ id: z.string() });

    const exchange = defineExchange("worker-cancel-log-exchange", { durable: false });
    const queue = defineQueue("worker-cancel-log-queue", { type: "classic", durable: false });
    const testMessage = defineMessage(TestMessage);
    const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "cancel.test" });

    const contract = defineContract({
      publishers: { testPublisher: testEvent },
      consumers: {
        testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "cancel.#" }),
      },
    });

    // Create a mock logger to capture warnings
    const mockLogger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };

    // Create worker with mock logger
    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: {
        testConsumer: defineHandler(contract, "testConsumer", (_msg) => Ok(undefined).toAsync()),
      },
      urls: [amqpConnectionUrl],
      logger: mockLogger,
    }).getOrElse((e) => {
      throw e;
    });

    // Wait for worker setup
    const WORKER_SETUP_WAIT_MS = 500;
    await new Promise((resolve) => setTimeout(resolve, WORKER_SETUP_WAIT_MS));

    // WHEN - Verify consumer is working by publishing and consuming a test message
    publishMessage(exchange.name, "cancel.test", { id: "test" });
    await vi.waitFor(
      () => {
        const infoCalls = mockLogger.info.mock.calls;
        if (!infoCalls.some((call) => call[0] === "Message consumed successfully")) {
          throw new Error("Test message not yet consumed");
        }
      },
      { timeout: 2000 },
    );

    // THEN - Verify the worker was created successfully and can consume messages
    // The worker code has null message handling that will log "Consumer cancelled
    // by server" when RabbitMQ sends a null message during consumer cancellation
    expect(mockLogger.info).toHaveBeenCalledWith(
      "Message consumed successfully",
      expect.objectContaining({
        consumerName: "testConsumer",
        queueName: extractQueue(queue).name,
      }),
    );

    // Verify only expected warnings were logged during normal operation
    // The warning about missing deadLetter configuration is expected for this test
    const warnCalls = mockLogger.warn.mock.calls;
    const unexpectedWarnings = warnCalls.filter(
      (call) => !call[0]?.includes("no deadLetter configured"),
    );
    expect(unexpectedWarnings).toHaveLength(0);

    // Clean up
    await worker.close().getOrElse((e) => {
      throw e;
    });
  });
});
