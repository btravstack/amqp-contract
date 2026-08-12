import { describe, expect, it } from "vitest";
import { z } from "zod";

import { brand } from "./brand.js";
import {
  defineCommandConsumer,
  defineCommandPublisher,
  defineConsumer,
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineExchangeBinding,
  defineMessage,
  definePublisher,
  defineQueue,
  defineQueueBinding,
  isBridgedPublisherConfig,
} from "./builder/index.js";

describe("builder", () => {
  describe("defineExchange", () => {
    it("should create an exchange definition", () => {
      // WHEN
      const exchange = defineExchange("test-exchange");

      // THEN
      expect(exchange).toEqual({
        name: "test-exchange",
        type: "topic",
        durable: true,
      });
    });

    it("should create an exchange with minimal options", () => {
      // WHEN
      const exchange = defineExchange("test-exchange", { type: "fanout" });

      // THEN
      expect(exchange).toEqual({
        name: "test-exchange",
        type: "fanout",
        durable: true,
      });
    });
  });

  describe("defineQueue", () => {
    it("should create a queue definition with quorum type by default", () => {
      // WHEN
      const queue = defineQueue("test-queue");

      // THEN
      expect(queue).toEqual({
        name: "test-queue",
        type: "quorum",
        durable: true,
        retry: { mode: "none" },
      });
    });

    it("should create a queue with minimal options and quorum type", () => {
      // WHEN
      const queue = defineQueue("test-queue");

      // THEN
      expect(queue).toEqual({
        name: "test-queue",
        type: "quorum",
        durable: true,
        retry: { mode: "none" },
      });
    });

    it("should create a classic queue when explicitly specified", () => {
      // WHEN
      const queue = defineQueue("test-queue", { type: "classic" });

      // THEN
      expect(queue).toEqual({
        name: "test-queue",
        type: "classic",
        durable: true,
        retry: { mode: "none" },
      });
    });

    it("should create a queue with dead letter exchange", () => {
      // GIVEN
      const dlx = defineExchange("test-dlx");

      // WHEN
      const queue = defineQueue("test-queue", {
        deadLetter: {
          exchange: dlx,
          routingKey: "failed",
        },
      });

      // THEN
      expect(queue).toEqual({
        name: "test-queue",
        type: "quorum",
        durable: true,
        deadLetter: {
          exchange: dlx,
          routingKey: "failed",
        },
        retry: { mode: "none" },
      });
    });

    it("should create a queue with dead letter exchange without routing key", () => {
      // GIVEN
      const dlx = defineExchange("test-dlx", { type: "fanout" });

      // WHEN
      const queue = defineQueue("test-queue", {
        deadLetter: {
          exchange: dlx,
        },
      });

      // THEN
      expect(queue).toEqual({
        name: "test-queue",
        type: "quorum",
        durable: true,
        deadLetter: {
          exchange: dlx,
        },
        retry: { mode: "none" },
      });
    });

    it("should allow exclusive with classic queue", () => {
      // WHEN
      const queue = defineQueue("test-queue", { type: "classic", exclusive: true });

      // THEN
      expect(queue).toEqual({
        name: "test-queue",
        type: "classic",
        durable: true,
        exclusive: true,
        retry: { mode: "none" },
      });
    });

    it("should allow autoDelete with classic queue", () => {
      // WHEN
      const queue = defineQueue("test-queue", { type: "classic", autoDelete: true });

      // THEN
      expect(queue).toEqual({
        name: "test-queue",
        type: "classic",
        durable: true,
        autoDelete: true,
        retry: { mode: "none" },
      });
    });
  });

  describe("defineQueue with maxPriority", () => {
    it("should create a priority queue with maxPriority using classic type", () => {
      // WHEN
      const queue = defineQueue("priority-queue", {
        type: "classic",
        maxPriority: 10,
      });

      // THEN
      expect(queue).toEqual({
        name: "priority-queue",
        type: "classic",
        durable: true,
        maxPriority: 10,
        retry: { mode: "none" },
      });
    });

    it("should create a priority queue with minimal options using classic type", () => {
      // WHEN
      const queue = defineQueue("priority-queue", { type: "classic", maxPriority: 5 });

      // THEN
      expect(queue).toEqual({
        name: "priority-queue",
        type: "classic",
        durable: true,
        maxPriority: 5,
        retry: { mode: "none" },
      });
    });

    it("should merge additional arguments with maxPriority", () => {
      // WHEN
      const queue = defineQueue("priority-queue", {
        type: "classic",
        maxPriority: 10,
        arguments: {
          "x-message-ttl": 60000,
        },
      });

      // THEN
      expect(queue).toEqual({
        name: "priority-queue",
        type: "classic",
        durable: true,
        maxPriority: 10,
        retry: { mode: "none" },
        arguments: {
          "x-message-ttl": 60000,
        },
      });
    });

    it("should create a priority queue with dead letter exchange", () => {
      // GIVEN
      const dlx = defineExchange("test-dlx");

      // WHEN
      const queue = defineQueue("priority-queue", {
        type: "classic",
        maxPriority: 10,
        deadLetter: {
          exchange: dlx,
          routingKey: "failed",
        },
      });

      expect(queue).toEqual({
        name: "priority-queue",
        type: "classic",
        durable: true,
        maxPriority: 10,
        deadLetter: {
          exchange: dlx,
          routingKey: "failed",
        },
        retry: { mode: "none" },
      });
    });

    it("should throw error for maxPriority less than 1", () => {
      // WHEN/THEN
      expect(() => defineQueue("priority-queue", { type: "classic", maxPriority: 0 })).toThrow(
        "Invalid maxPriority: 0. Must be between 1 and 255. Recommended range: 1-10.",
      );
    });

    it("should throw error for maxPriority greater than 255", () => {
      // WHEN/THEN
      expect(() => defineQueue("priority-queue", { type: "classic", maxPriority: 256 })).toThrow(
        "Invalid maxPriority: 256. Must be between 1 and 255. Recommended range: 1-10.",
      );
    });

    it("should accept maxPriority of 1", () => {
      // WHEN
      const queue = defineQueue("priority-queue", { type: "classic", maxPriority: 1 });

      // THEN
      expect(queue).toEqual({
        name: "priority-queue",
        type: "classic",
        durable: true,
        maxPriority: 1,
        retry: { mode: "none" },
      });
    });

    it("should accept maxPriority of 255", () => {
      // WHEN
      const queue = defineQueue("priority-queue", { type: "classic", maxPriority: 255 });

      // THEN
      expect(queue).toEqual({
        name: "priority-queue",
        type: "classic",
        durable: true,
        maxPriority: 255,
        retry: { mode: "none" },
      });
    });
  });

  describe("defineQueue with immediate-requeue retry", () => {
    it("should create a quorum queue with immediate-requeue retry", () => {
      // WHEN
      const queue = defineQueue("retry-queue", {
        type: "quorum",
        retry: { mode: "immediate-requeue", maxRetries: 3 },
      });

      // THEN
      expect(queue).toEqual({
        name: "retry-queue",
        type: "quorum",
        durable: true,
        retry: { mode: "immediate-requeue", maxRetries: 3 },
      });
    });

    it("should create a default quorum queue with immediate-requeue retry", () => {
      // WHEN (type defaults to "quorum")
      const queue = defineQueue("retry-queue", {
        retry: { mode: "immediate-requeue", maxRetries: 5 },
      });

      // THEN
      expect(queue).toEqual({
        name: "retry-queue",
        type: "quorum",
        durable: true,
        retry: {
          mode: "immediate-requeue",
          maxRetries: 5,
        },
      });
    });

    it("should create a quorum queue with immediate-requeue retry and dead letter exchange", () => {
      // GIVEN
      const dlx = defineExchange("test-dlx");

      // WHEN
      const queue = defineQueue("retry-queue", {
        type: "quorum",
        retry: { mode: "immediate-requeue", maxRetries: 3 },
        deadLetter: {
          exchange: dlx,
          routingKey: "failed",
        },
      });

      // THEN
      expect(queue).toEqual({
        name: "retry-queue",
        type: "quorum",
        durable: true,
        deadLetter: {
          exchange: dlx,
          routingKey: "failed",
        },
        retry: { mode: "immediate-requeue", maxRetries: 3 },
      });
    });

    it("should throw error for maxRetries less than 1", () => {
      // WHEN/THEN
      expect(() =>
        defineQueue("retry-queue", {
          retry: { mode: "immediate-requeue", maxRetries: 0 },
        }),
      ).toThrow(
        'Queue "retry-queue" uses immediate-requeue retry mode with invalid maxRetries: 0. Must be a positive integer.',
      );
    });

    it("should throw error for non-integer maxRetries", () => {
      // WHEN/THEN
      expect(() =>
        defineQueue("retry-queue", {
          retry: { mode: "immediate-requeue", maxRetries: 2.5 },
        }),
      ).toThrow(
        'Queue "retry-queue" uses immediate-requeue retry mode with invalid maxRetries: 2.5. Must be a positive integer.',
      );
    });

    it("should accept maxRetries of 1", () => {
      // WHEN
      const queue = defineQueue("retry-queue", {
        retry: { mode: "immediate-requeue", maxRetries: 1 },
      });

      // THEN
      expect(queue).toEqual({
        name: "retry-queue",
        type: "quorum",
        durable: true,
        retry: { mode: "immediate-requeue", maxRetries: 1 },
      });
    });
  });

  describe("defineQueue with classic queue immediate-requeue retry", () => {
    it("should create a classic queue with immediate-requeue retry", () => {
      // WHEN
      const queue = defineQueue("retry-queue", {
        type: "classic",
        retry: { mode: "immediate-requeue", maxRetries: 3 },
      });

      // THEN
      expect(queue).toEqual({
        name: "retry-queue",
        type: "classic",
        durable: true,
        retry: { mode: "immediate-requeue", maxRetries: 3 },
      });
    });

    it("should create a classic queue with immediate-requeue and dead letter exchange", () => {
      // GIVEN
      const dlx = defineExchange("retry-dlx");

      // WHEN
      const queue = defineQueue("retry-queue", {
        type: "classic",
        deadLetter: { exchange: dlx, routingKey: "retry.failed" },
        retry: { mode: "immediate-requeue", maxRetries: 5 },
      });

      // THEN
      expect(queue).toEqual({
        name: "retry-queue",
        type: "classic",
        durable: true,
        deadLetter: { exchange: dlx, routingKey: "retry.failed" },
        retry: { mode: "immediate-requeue", maxRetries: 5 },
      });
    });

    it("should create a classic queue with immediate-requeue and exclusive", () => {
      // WHEN
      const queue = defineQueue("retry-queue", {
        type: "classic",
        exclusive: true,
        retry: { mode: "immediate-requeue", maxRetries: 2 },
      });

      // THEN
      expect(queue).toEqual({
        name: "retry-queue",
        type: "classic",
        durable: true,
        exclusive: true,
        retry: { mode: "immediate-requeue", maxRetries: 2 },
      });
    });

    it("should apply default maxRetries of 3 for classic queue with immediate-requeue", () => {
      // WHEN
      const queue = defineQueue("retry-queue", {
        type: "classic",
        retry: { mode: "immediate-requeue" },
      });

      // THEN
      expect(queue).toEqual({
        name: "retry-queue",
        type: "classic",
        durable: true,
        retry: { mode: "immediate-requeue", maxRetries: 3 },
      });
    });

    it("should throw error for non-integer maxRetries on classic queue", () => {
      // WHEN/THEN
      expect(() =>
        defineQueue("retry-queue", {
          type: "classic",
          retry: { mode: "immediate-requeue", maxRetries: 2.5 },
        }),
      ).toThrow(
        'Queue "retry-queue" uses immediate-requeue retry mode with invalid maxRetries: 2.5. Must be a positive integer.',
      );
    });

    it("should throw error for negative maxRetries on classic queue", () => {
      // WHEN/THEN
      expect(() =>
        defineQueue("retry-queue", {
          type: "classic",
          retry: { mode: "immediate-requeue", maxRetries: -1 },
        }),
      ).toThrow(
        'Queue "retry-queue" uses immediate-requeue retry mode with invalid maxRetries: -1. Must be a positive integer.',
      );
    });
  });

  describe("defineQueue with TTL-backoff retry", () => {
    it("should create a plain quorum queue definition with resolved TTL-backoff retry", () => {
      // WHEN
      const queue = defineQueue("retry-queue", {
        type: "quorum",
        retry: { mode: "ttl-backoff", maxRetries: 3 },
      });

      // THEN — a single uniform QueueDefinition; no wrapper, no wait
      // infrastructure (that is derived at topology-setup time).
      expect(queue).toEqual({
        name: "retry-queue",
        type: "quorum",
        durable: true,
        retry: {
          mode: "ttl-backoff",
          maxRetries: 3,
          initialDelayMs: 1000,
          maxDelayMs: 30000,
          backoffMultiplier: 2,
          jitter: true,
        },
      });
    });

    it("should create a quorum queue with TTL-backoff retry and custom options", () => {
      // WHEN
      const queue = defineQueue("retry-queue", {
        type: "quorum",
        retry: {
          mode: "ttl-backoff",
          maxRetries: 5,
          initialDelayMs: 2000,
          maxDelayMs: 60000,
          backoffMultiplier: 3,
          jitter: false,
        },
      });

      // THEN
      expect(queue).toMatchObject({
        name: "retry-queue",
        type: "quorum",
        retry: {
          mode: "ttl-backoff",
          maxRetries: 5,
          initialDelayMs: 2000,
          maxDelayMs: 60000,
          backoffMultiplier: 3,
          jitter: false,
        },
      });
    });

    it("should create a quorum queue with TTL-backoff retry and dead letter exchange", () => {
      // GIVEN
      const dlx = defineExchange("test-dlx");

      // WHEN
      const queue = defineQueue("retry-queue", {
        type: "quorum",
        retry: { mode: "ttl-backoff", maxRetries: 3 },
        deadLetter: {
          exchange: dlx,
          routingKey: "failed",
        },
      });

      // THEN
      expect(queue).toMatchObject({
        name: "retry-queue",
        type: "quorum",
        deadLetter: {
          exchange: dlx,
          routingKey: "failed",
        },
        retry: { mode: "ttl-backoff", maxRetries: 3 },
      });
    });

    it("should throw error for maxRetries less than 1", () => {
      // WHEN/THEN
      expect(() =>
        defineQueue("retry-queue", {
          retry: { mode: "ttl-backoff", maxRetries: 0 },
        }),
      ).toThrow(
        'Queue "retry-queue" uses ttl-backoff retry mode with invalid maxRetries: 0. Must be a positive integer.',
      );
    });

    it("should throw error for non-integer maxRetries", () => {
      // WHEN/THEN
      expect(() =>
        defineQueue("retry-queue", {
          retry: { mode: "ttl-backoff", maxRetries: 2.5 },
        }),
      ).toThrow(
        'Queue "retry-queue" uses ttl-backoff retry mode with invalid maxRetries: 2.5. Must be a positive integer.',
      );
    });

    it("should accept maxRetries of 1", () => {
      // WHEN
      const queue = defineQueue("retry-queue", {
        retry: { mode: "ttl-backoff", maxRetries: 1 },
      });

      // THEN
      expect(queue).toMatchObject({
        name: "retry-queue",
        retry: { mode: "ttl-backoff", maxRetries: 1 },
      });
    });
  });

  describe("defineQueueBinding", () => {
    it("should create a queue binding definition", () => {
      // GIVEN
      const queue = defineQueue("test-queue");
      const exchange = defineExchange("test-exchange");

      // WHEN
      const binding = defineQueueBinding(queue, exchange, {
        routingKey: "test.key",
      });

      // THEN
      expect(binding).toEqual({
        type: "queue",
        queue,
        exchange,
        routingKey: "test.key",
      });
    });

    it("should create a queue binding with minimal options", () => {
      // GIVEN
      const queue = defineQueue("test-queue");
      const exchange = defineExchange("test-exchange", { type: "fanout" });

      // WHEN
      const binding = defineQueueBinding(queue, exchange);

      // THEN
      expect(binding).toEqual({
        type: "queue",
        queue,
        exchange,
      });
    });
  });

  describe("defineExchangeBinding", () => {
    it("should create an exchange binding definition", () => {
      // GIVEN
      const destination = defineExchange("destination-exchange");
      const source = defineExchange("source-exchange");

      // WHEN
      const binding = defineExchangeBinding(destination, source, {
        routingKey: "test.key",
      });

      // THEN
      expect(binding).toEqual({
        type: "exchange",
        destination,
        source,
        routingKey: "test.key",
      });
    });

    it("should create an exchange binding with minimal options", () => {
      // GIVEN
      const destination = defineExchange("destination-exchange", { type: "fanout" });
      const source = defineExchange("source-exchange", { type: "fanout" });

      // WHEN
      const binding = defineExchangeBinding(destination, source);

      // THEN
      expect(binding).toEqual({
        type: "exchange",
        destination,
        source,
      });
    });

    it("should create an exchange binding with arguments", () => {
      // GIVEN
      const destination = defineExchange("destination-exchange");
      const source = defineExchange("source-exchange");

      // WHEN
      const binding = defineExchangeBinding(destination, source, {
        routingKey: "order.*",
        arguments: { "x-match": "any" },
      });

      // THEN
      expect(binding).toEqual({
        type: "exchange",
        destination,
        source,
        routingKey: "order.*",
        arguments: { "x-match": "any" },
      });
    });
  });

  describe("defineMessage", () => {
    it("should create a message definition with payload only", () => {
      // GIVEN
      const payload = z.object({
        id: z.string(),
        name: z.string(),
      });

      // WHEN
      const message = defineMessage(payload);

      // THEN
      expect(message).toEqual({
        payload,
      });
    });

    it("should create a message definition with payload and summary", () => {
      // GIVEN
      const payload = z.object({
        orderId: z.string(),
        amount: z.number(),
      });

      // WHEN
      const message = defineMessage(payload, {
        summary: "Order created event",
      });

      // THEN
      expect(message).toEqual({
        payload,
        summary: "Order created event",
      });
    });

    it("should create a message definition with payload, summary and description", () => {
      // GIVEN
      const payload = z.object({
        userId: z.string(),
        email: z.string().email(),
      });

      // WHEN
      const message = defineMessage(payload, {
        summary: "User registered event",
        description: "Emitted when a new user registers in the system",
      });

      // THEN
      expect(message).toEqual({
        payload,
        summary: "User registered event",
        description: "Emitted when a new user registers in the system",
      });
    });

    it("should create a message definition with headers", () => {
      // GIVEN
      const payload = z.object({
        orderId: z.string(),
      });
      const headers = z.object({
        "x-correlation-id": z.string(),
        "x-request-id": z.string(),
      });

      // WHEN
      const message = defineMessage(payload, {
        headers,
        summary: "Order event with headers",
      });

      // THEN
      expect(message).toEqual({
        payload,
        headers,
        summary: "Order event with headers",
      });
    });
  });

  describe("definePublisher", () => {
    it("should create a publisher definition", () => {
      // GIVEN
      const message = defineMessage(z.object({ id: z.string() }));
      const exchange = defineExchange("test-exchange");

      // WHEN
      const publisher = definePublisher(exchange, message, {
        routingKey: "test.key",
      });

      // THEN
      expect(publisher).toEqual({
        exchange,
        message,
        routingKey: "test.key",
      });
    });

    it("should create a publisher with minimal options", () => {
      // GIVEN
      const message = defineMessage(z.object({ id: z.string() }));
      const exchange = defineExchange("test-exchange", { type: "fanout" });

      // WHEN
      const publisher = definePublisher(exchange, message);

      // THEN
      expect(publisher).toEqual({
        exchange,
        message,
      });
    });
  });

  describe("defineConsumer", () => {
    it("should create a consumer definition", () => {
      // GIVEN
      const message = defineMessage(z.object({ id: z.string() }));
      const queue = defineQueue("test-queue");

      // WHEN
      const consumer = defineConsumer(queue, message);

      // THEN
      expect(consumer).toEqual({
        queue,
        message,
      });
    });

    it("should create a consumer with minimal options", () => {
      // GIVEN
      const message = defineMessage(z.object({ id: z.string() }));
      const queue = defineQueue("test-queue");

      // WHEN
      const consumer = defineConsumer(queue, message);

      // THEN
      expect(consumer).toEqual({
        queue,
        message,
      });
    });
  });

  describe("defineContract", () => {
    it("should create a complete contract", () => {
      // GIVEN
      const message = defineMessage(
        z.object({
          orderId: z.string(),
          amount: z.number(),
        }),
      );
      const ordersExchange = defineExchange("orders");
      const orderProcessingQueue = defineQueue("order-processing", { onPoison: "drop" });

      // WHEN
      const contract = defineContract({
        publishers: {
          orderCreated: definePublisher(ordersExchange, message, {
            routingKey: "order.created",
          }),
        },
        consumers: {
          processOrder: defineConsumer(orderProcessingQueue, message),
        },
        // A plain consumer contributes no binding, so the queue has to be
        // bound explicitly — without it the publisher reaches no queue.
        bindings: {
          orderProcessing: defineQueueBinding(orderProcessingQueue, ordersExchange, {
            routingKey: "order.created",
          }),
        },
      });

      // THEN - exchanges and queues are auto-extracted using resource names as keys
      expect(contract).toMatchObject({
        exchanges: {
          orders: { name: "orders", type: "topic", durable: true },
        },
        queues: {
          "order-processing": { name: "order-processing", durable: true },
        },
        bindings: {
          orderProcessing: {
            type: "queue",
            queue: orderProcessingQueue,
            exchange: ordersExchange,
            routingKey: "order.created",
          },
        },
        publishers: {
          orderCreated: {
            exchange: ordersExchange,
            message,
            routingKey: "order.created",
          },
        },
        consumers: {
          processOrder: {
            queue: orderProcessingQueue,
            message,
          },
        },
      });
    });

    it("should create a minimal contract", () => {
      // WHEN
      const contract = defineContract({});

      // THEN - minimal contract now includes empty objects for all properties
      expect(contract).toEqual({
        exchanges: {},
        queues: {},
        bindings: {},
        publishers: {},
        consumers: {},
        rpcs: {},
      });
    });

    it("should create a contract with multiple exchanges and queues", () => {
      // GIVEN
      const message = defineMessage(
        z.object({
          orderId: z.string(),
          amount: z.number(),
        }),
      );
      const sourceExchange = defineExchange("source-exchange");
      const finalQueue = defineQueue("final-queue", { onPoison: "drop" });

      // WHEN - exchanges and queues are auto-extracted from publishers/consumers
      const contract = defineContract({
        publishers: {
          orderCreated: definePublisher(sourceExchange, message, {
            routingKey: "order.created",
          }),
        },
        consumers: {
          processOrder: defineConsumer(finalQueue, message),
        },
        bindings: {
          finalQueue: defineQueueBinding(finalQueue, sourceExchange, {
            routingKey: "order.created",
          }),
        },
      });

      // THEN - exchanges and queues use resource names as keys
      expect(contract).toMatchObject({
        exchanges: {
          "source-exchange": { name: "source-exchange" },
        },
        queues: {
          "final-queue": { name: "final-queue" },
        },
        bindings: {
          finalQueue: {
            type: "queue",
            queue: finalQueue,
            exchange: sourceExchange,
            routingKey: "order.created",
          },
        },
      });
    });
  });

  describe("defineEventPublisher and defineEventConsumer", () => {
    it("should create an event publisher with fanout exchange", () => {
      // GIVEN
      const message = defineMessage(z.object({ id: z.string() }));
      const exchange = defineExchange("test-exchange", { type: "fanout" });
      const queue = defineQueue("test-queue");

      // WHEN
      const eventPublisher = defineEventPublisher(exchange, message);
      const { consumer, binding } = defineEventConsumer(eventPublisher, queue);

      // THEN
      expect(eventPublisher).toMatchObject({
        [brand]: "EventPublisherConfig",
        exchange,
        message,
        routingKey: undefined,
      });
      expect(binding).toEqual({
        type: "queue",
        queue,
        exchange,
      });
      expect(consumer).toEqual({
        queue,
        message,
      });
    });

    it("should create an event publisher with headers exchange", () => {
      // GIVEN
      const message = defineMessage(z.object({ id: z.string() }));
      const exchange = defineExchange("test-exchange", { type: "headers" });
      const queue = defineQueue("test-queue");

      // WHEN
      const eventPublisher = defineEventPublisher(exchange, message);
      const { consumer, binding } = defineEventConsumer(eventPublisher, queue);

      // THEN
      expect(eventPublisher).toMatchObject({
        [brand]: "EventPublisherConfig",
        exchange,
        message,
        routingKey: undefined,
      });
      expect(binding).toEqual({
        type: "queue",
        queue,
        exchange,
      });
      expect(consumer).toEqual({
        queue,
        message,
      });
    });

    it("should create an event publisher with topic exchange", () => {
      // GIVEN
      const message = defineMessage(
        z.object({
          orderId: z.string(),
          amount: z.number(),
        }),
      );
      const exchange = defineExchange("orders");
      const queue = defineQueue("order-processing");

      // WHEN
      const eventPublisher = defineEventPublisher(exchange, message, {
        routingKey: "order.created",
      });
      const { consumer, binding } = defineEventConsumer(eventPublisher, queue);

      // THEN
      expect(eventPublisher).toMatchObject({
        [brand]: "EventPublisherConfig",
        exchange,
        message,
        routingKey: "order.created",
      });
      expect(binding).toEqual({
        type: "queue",
        queue,
        exchange,
        routingKey: "order.created",
      });
      expect(consumer).toEqual({
        queue,
        message,
      });
    });

    it("should create an event publisher with direct exchange", () => {
      // GIVEN
      const message = defineMessage(z.object({ taskId: z.string() }));
      const exchange = defineExchange("tasks", { type: "direct" });
      const queue = defineQueue("task-queue");

      // WHEN
      const eventPublisher = defineEventPublisher(exchange, message, {
        routingKey: "task.execute",
      });
      const { consumer, binding } = defineEventConsumer(eventPublisher, queue);

      // THEN
      expect(eventPublisher).toMatchObject({
        [brand]: "EventPublisherConfig",
        routingKey: "task.execute",
      });
      expect(binding).toEqual({
        type: "queue",
        queue,
        exchange,
        routingKey: "task.execute",
      });
      expect(consumer).toEqual({
        queue,
        message,
      });
    });

    it("should allow consumer to override routing key for topic exchange", () => {
      // GIVEN
      const message = defineMessage(z.object({ orderId: z.string() }));
      const exchange = defineExchange("orders");
      const queue1 = defineQueue("order-processing");
      const queue2 = defineQueue("all-orders");

      // WHEN
      const eventPublisher = defineEventPublisher(exchange, message, {
        routingKey: "order.created",
      });
      const { binding: binding1 } = defineEventConsumer(eventPublisher, queue1);
      const { binding: binding2 } = defineEventConsumer(eventPublisher, queue2, {
        routingKey: "order.*",
      });

      // THEN
      expect(binding1).toMatchObject({
        routingKey: "order.created", // Uses publisher's key
      });
      expect(binding2).toMatchObject({
        routingKey: "order.*", // Overridden with pattern
      });
    });

    it("should forward the publisher's bindingArguments to consumer bindings as a default", () => {
      // GIVEN
      const message = defineMessage(z.object({ level: z.string() }));
      const exchange = defineExchange("logs", { type: "headers" });
      const queue1 = defineQueue("all-logs");
      const queue2 = defineQueue("error-logs");

      // WHEN — publisher declares default binding arguments
      const eventPublisher = defineEventPublisher(exchange, message, {
        bindingArguments: { "x-match": "all", level: "info" },
      });
      const { binding: defaulted } = defineEventConsumer(eventPublisher, queue1);
      const { binding: overridden } = defineEventConsumer(eventPublisher, queue2, {
        arguments: { "x-match": "all", level: "error" },
      });

      // THEN — consumers inherit them unless they pass their own `arguments`
      expect(eventPublisher.bindingArguments).toEqual({ "x-match": "all", level: "info" });
      expect(defaulted).toMatchObject({
        arguments: { "x-match": "all", level: "info" },
      });
      expect(overridden).toMatchObject({
        arguments: { "x-match": "all", level: "error" },
      });
    });

    it("should extract EventPublisherConfig from publishers section", () => {
      // GIVEN
      const message = defineMessage(
        z.object({
          orderId: z.string(),
          amount: z.number(),
        }),
      );
      const ordersExchange = defineExchange("orders");
      const orderQueue = defineQueue("order-processing", { onPoison: "drop" });

      // WHEN - EventPublisherConfig goes directly in publishers
      const orderCreated = defineEventPublisher(ordersExchange, message, {
        routingKey: "order.created",
      });

      const contract = defineContract({
        publishers: {
          // EventPublisherConfig auto-extracted to publisher
          orderCreated,
        },
        consumers: {
          // EventConsumerResult auto-extracted to consumer + binding
          processOrder: defineEventConsumer(orderCreated, orderQueue),
        },
      });

      // THEN - EventPublisherConfig is converted to publisher, resources use name as key
      expect(contract).toMatchObject({
        exchanges: {
          orders: { name: "orders", type: "topic", durable: true },
        },
        queues: {
          "order-processing": { name: "order-processing", durable: true },
        },
        bindings: {
          processOrderBinding: {
            type: "queue",
            queue: orderQueue,
            exchange: ordersExchange,
            routingKey: "order.created",
          },
        },
        publishers: {
          orderCreated: {
            exchange: ordersExchange,
            message,
            routingKey: "order.created",
          },
        },
        consumers: {
          processOrder: {
            queue: orderQueue,
            message,
          },
        },
      });
    });

    it("should auto-extract binding when passing EventConsumerResult directly to consumers", () => {
      // GIVEN
      const message = defineMessage(
        z.object({
          orderId: z.string(),
          amount: z.number(),
        }),
      );
      const ordersExchange = defineExchange("orders");
      const orderQueue = defineQueue("order-processing", { onPoison: "drop" });
      const notificationQueue = defineQueue("notifications", { onPoison: "drop" });

      // WHEN - No manual destructuring needed!
      const orderCreated = defineEventPublisher(ordersExchange, message, {
        routingKey: "order.created",
      });

      const contract = defineContract({
        publishers: {
          orderCreated,
        },
        // Pass EventConsumerResult directly - bindings are auto-extracted
        consumers: {
          processOrder: defineEventConsumer(orderCreated, orderQueue),
          sendNotification: defineEventConsumer(orderCreated, notificationQueue),
        },
      });

      // THEN - bindings are auto-generated from EventConsumerResult
      expect(contract).toMatchObject({
        bindings: {
          // Auto-generated bindings with naming convention: {consumerName}Binding
          processOrderBinding: {
            type: "queue",
            queue: orderQueue,
            exchange: ordersExchange,
            routingKey: "order.created",
          },
          sendNotificationBinding: {
            type: "queue",
            queue: notificationQueue,
            exchange: ordersExchange,
            routingKey: "order.created",
          },
        },
        consumers: {
          processOrder: {
            queue: orderQueue,
            message,
          },
          sendNotification: {
            queue: notificationQueue,
            message,
          },
        },
      });
    });

    it("should support mixing plain ConsumerDefinition and EventConsumerResult in consumers", () => {
      // GIVEN
      const message = defineMessage(z.object({ id: z.string() }));
      const exchange = defineExchange("events", { type: "fanout" });
      const queue1 = defineQueue("queue-1", { onPoison: "drop" });
      const queue2 = defineQueue("queue-2", { onPoison: "drop" });

      const eventPublisher = defineEventPublisher(exchange, message);

      // WHEN - Mix of plain consumer and EventConsumerResult
      const contract = defineContract({
        consumers: {
          // Plain ConsumerDefinition
          plainConsumer: defineConsumer(queue1, message),
          // EventConsumerResult - binding auto-extracted
          eventConsumer: defineEventConsumer(eventPublisher, queue2),
        },
      });

      // THEN
      expect(contract.consumers).toMatchObject({
        plainConsumer: { queue: queue1, message },
        eventConsumer: { queue: queue2, message },
      });

      // Only the EventConsumerResult generates a binding
      expect(contract.bindings).toMatchObject({
        eventConsumerBinding: {
          type: "queue",
          queue: queue2,
          exchange,
        },
      });
      // Plain consumers don't auto-generate bindings
      expect(Object.keys(contract.bindings ?? {})).not.toContain("plainConsumerBinding");
    });
  });

  describe("defineCommandConsumer and defineCommandPublisher", () => {
    it("should create a command consumer with fanout exchange", () => {
      // GIVEN
      const message = defineMessage(z.object({ id: z.string() }));
      const queue = defineQueue("test-queue");
      const exchange = defineExchange("test-exchange", { type: "fanout" });

      // WHEN
      const command = defineCommandConsumer(queue, exchange, message);
      const publisher = defineCommandPublisher(command);

      // THEN
      expect(command).toMatchObject({
        [brand]: "CommandConsumerConfig",
        consumer: { queue, message },
        binding: { type: "queue", queue, exchange },
        exchange,
        message,
        routingKey: undefined,
      });
      expect(publisher).toEqual({
        exchange,
        message,
      });
    });

    it("should create a command consumer with headers exchange", () => {
      // GIVEN
      const message = defineMessage(z.object({ id: z.string() }));
      const queue = defineQueue("test-queue");
      const exchange = defineExchange("test-exchange", { type: "headers" });

      // WHEN
      const command = defineCommandConsumer(queue, exchange, message);
      const publisher = defineCommandPublisher(command);

      // THEN
      expect(command).toMatchObject({
        [brand]: "CommandConsumerConfig",
        consumer: { queue, message },
        binding: { type: "queue", queue, exchange },
        exchange,
        message,
        routingKey: undefined,
      });
      expect(publisher).toEqual({
        exchange,
        message,
      });
    });

    it("should create a command consumer with direct exchange", () => {
      // GIVEN
      const message = defineMessage(
        z.object({
          taskId: z.string(),
          payload: z.record(z.string(), z.unknown()),
        }),
      );
      const queue = defineQueue("tasks");
      const exchange = defineExchange("tasks", { type: "direct" });

      // WHEN
      const command = defineCommandConsumer(queue, exchange, message, {
        routingKey: "task.execute",
      });
      const publisher = defineCommandPublisher(command);

      // THEN
      expect(command.consumer).toEqual({
        queue,
        message,
      });
      expect(command.binding).toEqual({
        type: "queue",
        queue,
        exchange,
        routingKey: "task.execute",
      });
      expect(publisher).toEqual({
        exchange,
        message,
        routingKey: "task.execute",
      });
    });

    it("should create a command consumer with topic exchange", () => {
      // GIVEN
      const message = defineMessage(z.object({ eventId: z.string() }));
      const queue = defineQueue("event-queue");
      const exchange = defineExchange("events");

      // WHEN
      const command = defineCommandConsumer(queue, exchange, message, {
        routingKey: "event.*",
      });
      const publisher = defineCommandPublisher(command, {
        routingKey: "event.processed",
      });

      // THEN
      expect(command.binding).toMatchObject({
        routingKey: "event.*",
      });
      expect(publisher).toMatchObject({
        routingKey: "event.processed",
      });
    });

    it("should extract CommandConsumerConfig from consumers section", () => {
      // GIVEN
      const message = defineMessage(
        z.object({
          userId: z.string(),
          action: z.string(),
        }),
      );
      const auditQueue = defineQueue("audit-log", { onPoison: "drop" });
      const auditExchange = defineExchange("audit");

      // WHEN - CommandConsumerConfig goes directly in consumers
      const processAudit = defineCommandConsumer(auditQueue, auditExchange, message, {
        routingKey: "audit.log",
      });
      const logAuditPublisher = defineCommandPublisher(processAudit);

      const contract = defineContract({
        publishers: {
          logAudit: logAuditPublisher,
        },
        consumers: {
          // CommandConsumerConfig auto-extracted to consumer + binding
          processAudit,
        },
      });

      // THEN - CommandConsumerConfig is converted to consumer and binding, resources use name as key
      expect(contract).toMatchObject({
        exchanges: {
          audit: { name: "audit", type: "topic", durable: true },
        },
        queues: {
          "audit-log": { name: "audit-log", durable: true },
        },
        bindings: {
          processAuditBinding: {
            type: "queue",
            queue: auditQueue,
            exchange: auditExchange,
            routingKey: "audit.log",
          },
        },
        publishers: {
          logAudit: {
            exchange: auditExchange,
            message,
            routingKey: "audit.log",
          },
        },
        consumers: {
          processAudit: {
            queue: auditQueue,
            message,
          },
        },
      });
    });

    it("should allow multiple publishers for topic exchange command", () => {
      // GIVEN
      const message = defineMessage(
        z.object({
          orderId: z.string(),
          amount: z.number(),
        }),
      );
      const queue = defineQueue("order-processing");
      const exchange = defineExchange("orders");

      // WHEN - Consumer bound with pattern, publishers use concrete keys
      const processOrder = defineCommandConsumer(queue, exchange, message, {
        routingKey: "order.*",
      });
      const publisherCreated = defineCommandPublisher(processOrder, {
        routingKey: "order.created",
      });
      const publisherUpdated = defineCommandPublisher(processOrder, {
        routingKey: "order.updated",
      });

      // THEN
      expect(processOrder.binding).toMatchObject({
        routingKey: "order.*",
      });
      expect(publisherCreated).toEqual({
        exchange,
        message,
        routingKey: "order.created",
      });
      expect(publisherUpdated).toEqual({
        exchange,
        message,
        routingKey: "order.updated",
      });
    });
  });

  describe("event and command patterns with external resources", () => {
    it("should support using external exchange with event pattern", () => {
      // GIVEN - External exchange from another contract
      const externalExchange = defineExchange("external-events");
      const localQueue = defineQueue("local-queue");
      const message = defineMessage(z.object({ eventId: z.string() }));

      // WHEN - Use external exchange with local queue
      const eventPublisher = defineEventPublisher(externalExchange, message, {
        routingKey: "external.event",
      });
      const { consumer, binding } = defineEventConsumer(eventPublisher, localQueue);

      // THEN
      expect(eventPublisher).toMatchObject({
        exchange: externalExchange,
        message,
      });
      expect(binding).toMatchObject({
        exchange: externalExchange,
      });
      expect(consumer.queue).toBe(localQueue);
    });

    it("should support using external queue with command pattern", () => {
      // GIVEN - External queue from another contract
      const externalQueue = defineQueue("external-queue");
      const localExchange = defineExchange("local-exchange", { type: "direct" });
      const message = defineMessage(z.object({ data: z.string() }));

      // WHEN - Use external queue with local exchange
      const command = defineCommandConsumer(externalQueue, localExchange, message, {
        routingKey: "local.route",
      });
      const publisher = defineCommandPublisher(command);

      // THEN
      expect(command.consumer.queue).toBe(externalQueue);
      expect(command.binding.queue).toBe(externalQueue);
      expect(publisher).toMatchObject({ exchange: localExchange });
    });

    it("should support mixed EventPublisherConfig and CommandConsumerConfig in a contract", () => {
      // GIVEN
      const ordersExchange = defineExchange("orders");
      const notificationsExchange = defineExchange("notifications", {
        type: "fanout",
      });
      const orderQueue = defineQueue("order-queue", { onPoison: "drop" });
      const notificationQueue = defineQueue("notification-queue", { onPoison: "drop" });

      const orderMessage = defineMessage(z.object({ orderId: z.string() }));
      const notificationMessage = defineMessage(z.object({ message: z.string() }));

      // WHEN - All configs go directly in publishers and consumers
      const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
        routingKey: "order.created",
      });

      const sendNotification = defineCommandConsumer(
        notificationQueue,
        notificationsExchange,
        notificationMessage,
      );

      const contract = defineContract({
        publishers: {
          // EventPublisherConfig auto-extracted to publisher
          orderCreated,
        },
        consumers: {
          // EventConsumerResult auto-extracted to consumer + binding
          processOrder: defineEventConsumer(orderCreated, orderQueue),
          // CommandConsumerConfig auto-extracted to consumer + binding
          sendNotification,
        },
      });

      // THEN - All configs are expanded and bindings auto-generated
      expect(contract).toMatchObject({
        publishers: {
          orderCreated: expect.objectContaining({
            exchange: ordersExchange,
            routingKey: "order.created",
          }),
        },
        consumers: {
          processOrder: expect.objectContaining({
            queue: orderQueue,
          }),
          sendNotification: expect.objectContaining({
            queue: notificationQueue,
          }),
        },
        bindings: {
          processOrderBinding: expect.objectContaining({
            type: "queue",
          }),
          sendNotificationBinding: expect.objectContaining({
            type: "queue",
          }),
        },
      });
    });

    it("keeps a TTL-backoff consumer queue out of the wait-queue business (derived, not stored)", () => {
      // GIVEN
      const dlx = defineExchange("orders-dlx", { type: "direct" });
      const ordersExchange = defineExchange("orders");
      const orderMessage = defineMessage(z.object({ orderId: z.string() }));
      const orderQueue = defineQueue("order-processing", {
        // The DLX is `direct`, so the binding below must match exactly — and
        // with ttl-backoff the arriving key is NOT the publisher's. A retried
        // message re-enters the main queue through the wait queue's
        // `x-dead-letter-routing-key` (the queue name), so from the second
        // delivery on its key is "order-processing", not "order.created".
        // Naming the dead-letter key here makes the DLX hop deterministic.
        deadLetter: { exchange: dlx, routingKey: "order-processing.dlq" },
        retry: { mode: "ttl-backoff", maxRetries: 5, initialDelayMs: 2000 },
      });

      const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
        routingKey: "order.created",
      });
      const orderDlq = defineQueue("order-processing-dlq");

      // WHEN
      const contract = defineContract({
        publishers: { orderCreated },
        consumers: {
          processOrder: defineEventConsumer(orderCreated, orderQueue),
        },
        queues: { orderDlq },
        bindings: {
          dlqBinding: defineQueueBinding(orderDlq, dlx, { routingKey: "order-processing.dlq" }),
        },
      });

      // THEN — the contract holds exactly the declared topology: the DLX is
      // auto-extracted, but NO wait queues, wait/retry exchanges, or retry
      // bindings appear (they are derived by setupAmqpTopology from the
      // queue's retry config).
      expect(Object.keys(contract.exchanges).sort()).toEqual(["orders", "orders-dlx"]);
      expect(Object.keys(contract.queues).sort()).toEqual([
        "order-processing",
        "order-processing-dlq",
      ]);
      expect(Object.keys(contract.bindings).sort()).toEqual(["dlqBinding", "processOrderBinding"]);

      expect(contract.queues["order-processing"]).toBe(orderQueue);
      expect(contract.bindings.processOrderBinding).toMatchObject({
        type: "queue",
        queue: orderQueue,
        exchange: ordersExchange,
        routingKey: "order.created",
      });
    });
  });

  describe("bridgeExchange support", () => {
    it("should create a bridged event consumer with topic exchange", () => {
      // GIVEN - Two domains: orders (source) and billing (local)
      const ordersExchange = defineExchange("orders");
      const billingExchange = defineExchange("billing");
      const message = defineMessage(z.object({ orderId: z.string() }));
      const billingQueue = defineQueue("billing-order-processing");

      // WHEN - Subscribe to orders events via bridge
      const orderCreated = defineEventPublisher(ordersExchange, message, {
        routingKey: "order.created",
      });
      const result = defineEventConsumer(orderCreated, billingQueue, {
        bridgeExchange: billingExchange,
      });

      // THEN - Queue binds to bridge, e2e binding from source → bridge
      expect(result).toMatchObject({
        [brand]: "EventConsumerResult",
        exchange: ordersExchange,
        bridgeExchange: billingExchange,
        binding: {
          type: "queue",
          queue: billingQueue,
          exchange: billingExchange,
          routingKey: "order.created",
        },
        exchangeBinding: {
          type: "exchange",
          source: ordersExchange,
          destination: billingExchange,
          routingKey: "order.created",
        },
      });
    });

    it("should create a bridged event consumer with fanout exchange", () => {
      // GIVEN
      const logsExchange = defineExchange("logs", { type: "fanout" });
      const analyticsExchange = defineExchange("analytics", { type: "fanout" });
      const message = defineMessage(z.object({ level: z.string() }));
      const analyticsQueue = defineQueue("analytics-logs");

      // WHEN
      const logEvent = defineEventPublisher(logsExchange, message);
      const result = defineEventConsumer(logEvent, analyticsQueue, {
        bridgeExchange: analyticsExchange,
      });

      // THEN
      expect(result).toMatchObject({
        bridgeExchange: analyticsExchange,
        binding: {
          type: "queue",
          queue: analyticsQueue,
          exchange: analyticsExchange,
        },
        exchangeBinding: {
          type: "exchange",
          source: logsExchange,
          destination: analyticsExchange,
        },
      });
    });

    it("should create a bridged event consumer with headers exchange", () => {
      // GIVEN
      const logsExchange = defineExchange("logs", { type: "headers" });
      const analyticsExchange = defineExchange("analytics", { type: "headers" });
      const message = defineMessage(z.object({ level: z.string() }));
      const analyticsQueue = defineQueue("analytics-logs");

      // WHEN
      const logEvent = defineEventPublisher(logsExchange, message);
      const result = defineEventConsumer(logEvent, analyticsQueue, {
        bridgeExchange: analyticsExchange,
      });

      // THEN
      expect(result).toMatchObject({
        bridgeExchange: analyticsExchange,
        binding: {
          type: "queue",
          queue: analyticsQueue,
          exchange: analyticsExchange,
        },
        exchangeBinding: {
          type: "exchange",
          source: logsExchange,
          destination: analyticsExchange,
        },
      });
    });

    it("should create a bridged command publisher with topic exchange", () => {
      // GIVEN - Remote domain owns the command consumer
      const remoteExchange = defineExchange("remote-commands");
      const localExchange = defineExchange("local-commands");
      const message = defineMessage(z.object({ taskId: z.string() }));
      const remoteQueue = defineQueue("remote-task-queue");

      // WHEN - Create command consumer on remote, publish via bridge
      const command = defineCommandConsumer(remoteQueue, remoteExchange, message, {
        routingKey: "task.execute",
      });
      const publisher = defineCommandPublisher(command, {
        bridgeExchange: localExchange,
      });

      // THEN - Publisher is bridged
      expect(isBridgedPublisherConfig(publisher)).toBe(true);
      expect(publisher).toMatchObject({
        [brand]: "BridgedPublisherConfig",
        bridgeExchange: localExchange,
        targetExchange: remoteExchange,
        publisher: {
          exchange: localExchange,
          message,
          routingKey: "task.execute",
        },
        exchangeBinding: {
          type: "exchange",
          source: localExchange,
          destination: remoteExchange,
          routingKey: "task.execute",
        },
      });
    });

    it("should extract bridged event consumer into contract", () => {
      // GIVEN
      const ordersExchange = defineExchange("orders");
      const billingExchange = defineExchange("billing");
      const message = defineMessage(z.object({ orderId: z.string() }));
      const billingQueue = defineQueue("billing-orders", { onPoison: "drop" });

      const orderCreated = defineEventPublisher(ordersExchange, message, {
        routingKey: "order.created",
      });

      // WHEN
      const contract = defineContract({
        consumers: {
          processOrder: defineEventConsumer(orderCreated, billingQueue, {
            bridgeExchange: billingExchange,
          }),
        },
      });

      // THEN - All resources extracted
      expect(contract).toMatchObject({
        exchanges: {
          orders: ordersExchange,
          billing: billingExchange,
        },
        queues: {
          "billing-orders": billingQueue,
        },
        bindings: {
          processOrderBinding: {
            type: "queue",
            queue: billingQueue,
            exchange: billingExchange,
            routingKey: "order.created",
          },
          processOrderExchangeBinding: {
            type: "exchange",
            source: ordersExchange,
            destination: billingExchange,
            routingKey: "order.created",
          },
        },
        consumers: {
          processOrder: {
            queue: billingQueue,
            message,
          },
        },
      });
    });

    it("should extract bridged command publisher into contract", () => {
      // GIVEN
      const remoteExchange = defineExchange("remote");
      const localExchange = defineExchange("local");
      const message = defineMessage(z.object({ id: z.string() }));
      const remoteQueue = defineQueue("remote-queue");

      const command = defineCommandConsumer(remoteQueue, remoteExchange, message, {
        routingKey: "cmd.run",
      });

      // WHEN
      const contract = defineContract({
        publishers: {
          runCommand: defineCommandPublisher(command, {
            bridgeExchange: localExchange,
            // The local domain only owns the bridge; the remote domain owns
            // `remote-queue` and its binding, so they are not in this contract.
            externalConsumers: true,
          }),
        },
      });

      // THEN - All resources extracted
      expect(contract).toMatchObject({
        exchanges: {
          local: localExchange,
          remote: remoteExchange,
        },
        bindings: {
          runCommandExchangeBinding: {
            type: "exchange",
            source: localExchange,
            destination: remoteExchange,
            routingKey: "cmd.run",
          },
        },
        publishers: {
          runCommand: {
            exchange: localExchange,
            message,
            routingKey: "cmd.run",
          },
        },
      });
    });

    it("should mix bridged and non-bridged entries in contract", () => {
      // GIVEN
      const ordersExchange = defineExchange("orders");
      const billingExchange = defineExchange("billing");
      const localExchange = defineExchange("local");
      const orderMessage = defineMessage(z.object({ orderId: z.string() }));
      const localMessage = defineMessage(z.object({ id: z.string() }));
      const billingQueue = defineQueue("billing-orders", { onPoison: "drop" });
      const localQueue = defineQueue("local-processing", { onPoison: "drop" });

      const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
        routingKey: "order.created",
      });
      const localEvent = defineEventPublisher(localExchange, localMessage, {
        routingKey: "local.event",
      });

      // WHEN - Mix bridged and non-bridged
      const contract = defineContract({
        publishers: {
          orderCreated,
          localEvent,
        },
        consumers: {
          // Bridged: subscribe to remote orders via billing exchange
          processBillingOrder: defineEventConsumer(orderCreated, billingQueue, {
            bridgeExchange: billingExchange,
          }),
          // Non-bridged: direct local consumer
          processLocal: defineEventConsumer(localEvent, localQueue),
        },
      });

      // THEN - Both types extracted correctly
      expect(contract).toMatchObject({
        exchanges: {
          orders: ordersExchange,
          billing: billingExchange,
          local: localExchange,
        },
        bindings: {
          processBillingOrderBinding: {
            type: "queue",
            exchange: billingExchange,
          },
          processBillingOrderExchangeBinding: {
            type: "exchange",
            source: ordersExchange,
            destination: billingExchange,
          },
          processLocalBinding: {
            type: "queue",
            exchange: localExchange,
          },
        },
      });
      // Non-bridged consumer should NOT have exchange binding
      expect(contract.bindings).not.toHaveProperty("processLocalExchangeBinding");
    });

    it("should not produce bridge fields when bridgeExchange is not provided", () => {
      // GIVEN
      const exchange = defineExchange("test");
      const message = defineMessage(z.object({ id: z.string() }));
      const queue = defineQueue("test-queue");

      const event = defineEventPublisher(exchange, message, { routingKey: "test.event" });

      // WHEN - No bridgeExchange
      const result = defineEventConsumer(event, queue);

      // THEN - No bridge fields
      expect(result).toMatchObject({
        exchangeBinding: undefined,
        bridgeExchange: undefined,
      });
    });
  });

  describe("defineContract collision detection", () => {
    it("dedupes the same exchange referenced by both a publisher and a consumer", () => {
      const exchange = defineExchange("orders");
      const queue = defineQueue("order-processing", { onPoison: "drop" });
      const message = defineMessage(z.object({ orderId: z.string() }));
      const event = defineEventPublisher(exchange, message, { routingKey: "order.created" });

      const contract = defineContract({
        publishers: { orderCreated: event },
        consumers: { processOrder: defineEventConsumer(event, queue) },
      });

      expect(Object.keys(contract.exchanges)).toEqual(["orders"]);
    });

    it("throws when two publishers declare the same exchange name with different types", () => {
      const topicExchange = defineExchange("orders", { type: "topic" });
      const directExchange = defineExchange("orders", { type: "direct" });
      const message = defineMessage(z.object({ orderId: z.string() }));
      const a = defineEventPublisher(topicExchange, message, { routingKey: "order.created" });
      const b = defineEventPublisher(directExchange, message, { routingKey: "order.created" });

      expect(() =>
        defineContract({
          publishers: { a, b },
        }),
      ).toThrow(/exchange "orders" was declared with conflicting definitions/);
    });

    it("throws when two consumers declare the same queue with different retry config", () => {
      const exchange = defineExchange("orders");
      const message = defineMessage(z.object({ orderId: z.string() }));
      const event = defineEventPublisher(exchange, message, { routingKey: "order.created" });

      const queueA = defineQueue("processing", {
        retry: { mode: "immediate-requeue", maxRetries: 3 },
      });
      const queueB = defineQueue("processing", {
        retry: { mode: "immediate-requeue", maxRetries: 10 },
      });

      expect(() =>
        defineContract({
          publishers: { ev: event },
          consumers: {
            a: defineEventConsumer(event, queueA),
            b: defineEventConsumer(event, queueB),
          },
        }),
      ).toThrow(/queue "processing" was declared with conflicting definitions/);
    });
  });
});
