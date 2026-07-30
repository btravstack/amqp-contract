import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import { ErrAsync, OkAsync } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

import { RetryableError } from "../errors.js";
import { it } from "./fixture.js";

describe("Worker Retry Mechanism", () => {
  describe("Retry with Exponential Backoff", () => {
    it("should route retried message through wait queue with TTL", async ({
      workerFactory,
      publishMessage,
      amqpChannel,
    }) => {
      // GIVEN a worker with retry configuration
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("retry-flow-exchange", { durable: false });
      const dlx = defineExchange("retry-flow-dlx", { durable: false });
      const queue = defineQueue("retry-flow-queue", {
        type: "classic",
        durable: false,
        deadLetter: {
          exchange: dlx,
          routingKey: "retry-flow-queue.dlq",
        },
        retry: {
          mode: "ttl-backoff",
          maxRetries: 3,
          initialDelayMs: 500,
          maxDelayMs: 5000,
          backoffMultiplier: 2,
          jitter: false, // Disable jitter for predictable testing
        },
      });

      const testMessage = defineMessage(TestMessage);
      const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

      const contract = defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
        },
      });

      let attemptCount = 0;
      await workerFactory(contract, {
        testConsumer: () => {
          attemptCount++;
          if (attemptCount === 1) {
            return ErrAsync(new RetryableError("First attempt failed"));
          }
          return OkAsync(undefined);
        },
      });

      // Verify the first delay tier's wait queue was created (one wait queue
      // per distinct backoff delay: 500ms, 1000ms, 2000ms)
      const waitQueueInfo = await amqpChannel.checkQueue("retry-flow-queue-wait-500ms");
      expect(waitQueueInfo.queue).toBe("retry-flow-queue-wait-500ms");

      // WHEN publishing a message that fails on first attempt
      publishMessage(exchange.name, "test.message", { id: "retry-1" });

      // THEN wait for first processing attempt
      await vi.waitFor(
        () => {
          if (attemptCount < 1) {
            throw new Error("Message not yet processed");
          }
        },
        { timeout: 2000 },
      );

      expect(attemptCount).toBe(1);

      // AND message should appear in wait queue with correct headers and TTL
      await vi.waitFor(
        async () => {
          const waitMsg = await amqpChannel.get("retry-flow-queue-wait-500ms", { noAck: false });
          if (!waitMsg) {
            throw new Error("Message not in wait queue");
          }

          expect(waitMsg.properties).toMatchObject({
            expiration: "500",
            headers: expect.objectContaining({
              "x-retry-count": 1,
              "x-last-error": "First attempt failed",
            }),
          });
          expect(waitMsg.properties.headers?.["x-first-failure-timestamp"]).toBeDefined();

          // Nack to return message for retry
          amqpChannel.nack(waitMsg, false, true);
        },
        { timeout: 2000 },
      );

      // AND after TTL expires, message should be retried successfully
      await vi.waitFor(
        () => {
          if (attemptCount < 2) {
            throw new Error("Message not yet retried");
          }
        },
        { timeout: 3000 },
      );

      expect(attemptCount).toBe(2);
    });

    it("should apply exponential backoff with configurable parameters", async ({
      workerFactory,
      publishMessage,
      amqpChannel,
    }) => {
      // GIVEN a worker with custom backoff configuration
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("backoff-exchange", { durable: false });
      const dlx = defineExchange("backoff-dlx", { durable: false });
      const queue = defineQueue("backoff-queue", {
        type: "classic",
        durable: false,
        deadLetter: {
          exchange: dlx,
          routingKey: "backoff-queue.dlq",
        },
        retry: {
          mode: "ttl-backoff",
          maxRetries: 3,
          initialDelayMs: 100,
          maxDelayMs: 1000,
          backoffMultiplier: 3,
          jitter: false,
        },
      });

      const testMessage = defineMessage(TestMessage);
      const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

      const contract = defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
        },
      });

      await workerFactory(contract, {
        testConsumer: () => ErrAsync(new RetryableError("Always fails")),
      });

      // Set up DLQ manually for verification (after worker creates the DLX exchange)
      await amqpChannel.assertQueue("backoff-dlq", { durable: false });
      await amqpChannel.bindQueue("backoff-dlq", dlx.name, "backoff-queue.dlq");

      // WHEN publishing a message that always fails
      publishMessage(exchange.name, "test.message", { id: "backoff-1" });

      // THEN each retry should have exponentially increasing TTL: 100, 300, 900
      // — and land in its own per-delay-tier wait queue.
      const expectedDelays = [100, 300, 900]; // 100 * 3^0, 100 * 3^1, 100 * 3^2

      for (let i = 0; i < expectedDelays.length; i++) {
        await vi.waitFor(
          async () => {
            const waitMsg = await amqpChannel.get(`backoff-queue-wait-${expectedDelays[i]}ms`, {
              noAck: false,
            });
            if (!waitMsg) {
              throw new Error(`Retry ${i + 1} not in wait queue`);
            }

            const expectedDelay = expectedDelays[i]!; // Safe: i is within array bounds
            expect(waitMsg.properties).toMatchObject({
              expiration: expectedDelay.toString(),
              headers: expect.objectContaining({
                "x-retry-count": i + 1,
              }),
            });

            // Nack to trigger next retry
            amqpChannel.nack(waitMsg, false, false);
          },
          { timeout: 2000 },
        );
      }

      // AND after max retries, message should go to DLQ
      await vi.waitFor(
        async () => {
          const dlqMsg = await amqpChannel.get("backoff-dlq", { noAck: false });
          if (!dlqMsg) {
            throw new Error("Message not in DLQ");
          }
          amqpChannel.ack(dlqMsg);
        },
        { timeout: 2000 },
      );
    });
  });

  describe("Max Retries", () => {
    it("should send to DLQ after max retries exceeded", async ({
      workerFactory,
      publishMessage,
      amqpChannel,
    }) => {
      // GIVEN a worker with maxRetries set to 2
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("maxretry-exchange", { durable: false });
      const dlx = defineExchange("maxretry-dlx", { durable: false });
      const queue = defineQueue("maxretry-queue", {
        type: "classic",
        durable: false,
        deadLetter: {
          exchange: dlx,
          routingKey: "maxretry-queue.dlq",
        },
        retry: {
          mode: "ttl-backoff",
          maxRetries: 2,
          initialDelayMs: 100,
          maxDelayMs: 500,
          backoffMultiplier: 2,
          jitter: false,
        },
      });

      const testMessage = defineMessage(TestMessage);
      const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

      const contract = defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
        },
      });

      let attemptCount = 0;
      await workerFactory(contract, {
        testConsumer: () => {
          attemptCount++;
          return ErrAsync(new RetryableError("Always fails"));
        },
      });

      // Set up DLQ manually for verification (after worker creates the DLX exchange)
      await amqpChannel.assertQueue("maxretry-dlq", { durable: false });
      await amqpChannel.bindQueue("maxretry-dlq", dlx.name, "maxretry-queue.dlq");

      // WHEN publishing a message that always fails
      publishMessage(exchange.name, "test.message", { id: "maxretry-1" });

      // THEN should retry exactly maxRetries times (initial attempt + 2 retries = 3 total)
      await vi.waitFor(
        () => {
          if (attemptCount < 3) {
            throw new Error("Not all retry attempts completed");
          }
        },
        { timeout: 5000 },
      );

      expect(attemptCount).toBe(3);

      // AND message should end up in DLQ
      await vi.waitFor(
        async () => {
          const dlqMsg = await amqpChannel.get("maxretry-dlq", { noAck: false });
          if (!dlqMsg) {
            throw new Error("Message not in DLQ");
          }
          const content = JSON.parse(dlqMsg.content.toString());
          expect(content).toEqual({ id: "maxretry-1" });
          amqpChannel.ack(dlqMsg);
        },
        { timeout: 2000 },
      );
    });
  });

  describe("Retry Headers Tracking", () => {
    it("should track retry count, last error, and first failure timestamp", async ({
      workerFactory,
      publishMessage,
      amqpChannel,
    }) => {
      // GIVEN a worker that always fails
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("headers-exchange", { durable: false });
      const dlx = defineExchange("headers-dlx", { durable: false });
      const queue = defineQueue("headers-queue", {
        type: "classic",
        durable: false,
        deadLetter: {
          exchange: dlx,
          routingKey: "headers-queue.dlq",
        },
        retry: {
          mode: "ttl-backoff",
          maxRetries: 1,
          initialDelayMs: 100,
          jitter: false,
        },
      });

      const testMessage = defineMessage(TestMessage);
      const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

      const contract = defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
        },
      });

      await workerFactory(contract, {
        testConsumer: () => ErrAsync(new RetryableError("Test error message")),
      });

      // WHEN publishing a message that fails
      const startTime = Date.now();
      publishMessage(exchange.name, "test.message", { id: "headers-1" });

      // THEN message in wait queue should have retry tracking headers
      await vi.waitFor(
        async () => {
          const waitMsg = await amqpChannel.get("headers-queue-wait-100ms", { noAck: false });
          if (!waitMsg) {
            throw new Error("Message not in wait queue");
          }

          const firstFailureTimestamp = waitMsg.properties.headers?.["x-first-failure-timestamp"];
          expect(waitMsg.properties.headers).toMatchObject({
            "x-retry-count": 1,
            "x-last-error": "Test error message",
          });
          expect(firstFailureTimestamp).toBeGreaterThanOrEqual(startTime);
          expect(firstFailureTimestamp).toBeLessThanOrEqual(Date.now());

          // Nack to let it go to DLQ
          amqpChannel.nack(waitMsg, false, false);
        },
        { timeout: 2000 },
      );
    });
  });

  describe("Queue Without DLX", () => {
    it("should not retry by default when queue has no DLX (none retry mode)", async ({
      workerFactory,
      publishMessage,
    }) => {
      // GIVEN a queue without dead letter exchange configuration
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("nodlx-exchange", { durable: false });
      const queue = defineQueue("nodlx-queue", {
        type: "classic",
        durable: false,
        // No deadLetter configuration
      });

      const testMessage = defineMessage(TestMessage);
      const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

      const contract = defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
        },
      });

      let attemptCount = 0;
      await workerFactory(contract, {
        testConsumer: () => {
          attemptCount++;
          return ErrAsync(new RetryableError("Will not retry"));
        },
      });

      // WHEN publishing a message that fails on first attempt
      publishMessage(exchange.name, "test.message", { id: "nodlx-1" });

      // THEN should process only once and not retry
      await vi.waitFor(
        () => {
          if (attemptCount < 1) {
            throw new Error("Message not yet processed");
          }
        },
        { timeout: 2000 },
      );

      expect(attemptCount).toBe(1);
    });
  });

  describe("None Retry Mode", () => {
    it("should send failed message directly to DLQ and not retry", async ({
      workerFactory,
      publishMessage,
      amqpChannel,
    }) => {
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("none-retry-exchange", { durable: false });
      const dlx = defineExchange("none-retry-dlx", { durable: false });
      const queue = defineQueue("none-retry-queue", {
        type: "classic",
        durable: false,
        deadLetter: {
          exchange: dlx,
          routingKey: "none-retry-queue.dlq",
        },
        retry: { mode: "none" },
      });

      const testMessage = defineMessage(TestMessage);
      const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

      const contract = defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
        },
      });

      let attemptCount = 0;
      await workerFactory(contract, {
        testConsumer: () => {
          attemptCount++;
          return ErrAsync(new RetryableError("No retry"));
        },
      });

      await amqpChannel.assertQueue("none-retry-dlq", { durable: false });
      await amqpChannel.bindQueue("none-retry-dlq", dlx.name, "none-retry-queue.dlq");

      publishMessage(exchange.name, "test.message", { id: "none-1" });

      await vi.waitFor(
        () => {
          if (attemptCount < 1) {
            throw new Error("Message not yet processed");
          }
        },
        { timeout: 2000 },
      );

      expect(attemptCount).toBe(1);

      await vi.waitFor(
        async () => {
          const dlqMsg = await amqpChannel.get("none-retry-dlq", { noAck: false });
          if (!dlqMsg) {
            throw new Error("Message not in DLQ");
          }
          const content = JSON.parse(dlqMsg.content.toString());
          expect(content).toEqual({ id: "none-1" });
          amqpChannel.ack(dlqMsg);
        },
        { timeout: 2000 },
      );
    });
  });

  describe("Immediate Requeue Retry Mode", () => {
    describe("For quorum queues", () => {
      it("should requeue message immediately on failure", async ({
        workerFactory,
        publishMessage,
      }) => {
        // GIVEN a quorum queue with immediate-requeue retry configured
        const TestMessage = z.object({ id: z.string() });

        const exchange = defineExchange("quorum-exchange");
        const dlx = defineExchange("quorum-dlx");

        const queue = defineQueue("quorum-queue", {
          type: "quorum",
          deadLetter: {
            exchange: dlx,
            routingKey: "quorum-queue.dlq",
          },
          // Retry config with immediate-requeue mode
          retry: {
            mode: "immediate-requeue",
            maxRetries: 3, // Allow up to 3 retry attempts before dead-lettering
          },
        });

        const testMessage = defineMessage(TestMessage);
        const testEvent = defineEventPublisher(exchange, testMessage, {
          routingKey: "test.message",
        });

        const contract = defineContract({
          publishers: { testPublisher: testEvent },
          consumers: {
            testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
          },
        });

        let attemptCount = 0;
        await workerFactory(contract, {
          testConsumer: () => {
            attemptCount++;
            if (attemptCount < 2) {
              // This triggers a requeue in immediate-requeue mode
              return ErrAsync(new RetryableError("Simulated failure"));
            }
            return OkAsync(undefined);
          },
        });

        // WHEN publishing a message that fails on first attempt
        publishMessage(exchange.name, "test.message", { id: "quorum-1" });

        // THEN message should be requeued immediately and succeed on second attempt
        await vi.waitFor(
          () => {
            if (attemptCount < 2) {
              throw new Error("Message not yet processed twice");
            }
          },
          { timeout: 5000 },
        );

        expect(attemptCount).toBe(2);
      });

      it("should send message to DLQ after exceeding maxRetries", async ({
        workerFactory,
        publishMessage,
        amqpChannel,
      }) => {
        // GIVEN a quorum queue with immediate-requeue retry configured
        const TestMessage = z.object({ id: z.string() });

        const exchange = defineExchange("quorum-dlq-exchange");
        const dlx = defineExchange("quorum-dlq-dlx");

        const queue = defineQueue("quorum-dlq-queue", {
          type: "quorum",
          deadLetter: {
            exchange: dlx,
            routingKey: "quorum-dlq-queue.dlq",
          },
          // Retry config with immediate-requeue mode
          retry: {
            mode: "immediate-requeue",
            maxRetries: 2, // Message dead-lettered after 2 retry attempts
          },
        });

        const testMessage = defineMessage(TestMessage);
        const testEvent = defineEventPublisher(exchange, testMessage, {
          routingKey: "test.message",
        });

        const contract = defineContract({
          publishers: { testPublisher: testEvent },
          consumers: {
            testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
          },
        });

        let attemptCount = 0;
        await workerFactory(contract, {
          testConsumer: () => {
            attemptCount++;
            // Always fail - message should be dead-lettered after exceeding maxRetries
            return ErrAsync(new RetryableError("Always fails"));
          },
        });

        // Set up DLQ manually for verification (after worker creates the DLX exchange)
        await amqpChannel.assertQueue("quorum-dlq-dlq", { durable: true });
        await amqpChannel.bindQueue("quorum-dlq-dlq", dlx.name, "quorum-dlq-queue.dlq");

        // WHEN publishing a message that always fails
        publishMessage(exchange.name, "test.message", { id: "quorum-dlq-1" });

        // THEN message should be dead-lettered after exceeding maxRetries
        // Wait for the message to appear in DLQ
        await vi.waitFor(
          async () => {
            const dlqMsg = await amqpChannel.get("quorum-dlq-dlq", { noAck: false });
            if (!dlqMsg) {
              throw new Error("Message not in DLQ yet");
            }
            const content = JSON.parse(dlqMsg.content.toString());
            expect(content).toEqual({ id: "quorum-dlq-1" });
            amqpChannel.ack(dlqMsg);
          },
          { timeout: 10000 },
        );

        // Message should retry exactly maxRetries times (initial attempt + 2 retries = 3 total)
        expect(attemptCount).toBe(3);
      });
    });

    describe("For classic queues", () => {
      it("should requeue message immediately on failure", async ({
        workerFactory,
        publishMessage,
      }) => {
        // GIVEN a classic queue with immediate-requeue retry configured
        const TestMessage = z.object({ id: z.string() });

        const exchange = defineExchange("classic-exchange", { durable: false });
        const dlx = defineExchange("classic-dlx", { durable: false });

        const queue = defineQueue("classic-queue", {
          type: "classic",
          durable: false,
          deadLetter: {
            exchange: dlx,
            routingKey: "classic-queue.dlq",
          },
          // Retry config with immediate-requeue mode
          retry: {
            mode: "immediate-requeue",
            maxRetries: 3, // Allow up to 3 retry attempts before dead-lettering
          },
        });

        const testMessage = defineMessage(TestMessage);
        const testEvent = defineEventPublisher(exchange, testMessage, {
          routingKey: "test.message",
        });

        const contract = defineContract({
          publishers: { testPublisher: testEvent },
          consumers: {
            testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
          },
        });

        let attemptCount = 0;
        await workerFactory(contract, {
          testConsumer: () => {
            attemptCount++;
            if (attemptCount < 2) {
              // This triggers a requeue in immediate-requeue mode
              return ErrAsync(new RetryableError("Simulated failure"));
            }
            return OkAsync(undefined);
          },
        });

        // WHEN publishing a message that fails on first attempt
        publishMessage(exchange.name, "test.message", { id: "classic-1" });

        // THEN message should be requeued immediately and succeed on second attempt
        await vi.waitFor(
          () => {
            if (attemptCount < 2) {
              throw new Error("Message not yet processed twice");
            }
          },
          { timeout: 5000 },
        );

        expect(attemptCount).toBe(2);
      });

      it("should send message to DLQ after exceeding maxRetries", async ({
        workerFactory,
        publishMessage,
        amqpChannel,
      }) => {
        // GIVEN a classic queue with immediate-requeue retry configured
        const TestMessage = z.object({ id: z.string() });

        const exchange = defineExchange("classic-dlq-exchange", { durable: false });
        const dlx = defineExchange("classic-dlq-dlx", { durable: false });

        const queue = defineQueue("classic-dlq-queue", {
          type: "classic",
          durable: false,
          deadLetter: {
            exchange: dlx,
            routingKey: "classic-dlq-queue.dlq",
          },
          // Retry config with immediate-requeue mode
          retry: {
            mode: "immediate-requeue",
            maxRetries: 2, // Message dead-lettered after 2 retry attempts
          },
        });

        const testMessage = defineMessage(TestMessage);
        const testEvent = defineEventPublisher(exchange, testMessage, {
          routingKey: "test.message",
        });

        const contract = defineContract({
          publishers: { testPublisher: testEvent },
          consumers: {
            testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
          },
        });

        let attemptCount = 0;
        await workerFactory(contract, {
          testConsumer: () => {
            attemptCount++;
            // Always fail - message should be dead-lettered after exceeding maxRetries
            return ErrAsync(new RetryableError("Always fails"));
          },
        });

        // Set up DLQ manually for verification (after worker creates the DLX exchange)
        await amqpChannel.assertQueue("classic-dlq-dlq", { durable: false });
        await amqpChannel.bindQueue("classic-dlq-dlq", dlx.name, "classic-dlq-queue.dlq");

        // WHEN publishing a message that always fails
        publishMessage(exchange.name, "test.message", { id: "classic-dlq-1" });

        // THEN message should be dead-lettered after exceeding maxRetries
        // Wait for the message to appear in DLQ
        await vi.waitFor(
          async () => {
            const dlqMsg = await amqpChannel.get("classic-dlq-dlq", { noAck: false });
            if (!dlqMsg) {
              throw new Error("Message not in DLQ yet");
            }
            const content = JSON.parse(dlqMsg.content.toString());
            expect(content).toEqual({ id: "classic-dlq-1" });
            amqpChannel.ack(dlqMsg);
          },
          { timeout: 10000 },
        );

        // Message should retry exactly maxRetries times (initial attempt + 2 retries = 3 total)
        expect(attemptCount).toBe(3);
      });

      it("should handle classic queue exclusive mode with immediate-requeue", async ({
        workerFactory,
        publishMessage,
      }) => {
        // GIVEN an exclusive classic queue with immediate-requeue retry
        const TestMessage = z.object({ id: z.string() });

        const exchange = defineExchange("exclusive-exchange", { durable: false });
        const dlx = defineExchange("exclusive-dlx", { durable: false });

        const queue = defineQueue("exclusive-queue", {
          type: "classic",
          exclusive: true,
          durable: false,
          deadLetter: {
            exchange: dlx,
            routingKey: "exclusive-queue.dlq",
          },
          retry: {
            mode: "immediate-requeue",
            maxRetries: 2,
          },
        });

        const testMessage = defineMessage(TestMessage);
        const testEvent = defineEventPublisher(exchange, testMessage, {
          routingKey: "test.message",
        });

        const contract = defineContract({
          publishers: { testPublisher: testEvent },
          consumers: {
            testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
          },
        });

        let attemptCount = 0;
        await workerFactory(contract, {
          testConsumer: () => {
            attemptCount++;
            if (attemptCount < 2) {
              return ErrAsync(new RetryableError("Fail once"));
            }
            return OkAsync(undefined);
          },
        });

        // WHEN publishing a message to exclusive queue
        publishMessage(exchange.name, "test.message", { id: "exclusive-1" });

        // THEN should process with immediate-requeue retry
        await vi.waitFor(
          () => {
            if (attemptCount < 2) {
              throw new Error("Message not yet processed twice");
            }
          },
          { timeout: 5000 },
        );

        expect(attemptCount).toBe(2);
      });

      it("should track retry count, last error, and first failure timestamp", async ({
        workerFactory,
        publishMessage,
      }) => {
        // GIVEN a classic queue with immediate-requeue retry
        const TestMessage = z.object({ id: z.string() });

        const exchange = defineExchange("headers-exchange", { durable: false });
        const dlx = defineExchange("headers-dlx", { durable: false });

        const queue = defineQueue("headers-queue", {
          type: "classic",
          durable: false,
          deadLetter: {
            exchange: dlx,
            routingKey: "headers-queue.dlq",
          },
          retry: {
            mode: "immediate-requeue",
            maxRetries: 2,
          },
        });

        const testMessage = defineMessage(TestMessage);
        const testEvent = defineEventPublisher(exchange, testMessage, {
          routingKey: "test.message",
        });

        const contract = defineContract({
          publishers: { testPublisher: testEvent },
          consumers: {
            testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
          },
        });

        let attemptCount = 0;
        const retryCountHeaders: Array<number> = [];
        const lastErrorHeaders: string[] = [];
        const firstFailureTimestampHeaders: number[] = [];

        await workerFactory(contract, {
          testConsumer: (_, msg) => {
            attemptCount++;
            const retryCount = (msg.properties.headers?.["x-retry-count"] as number) ?? 0;
            const lastError = msg.properties.headers?.["x-last-error"] as string | undefined;
            const firstFailureTimestamp = msg.properties.headers?.[
              "x-first-failure-timestamp"
            ] as number;
            retryCountHeaders.push(retryCount);
            if (lastError) {
              lastErrorHeaders.push(lastError);
            }
            if (firstFailureTimestamp) {
              firstFailureTimestampHeaders.push(firstFailureTimestamp);
            }

            // Fail first two attempts to trigger retries
            if (attemptCount === 1) {
              return ErrAsync(new RetryableError("First failure"));
            } else if (attemptCount === 2) {
              return ErrAsync(new RetryableError("Second failure"));
            }

            // Succeed on third attempt
            return OkAsync(undefined);
          },
        });

        // WHEN publishing a message that will be retried
        const startTime = Date.now();
        publishMessage(exchange.name, "test.message", { id: "headers-1" });

        // THEN should track retry headers
        await vi.waitFor(
          () => {
            if (attemptCount < 3) {
              throw new Error("Not all processing attempts completed");
            }
          },
          { timeout: 5000 },
        );

        // Verify retry count progression: 0 (initial) -> 1 (first retry) -> 2 (second retry)
        expect(retryCountHeaders).toEqual([0, 1, 2]);

        // Verify last error messages are captured correctly in headers for each retry attempt
        expect(lastErrorHeaders).toEqual(["First failure", "Second failure"]);

        // Verify first failure timestamp is set on first retry and remains the same for subsequent retries
        expect(firstFailureTimestampHeaders.length).toBe(2);
        expect(firstFailureTimestampHeaders[0]).toBe(firstFailureTimestampHeaders[1]);
        expect(firstFailureTimestampHeaders[0]).toBeGreaterThanOrEqual(startTime);
        expect(firstFailureTimestampHeaders[0]).toBeLessThanOrEqual(Date.now());
      });
    });
  });

  describe("TTL-Backoff Retry without Dead Letter Exchange", () => {
    it("should retry via the per-tier wait queue and the default exchange", async ({
      workerFactory,
      publishMessage,
      amqpChannel,
    }) => {
      // GIVEN a worker with TTL-backoff retry configured without DLX
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("headers-retry-exchange", { durable: false });
      const queue = defineQueue("headers-retry-queue", {
        type: "classic",
        durable: false,
        // No deadLetter configuration
        retry: {
          mode: "ttl-backoff",
          maxRetries: 2,
          initialDelayMs: 200,
          maxDelayMs: 1000,
          backoffMultiplier: 2,
          jitter: false,
        },
      });

      const testMessage = defineMessage(TestMessage);
      const testEvent = defineEventPublisher(exchange, testMessage, {
        routingKey: "test.message",
      });

      const contract = defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
        },
      });

      let attemptCount = 0;
      await workerFactory(contract, {
        testConsumer: () => {
          attemptCount++;
          if (attemptCount === 1) {
            return ErrAsync(new RetryableError("First attempt failed"));
          }
          return OkAsync(undefined);
        },
      });

      // WHEN publishing a message that fails on first attempt
      publishMessage(exchange.name, "test.message", { id: "headers-retry-1" });

      // THEN wait for first processing attempt
      await vi.waitFor(
        () => {
          if (attemptCount < 1) {
            throw new Error("Message not yet processed");
          }
        },
        { timeout: 2000 },
      );

      expect(attemptCount).toBe(1);

      // AND message should appear in wait queue with correct headers and TTL
      await vi.waitFor(
        async () => {
          const waitMsg = await amqpChannel.get("headers-retry-queue-wait-200ms", {
            noAck: false,
          });
          if (!waitMsg) {
            throw new Error("Message not in wait queue");
          }

          expect(waitMsg.properties).toMatchObject({
            expiration: "200", // initialDelayMs
            headers: expect.objectContaining({
              "x-retry-count": 1,
              "x-last-error": "First attempt failed",
              "x-original-routing-key": "test.message",
            }),
          });
          expect(waitMsg.properties.headers?.["x-first-failure-timestamp"]).toBeDefined();

          // Nack to return message for retry
          amqpChannel.nack(waitMsg, false, true);
        },
        { timeout: 2000 },
      );

      // AND after TTL expires, message should be retried successfully
      await vi.waitFor(
        () => {
          if (attemptCount < 2) {
            throw new Error("Message not yet retried");
          }
        },
        { timeout: 3000 },
      );

      expect(attemptCount).toBe(2);
    });

    it("should preserve the original routing key in the x-original-routing-key header", async ({
      workerFactory,
      publishMessage,
    }) => {
      // GIVEN a worker with TTL-backoff retry. The retry hop goes through the
      // default exchange (publish to the tier wait queue, dead-letter back to
      // the main queue), so the redelivered `fields.routingKey` is the queue
      // name — the original routing key travels in `x-original-routing-key`.
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("routing-key-exchange", { durable: false });
      const queue = defineQueue("routing-key-queue", {
        type: "classic",
        durable: false,
        retry: {
          mode: "ttl-backoff",
          maxRetries: 1,
          initialDelayMs: 100,
          jitter: false,
        },
      });

      const testMessage = defineMessage(TestMessage);
      const testEvent = defineEventPublisher(exchange, testMessage, {
        routingKey: "orders.created",
      });

      const contract = defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "orders.#" }),
        },
      });

      let attemptCount = 0;
      const capturedRoutingKeys: string[] = [];
      const capturedOriginalRoutingKeys: Array<unknown> = [];

      await workerFactory(contract, {
        testConsumer: (_, msg) => {
          attemptCount++;
          capturedRoutingKeys.push(msg.fields.routingKey);
          capturedOriginalRoutingKeys.push(msg.properties.headers?.["x-original-routing-key"]);
          if (attemptCount === 1) {
            return ErrAsync(new RetryableError("First attempt failed"));
          }
          return OkAsync(undefined);
        },
      });

      // WHEN publishing a message with specific routing key that fails on first attempt
      publishMessage(exchange.name, "orders.created", { id: "routing-key-1" });

      // THEN wait for both attempts to complete
      await vi.waitFor(
        () => {
          if (attemptCount < 2) {
            throw new Error("Message not yet retried");
          }
        },
        { timeout: 3000 },
      );

      // AND the first delivery carries the published routing key; the retried
      // delivery arrives via the default exchange under the queue name, with
      // the original key preserved in the header.
      expect(capturedRoutingKeys).toEqual(["orders.created", "routing-key-queue"]);
      expect(capturedOriginalRoutingKeys).toEqual([undefined, "orders.created"]);
      expect(attemptCount).toBe(2);
    });

    it("should handle max retries exceeded without a dead letter exchange", async ({
      workerFactory,
      publishMessage,
    }) => {
      // GIVEN a worker with TTL-backoff retry and low maxRetries
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("max-retries-exchange", { durable: false });
      const queue = defineQueue("max-retries-queue", {
        type: "classic",
        durable: false,
        retry: {
          mode: "ttl-backoff",
          maxRetries: 1, // Only allow 1 retry
          initialDelayMs: 100,
          jitter: false,
        },
      });

      const testMessage = defineMessage(TestMessage);
      const testEvent = defineEventPublisher(exchange, testMessage, {
        routingKey: "test.message",
      });

      const contract = defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
        },
      });

      let attemptCount = 0;
      await workerFactory(contract, {
        testConsumer: () => {
          attemptCount++;
          // Always fail
          return ErrAsync(new RetryableError("Always fails"));
        },
      });

      // WHEN publishing a message that always fails
      publishMessage(exchange.name, "test.message", { id: "max-retries-1" });

      // THEN should attempt exactly maxRetries + 1 times (initial + retries)
      await vi.waitFor(
        () => {
          if (attemptCount < 2) {
            throw new Error("Not all retry attempts completed");
          }
        },
        { timeout: 3000 },
      );

      expect(attemptCount).toBe(2);

      // AND since no DLX is configured, the message should be lost (nacked without requeue)
      // This is expected behavior when no DLX is configured and max retries exceeded
    });
  });

  describe("Validation failures bypass the retry pipeline", () => {
    it("INVARIANT: an invalid payload on a retry-configured queue goes straight to the DLQ with zero retries", async ({
      workerFactory,
      publishMessage,
      amqpChannel,
    }) => {
      // GIVEN a queue WITH retry configured — the invariant is that a
      // deterministic poison message must NOT ride the retry pipeline (it
      // would burn the retry budget on guaranteed failures) but land in the
      // DLQ on the first delivery.
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("poison-exchange", { durable: false });
      const dlx = defineExchange("poison-dlx", { durable: false });
      const queue = defineQueue("poison-queue", {
        type: "classic",
        durable: false,
        deadLetter: {
          exchange: dlx,
          routingKey: "poison-queue.dlq",
        },
        retry: {
          mode: "ttl-backoff",
          maxRetries: 3,
          initialDelayMs: 100,
          maxDelayMs: 1000,
          backoffMultiplier: 2,
          jitter: false,
        },
      });

      const testMessage = defineMessage(TestMessage);
      const testEvent = defineEventPublisher(exchange, testMessage, { routingKey: "test.message" });

      const contract = defineContract({
        publishers: { testPublisher: testEvent },
        consumers: {
          testConsumer: defineEventConsumer(testEvent, queue, { routingKey: "test.#" }),
        },
      });

      let handlerCalls = 0;
      await workerFactory(contract, {
        testConsumer: () => {
          handlerCalls++;
          return OkAsync(undefined);
        },
      });

      await amqpChannel.assertQueue("poison-dlq", { durable: false });
      await amqpChannel.bindQueue("poison-dlq", dlx.name, "poison-queue.dlq");

      // WHEN publishing a payload that fails schema validation (id must be a string)
      publishMessage(exchange.name, "test.message", { id: 12345 });

      // THEN the message lands in the DLQ on the first delivery
      const dlqMsg = await vi.waitFor(
        async () => {
          const msg = await amqpChannel.get("poison-dlq", { noAck: false });
          if (!msg) {
            throw new Error("Poison message not yet in DLQ");
          }
          return msg;
        },
        { timeout: 3000 },
      );
      amqpChannel.ack(dlqMsg);

      // AND it never entered the retry pipeline: no retry accounting header,
      // and the wait queue never saw it.
      expect(dlqMsg.properties.headers?.["x-retry-count"]).toBeUndefined();
      const waitQueue = await amqpChannel.checkQueue("poison-queue-wait-100ms");
      expect(waitQueue.messageCount).toBe(0);

      // AND the handler never ran.
      expect(handlerCalls).toBe(0);
    });
  });
});
