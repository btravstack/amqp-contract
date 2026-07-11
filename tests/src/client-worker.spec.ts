import { TypedAmqpClient } from "@amqp-contract/client";
import {
  ContractDefinition,
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import { it as baseIt } from "@amqp-contract/testing/extension";
import { TypedAmqpWorker, type WorkerInferHandlers, defineHandlers } from "@amqp-contract/worker";
import { Ok } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

const it = baseIt.extend<{
  clientFactory: <TContract extends ContractDefinition>(
    contract: TContract,
  ) => Promise<TypedAmqpClient<TContract>>;
  workerFactory: <TContract extends ContractDefinition>(
    contract: TContract,
    handlers: WorkerInferHandlers<TContract>,
  ) => Promise<TypedAmqpWorker<TContract>>;
}>({
  clientFactory: async ({ amqpConnectionUrl }, use) => {
    const clients: Array<TypedAmqpClient<ContractDefinition>> = [];

    try {
      await use(async <TContract extends ContractDefinition>(contract: TContract) => {
        const client = (
          await TypedAmqpClient.create({
            contract,
            urls: [amqpConnectionUrl],
          })
        ).getOrThrow();

        clients.push(client);
        return client;
      });
    } finally {
      // Clean up all clients before fixture cleanup (which deletes the vhost)
      await Promise.all(
        clients.map(async (client) => {
          try {
            (await client.close()).getOrThrow();
          } catch (error) {
            // Swallow errors during cleanup to avoid unhandled rejections
            // eslint-disable-next-line no-console
            console.error("Failed to close AMQP client during fixture cleanup:", error);
          }
        }),
      );
    }
  },
  workerFactory: async ({ amqpConnectionUrl }, use) => {
    const workers: Array<TypedAmqpWorker<ContractDefinition>> = [];
    try {
      await use(
        async <TContract extends ContractDefinition>(
          contract: TContract,
          handlers: WorkerInferHandlers<TContract>,
        ) => {
          const worker = (
            await TypedAmqpWorker.create({
              contract,
              handlers: defineHandlers(contract, handlers),
              urls: [amqpConnectionUrl],
            })
          ).getOrThrow();

          workers.push(worker);
          return worker;
        },
      );
    } finally {
      // Clean up all workers before fixture cleanup (which deletes the vhost)
      await Promise.all(
        workers.map(async (worker) => {
          try {
            (await worker.close()).getOrThrow();
          } catch (error) {
            // Swallow errors during cleanup to avoid unhandled rejections
            // eslint-disable-next-line no-console
            console.error("Failed to close worker during fixture cleanup:", error);
          }
        }),
      );
    }
  },
});

/**
 * Helper function to wait for worker to be ready to consume messages.
 * Workers need a brief moment to establish their connection and start consuming.
 * This is a pragmatic solution for integration tests since workers don't expose a ready event.
 */
async function waitForWorkerReady(delayMs = 500): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, delayMs));
}

describe("Client and Worker Integration", () => {
  describe("end-to-end message flow", () => {
    it("should successfully publish and consume messages between client and worker", async ({
      clientFactory,
      workerFactory,
    }) => {
      // GIVEN
      const exchange = defineExchange("orders", { durable: false });
      const queue = defineQueue("order-processing", { type: "classic", durable: false });
      const orderMessage = defineMessage(
        z.object({
          orderId: z.string(),
          amount: z.number().positive(),
          customerId: z.string(),
          count: z.number().default(1),
        }),
        {
          headers: z.object({
            "x-test-header": z.string(),
            "x-default-header": z.string().default("default-header-value"),
          }),
          summary: "Order created event",
          description: "Emitted when a new order is created",
        },
      );

      const orderCreatedEvent = defineEventPublisher(exchange, orderMessage, {
        routingKey: "order.created",
      });

      const contract = defineContract({
        publishers: {
          orderCreated: orderCreatedEvent,
        },
        consumers: {
          processOrder: defineEventConsumer(orderCreatedEvent, queue),
        },
      });

      // GIVEN
      const mockHandler = vi.fn().mockReturnValue(Ok(undefined).toAsync());
      await workerFactory(contract, {
        processOrder: mockHandler,
      });
      const client = await clientFactory(contract);

      // Wait for worker to be ready to consume messages
      await waitForWorkerReady();

      // WHEN
      const publishResult = await client.publish(
        "orderCreated",
        {
          orderId: "ORD-123",
          amount: 99.99,
          customerId: "CUST-456",
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
      expect(publishResult).toEqual(Ok(undefined));

      await vi.waitFor(
        () => {
          expect(mockHandler).toHaveBeenCalledTimes(1);
          expect(mockHandler).toHaveBeenNthCalledWith(
            1,
            {
              payload: {
                orderId: "ORD-123",
                amount: 99.99,
                customerId: "CUST-456",
                count: 1, // Default value applied
              },
              headers: {
                "x-test-header": "test-header-value",
                "x-default-header": "default-header-value", // Default value applied
              },
            },
            expect.anything(), // rawMessage
            expect.anything(), // middleware context
          );
        },
        { timeout: 5000 },
      );
    });

    it("should handle multiple messages in sequence", async ({ clientFactory, workerFactory }) => {
      // GIVEN
      const exchange = defineExchange("events", { durable: false });
      const queue = defineQueue("event-processing", { type: "classic", durable: false });
      const eventMessage = defineMessage(
        z.object({
          eventId: z.string(),
          type: z.enum(["created", "updated", "deleted"]),
          data: z.record(z.string(), z.unknown()),
        }),
      );

      const eventPublisherConfig = defineEventPublisher(exchange, eventMessage, {
        routingKey: "event.general",
      });

      const contract = defineContract({
        publishers: {
          eventPublisher: eventPublisherConfig,
        },
        consumers: {
          processEvent: defineEventConsumer(eventPublisherConfig, queue, {
            routingKey: "event.#",
          }),
        },
      });

      // GIVEN
      const receivedMessages: unknown[] = [];
      const mockHandler = vi.fn().mockImplementation((message: unknown) => {
        receivedMessages.push(message);
        return Ok(undefined).toAsync();
      });
      await workerFactory(contract, {
        processEvent: mockHandler,
      });
      const client = await clientFactory(contract);

      // Wait for worker to be ready to consume messages
      await waitForWorkerReady();

      // WHEN
      const messages = [
        { eventId: "EVT-1", type: "created" as const, data: { name: "Test 1" } },
        { eventId: "EVT-2", type: "updated" as const, data: { name: "Test 2" } },
        { eventId: "EVT-3", type: "deleted" as const, data: { id: "123" } },
      ];

      for (const message of messages) {
        const result = await client.publish("eventPublisher", message);
        expect(result).toEqual(Ok(undefined));
      }

      // THEN
      await vi.waitFor(
        () => {
          expect(mockHandler).toHaveBeenCalledTimes(3);
          expect(receivedMessages).toEqual([
            {
              payload: { eventId: "EVT-1", type: "created", data: { name: "Test 1" } },
            },
            {
              payload: { eventId: "EVT-2", type: "updated", data: { name: "Test 2" } },
            },
            {
              payload: { eventId: "EVT-3", type: "deleted", data: { id: "123" } },
            },
          ]);
        },
        { timeout: 5000 },
      );
    });

    it("should handle validation errors gracefully", async ({ clientFactory, workerFactory }) => {
      // GIVEN
      const exchange = defineExchange("strict", { durable: false });
      const queue = defineQueue("strict-processing", { type: "classic", durable: false });
      const strictMessage = defineMessage(
        z.object({
          id: z.string().uuid(),
          value: z.number().int().positive(),
        }),
      );

      const strictEvent = defineEventPublisher(exchange, strictMessage, {
        routingKey: "strict.message",
      });

      const contract = defineContract({
        publishers: {
          strictPublisher: strictEvent,
        },
        consumers: {
          processStrict: defineEventConsumer(strictEvent, queue),
        },
      });

      const mockHandler = vi.fn().mockReturnValue(Ok(undefined).toAsync());
      await workerFactory(contract, {
        processStrict: mockHandler,
      });
      const client = await clientFactory(contract);

      // Wait for worker to be ready to consume messages
      await waitForWorkerReady();

      // WHEN
      const invalidResult = await client.publish("strictPublisher", {
        id: "not-a-uuid",
        value: 42,
      } as never);

      // THEN
      expect(invalidResult.isErr()).toBe(true);

      // WHEN
      const validResult = await client.publish("strictPublisher", {
        id: "123e4567-e89b-12d3-a456-426614174000",
        value: 42,
      });

      // THEN
      expect(validResult).toEqual(Ok(undefined));

      await vi.waitFor(
        () => {
          expect(mockHandler).toHaveBeenCalled();
        },
        { timeout: 5000 },
      );
    });
  });

  describe("routing patterns", () => {
    it("should route messages based on routing keys with topic exchange", async ({
      clientFactory,
      workerFactory,
    }) => {
      // GIVEN
      const exchange = defineExchange("notifications", { durable: false });
      const emailQueue = defineQueue("email-queue", { type: "classic", durable: false });
      const smsQueue = defineQueue("sms-queue", { type: "classic", durable: false });

      const notificationMessage = defineMessage(
        z.object({
          recipient: z.string(),
          message: z.string(),
        }),
      );

      const emailEvent = defineEventPublisher(exchange, notificationMessage, {
        routingKey: "notification.email.send",
      });
      const smsEvent = defineEventPublisher(exchange, notificationMessage, {
        routingKey: "notification.sms.send",
      });

      const contract = defineContract({
        publishers: {
          emailNotification: emailEvent,
          smsNotification: smsEvent,
        },
        consumers: {
          processEmail: defineEventConsumer(emailEvent, emailQueue, {
            routingKey: "notification.email.*",
          }),
          processSms: defineEventConsumer(smsEvent, smsQueue, {
            routingKey: "notification.sms.*",
          }),
        },
      });

      // GIVEN
      const emailHandler = vi.fn().mockReturnValue(Ok(undefined).toAsync());
      const smsHandler = vi.fn().mockReturnValue(Ok(undefined).toAsync());

      await workerFactory(contract, {
        processEmail: emailHandler,
        processSms: smsHandler,
      });
      const client = await clientFactory(contract);

      // Wait for worker to be ready to consume messages
      await waitForWorkerReady();

      // WHEN
      const emailResult = await client.publish("emailNotification", {
        recipient: "user@example.com",
        message: "Test email",
      });
      expect(emailResult).toEqual(Ok(undefined));

      // WHEN
      const smsResult = await client.publish("smsNotification", {
        recipient: "+1234567890",
        message: "Test SMS",
      });
      expect(smsResult).toEqual(Ok(undefined));

      // THEN
      await vi.waitFor(
        () => {
          expect(emailHandler).toHaveBeenCalledTimes(1);
          expect(emailHandler).toHaveBeenNthCalledWith(
            1,
            {
              payload: {
                recipient: "user@example.com",
                message: "Test email",
              },
            },
            expect.anything(), // rawMessage
            expect.anything(), // middleware context
          );
          expect(smsHandler).toHaveBeenCalledTimes(1);
          expect(smsHandler).toHaveBeenNthCalledWith(
            1,
            {
              payload: {
                recipient: "+1234567890",
                message: "Test SMS",
              },
            },
            expect.anything(), // rawMessage
            expect.anything(), // middleware context
          );
        },
        { timeout: 5000 },
      );
    });
  });
});
