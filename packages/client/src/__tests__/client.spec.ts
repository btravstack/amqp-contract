import {
  ContractDefinition,
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  definePublisher,
  defineQueue,
} from "@amqp-contract/contract";
import { it as baseIt } from "@amqp-contract/testing/extension";
import { describe, expect } from "vitest";
import { z } from "zod";
import { CreateClientOptions, TypedAmqpClient } from "../client.js";

const it = baseIt.extend<{
  clientFactory: <TContract extends ContractDefinition>(
    contract: TContract,
    options?: Omit<CreateClientOptions<TContract>, "contract" | "urls">,
  ) => Promise<TypedAmqpClient<TContract>>;
}>({
  clientFactory: async ({ amqpConnectionUrl }, use) => {
    const clients: Array<TypedAmqpClient<ContractDefinition>> = [];

    try {
      await use(
        async <TContract extends ContractDefinition>(
          contract: TContract,
          options?: Omit<CreateClientOptions<TContract>, "contract" | "urls">,
        ) => {
          const client = (
            await TypedAmqpClient.create({
              contract,
              urls: [amqpConnectionUrl],
              ...options,
            }).recover((e) => {
              throw e;
            })
          ).unwrap();

          clients.push(client);
          return client;
        },
      );
    } finally {
      // Clean up all clients before fixture cleanup (which deletes the vhost)
      await Promise.all(
        clients.map(async (client) => {
          try {
            (
              await client.close().recover((e) => {
                throw e;
              })
            ).unwrap();
          } catch (error) {
            // Swallow errors during cleanup to avoid unhandled rejections
            // eslint-disable-next-line no-console
            console.error("Failed to close AMQP client during fixture cleanup:", error);
          }
        }),
      );
    }
  },
});

describe("AmqpClient Integration", () => {
  describe("end-to-end publishing", () => {
    it("should publish messages to a real RabbitMQ instance", async ({
      clientFactory,
      initConsumer,
    }) => {
      // GIVEN
      const TestMessage = z.object({
        id: z.string(),
        message: z.string(),
      });

      const exchange = defineExchange("test-exchange", { durable: false });

      const contract = defineContract({
        publishers: {
          testPublisher: definePublisher(exchange, defineMessage(TestMessage), {
            routingKey: "test.key",
          }),
        },
      });

      const client = await clientFactory(contract);

      const pendingMessages = await initConsumer(
        contract.publishers.testPublisher.exchange.name,
        contract.publishers.testPublisher.routingKey,
      );

      // WHEN
      const result = await client.publish("testPublisher", {
        id: "123",
        message: "Hello, RabbitMQ!",
      });

      // THEN
      expect(result).toBeOkWith(undefined);

      await expect(pendingMessages()).resolves.toEqual([
        expect.objectContaining({
          content: Buffer.from(JSON.stringify({ id: "123", message: "Hello, RabbitMQ!" })),
        }),
      ]);
    });

    it("should validate messages before publishing", async ({ clientFactory }) => {
      // GIVEN
      const TestMessage = z.object({
        id: z.string(),
        count: z.number().positive(),
      });

      const exchange = defineExchange("test-validation-exchange", {
        durable: false,
      });

      const contract = defineContract({
        publishers: {
          testPublisher: definePublisher(exchange, defineMessage(TestMessage), {
            routingKey: "validation.test",
          }),
        },
      });

      const client = await clientFactory(contract);

      // WHEN
      const result = await client.publish("testPublisher", {
        id: "123",
        count: -5, // Invalid: count must be positive
      });

      // THEN
      expect(result).toBeErrTagged("@amqp-contract/MessageValidationError");
    });

    it("should publish messages with default values", async ({ clientFactory, initConsumer }) => {
      // GIVEN
      const TestMessage = z.object({
        id: z.string(),
        count: z.number().default(1),
      });

      const exchange = defineExchange("test-default-values-exchange", {
        durable: false,
      });

      const contract = defineContract({
        publishers: {
          testPublisher: definePublisher(exchange, defineMessage(TestMessage), {
            routingKey: "test.key",
          }),
        },
      });

      const client = await clientFactory(contract);

      const pendingMessages = await initConsumer(
        contract.publishers.testPublisher.exchange.name,
        contract.publishers.testPublisher.routingKey,
      );

      // WHEN
      const result = await client.publish("testPublisher", {
        id: "123",
        // count is omitted, should use default value of 1
      });

      // THEN
      expect(result).toBeOkWith(undefined);

      await expect(pendingMessages()).resolves.toEqual([
        expect.objectContaining({
          content: Buffer.from(
            JSON.stringify({
              id: "123",
              count: 1, // Default value applied
            }),
          ),
        }),
      ]);
    });

    it("should publish messages with headers", async ({ clientFactory, initConsumer }) => {
      // GIVEN
      const TestMessage = z.object({
        content: z.string(),
      });

      const exchange = defineExchange("test-options-exchange", { durable: false });

      const contract = defineContract({
        publishers: {
          testPublisher: definePublisher(exchange, defineMessage(TestMessage), {
            routingKey: "test.key",
          }),
        },
      });

      const client = await clientFactory(contract);

      const pendingMessages = await initConsumer(
        contract.publishers.testPublisher.exchange.name,
        contract.publishers.testPublisher.routingKey,
      );

      // WHEN
      const result = await client.publish(
        "testPublisher",
        { content: "test message" },
        { headers: { test: "value" } },
      );

      // THEN
      expect(result).toBeOkWith(undefined);

      await expect(pendingMessages()).resolves.toEqual([
        expect.objectContaining({
          content: Buffer.from(JSON.stringify({ content: "test message" })),
          properties: expect.objectContaining({
            headers: { test: "value" },
          }),
        }),
      ]);
    });

    it("should apply default publish options", async ({ clientFactory, initConsumer }) => {
      // GIVEN
      const TestMessage = z.object({ content: z.string() });
      const exchange = defineExchange("test-default-options-exchange", {
        durable: false,
      });

      const contract = defineContract({
        publishers: {
          testPublisher: definePublisher(exchange, defineMessage(TestMessage), {
            routingKey: "test.default",
          }),
        },
      });

      const client = await clientFactory(contract, {
        defaultPublishOptions: {
          headers: { default: "value" },
        },
      });

      const pendingMessages = await initConsumer(
        contract.publishers.testPublisher.exchange.name,
        contract.publishers.testPublisher.routingKey,
      );

      // WHEN
      const result = await client.publish("testPublisher", { content: "default publish" });

      // THEN
      expect(result).toBeOkWith(undefined);

      await expect(pendingMessages()).resolves.toEqual([
        expect.objectContaining({
          content: Buffer.from(JSON.stringify({ content: "default publish" })),
          properties: expect.objectContaining({
            headers: { default: "value" },
            deliveryMode: 2,
          }),
        }),
      ]);

      (
        await client.close().recover((e) => {
          throw e;
        })
      ).unwrap();
    });

    it("should override default publish options with publish-specific options", async ({
      clientFactory,
      initConsumer,
    }) => {
      // GIVEN
      const TestMessage = z.object({ content: z.string() });
      const exchange = defineExchange("test-overridden-options-exchange", {
        durable: false,
      });

      const contract = defineContract({
        publishers: {
          testPublisher: definePublisher(exchange, defineMessage(TestMessage), {
            routingKey: "test.override",
          }),
        },
      });

      const client = await clientFactory(contract, {
        defaultPublishOptions: {
          headers: { default: "value" },
          priority: 1,
        },
      });

      const pendingMessages = await initConsumer(
        contract.publishers.testPublisher.exchange.name,
        contract.publishers.testPublisher.routingKey,
      );

      // WHEN
      const result = await client.publish(
        "testPublisher",
        { content: "override publish" },
        { headers: { override: "value" }, priority: 5 },
      );

      // THEN
      expect(result).toBeOkWith(undefined);

      await expect(pendingMessages()).resolves.toEqual([
        expect.objectContaining({
          content: Buffer.from(JSON.stringify({ content: "override publish" })),
          properties: expect.objectContaining({
            headers: { override: "value" },
            priority: 5,
            deliveryMode: 2,
          }),
        }),
      ]);

      (
        await client.close().recover((e) => {
          throw e;
        })
      ).unwrap();
    });
  });

  describe("topology setup", () => {
    it("should setup exchanges, queues, and bindings with quorum queue", async ({
      clientFactory,
    }) => {
      // GIVEN
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("integration-orders");
      const queue = defineQueue("integration-processing"); // Default quorum queue
      const message = defineMessage(TestMessage);

      const orderCreatedEvent = defineEventPublisher(exchange, message, {
        routingKey: "order.created",
      });

      const contract = defineContract({
        publishers: {
          createOrder: orderCreatedEvent,
        },
        consumers: {
          processOrder: defineEventConsumer(orderCreatedEvent, queue, {
            routingKey: "order.#",
          }),
        },
      });

      // WHEN
      const client = await clientFactory(contract);

      // THEN
      expect(client).toBeDefined();
    });

    it("should setup classic queue for non-durable use cases", async ({ clientFactory }) => {
      // GIVEN
      const TestMessage = z.object({ id: z.string() });

      const exchange = defineExchange("integration-classic-orders", {
        durable: false,
      });
      const queue = defineQueue("integration-classic-processing", {
        type: "classic",
        durable: false,
      });
      const message = defineMessage(TestMessage);

      const orderCreatedEvent = defineEventPublisher(exchange, message, {
        routingKey: "order.created",
      });

      const contract = defineContract({
        publishers: {
          createOrder: orderCreatedEvent,
        },
        consumers: {
          processOrder: defineEventConsumer(orderCreatedEvent, queue, {
            routingKey: "order.#",
          }),
        },
      });

      // WHEN
      const client = await clientFactory(contract);

      // THEN
      expect(client).toBeDefined();
    });

    it("should handle exchange-to-exchange bindings", async ({
      clientFactory,
      amqpChannel,
      initConsumer,
    }) => {
      // GIVEN
      const sourceExchange = defineExchange("integration-source");

      const contract = defineContract({
        publishers: {
          sendMessage: definePublisher(
            sourceExchange,
            defineMessage(z.object({ msg: z.string() })),
            {
              routingKey: "test.important",
            },
          ),
        },
      });

      const client = await clientFactory(contract);

      // Manually set up destination exchange and exchange-to-exchange binding
      // (not supported by defineContract)
      await amqpChannel.assertExchange("integration-dest", "topic", { durable: true });
      await amqpChannel.bindExchange("integration-dest", "integration-source", "*.important");

      // Setup consumer on destination exchange
      const pendingMessages = await initConsumer("integration-dest", "test.important");

      // WHEN
      await client.publish("sendMessage", { msg: "routed" });

      // THEN
      await expect(pendingMessages()).resolves.toEqual([
        expect.objectContaining({
          content: Buffer.from(JSON.stringify({ msg: "routed" })),
        }),
      ]);
    });

    it("should handle fanout exchange topology with quorum queue", async ({
      clientFactory,
      initConsumer,
    }) => {
      // GIVEN
      const fanoutExchange = defineExchange("integration-fanout", {
        type: "fanout",
      });
      const queue = defineQueue("integration-fanout-queue"); // Default quorum queue

      const broadcastEvent = defineEventPublisher(
        fanoutExchange,
        defineMessage(z.object({ data: z.string() })),
      );

      const contract = defineContract({
        publishers: {
          broadcast: broadcastEvent,
        },
        consumers: {
          fanoutConsumer: defineEventConsumer(broadcastEvent, queue),
        },
      });

      const client = await clientFactory(contract);

      const pendingMessages = await initConsumer("integration-fanout", "");

      // WHEN
      await client.publish("broadcast", { data: "broadcast message" });

      // THEN
      await expect(pendingMessages()).resolves.toEqual([
        expect.objectContaining({
          content: Buffer.from(JSON.stringify({ data: "broadcast message" })),
        }),
      ]);
    });
  });

  describe("connection management", () => {
    it("should close cleanly", async ({ clientFactory }) => {
      // GIVEN
      const exchange = defineExchange("integration-close-test", { durable: false });

      const contract = defineContract({
        publishers: {
          testPublisher: definePublisher(exchange, defineMessage(z.object({ id: z.string() })), {
            routingKey: "test.key",
          }),
        },
      });

      const client = await clientFactory(contract);

      // WHEN
      const closeResult = await client.close();

      // THEN
      expect(closeResult).toBeOk();
    });

    it("should handle multiple close calls gracefully", async ({ clientFactory }) => {
      // GIVEN
      const exchange = defineExchange("integration-multi-close", { durable: false });

      const contract: ContractDefinition = {
        exchanges: {
          test: exchange,
        },
      };

      const client = await clientFactory(contract);

      // WHEN
      await client.close();
      const secondCloseResult = await client.close();

      // THEN
      expect(secondCloseResult).toBeOk();
    });

    it("should publish after connection", async ({ clientFactory, initConsumer }) => {
      // GIVEN
      const exchange = defineExchange("integration-post-connect", {
        durable: false,
      });

      const contract = defineContract({
        publishers: {
          testPublisher: definePublisher(exchange, defineMessage(z.object({ value: z.number() })), {
            routingKey: "test.value",
          }),
        },
      });

      const client = await clientFactory(contract);

      const pendingMessages = await initConsumer("integration-post-connect", "test.value");

      // WHEN
      const result = await client.publish("testPublisher", { value: 42 });

      // THEN
      expect(result).toBeOk();
      await expect(pendingMessages()).resolves.toEqual([
        expect.objectContaining({
          content: Buffer.from(JSON.stringify({ value: 42 })),
        }),
      ]);
    });
  });

  describe("error handling", () => {
    it("should validate message schema and return error", async ({ clientFactory }) => {
      // GIVEN
      const TestMessage = z.object({
        id: z.string(),
        count: z.number().positive(),
      });

      const exchange = defineExchange("integration-validation-error", {
        durable: false,
      });

      const contract = defineContract({
        publishers: {
          testPublisher: definePublisher(exchange, defineMessage(TestMessage), {
            routingKey: "validation.test",
          }),
        },
      });

      const client = await clientFactory(contract);

      // WHEN - Invalid data (count must be positive)
      const result = await client.publish("testPublisher", {
        id: "123",
        // @ts-expect-error - testing runtime validation
        count: "not-a-number",
      });

      // THEN
      expect(result).toBeErrTagged(
        "@amqp-contract/MessageValidationError",
        expect.objectContaining({ source: "testPublisher", issues: expect.any(Array) }),
      );
    });
  });
});
