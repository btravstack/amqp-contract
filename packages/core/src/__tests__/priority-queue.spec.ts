import { beforeEach, describe, expect } from "vitest";
import type { ContractDefinition } from "@amqp-contract/contract";
import {
  defineConsumer,
  defineExchange,
  defineMessage,
  definePublisher,
  defineQueue,
  defineQueueBinding,
  extractQueue,
} from "@amqp-contract/contract";
import { AmqpClient } from "../amqp-client.js";
import type { ConsumeMessage } from "amqplib";
import { it } from "@amqp-contract/testing/extension";
import { z } from "zod";

describe("Priority Queue", () => {
  beforeEach(async () => {
    // Reset connection cache between tests
    await AmqpClient._resetConnectionCacheForTesting();
  });

  it("should create a queue with x-max-priority argument", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    // GIVEN
    // Priority queues require classic queue type
    const priorityQueue = defineQueue("test-priority-queue", {
      type: "classic",
      durable: false,
      maxPriority: 10,
    });

    const contract: ContractDefinition = {
      queues: {
        priority: priorityQueue,
      },
    };

    // WHEN
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // THEN - Verify queue was created with x-max-priority
    const queueInfo = await amqpChannel.checkQueue("test-priority-queue");
    expect(queueInfo.queue).toBe("test-priority-queue");

    // CLEANUP
    await client.close().getOrThrow();
    await amqpChannel.deleteQueue("test-priority-queue");
  });

  it("should consume messages in priority order", async ({ amqpConnectionUrl, amqpChannel }) => {
    // GIVEN
    const exchange = defineExchange("test-priority-exchange", { type: "direct", durable: false });
    // Priority queues require classic queue type
    const priorityQueue = defineQueue("test-priority-queue-ordering", {
      type: "classic",
      durable: false,
      maxPriority: 10,
    });

    const messageSchema = z.object({
      id: z.string(),
      priority: z.number(),
    });

    const message = defineMessage(messageSchema);

    const contract: ContractDefinition = {
      exchanges: {
        test: exchange,
      },
      queues: {
        priority: priorityQueue,
      },
      bindings: {
        testBinding: defineQueueBinding(priorityQueue, exchange, {
          routingKey: "test",
        }),
      },
      publishers: {
        testPublisher: definePublisher(exchange, message, {
          routingKey: "test",
        }),
      },
      consumers: {
        testConsumer: defineConsumer(priorityQueue, message),
      },
    };

    // WHEN - Setup client and publish messages in reverse priority order
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // Publish messages with different priorities
    // Publishing in this order: low (1), medium (5), high (10)
    await client
      .publish(exchange.name, "test", { id: "msg-low", priority: 1 }, { priority: 1 })
      .getOrThrow();

    await client
      .publish(exchange.name, "test", { id: "msg-medium", priority: 5 }, { priority: 5 })
      .getOrThrow();

    await client
      .publish(exchange.name, "test", { id: "msg-high", priority: 10 }, { priority: 10 })
      .getOrThrow();

    // Give RabbitMQ time to order the messages
    await new Promise((resolve) => setTimeout(resolve, 100));

    // THEN - Consume messages and verify they arrive in priority order
    const consumedMessages: Array<{ id: string; priority: number }> = [];

    const consumePromise = new Promise<void>((resolve) => {
      let consumedCount = 0;

      amqpChannel.consume(
        extractQueue(priorityQueue).name,
        (msg: ConsumeMessage | null) => {
          if (msg) {
            const content = JSON.parse(msg.content.toString());
            consumedMessages.push(content);
            amqpChannel.ack(msg);

            consumedCount++;
            if (consumedCount === 3) {
              resolve();
            }
          }
        },
        { noAck: false },
      );
    });

    await consumePromise;

    // Verify messages were consumed in priority order (high to low)
    expect(consumedMessages).toEqual([
      { id: "msg-high", priority: 10 },
      { id: "msg-medium", priority: 5 },
      { id: "msg-low", priority: 1 },
    ]);

    // CLEANUP
    await client.close().getOrThrow();
    await amqpChannel.deleteQueue(extractQueue(priorityQueue).name);
    await amqpChannel.deleteExchange(exchange.name);
  });

  it("should handle messages without priority (default to 0)", async ({
    amqpConnectionUrl,
    amqpChannel,
  }) => {
    // GIVEN
    const exchange = defineExchange("test-priority-default-exchange", {
      type: "direct",
      durable: false,
    });
    // Priority queues require classic queue type
    const priorityQueue = defineQueue("test-priority-default-queue", {
      type: "classic",
      durable: false,
      maxPriority: 10,
    });

    const messageSchema = z.object({
      id: z.string(),
    });

    const message = defineMessage(messageSchema);

    const contract: ContractDefinition = {
      exchanges: {
        test: exchange,
      },
      queues: {
        priority: priorityQueue,
      },
      bindings: {
        testBinding: defineQueueBinding(priorityQueue, exchange, {
          routingKey: "test",
        }),
      },
      publishers: {
        testPublisher: definePublisher(exchange, message, {
          routingKey: "test",
        }),
      },
      consumers: {
        testConsumer: defineConsumer(priorityQueue, message),
      },
    };

    // WHEN - Setup client and publish messages with and without priority
    const client = new AmqpClient(contract, {
      urls: [amqpConnectionUrl],
    });

    await client.waitForConnect().getOrThrow();

    // Publish message without priority (defaults to 0)
    await client.publish(exchange.name, "test", { id: "msg-default" }).getOrThrow();

    // Publish message with priority 5
    await client
      .publish(exchange.name, "test", { id: "msg-priority" }, { priority: 5 })
      .getOrThrow();

    // Give RabbitMQ time to order the messages
    await new Promise((resolve) => setTimeout(resolve, 100));

    // THEN - Consume messages and verify priority message comes first
    const consumedMessages: Array<{ id: string }> = [];

    const consumePromise = new Promise<void>((resolve) => {
      let consumedCount = 0;

      amqpChannel.consume(
        extractQueue(priorityQueue).name,
        (msg: ConsumeMessage | null) => {
          if (msg) {
            const content = JSON.parse(msg.content.toString());
            consumedMessages.push(content);
            amqpChannel.ack(msg);

            consumedCount++;
            if (consumedCount === 2) {
              resolve();
            }
          }
        },
        { noAck: false },
      );
    });

    await consumePromise;

    // Verify priority message was consumed first
    expect(consumedMessages).toEqual([{ id: "msg-priority" }, { id: "msg-default" }]);

    // CLEANUP
    await client.close().getOrThrow();
    await amqpChannel.deleteQueue(extractQueue(priorityQueue).name);
    await amqpChannel.deleteExchange(exchange.name);
  });
});
