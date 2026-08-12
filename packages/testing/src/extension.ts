/**
 * Vitest extension module for AMQP testing utilities
 *
 * This module provides a Vitest test extension that adds AMQP-specific fixtures
 * to your tests. Each test gets an isolated virtual host (vhost) with pre-configured
 * connections, channels, and helper functions for publishing and consuming messages.
 *
 * @module extension
 * @packageDocumentation
 */

import { randomUUID } from "node:crypto";

import amqpLib, { type Options, type Channel, type ChannelModel } from "amqplib";
import { inject, vi, it as vitestIt } from "vitest";

/**
 * Options for the message-wait function returned by the `initConsumer`
 * fixture.
 */
export type WaitForMessagesOptions = {
  /** Number of messages to wait for. Defaults to 1. */
  count?: number;
  /** Maximum time in ms to wait before rejecting. Defaults to 5000. */
  timeoutMs?: number;
};

/**
 * The AMQP test fixtures provided by the {@link it} extension — named so
 * helpers accepting fixture values (`amqpChannel`, `publishMessage`, …) can
 * be typed without re-deriving the record from `it`.
 */
export type AmqpTestFixtures = {
  vhost: string;
  amqpConnectionUrl: string;
  amqpConnection: ChannelModel;
  amqpChannel: Channel;
  publishMessage: (
    target: { exchange: string; routingKey: string },
    content: unknown,
    options?: Options.Publish,
  ) => void;
  initConsumer: (
    exchange: string,
    routingKey: string,
  ) => Promise<(options?: WaitForMessagesOptions) => Promise<amqpLib.ConsumeMessage[]>>;
};

export const it = vitestIt.extend<AmqpTestFixtures>({
  /**
   * Test fixture that provides an isolated RabbitMQ virtual host (vhost) for the test.
   *
   * Creates a new vhost with a random UUID name for test isolation. The vhost is automatically
   * created before the test runs using the RabbitMQ Management API.
   *
   * @example
   * ```typescript
   * it('should use isolated vhost', async ({ vhost }) => {
   *   console.log(`Test running in vhost: ${vhost}`);
   * });
   * ```
   */
  // oxlint-disable-next-line no-empty-pattern
  vhost: async ({}, use) => {
    const vhost = await createVhost();
    try {
      await use(vhost);
    } finally {
      await deleteVhost(vhost);
    }
  },
  /**
   * Test fixture that provides the AMQP connection URL for the test container.
   *
   * Constructs a connection URL using the test container's IP and port, along with
   * the isolated vhost. The URL follows the format: `amqp://guest:guest@host:port/vhost`.
   *
   * @example
   * ```typescript
   * it('should connect with URL', async ({ amqpConnectionUrl }) => {
   *   console.log(`Connecting to: ${amqpConnectionUrl}`);
   * });
   * ```
   */
  amqpConnectionUrl: async ({ vhost }, use) => {
    const url = `amqp://guest:guest@${inject("__TESTCONTAINERS_RABBITMQ_IP__")}:${inject("__TESTCONTAINERS_RABBITMQ_PORT_5672__")}/${vhost}`;
    await use(url);
  },
  /**
   * Test fixture that provides an active AMQP connection to RabbitMQ.
   *
   * Establishes a connection using the provided connection URL and automatically closes
   * it after the test completes. This fixture is useful for tests that need direct
   * access to the connection object (e.g., to create multiple channels).
   *
   * @example
   * ```typescript
   * it('should use connection', async ({ amqpConnection }) => {
   *   const channel = await amqpConnection.createChannel();
   *   // ... use channel
   * });
   * ```
   */
  amqpConnection: async ({ amqpConnectionUrl }, use) => {
    const connection = await amqpLib.connect(amqpConnectionUrl);
    await use(connection);
    await connection.close();
  },
  /**
   * Test fixture that provides an AMQP channel for interacting with RabbitMQ.
   *
   * Creates a channel from the active connection and automatically closes it after
   * the test completes. The channel is used for declaring exchanges, queues, bindings,
   * and publishing/consuming messages.
   *
   * @example
   * ```typescript
   * it('should use channel', async ({ amqpChannel }) => {
   *   await amqpChannel.assertExchange('test-exchange', 'topic');
   *   await amqpChannel.assertQueue('test-queue');
   * });
   * ```
   */
  amqpChannel: async ({ amqpConnection }, use) => {
    const channel = await amqpConnection.createChannel();
    await use(channel);
    await channel.close();
  },
  /**
   * Test fixture for publishing messages to an AMQP exchange.
   *
   * Provides a helper function to publish messages directly to an exchange during tests.
   * The message content is automatically serialized to JSON and converted to a Buffer.
   *
   * @param target - The exchange and routing key to publish to
   * @param content - The message payload (will be JSON serialized)
   * @throws Error if the message cannot be published (e.g., write buffer is full)
   *
   * @example
   * ```typescript
   * it('should publish message', async ({ publishMessage }) => {
   *   publishMessage({ exchange: 'my-exchange', routingKey: 'routing.key' }, { data: 'test' });
   * });
   * ```
   */
  publishMessage: async ({ amqpChannel }, use) => {
    function publishMessage(
      target: { exchange: string; routingKey: string },
      content: unknown,
      options?: Options.Publish,
    ): void {
      const { exchange, routingKey } = target;
      const success = amqpChannel.publish(
        exchange,
        routingKey,
        Buffer.from(JSON.stringify(content)),
        options,
      );
      if (!success) {
        // oxlint-disable-next-line unthrown/no-throw -- vitest fixture — throwing is how a fixture fails the test
        throw new Error(
          `Failed to publish message to exchange "${exchange}" with routing key "${routingKey}"`,
        );
      }
    }
    await use(publishMessage);
  },
  /**
   * Test fixture for initializing a message consumer on an AMQP queue.
   *
   * Creates a temporary queue, binds it to the specified exchange with the given routing key,
   * and returns a function to collect messages from that queue. The queue is automatically
   * created with a random UUID name to avoid conflicts between tests.
   *
   * The returned function uses `vi.waitFor()` with a configurable timeout to wait for messages.
   * If the expected number of messages is not received within the timeout period, the Promise
   * will reject with a timeout error, preventing tests from hanging indefinitely.
   *
   * @param exchange - The name of the exchange to bind the queue to
   * @param routingKey - The routing key pattern for message filtering
   * @returns A function that accepts optional configuration ({ count?, timeoutMs? }) and returns a Promise that resolves to an array of ConsumeMessage objects
   *
   * @example
   * ```typescript
   * it('should consume messages', async ({ initConsumer, publishMessage }) => {
   *   const waitForMessages = await initConsumer('my-exchange', 'routing.key');
   *   publishMessage({ exchange: 'my-exchange', routingKey: 'routing.key' }, { data: 'test' });
   *   // With defaults (1 message, 5000ms timeout)
   *   const messages = await waitForMessages();
   *   expect(messages).toHaveLength(1);
   *
   *   // With custom options
   *   publishMessage({ exchange: 'my-exchange', routingKey: 'routing.key' }, { data: 'test2' });
   *   publishMessage({ exchange: 'my-exchange', routingKey: 'routing.key' }, { data: 'test3' });
   *   const messages2 = await waitForMessages({ count: 2, timeoutMs: 10000 });
   *   expect(messages2).toHaveLength(2);
   * });
   * ```
   */
  initConsumer: async ({ amqpChannel }, use) => {
    const consumerTags: string[] = [];

    async function initConsumer(
      exchange: string,
      routingKey: string,
    ): Promise<(options?: WaitForMessagesOptions) => Promise<amqpLib.ConsumeMessage[]>> {
      const queue = randomUUID();

      await amqpChannel.assertQueue(queue);
      await amqpChannel.bindQueue(queue, exchange, routingKey);

      const messages: amqpLib.ConsumeMessage[] = [];
      const consumer = await amqpChannel.consume(
        queue,
        (msg) => {
          if (msg) {
            messages.push(msg);
          }
        },
        { noAck: true },
      );

      consumerTags.push(consumer.consumerTag);

      return async (options = {}) => {
        const { count = 1, timeoutMs = 5000 } = options;
        await vi.waitFor(
          () => {
            if (messages.length < count) {
              // oxlint-disable-next-line unthrown/no-throw -- vitest fixture — throwing is how a fixture fails the test
              throw new Error(`Expected ${count} message(s) but only received ${messages.length}`);
            }
          },
          { timeout: timeoutMs },
        );
        return messages.splice(0, count);
      };
    }

    try {
      await use(initConsumer);
    } finally {
      // Cancel all consumers before fixture cleanup (which deletes the vhost)
      await Promise.all(
        consumerTags.map(async (consumerTag) => {
          try {
            await amqpChannel.cancel(consumerTag);
          } catch (error) {
            // Swallow cancellation errors during cleanup
            console.error("Failed to cancel AMQP consumer during fixture cleanup:", error);
          }
        }),
      );
    }
  },
});

/**
 * Call the RabbitMQ Management API's vhost endpoint, failing the test on any
 * status outside `okStatuses`.
 *
 * @param verb - Used in the failure message ("create", "delete").
 * @param okStatuses - Statuses that count as success. Delete accepts 404 too:
 *   already-gone is the outcome it wanted.
 */
async function vhostRequest(
  method: "PUT" | "DELETE",
  vhost: string,
  verb: string,
  okStatuses: readonly number[],
): Promise<void> {
  const username = inject("__TESTCONTAINERS_RABBITMQ_USERNAME__");
  const password = inject("__TESTCONTAINERS_RABBITMQ_PASSWORD__");

  const response = await fetch(
    `http://${inject("__TESTCONTAINERS_RABBITMQ_IP__")}:${inject("__TESTCONTAINERS_RABBITMQ_PORT_15672__")}/api/vhosts/${encodeURIComponent(vhost)}`,
    { method, headers: { Authorization: `Basic ${btoa(`${username}:${password}`)}` } },
  );

  if (okStatuses.includes(response.status)) return;

  const responseBody = await response.text().catch(() => "");
  const detail = responseBody ? ` - ${responseBody}` : "";
  // oxlint-disable-next-line unthrown/no-throw -- vitest fixture — throwing is how a fixture fails the test
  throw new Error(`Failed to ${verb} vhost '${vhost}': ${response.status}${detail}`, {
    cause: response,
  });
}

async function createVhost(): Promise<string> {
  const namespace = randomUUID();
  await vhostRequest("PUT", namespace, "create", [201]);
  return namespace;
}

async function deleteVhost(vhost: string): Promise<void> {
  // 204 = successfully deleted, 404 = already deleted or doesn't exist
  await vhostRequest("DELETE", vhost, "delete", [204, 404]);
}
