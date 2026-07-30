import { TypedAmqpClient } from "@amqp-contract/client";
import {
  type CompressionAlgorithm,
  type ContractDefinition,
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import { it as baseIt } from "@amqp-contract/testing/extension";
import { TypedAmqpWorker, type WorkerInferHandlers, declareHandlers } from "@amqp-contract/worker";
import { OkAsync } from "unthrown";
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
        const client = await TypedAmqpClient.create({
          contract,
          urls: [amqpConnectionUrl],
        }).get();
        clients.push(client);
        return client;
      });
    } finally {
      await Promise.all(
        clients.map(async (client) => {
          try {
            await client.close().get();
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
          const worker = await TypedAmqpWorker.create({
            contract,
            handlers: declareHandlers(contract, handlers),
            urls: [amqpConnectionUrl],
          }).get();
          workers.push(worker);
          return worker;
        },
      );
    } finally {
      await Promise.all(
        workers.map(async (worker) => {
          try {
            await worker.close().get();
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

async function waitForWorkerReady(delayMs = 500): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, delayMs));
}

function buildContract() {
  const exchange = defineExchange("orders", { durable: false });
  const queue = defineQueue("order-processing", { type: "classic", durable: false });
  const orderMessage = defineMessage(
    z.object({
      orderId: z.string(),
      // A payload large enough that compression genuinely shrinks it.
      description: z.string(),
    }),
  );
  const orderCreated = defineEventPublisher(exchange, orderMessage, {
    routingKey: "order.created",
  });
  return defineContract({
    publishers: { orderCreated },
    consumers: { processOrder: defineEventConsumer(orderCreated, queue) },
  });
}

describe("Compression end-to-end", () => {
  // Guards the wire format itself: the compressed Buffer must reach the broker
  // byte-for-byte (a JSON-serializing channel would corrupt it into
  // '{"type":"Buffer","data":[...]}' and the worker would DLQ every message).
  it.for<CompressionAlgorithm>(["gzip", "deflate"])(
    "round-trips a %s-compressed message from client to worker handler",
    async (algorithm, { clientFactory, workerFactory }) => {
      // GIVEN
      const contract = buildContract();
      const payload = {
        orderId: "compressed-123",
        description: "x".repeat(2_000),
      };

      const received = vi.fn().mockReturnValue(OkAsync(undefined));
      await workerFactory(contract, { processOrder: received });
      await waitForWorkerReady();

      const client = await clientFactory(contract);

      // WHEN
      const result = await client.publish("orderCreated", payload, {
        compression: algorithm,
      });

      // THEN
      expect(result.isOk()).toBe(true);
      await vi.waitFor(() => {
        expect(received).toHaveBeenCalledTimes(1);
      });
      expect(received).toHaveBeenCalledWith(
        expect.objectContaining({ payload }),
        expect.anything(),
        expect.anything(),
      );
    },
  );

  it("delivers an uncompressed message unchanged (wire-format regression guard)", async ({
    clientFactory,
    workerFactory,
  }) => {
    // GIVEN
    const contract = buildContract();
    const payload = { orderId: "plain-456", description: "no compression" };

    const received = vi.fn().mockReturnValue(OkAsync(undefined));
    await workerFactory(contract, { processOrder: received });
    await waitForWorkerReady();

    const client = await clientFactory(contract);

    // WHEN
    const result = await client.publish("orderCreated", payload);

    // THEN
    expect(result.isOk()).toBe(true);
    await vi.waitFor(() => {
      expect(received).toHaveBeenCalledTimes(1);
    });
    expect(received).toHaveBeenCalledWith(
      expect.objectContaining({ payload }),
      expect.anything(),
      expect.anything(),
    );
  });
});
