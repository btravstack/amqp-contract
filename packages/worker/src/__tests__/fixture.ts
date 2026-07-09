import type { ContractDefinition } from "@amqp-contract/contract";
import { TypedAmqpWorker } from "../worker.js";
import type { WorkerInferHandlers } from "../types.js";
import { it as baseIt } from "@amqp-contract/testing/extension";

export const it = baseIt.extend<{
  workerFactory: <TContract extends ContractDefinition>(
    contract: TContract,
    handlers: WorkerInferHandlers<TContract>,
  ) => Promise<TypedAmqpWorker<TContract>>;
}>({
  workerFactory: async ({ amqpConnectionUrl }, use) => {
    const workers: Array<TypedAmqpWorker<ContractDefinition>> = [];

    try {
      await use(
        async <TContract extends ContractDefinition>(
          contract: TContract,
          handlers: WorkerInferHandlers<TContract>,
        ) => {
          // Topology setup (exchanges, queues, bindings, wait queues) is done automatically
          // by AmqpClient in the channel setup callback
          const worker = (
            await TypedAmqpWorker.create({
              contract,
              handlers,
              urls: [amqpConnectionUrl],
              logger: console,
            }).recover((e) => {
              throw e;
            })
          ).unwrap();

          workers.push(worker);
          return worker;
        },
      );
    } finally {
      // Clean up all workers before fixture cleanup (which deletes the vhost)
      await Promise.all(
        workers.map(async (worker) => {
          try {
            (
              await worker.close().recover((e) => {
                throw e;
              })
            ).unwrap();
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
