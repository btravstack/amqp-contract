import type { ContractDefinition } from "@amqp-contract/contract";
import { extractQueue } from "@amqp-contract/contract";
import type { Channel } from "amqplib";
import { TechnicalError } from "./errors.js";

/**
 * Setup AMQP topology (exchanges, queues, and bindings) from a contract definition.
 *
 * This function sets up the complete AMQP topology in the correct order:
 * 1. Assert all exchanges defined in the contract
 * 2. Validate dead letter exchanges are declared before referencing them
 * 3. Assert all queues with their configurations (including dead letter settings)
 * 4. Create all bindings (queue-to-exchange and exchange-to-exchange)
 *
 * @param channel - The AMQP channel to use for topology setup
 * @param contract - The contract definition containing the topology specification
 * @throws {AggregateError} If any exchanges, queues, or bindings fail to be created
 * @throws {TechnicalError} If a queue references a dead letter exchange not declared in the contract
 *
 * @example
 * ```typescript
 * const channel = await connection.createChannel();
 * await setupAmqpTopology(channel, contract);
 * ```
 */
export async function setupAmqpTopology(
  channel: Channel,
  contract: ContractDefinition,
): Promise<void> {
  // Setup exchanges. The AMQP default exchange (name "") is implicit; RabbitMQ
  // does not allow asserting it, so we skip empty-named exchange entries.
  const exchanges = Object.values(contract.exchanges ?? {}).filter((e) => e.name !== "");
  const exchangeResults = await Promise.allSettled(
    exchanges.map((exchange) =>
      channel.assertExchange(exchange.name, exchange.type, {
        ...(exchange.durable !== undefined && { durable: exchange.durable }),
        ...(exchange.autoDelete !== undefined && { autoDelete: exchange.autoDelete }),
        ...(exchange.internal !== undefined && { internal: exchange.internal }),
        ...(exchange.arguments !== undefined && { arguments: exchange.arguments }),
      }),
    ),
  );
  const exchangeErrors = exchangeResults
    .map((result, i) => ({ result, name: exchanges[i]!.name }))
    .filter(
      (entry): entry is { result: PromiseRejectedResult; name: string } =>
        entry.result.status === "rejected",
    );
  if (exchangeErrors.length > 0) {
    const names = exchangeErrors.map((e) => e.name).join(", ");
    throw new AggregateError(
      exchangeErrors.map(({ result }) => result.reason),
      `Failed to setup exchanges: ${names}`,
    );
  }

  // Validate dead letter exchanges before setting up queues
  for (const queueEntry of Object.values(contract.queues ?? {})) {
    const queue = extractQueue(queueEntry);
    if (queue.deadLetter) {
      const dlxName = queue.deadLetter.exchange.name;
      const exchangeExists = Object.values(contract.exchanges ?? {}).some(
        (exchange) => exchange.name === dlxName,
      );

      if (!exchangeExists) {
        throw new TechnicalError(
          `Queue "${queue.name}" references dead letter exchange "${dlxName}" which is not declared in the contract. ` +
            `Add the exchange to contract.exchanges to ensure it is created before the queue.`,
        );
      }
    }
  }

  // Setup queues
  const queueEntries = Object.values(contract.queues ?? {});
  const queueResults = await Promise.allSettled(
    queueEntries.map((queueEntry) => {
      const queue = extractQueue(queueEntry);
      // Build queue arguments, merging dead letter configuration and queue type
      const queueArguments: Record<string, unknown> = { ...queue.arguments };

      // Set queue type
      queueArguments["x-queue-type"] = queue.type;

      if (queue.deadLetter) {
        queueArguments["x-dead-letter-exchange"] = queue.deadLetter.exchange.name;
        if (queue.deadLetter.routingKey) {
          queueArguments["x-dead-letter-routing-key"] = queue.deadLetter.routingKey;
        }
      }

      // Handle type-specific properties using discriminated union
      if (queue.type === "quorum") {
        return channel.assertQueue(queue.name, {
          durable: true, // Quorum queues are always durable
          arguments: queueArguments,
        });
      }

      if (queue.maxPriority !== undefined) {
        queueArguments["x-max-priority"] = queue.maxPriority;
      }

      // Classic queue
      return channel.assertQueue(queue.name, {
        ...(queue.durable !== undefined && { durable: queue.durable }),
        ...(queue.exclusive !== undefined && { exclusive: queue.exclusive }),
        ...(queue.autoDelete !== undefined && { autoDelete: queue.autoDelete }),
        arguments: queueArguments,
      });
    }),
  );
  const queueErrors = queueResults
    .map((result, i) => ({ result, name: extractQueue(queueEntries[i]!).name }))
    .filter(
      (entry): entry is { result: PromiseRejectedResult; name: string } =>
        entry.result.status === "rejected",
    );
  if (queueErrors.length > 0) {
    const names = queueErrors.map((e) => e.name).join(", ");
    throw new AggregateError(
      queueErrors.map(({ result }) => result.reason),
      `Failed to setup queues: ${names}`,
    );
  }

  // Setup bindings
  const bindings = Object.values(contract.bindings ?? {});
  const bindingResults = await Promise.allSettled(
    bindings.map((binding) => {
      if (binding.type === "queue") {
        return channel.bindQueue(
          binding.queue.name,
          binding.exchange.name,
          binding.routingKey ?? "",
          binding.arguments,
        );
      }

      return channel.bindExchange(
        binding.destination.name,
        binding.source.name,
        binding.routingKey ?? "",
        binding.arguments,
      );
    }),
  );
  const bindingErrors = bindingResults
    .map((result, i) => {
      const binding = bindings[i]!;
      const name =
        binding.type === "queue"
          ? `${binding.exchange.name} -> ${binding.queue.name}`
          : `${binding.source.name} -> ${binding.destination.name}`;
      return { result, name };
    })
    .filter(
      (entry): entry is { result: PromiseRejectedResult; name: string } =>
        entry.result.status === "rejected",
    );
  if (bindingErrors.length > 0) {
    const names = bindingErrors.map((e) => e.name).join(", ");
    throw new AggregateError(
      bindingErrors.map(({ result }) => result.reason),
      `Failed to setup bindings: ${names}`,
    );
  }
}
