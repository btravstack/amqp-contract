import type { ContractDefinition } from "@amqp-contract/contract";
import { deriveTtlBackoffInfrastructure } from "@amqp-contract/contract";
import type { Channel } from "amqplib";

import { TechnicalError } from "./errors.js";

/**
 * What topology setup does with the contract's resources on (re)connect:
 *
 * - `"assert"` (default) — declare them (`assertExchange` / `assertQueue`,
 *   plus bindings): the broker creates what is missing and refuses a mismatch.
 * - `"passive"` — only check they exist (`checkExchange` / `checkQueue`) and
 *   declare nothing, for an application whose credentials may not configure
 *   the broker; bindings cannot be checked passively and are skipped. A
 *   missing resource fails `create()`.
 * - `"none"` — touch nothing: topology is owned elsewhere (IaC, definitions
 *   import, another service).
 */
export type TopologyMode = "assert" | "passive" | "none";

/**
 * The slice of a contract a PUBLISHER needs on the broker: the exchanges its
 * publishers publish to, plus — transitively — every exchange those forward
 * to through exchange-to-exchange bindings, and those bindings. No queues:
 * queues (and their dead-lettering and retry infrastructure) are the
 * consumer's to declare, so a publisher never re-asserts, and never fights
 * over, another service's queue arguments.
 */
export function publisherTopology(contract: ContractDefinition): ContractDefinition {
  const exchangeBindings = Object.entries(contract.bindings ?? {}).filter(
    ([, binding]) => binding.type === "exchange",
  );
  const needed = new Set(Object.values(contract.publishers ?? {}).map((p) => p.exchange.name));
  for (let grew = true; grew;) {
    grew = false;
    for (const [, binding] of exchangeBindings) {
      if (
        binding.type === "exchange" &&
        needed.has(binding.source.name) &&
        !needed.has(binding.destination.name)
      ) {
        needed.add(binding.destination.name);
        grew = true;
      }
    }
  }
  return {
    exchanges: Object.fromEntries(
      Object.entries(contract.exchanges ?? {}).filter(([, exchange]) => needed.has(exchange.name)),
    ),
    bindings: Object.fromEntries(
      exchangeBindings.filter(
        ([, binding]) => binding.type === "exchange" && needed.has(binding.source.name),
      ),
    ),
  };
}

/**
 * Declare every item concurrently and, if any rejected, throw ONE
 * `AggregateError` naming all of them.
 *
 * Concurrent-then-collect rather than fail-fast: when a topology is wrong it is
 * usually wrong in more than one place, and a report listing every broken
 * exchange beats discovering them one redeploy at a time.
 *
 * @param label - Plural noun for the message ("exchanges", "queues", …).
 * @param describe - How an item names itself in the failure list.
 */
async function settleAll<T>(
  items: readonly T[],
  label: string,
  describe: (item: T) => string,
  run: (item: T) => Promise<unknown>,
): Promise<void> {
  const results = await Promise.allSettled(items.map(run));
  const failures = results.flatMap((result, index) =>
    result.status === "rejected"
      ? [{ reason: result.reason as unknown, name: describe(items[index]!) }]
      : [],
  );
  if (failures.length === 0) return;

  // oxlint-disable-next-line unthrown/no-throw -- plain async helper; the rejection is adopted as a Defect at the channel-setup boundary (documented @throws)
  throw new AggregateError(
    failures.map(({ reason }) => reason),
    `Failed to setup ${label}: ${failures.map(({ name }) => name).join(", ")}`,
  );
}

/**
 * Setup AMQP topology (exchanges, queues, and bindings) from a contract definition.
 *
 * This function sets up the complete AMQP topology in the correct order:
 * 1. Assert all exchanges defined in the contract
 * 2. Validate dead letter exchanges are declared before referencing them
 * 3. Assert all queues with their configurations (including dead letter
 *    settings), plus the TTL-backoff wait queues derived from each queue's
 *    retry config (one per distinct backoff delay — see
 *    `deriveTtlBackoffInfrastructure`)
 * 4. Create all bindings (queue-to-exchange and exchange-to-exchange)
 *
 * @param channel - The AMQP channel to use for topology setup
 * @param contract - The contract definition containing the topology specification
 *   (pass a slice, e.g. `publisherTopology(contract)`, to scope setup to a role)
 * @param options.mode - {@link TopologyMode}; defaults to `"assert"`
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
  options?: { mode?: TopologyMode | undefined },
): Promise<void> {
  const mode = options?.mode ?? "assert";
  if (mode === "none") return;
  const passive = mode === "passive";

  // Setup exchanges. The AMQP default exchange (name "") is implicit; RabbitMQ
  // does not allow asserting it, so we skip empty-named exchange entries.
  const exchanges = Object.values(contract.exchanges ?? {}).filter((e) => e.name !== "");
  await settleAll(
    exchanges,
    "exchanges",
    (exchange) => exchange.name,
    (exchange) =>
      passive
        ? channel.checkExchange(exchange.name)
        : channel.assertExchange(exchange.name, exchange.type, {
            ...(exchange.durable !== undefined && { durable: exchange.durable }),
            ...(exchange.autoDelete !== undefined && { autoDelete: exchange.autoDelete }),
            ...(exchange.internal !== undefined && { internal: exchange.internal }),
            ...(exchange.arguments !== undefined && { arguments: exchange.arguments }),
          }),
  );

  // Validate dead letter exchanges before setting up queues
  for (const queue of Object.values(contract.queues ?? {})) {
    if (queue.deadLetter) {
      const dlxName = queue.deadLetter.exchange.name;
      const exchangeExists = Object.values(contract.exchanges ?? {}).some(
        (exchange) => exchange.name === dlxName,
      );

      if (!exchangeExists) {
        // oxlint-disable-next-line unthrown/no-throw -- plain async helper; the rejection is adopted as a Defect at the channel-setup boundary (documented @throws)
        throw new TechnicalError(
          `Queue "${queue.name}" references dead letter exchange "${dlxName}" which is not declared in the contract. ` +
            `Add the exchange to contract.exchanges to ensure it is created before the queue.`,
        );
      }
    }
  }

  // Setup queues, including the TTL-backoff wait queues derived from each
  // queue's retry config. Wait queues are per-delay-tier: each is declared
  // with a queue-level `x-message-ttl` backstop and dead-letters straight
  // back to its main queue via the default exchange.
  const queues = Object.values(contract.queues ?? {});
  const queueAsserts: Array<{ name: string; assert: () => Promise<unknown> }> = [];
  for (const queue of queues) {
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
      queueAsserts.push({
        name: queue.name,
        assert: () =>
          channel.assertQueue(queue.name, {
            durable: true, // Quorum queues are always durable
            arguments: queueArguments,
          }),
      });
    } else {
      if (queue.maxPriority !== undefined) {
        queueArguments["x-max-priority"] = queue.maxPriority;
      }

      // Classic queue
      queueAsserts.push({
        name: queue.name,
        assert: () =>
          channel.assertQueue(queue.name, {
            ...(queue.durable !== undefined && { durable: queue.durable }),
            ...(queue.exclusive !== undefined && { exclusive: queue.exclusive }),
            ...(queue.autoDelete !== undefined && { autoDelete: queue.autoDelete }),
            arguments: queueArguments,
          }),
      });
    }

    // Derived TTL-backoff wait queues (one per distinct backoff delay).
    const infra = deriveTtlBackoffInfrastructure(queue);
    if (infra) {
      for (const waitQueue of infra.waitQueues) {
        queueAsserts.push({
          name: waitQueue.name,
          assert: () =>
            channel.assertQueue(waitQueue.name, {
              durable: infra.durable,
              arguments: {
                "x-queue-type": infra.queueType,
                // Backstop TTL: per-message `expiration` carries the actual
                // (jittered) delay; this bounds how long any message can sit
                // in the tier to the tier's jitter ceiling.
                "x-message-ttl": waitQueue.messageTtlMs,
                // Route expired messages straight back to the main queue via
                // the default exchange.
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": infra.queueName,
              },
            }),
        });
      }
    }
  }
  await settleAll(
    queueAsserts,
    "queues",
    ({ name }) => name,
    ({ name, assert }) => (passive ? channel.checkQueue(name) : assert()),
  );

  // AMQP has no passive bind: a passive setup checks existence only.
  if (passive) return;

  // Setup bindings
  await settleAll(
    Object.values(contract.bindings ?? {}),
    "bindings",
    (binding) =>
      binding.type === "queue"
        ? `${binding.exchange.name} -> ${binding.queue.name}`
        : `${binding.source.name} -> ${binding.destination.name}`,
    (binding) =>
      binding.type === "queue"
        ? channel.bindQueue(
            binding.queue.name,
            binding.exchange.name,
            binding.routingKey ?? "",
            binding.arguments,
          )
        : channel.bindExchange(
            binding.destination.name,
            binding.source.name,
            binding.routingKey ?? "",
            binding.arguments,
          ),
  );
}
