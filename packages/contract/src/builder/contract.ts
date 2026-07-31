import type {
  BindingDefinition,
  ConsumerDefinition,
  ContractDefinition,
  ContractDefinitionInput,
  ContractOutput,
  ExchangeDefinition,
  PublisherDefinition,
  QueueDefinition,
  RpcDefinition,
} from "../types.js";
import { isBridgedPublisherConfig, isCommandConsumerConfig } from "./command.js";
import { isEventConsumerResult, isEventPublisherConfig } from "./event.js";
import { definePublisherInternal } from "./publisher.js";
import { _internal_assertPublisherRoutable } from "./validate.js";

/**
 * Structural equality for resource definitions. We compare on a JSON projection
 * after stripping non-comparable fields (Standard Schema instances, branded
 * symbols) so that, e.g., two `defineExchange("orders")` calls in different
 * files are treated as the same exchange.
 */
function resourcesEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  try {
    return JSON.stringify(a, replacer) === JSON.stringify(b, replacer);
  } catch {
    return false;
  }
}

function replacer(_key: string, value: unknown): unknown {
  // Standard Schema validators are functions / proxies that JSON.stringify
  // cannot meaningfully compare. Reduce them to a structural marker so two
  // independent `defineMessage(z.object({...}))` declarations of the same
  // shape don't trip the collision check.
  if (typeof value === "function") return "[function]";
  if (value && typeof value === "object" && "~standard" in (value as object)) {
    return "[standard-schema]";
  }
  return value;
}

/**
 * Add an entry to a name-keyed map, throwing if the name is already taken by a
 * structurally-different definition. Identical re-declarations are silently
 * deduplicated — that's how the same exchange can flow into the contract via
 * both a publisher and a consumer.
 */
function addResource<T>(
  bucket: Record<string, T>,
  name: string,
  value: T,
  kind: "exchange" | "queue" | "binding",
): void {
  const existing = bucket[name];
  if (existing === undefined) {
    bucket[name] = value;
    return;
  }
  if (!resourcesEqual(existing, value)) {
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
    throw new Error(
      `defineContract: ${kind} "${name}" was declared with conflicting definitions. ` +
        `Two ${kind}s sharing a name must be the exact same definition; ` +
        `define the ${kind} once and reference it from every publisher/consumer that needs it.`,
    );
  }
}

/**
 * Define an AMQP contract.
 *
 * A contract is the central definition of your AMQP messaging topology. It brings together
 * publishers and consumers in a single, type-safe definition. Exchanges, queues, and bindings
 * are automatically extracted from publishers and consumers.
 *
 * The contract is used by both clients (for publishing) and workers (for consuming) to ensure
 * type safety throughout your messaging infrastructure. TypeScript will infer all message types
 * and publisher/consumer names from the contract.
 *
 * @param definition - The contract definition containing publishers and consumers
 * @param definition.publishers - Named publisher definitions for sending messages
 * @param definition.consumers - Named consumer definitions for receiving messages
 * @returns The contract definition with fully inferred exchanges, queues, bindings, publishers, and consumers
 *
 * @example
 * ```typescript
 * import {
 *   defineContract,
 *   defineExchange,
 *   defineQueue,
 *   defineEventPublisher,
 *   defineEventConsumer,
 *   defineMessage,
 * } from '@amqp-contract/contract';
 * import { z } from 'zod';
 *
 * // Define resources
 * const ordersExchange = defineExchange('orders');
 * const dlx = defineExchange('orders-dlx', { type: 'direct' });
 * const orderQueue = defineQueue('order-processing', {
 *   deadLetter: { exchange: dlx },
 *   retry: { mode: 'immediate-requeue', maxRetries: 3 },
 * });
 * const orderMessage = defineMessage(
 *   z.object({
 *     orderId: z.string(),
 *     amount: z.number(),
 *   })
 * );
 *
 * // Define event publisher
 * const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
 *   routingKey: 'order.created',
 * });
 *
 * // Compose contract - exchanges, queues, bindings are auto-extracted
 * export const contract = defineContract({
 *   publishers: {
 *     orderCreated: orderCreatedEvent,
 *   },
 *   consumers: {
 *     processOrder: defineEventConsumer(orderCreatedEvent, orderQueue),
 *   },
 * });
 *
 * // TypeScript now knows:
 * // - contract.exchanges.orders, contract.exchanges['orders-dlx']
 * // - contract.queues['order-processing']
 * // - contract.bindings.processOrderBinding
 * // - client.publish('orderCreated', { orderId: string, amount: number })
 * // - handler: ({ payload }: { payload: { orderId: string, amount: number } }) => AsyncResult<void, HandlerError>
 * ```
 */
export function defineContract<TContract extends ContractDefinitionInput>(
  definition: TContract,
): ContractOutput<TContract> {
  const {
    publishers: inputPublishers,
    consumers: inputConsumers,
    rpcs: inputRpcs,
    exchanges: inputExchanges,
    queues: inputQueues,
    bindings: inputBindings,
  } = definition;

  // Consumer names and RPC names share the worker handler keyspace; if the
  // same key appeared in both, the worker would consume from two queues under
  // one name and dispatch ambiguously. Fail fast at contract-definition time
  // rather than producing surprising runtime behavior.
  if (inputConsumers && inputRpcs) {
    const collisions = Object.keys(inputConsumers).filter((name) => Object.hasOwn(inputRpcs, name));
    if (collisions.length > 0) {
      // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
      throw new Error(
        `defineContract: name collision between consumers and rpcs — keys must be disjoint. Conflicting names: ${collisions.join(", ")}`,
      );
    }
  }

  const result: ContractDefinition = {
    exchanges: {},
    queues: {},
    bindings: {},
    publishers: {},
    consumers: {},
    rpcs: {},
  };

  const exchanges: Record<string, ExchangeDefinition> = {};
  const queues: Record<string, QueueDefinition> = {};
  const bindings: Record<string, BindingDefinition> = {};

  /**
   * Register a queue plus its dead-letter exchange. TTL-backoff wait queues
   * are NOT part of the contract — they are derived from the queue's retry
   * config and declared by `setupAmqpTopology` at channel-setup time (see
   * `deriveTtlBackoffInfrastructure`).
   */
  const addQueueWithInfrastructure = (queue: QueueDefinition): void => {
    addResource(queues, queue.name, queue, "queue");
    if (queue.deadLetter?.exchange) {
      addResource(exchanges, queue.deadLetter.exchange.name, queue.deadLetter.exchange, "exchange");
    }
  };

  // Process standalone topology first — resources declared without a
  // publisher, consumer, or RPC attached (audit queues, a DLQ bound to the
  // auto-extracted DLX, another domain's exchange a binding references).
  // Declared-first means a structurally-equal re-declaration via a consumer
  // later dedupes silently, while a conflicting one fails the collision check.
  for (const exchange of Object.values(inputExchanges ?? {})) {
    addResource(exchanges, exchange.name, exchange, "exchange");
  }
  for (const queue of Object.values(inputQueues ?? {})) {
    addQueueWithInfrastructure(queue);
  }
  for (const [label, binding] of Object.entries(inputBindings ?? {})) {
    addResource(bindings, label, binding, "binding");
  }

  // Process publishers section - extract exchanges and convert EventPublisherConfig entries
  if (inputPublishers && Object.keys(inputPublishers).length > 0) {
    const processedPublishers: Record<string, PublisherDefinition> = {};

    for (const [name, entry] of Object.entries(inputPublishers)) {
      if (isBridgedPublisherConfig(entry)) {
        // BridgedPublisherConfig: extract publisher, exchanges, and e2e binding
        addResource(exchanges, entry.bridgeExchange.name, entry.bridgeExchange, "exchange");
        addResource(exchanges, entry.targetExchange.name, entry.targetExchange, "exchange");
        addResource(bindings, `${name}ExchangeBinding`, entry.exchangeBinding, "binding");
        processedPublishers[name] = entry.publisher;
      } else if (isEventPublisherConfig(entry)) {
        // EventPublisherConfig: extract exchange and convert to publisher definition
        addResource(exchanges, entry.exchange.name, entry.exchange, "exchange");
        const publisherOptions: { routingKey?: string; externalConsumers?: boolean } = {};
        if (entry.routingKey !== undefined) {
          publisherOptions.routingKey = entry.routingKey;
        }
        // Must be carried through: an option that is accepted by the builder
        // and then dropped here would silently fail to opt the event out.
        if (entry.externalConsumers !== undefined) {
          publisherOptions.externalConsumers = entry.externalConsumers;
        }
        processedPublishers[name] = definePublisherInternal(
          entry.exchange,
          entry.message,
          publisherOptions,
        );
      } else {
        // Plain PublisherDefinition: extract exchange
        const publisher = entry as PublisherDefinition;
        addResource(exchanges, publisher.exchange.name, publisher.exchange, "exchange");
        processedPublishers[name] = publisher;
      }
    }

    result.publishers = processedPublishers;
  }

  // Process consumers section - extract queues, exchanges, bindings, and consumer definitions
  if (inputConsumers && Object.keys(inputConsumers).length > 0) {
    const processedConsumers: Record<string, ConsumerDefinition> = {};

    for (const [name, entry] of Object.entries(inputConsumers)) {
      if (isEventConsumerResult(entry)) {
        // EventConsumerResult: extract consumer, binding, queue, and exchange
        processedConsumers[name] = entry.consumer;
        addResource(bindings, `${name}Binding`, entry.binding, "binding");
        addQueueWithInfrastructure(entry.consumer.queue);
        addResource(exchanges, entry.binding.exchange.name, entry.binding.exchange, "exchange");

        if (entry.exchangeBinding) {
          addResource(bindings, `${name}ExchangeBinding`, entry.exchangeBinding, "binding");
        }
        if (entry.bridgeExchange) {
          addResource(exchanges, entry.bridgeExchange.name, entry.bridgeExchange, "exchange");
        }
        // Source exchange (stored in entry.exchange for bridged consumers)
        if (entry.exchange) {
          addResource(exchanges, entry.exchange.name, entry.exchange, "exchange");
        }
      } else if (isCommandConsumerConfig(entry)) {
        // CommandConsumerConfig: extract consumer, binding, queue, and exchange
        processedConsumers[name] = entry.consumer;
        addResource(bindings, `${name}Binding`, entry.binding, "binding");
        addQueueWithInfrastructure(entry.consumer.queue);
        addResource(exchanges, entry.exchange.name, entry.exchange, "exchange");
      } else {
        // Plain ConsumerDefinition: extract queue
        const consumer = entry as ConsumerDefinition;
        processedConsumers[name] = consumer;
        addQueueWithInfrastructure(consumer.queue);
      }
    }

    result.consumers = processedConsumers;
  }

  // Process rpcs section — extract each RPC's queue (and DLX if any) into the
  // contract topology. RPCs use the AMQP default exchange with the queue name
  // as routing key, so no exchange or binding declarations are needed.
  if (inputRpcs && Object.keys(inputRpcs).length > 0) {
    const processedRpcs: Record<string, RpcDefinition> = {};

    for (const [name, rpc] of Object.entries(inputRpcs)) {
      processedRpcs[name] = rpc;
      addQueueWithInfrastructure(rpc.queue);
    }

    result.rpcs = processedRpcs;
  }

  result.exchanges = exchanges;
  result.queues = queues;
  result.bindings = bindings;

  // Runs last: consumers and bridged publishers contribute bindings, so
  // routability can only be decided once every binding is collected.
  const declaredBindings = Object.values(bindings);
  for (const [publisherName, publisher] of Object.entries(result.publishers ?? {})) {
    _internal_assertPublisherRoutable(
      publisherName,
      publisher.exchange,
      "routingKey" in publisher ? publisher.routingKey : undefined,
      publisher.externalConsumers,
      declaredBindings,
    );
  }

  return result as ContractOutput<TContract>;
}
