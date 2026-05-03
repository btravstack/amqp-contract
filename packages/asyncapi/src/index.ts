import {
  AsyncAPIObject,
  ChannelObject,
  ChannelsObject,
  MessageObject,
  MessagesObject,
  OperationsObject,
} from "@asyncapi/parser/esm/spec-types/v3.js";
import { ConditionalSchemaConverter, JSONSchema } from "@orpc/openapi";
import type {
  BindingDefinition,
  ContractDefinition,
  ExchangeBindingDefinition,
  ExchangeDefinition,
  MessageDefinition,
  QueueBindingDefinition,
  QueueDefinition,
} from "@amqp-contract/contract";
import { extractConsumer, extractQueue } from "@amqp-contract/contract";
import type { StandardSchemaV1 } from "@standard-schema/spec";

/**
 * Options for configuring the AsyncAPI generator.
 *
 * @example
 * ```typescript
 * import { AsyncAPIGenerator } from '@amqp-contract/asyncapi';
 * import { zodToJsonSchema } from '@orpc/zod';
 *
 * const generator = new AsyncAPIGenerator({
 *   schemaConverters: [zodToJsonSchema]
 * });
 * ```
 */
export type AsyncAPIGeneratorOptions = {
  /**
   * Schema converters for transforming validation schemas to JSON Schema.
   * Supports Zod, Valibot, ArkType, and other Standard Schema v1 compatible libraries.
   */
  schemaConverters?: ConditionalSchemaConverter[];
  /**
   * Optional logger for warnings during generation (e.g. unmatched schema converters).
   */
  logger?: { warn: (message: string) => void };
  /**
   * If true, the generator throws when a payload schema cannot be converted by
   * any of the configured `schemaConverters` instead of falling back to a
   * generic `{ type: "object" }` placeholder. Recommended for CI pipelines
   * that depend on the generated spec being faithful (e.g. for codegen).
   *
   * Defaults to `false` for backwards compatibility.
   */
  failOnMissingConverter?: boolean;
};

/**
 * Options for generating an AsyncAPI document.
 * These correspond to the top-level AsyncAPI document fields.
 */
export type AsyncAPIGeneratorGenerateOptions = Pick<AsyncAPIObject, "info"> &
  Partial<Pick<AsyncAPIObject, "id" | "servers">>;

/**
 * Generator for creating AsyncAPI 3.0 documentation from AMQP contracts.
 *
 * This class converts contract definitions into AsyncAPI 3.0 specification documents,
 * which can be used for API documentation, code generation, and tooling integration.
 *
 * @example
 * ```typescript
 * import { AsyncAPIGenerator } from '@amqp-contract/asyncapi';
 * import { defineExchange, defineMessage, defineContract, definePublisher } from '@amqp-contract/contract';
 * import { zodToJsonSchema } from '@orpc/zod';
 * import { z } from 'zod';
 *
 * const ordersExchange = defineExchange('orders');
 * const orderMessage = defineMessage(z.object({
 *   orderId: z.string(),
 *   amount: z.number()
 * }));
 *
 * const contract = defineContract({
 *   publishers: {
 *     orderCreated: definePublisher(ordersExchange, orderMessage, {
 *       routingKey: 'order.created'
 *     })
 *   }
 * });
 *
 * const generator = new AsyncAPIGenerator({
 *   schemaConverters: [zodToJsonSchema]
 * });
 *
 * const asyncapi = await generator.generate(contract, {
 *   id: 'urn:com:example:order-service',
 *   info: {
 *     title: 'Order Service API',
 *     version: '1.0.0',
 *     description: 'Async API for order processing'
 *   },
 *   servers: {
 *     production: {
 *       host: 'rabbitmq.example.com',
 *       protocol: 'amqp',
 *       protocolVersion: '0.9.1'
 *     }
 *   }
 * });
 * ```
 */
export class AsyncAPIGenerator {
  private readonly converters: ConditionalSchemaConverter[];
  private readonly logger?: { warn: (message: string) => void } | undefined;
  private readonly failOnMissingConverter: boolean;

  /**
   * Create a new AsyncAPI generator instance.
   *
   * @param options - Configuration options including schema converters
   */
  constructor(options: AsyncAPIGeneratorOptions = {}) {
    this.converters = options.schemaConverters ?? [];
    this.logger = options.logger;
    this.failOnMissingConverter = options.failOnMissingConverter ?? false;
  }

  /**
   * Generate an AsyncAPI 3.0 document from a contract definition.
   *
   * Converts AMQP exchanges, queues, publishers, and consumers into
   * AsyncAPI channels, operations, and messages with proper JSON Schema
   * validation definitions.
   *
   * @param contract - The AMQP contract definition to convert
   * @param options - AsyncAPI document metadata (id, info, servers)
   * @returns Promise resolving to a complete AsyncAPI 3.0 document
   *
   * @example
   * ```typescript
   * const asyncapi = await generator.generate(contract, {
   *   id: 'urn:com:example:api',
   *   info: {
   *     title: 'My API',
   *     version: '1.0.0'
   *   },
   *   servers: {
   *     dev: {
   *       host: 'localhost:5672',
   *       protocol: 'amqp'
   *     }
   *   }
   * });
   * ```
   */
  async generate(
    contract: ContractDefinition,
    options: AsyncAPIGeneratorGenerateOptions,
  ): Promise<AsyncAPIObject> {
    const convertedChannels: ChannelsObject = {};
    const convertedOperations: OperationsObject = {};
    const convertedMessages: MessagesObject = {};

    // First, collect all messages from publishers and consumers
    const publisherMessages = new Map<string, { message: MessageDefinition; channelKey: string }>();
    const consumerMessages = new Map<string, { message: MessageDefinition; channelKey: string }>();

    // Collect messages from publishers
    if (contract.publishers) {
      for (const [publisherName, publisher] of Object.entries(contract.publishers)) {
        const channelKey = this.getExchangeName(publisher.exchange, contract);
        publisherMessages.set(publisherName, { message: publisher.message, channelKey });
      }
    }

    // Collect messages from consumers
    if (contract.consumers) {
      for (const [consumerName, consumerEntry] of Object.entries(contract.consumers)) {
        const consumer = extractConsumer(consumerEntry);
        const queue = extractQueue(consumer.queue);
        const channelKey = this.getQueueName(queue, contract);
        consumerMessages.set(consumerName, { message: consumer.message, channelKey });
      }
    }

    // Generate channels from queues with their messages
    if (contract.queues) {
      for (const [queueName, queueEntry] of Object.entries(contract.queues)) {
        const queue = extractQueue(queueEntry);
        const channelMessages: MessagesObject = {};

        // Add messages from consumers that reference this queue
        for (const [consumerName, { message, channelKey }] of consumerMessages) {
          if (channelKey === queueName) {
            const messageName = `${consumerName}Message`;
            channelMessages[messageName] = await this.convertMessage(message);
            convertedMessages[messageName] = await this.convertMessage(message);
          }
        }

        // Find bindings for this queue
        const queueBindings = this.getQueueBindings(queue, contract);
        const channel: ChannelObject = {
          ...this.queueToChannel(queue, queueBindings),
        };

        if (Object.keys(channelMessages).length > 0) {
          channel.messages = channelMessages;
        }

        convertedChannels[queueName] = channel;
      }
    }

    // Generate channels from exchanges with their messages
    if (contract.exchanges) {
      for (const [exchangeName, exchange] of Object.entries(contract.exchanges)) {
        const channelMessages: MessagesObject = {};

        // Add messages from publishers that reference this exchange
        for (const [publisherName, { message, channelKey }] of publisherMessages) {
          if (channelKey === exchangeName) {
            const messageName = `${publisherName}Message`;
            channelMessages[messageName] = await this.convertMessage(message);
            convertedMessages[messageName] = await this.convertMessage(message);
          }
        }

        const channel: ChannelObject = {
          ...this.exchangeToChannel(exchange, contract),
        };

        if (Object.keys(channelMessages).length > 0) {
          channel.messages = channelMessages;
        }

        convertedChannels[exchangeName] = channel;
      }
    }

    // Generate publish operations from publishers
    if (contract.publishers) {
      for (const [publisherName, publisher] of Object.entries(contract.publishers)) {
        const exchangeName = this.getExchangeName(publisher.exchange, contract);
        const messageName = `${publisherName}Message`;

        // Build operation without type assertion
        if (publisher.routingKey) {
          convertedOperations[publisherName] = {
            action: "send",
            channel: { $ref: `#/channels/${exchangeName}` },
            messages: [{ $ref: `#/channels/${exchangeName}/messages/${messageName}` }],
            summary: `Publish to ${publisher.exchange.name}`,
            description: `Routing key: ${publisher.routingKey}`,
            bindings: {
              amqp: {
                cc: [publisher.routingKey],
                deliveryMode: 2, // Persistent by default
                bindingVersion: "0.3.0",
              } as Record<string, unknown>,
            },
          };
        } else {
          convertedOperations[publisherName] = {
            action: "send",
            channel: { $ref: `#/channels/${exchangeName}` },
            messages: [{ $ref: `#/channels/${exchangeName}/messages/${messageName}` }],
            summary: `Publish to ${publisher.exchange.name}`,
          };
        }
      }
    }

    // Generate receive operations from consumers
    if (contract.consumers) {
      for (const [consumerName, consumerEntry] of Object.entries(contract.consumers)) {
        const consumer = extractConsumer(consumerEntry);
        const queue = extractQueue(consumer.queue);
        const queueName = this.getQueueName(queue, contract);
        const messageName = `${consumerName}Message`;

        convertedOperations[consumerName] = {
          action: "receive",
          channel: { $ref: `#/channels/${queueName}` },
          messages: [{ $ref: `#/channels/${queueName}/messages/${messageName}` }],
          summary: `Consume from ${queue.name}`,
          bindings: {
            amqp: {
              bindingVersion: "0.3.0",
            } as Record<string, unknown>,
          },
        };
      }
    }

    return {
      ...options,
      asyncapi: "3.1.0",
      channels: convertedChannels,
      operations: convertedOperations,
      components: {
        messages: convertedMessages,
      },
    };
  }

  /**
   * Convert a message definition to AsyncAPI MessageObject
   */
  private async convertMessage(message: MessageDefinition): Promise<MessageObject> {
    const payload = message.payload;

    // Convert payload schema
    const payloadJsonSchema = await this.convertSchema(payload, "input");

    // Build result with required properties
    const result: Record<string, unknown> = {
      payload: payloadJsonSchema,
      contentType: "application/json",
    };

    // Add optional properties only if they exist
    if (message.headers) {
      const headersJsonSchema = await this.convertSchema(message.headers, "input");
      if (headersJsonSchema) {
        result["headers"] = headersJsonSchema;
      }
    }

    if (message.summary) {
      result["summary"] = message.summary;
    }

    if (message.description) {
      result["description"] = message.description;
    }

    return result as MessageObject;
  }

  /**
   * Convert a queue definition to AsyncAPI ChannelObject.
   *
   * The AMQP binding spec doesn't have first-class fields for dead-lettering
   * or retry policy, so we surface them in two places:
   *
   * 1. The `arguments` map carries the canonical RabbitMQ keys
   *    (`x-dead-letter-exchange`, `x-dead-letter-routing-key`) — these are
   *    what a consumer of the spec would actually use to recreate the queue.
   * 2. The channel description summarises DLX + retry policy in human-readable
   *    form so the topology is visible without reading the binding details.
   */
  private queueToChannel(
    queue: QueueDefinition,
    bindings: QueueBindingDefinition[] = [],
  ): ChannelObject {
    // Merge user-provided arguments with derived RabbitMQ args. User
    // arguments win on collision so consumers can override the derived ones
    // if they really need to. The DLX description below is then built from the
    // merged result so the human-readable summary cannot drift from the
    // structured `arguments` if the user overrode them.
    const derivedArgs: Record<string, unknown> = {};
    if (queue.deadLetter?.exchange) {
      derivedArgs["x-dead-letter-exchange"] = queue.deadLetter.exchange.name;
      if (queue.deadLetter.routingKey) {
        derivedArgs["x-dead-letter-routing-key"] = queue.deadLetter.routingKey;
      }
    }
    const mergedArgs = { ...derivedArgs, ...queue.arguments };

    let description = `AMQP Queue: ${queue.name}`;
    if (bindings.length > 0) {
      const bindingDescriptions = bindings
        .map((binding) => {
          const exchangeName = binding.exchange.name;
          const routingKey = "routingKey" in binding ? binding.routingKey : undefined;
          return routingKey
            ? `bound to exchange '${exchangeName}' with routing key '${routingKey}'`
            : `bound to exchange '${exchangeName}'`;
        })
        .join(", ");
      description += ` (${bindingDescriptions})`;
    }

    const effectiveDlx = mergedArgs["x-dead-letter-exchange"];
    if (typeof effectiveDlx === "string" && effectiveDlx.length > 0) {
      description += `. Dead-letters to '${effectiveDlx}'`;
      const effectiveDlxRoutingKey = mergedArgs["x-dead-letter-routing-key"];
      if (typeof effectiveDlxRoutingKey === "string" && effectiveDlxRoutingKey.length > 0) {
        description += ` (routing key '${effectiveDlxRoutingKey}')`;
      }
      description += ".";
    }

    if (queue.retry && queue.retry.mode !== "none") {
      const retry = queue.retry;
      if (retry.mode === "immediate-requeue") {
        description += ` Retry: immediate-requeue, max ${retry.maxRetries} attempts.`;
      } else if (retry.mode === "ttl-backoff") {
        const initial = retry.initialDelayMs;
        description += ` Retry: ttl-backoff, max ${retry.maxRetries} attempts`;
        if (initial !== undefined) {
          description += `, initial delay ${initial}ms`;
        }
        description += ".";
      }
    }

    const result: Record<string, unknown> = {
      address: queue.name,
      title: queue.name,
      description,
      bindings: {
        amqp: {
          is: "queue",
          queue: {
            name: queue.name,
            type: queue.type,
            durable: queue.durable,
            ...(queue.exclusive !== undefined && { exclusive: queue.exclusive }),
            ...(queue.autoDelete !== undefined && { autoDelete: queue.autoDelete }),
            ...(queue.maxPriority !== undefined && { maxPriority: queue.maxPriority }),
            ...(Object.keys(mergedArgs).length > 0 ? { arguments: mergedArgs } : {}),
            vhost: "/",
          },
          bindingVersion: "0.3.0",
        },
      },
    };

    if (queue.retry && queue.retry.mode !== "none") {
      // Spec extension: retry policy is non-standard but useful for tooling
      // that wants to inspect it programmatically. Prefixing with `x-` keeps
      // the spec valid for strict parsers.
      (result as Record<string, unknown>)["x-amqp-retry"] = {
        mode: queue.retry.mode,
        ...("maxRetries" in queue.retry && queue.retry.maxRetries !== undefined
          ? { maxRetries: queue.retry.maxRetries }
          : {}),
        ...(queue.retry.mode === "ttl-backoff"
          ? {
              ...(queue.retry.initialDelayMs !== undefined
                ? { initialDelayMs: queue.retry.initialDelayMs }
                : {}),
              ...(queue.retry.maxDelayMs !== undefined
                ? { maxDelayMs: queue.retry.maxDelayMs }
                : {}),
              ...(queue.retry.backoffMultiplier !== undefined
                ? { backoffMultiplier: queue.retry.backoffMultiplier }
                : {}),
              ...(queue.retry.jitter !== undefined ? { jitter: queue.retry.jitter } : {}),
            }
          : {}),
      };
    }

    return result as ChannelObject;
  }

  /**
   * Convert an exchange definition to AsyncAPI ChannelObject.
   *
   * Exchange-to-exchange bindings — used for bridge exchanges, fanout
   * fan-in/out, and other cross-domain routing — are surfaced both as a
   * line in the description and via the non-standard `x-amqp-exchange-bindings`
   * extension so tooling can recreate the topology.
   */
  private exchangeToChannel(
    exchange: ExchangeDefinition,
    contract: ContractDefinition,
  ): ChannelObject {
    const sourceBindings = this.getExchangeBindingsBySource(exchange, contract);
    const destinationBindings = this.getExchangeBindingsByDestination(exchange, contract);

    let description = `AMQP Exchange: ${exchange.name} (${exchange.type})`;
    if (sourceBindings.length > 0) {
      const summaries = sourceBindings
        .map((binding) => {
          const target = binding.destination.name;
          const routingKey = "routingKey" in binding ? binding.routingKey : undefined;
          return routingKey
            ? `forwards to '${target}' (routing key '${routingKey}')`
            : `forwards to '${target}'`;
        })
        .join(", ");
      description += `. ${summaries}.`;
    }
    if (destinationBindings.length > 0) {
      const summaries = destinationBindings
        .map((binding) => {
          const source = binding.source.name;
          const routingKey = "routingKey" in binding ? binding.routingKey : undefined;
          return routingKey
            ? `receives from '${source}' (routing key '${routingKey}')`
            : `receives from '${source}'`;
        })
        .join(", ");
      description += ` ${summaries}.`;
    }

    const result: Record<string, unknown> = {
      address: exchange.name,
      title: exchange.name,
      description,
      bindings: {
        amqp: {
          is: "routingKey",
          exchange: {
            name: exchange.name,
            type: exchange.type,
            durable: exchange.durable,
            ...(exchange.autoDelete !== undefined && { autoDelete: exchange.autoDelete }),
            ...(exchange.internal !== undefined && { internal: exchange.internal }),
            ...(exchange.arguments !== undefined && { arguments: exchange.arguments }),
            vhost: "/",
          },
          bindingVersion: "0.3.0",
        },
      },
    };

    if (sourceBindings.length > 0 || destinationBindings.length > 0) {
      const e2eBindings: Record<string, unknown> = {};
      if (sourceBindings.length > 0) {
        e2eBindings["forwardsTo"] = sourceBindings.map((b) => ({
          destination: b.destination.name,
          ...("routingKey" in b && b.routingKey !== undefined ? { routingKey: b.routingKey } : {}),
          ...(b.arguments !== undefined ? { arguments: b.arguments } : {}),
        }));
      }
      if (destinationBindings.length > 0) {
        e2eBindings["receivesFrom"] = destinationBindings.map((b) => ({
          source: b.source.name,
          ...("routingKey" in b && b.routingKey !== undefined ? { routingKey: b.routingKey } : {}),
          ...(b.arguments !== undefined ? { arguments: b.arguments } : {}),
        }));
      }
      (result as Record<string, unknown>)["x-amqp-exchange-bindings"] = e2eBindings;
    }

    return result as ChannelObject;
  }

  private getExchangeBindingsBySource(
    exchange: ExchangeDefinition,
    contract: ContractDefinition,
  ): ExchangeBindingDefinition[] {
    return this.exchangeBindings(contract).filter((b) => b.source.name === exchange.name);
  }

  private getExchangeBindingsByDestination(
    exchange: ExchangeDefinition,
    contract: ContractDefinition,
  ): ExchangeBindingDefinition[] {
    return this.exchangeBindings(contract).filter((b) => b.destination.name === exchange.name);
  }

  private exchangeBindings(contract: ContractDefinition): ExchangeBindingDefinition[] {
    if (!contract.bindings) return [];
    const result: ExchangeBindingDefinition[] = [];
    for (const binding of Object.values(contract.bindings) as BindingDefinition[]) {
      if (binding.type === "exchange") {
        result.push(binding);
      }
    }
    return result;
  }

  /**
   * Get the name/key of an exchange from the contract
   */
  private getExchangeName(exchange: ExchangeDefinition, contract: ContractDefinition): string {
    if (contract.exchanges) {
      for (const [name, ex] of Object.entries(contract.exchanges)) {
        if (ex === exchange || ex.name === exchange.name) {
          return name;
        }
      }
    }
    return exchange.name;
  }

  /**
   * Get the name/key of a queue from the contract
   */
  private getQueueName(queue: QueueDefinition, contract: ContractDefinition): string {
    if (contract.queues) {
      for (const [name, qEntry] of Object.entries(contract.queues)) {
        const q = extractQueue(qEntry);
        if (q === queue || q.name === queue.name) {
          return name;
        }
      }
    }
    return queue.name;
  }

  /**
   * Get all bindings for a queue from the contract
   */
  private getQueueBindings(
    queue: QueueDefinition,
    contract: ContractDefinition,
  ): QueueBindingDefinition[] {
    const result: QueueBindingDefinition[] = [];

    if (contract.bindings) {
      for (const binding of Object.values(contract.bindings)) {
        if (binding.type === "queue" && binding.queue.name === queue.name) {
          result.push(binding);
        }
      }
    }

    return result;
  }

  /**
   * Convert a Standard Schema to JSON Schema using oRPC converters
   */
  private async convertSchema(
    schema: StandardSchemaV1,
    strategy: "input" | "output",
  ): Promise<JSONSchema> {
    // Try each converter until one matches
    for (const converter of this.converters) {
      const matches = await converter.condition(schema, { strategy });
      if (matches) {
        const [_required, jsonSchema] = await converter.convert(schema, { strategy });
        return jsonSchema;
      }
    }

    const message =
      `No schema converter matched for schema. ` +
      `Configure schemaConverters (e.g. zodToJsonSchema) to generate accurate schemas.`;

    if (this.failOnMissingConverter) {
      throw new Error(`AsyncAPIGenerator: ${message}`);
    }

    // No converter matched — the output will contain a generic { type: "object" } placeholder.
    this.logger?.warn(
      `${message} The generated spec will use a generic { type: "object" } placeholder.`,
    );
    return { type: "object" };
  }
}
