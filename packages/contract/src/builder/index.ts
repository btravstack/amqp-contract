/**
 * Builder functions for defining AMQP contracts.
 *
 * @packageDocumentation
 */

// Exchange
export { defineExchange } from "./exchange.js";

// Message
export { defineMessage } from "./message.js";

// Queue
export { defineQueue } from "./queue.js";

// Bindings
export { defineExchangeBinding, defineQueueBinding } from "./binding.js";

// Publisher
export { definePublisher } from "./publisher.js";

// Consumer
export { defineConsumer, extractConsumer } from "./consumer.js";

// Contract
export { defineContract } from "./contract.js";

// Routing types
export type {
  BindingPattern,
  MatchingBindingPattern,
  MatchingRoutingKey,
  RoutableRoutingKey,
  RoutingKey,
} from "./routing-types.js";

// Event pattern
export {
  defineEventConsumer,
  defineEventPublisher,
  isEventConsumerResult,
  isEventPublisherConfig,
} from "./event.js";
export type { EventConsumerResult, EventPublisherConfig } from "./event.js";

// Command pattern
export {
  defineCommandConsumer,
  defineCommandPublisher,
  isBridgedPublisherConfig,
  isCommandConsumerConfig,
} from "./command.js";
export type { BridgedPublisherConfig, CommandConsumerConfig } from "./command.js";

// RPC pattern
export { defineRpc } from "./rpc.js";

// TTL-backoff infrastructure (derived, not stored in the contract)
export {
  deriveTtlBackoffInfrastructure,
  ttlBackoffBaseDelay,
  ttlBackoffWaitQueueName,
} from "./ttl-backoff.js";
