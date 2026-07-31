/**
 * Builder functions for defining AMQP contracts.
 *
 * This module re-exports all builder functions from the modular builder directory.
 * For implementation details, see the individual modules in `./builder/`.
 *
 * @packageDocumentation
 */

// Re-export everything from the modular builder
export {
  // Exchange
  defineExchange,
  // Message
  defineMessage,
  // Queue
  defineQueue,
  // Bindings
  defineQueueBinding,
  defineExchangeBinding,
  // Publisher
  definePublisher,
  // Consumer
  defineConsumer,
  extractConsumer,
  // Contract
  defineContract,
  // Event pattern
  defineEventPublisher,
  defineEventConsumer,
  isEventPublisherConfig,
  isEventConsumerResult,
  // Command pattern
  defineCommandConsumer,
  defineCommandPublisher,
  isBridgedPublisherConfig,
  isCommandConsumerConfig,
  // RPC pattern
  defineRpc,
  // TTL-backoff infrastructure (derived, not stored in the contract)
  deriveTtlBackoffInfrastructure,
  ttlBackoffBaseDelay,
  ttlBackoffWaitQueueName,
} from "./builder/index.js";

// Re-export types
export type {
  // Routing types
  RoutingKey,
  BindingPattern,
  MatchingBindingPattern,
  MatchingRoutingKey,
  RoutableRoutingKey,
  // Event pattern types
  EventPublisherConfig,
  EventConsumerResult,
  // Command pattern types
  BridgedPublisherConfig,
  CommandConsumerConfig,
} from "./builder/index.js";
