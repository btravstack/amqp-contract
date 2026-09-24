/**
 * `@amqp-contract/contract/internal` — cross-package internals with **no
 * semver guarantee**, kept off the package root so the public API surface
 * stays honest: the predicate that decides whether a queue dead-letters
 * (shared with `@amqp-contract/worker` so its terminal-nack logging can never
 * disagree with `defineContract`'s poison-loss guard), the consumer-entry
 * normalizer and entry-kind guards, and the TTL-backoff topology derivation
 * that core's setup and the worker's retry path must agree on.
 *
 * Nothing here is part of the contract-authoring API; application code has no
 * reason to import it.
 */
export { _internal_queueHasDeadLetterExchange } from "./dead-letter.js";
export {
  deriveTtlBackoffInfrastructure,
  extractConsumer,
  isBridgedPublisherConfig,
  isCommandConsumerConfig,
  isEventConsumerResult,
  isEventPublisherConfig,
  ttlBackoffBaseDelay,
  ttlBackoffWaitQueueName,
} from "./builder/index.js";
