/**
 * `@amqp-contract/contract/internal` — cross-package internals with **no
 * semver guarantee**, kept off the package root so the public API surface
 * stays honest. Today: the one predicate that decides whether a queue
 * dead-letters, shared with `@amqp-contract/worker` so its terminal-nack
 * logging can never disagree with `defineContract`'s poison-loss guard.
 *
 * Nothing here is part of the contract-authoring API; application code has no
 * reason to import it.
 */
export { _internal_queueHasDeadLetterExchange } from "./dead-letter.js";
