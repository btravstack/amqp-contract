import type { QueueDefinition } from "./types.js";

/** The RabbitMQ queue argument that names a queue's dead-letter exchange. */
const DEAD_LETTER_EXCHANGE_ARGUMENT = "x-dead-letter-exchange";

/**
 * Does this queue dead-letter on the broker?
 *
 * The single answer to that question. Three call sites ask it — the define-time
 * poison-loss guard in `@amqp-contract/contract`, and the two terminal-nack
 * logging branches in `@amqp-contract/worker` (`sendToDLQ` and the
 * validation-failure nack). They once each had their own copy and drifted: the
 * guard learned to accept the `arguments` passthrough while the logs kept
 * checking `deadLetter` alone, so a queue that was dead-lettering correctly
 * fired the "message will be lost on nack" warning that
 * `docs/how-to/add-logging.md` tells operators to treat as a bug report. One
 * predicate, so the guard and the logs cannot disagree again.
 *
 * Two ways to configure dead-lettering both count:
 *
 * - `deadLetter: { exchange: … }` — the typed form. `setupAmqpTopology` turns
 *   it into the `x-dead-letter-exchange` declare argument.
 * - `arguments: { "x-dead-letter-exchange": … }` — the raw passthrough.
 *   `setupAmqpTopology` spreads `queue.arguments` into the same declare
 *   arguments, so the queue really is dead-lettering. Documented and working,
 *   so it must not be treated as an undeclared loss.
 *
 * Only the presence of a declaration is checked. Whether the named exchange
 * has a queue bound to it is not something a contract can see, and the two
 * forms' precedence when both are set is a separate concern owned by
 * `setupAmqpTopology` — either way, the queue dead-letters, which is the only
 * thing this predicate claims.
 *
 * An empty or non-string `x-dead-letter-exchange` is not a declaration: `""`
 * on RabbitMQ names the default exchange, but reading it as "dead-lettering is
 * configured" here would silence the loss warning for a queue that has stated
 * nothing. False negatives are the safe direction for every caller.
 *
 * @internal
 */
export function _internal_queueHasDeadLetterExchange(queue: QueueDefinition): boolean {
  if (queue.deadLetter !== undefined) return true;

  const rawDlx = queue.arguments?.[DEAD_LETTER_EXCHANGE_ARGUMENT];
  return typeof rawDlx === "string" && rawDlx.length > 0;
}
