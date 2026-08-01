import { _internal_queueHasDeadLetterExchange } from "../dead-letter.js";
import type { QueueDefinition } from "../types.js";

/**
 * Reject a consumed queue that would silently drop its poison messages.
 *
 * A queue with no dead-letter exchange loses every message its consumer
 * rejects: `nack(requeue: false)` discards it, and nothing observes the loss.
 * The worker logs a warning at the moment it happens — too late, and only if a
 * logger is wired — so the decision moves to define time.
 *
 * Scoped to consumed queues deliberately. A dead-letter queue has no DLX of its
 * own (that would be infinite regress) and is usually inspected rather than
 * consumed; requiring one there would reject a correct contract.
 *
 * A false negative is acceptable here; rejecting a valid contract is not. Which
 * declarations count is {@link _internal_queueHasDeadLetterExchange}'s call —
 * the same predicate the worker's terminal-nack logging asks, so a queue this
 * guard accepts can never be logged as an undeclared loss.
 *
 * @internal
 */
export function _internal_assertNoSilentPoisonLoss(
  queue: QueueDefinition,
  consumedBy: string,
): void {
  if (_internal_queueHasDeadLetterExchange(queue)) return;
  if (queue.onPoison === "drop") return;

  // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)
  throw new Error(
    `Queue "${queue.name}" is consumed by "${consumedBy}" but has no dead-letter exchange, ` +
      `so every message its handler rejects is discarded with no record. Add ` +
      `\`deadLetter: { exchange: … }\` to keep failed messages for inspection, or set ` +
      `\`onPoison: "drop"\` on the queue if losing them is deliberate.`,
  );
}
