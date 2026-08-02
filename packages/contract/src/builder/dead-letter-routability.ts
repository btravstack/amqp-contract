import type { BindingDefinition, QueueDefinition } from "../types.js";
import {
  _internal_declaredPatternsFor,
  _internal_resolvePublisherRoutability,
} from "./routability.js";

/**
 * Can a queue's dead-lettered messages reach a queue?
 *
 * RabbitMQ drops a dead-lettered message routed to zero queues exactly as
 * silently as it drops an unroutable publish — the runtime signal is
 * indistinguishable from success, and the worker logs a reassuring
 * "Sending message to DLQ" while every message vanishes. The H2 guard checks
 * that a dead-letter exchange was *declared*; this one checks that something
 * is bound to it.
 *
 * Reuses {@link _internal_resolvePublisherRoutability} rather than adding a
 * second resolver: multi-hop forwards, cycle detection and per-exchange-type
 * semantics are the same problem on a different edge of the graph.
 *
 * The governing rule is never to reject a valid contract, so every case that
 * cannot be decided resolves to "skipped".
 *
 * @internal
 */
export type DeadLetterVerdict =
  | "skipped-undecidable"
  | "skipped-external"
  | "routable"
  | "unroutable";

/**
 * Decide the verdict for one queue. Rows are evaluated in order, first match
 * wins — see the decision table in the design spec.
 *
 * @internal
 */
export function _internal_resolveDeadLetterRoutability(
  queue: QueueDefinition,
  bindings: readonly BindingDefinition[],
): DeadLetterVerdict {
  const deadLetter = queue.deadLetter;

  // Row 1. No typed config: either the queue does not dead-letter at all (H2's
  // guard covers that), or the DLX came through the `arguments` passthrough as
  // a bare exchange NAME. There is no ExchangeDefinition to look bindings up
  // on, and the contract need not declare that exchange at all, so routability
  // is genuinely unknowable.
  if (deadLetter === undefined) {
    return "skipped-undecidable";
  }

  // Row 2.
  if (deadLetter.externalConsumers === true) {
    return "skipped-external";
  }

  const exchange = deadLetter.exchange;
  const ignoresRoutingKey = exchange.type === "fanout" || exchange.type === "headers";

  // Row 3. The key is known, or the exchange ignores it — the shared resolver
  // decides, exactly as it does for publishers.
  if (deadLetter.routingKey !== undefined || ignoresRoutingKey) {
    return _internal_resolvePublisherRoutability(exchange, deadLetter.routingKey, bindings).routable
      ? "routable"
      : "unroutable";
  }

  // Row 4. A direct or topic DLX with no routing key: RabbitMQ preserves the
  // message's ORIGINAL key, which is not knowable at define time. Proving this
  // case properly means showing every key that can reach the source queue also
  // matches a binding out of the DLX — pattern-subset reasoning, and getting it
  // wrong rejects a valid contract. "At least one binding" catches the defect
  // actually observed (a DLX with nothing bound) at zero false-positive risk,
  // and accepts a DLX bound only to non-matching patterns. A known, deliberate
  // false negative.
  return _internal_declaredPatternsFor(exchange.name, bindings).length > 0
    ? "routable"
    : "unroutable";
}
