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

/**
 * Throw when a queue's dead-lettered messages would reach no queue.
 *
 * The remedy deliberately never says "add a `deadLetter` config" — the author
 * has one; that is the premise of the finding. A remedy that does not apply is
 * worse than none, because it sends the reader after the wrong thing while
 * looking authoritative.
 *
 * On a DIRECT exchange the remedy carries an extra warning about `#`. Row 4 of
 * the decision table accepts ANY binding when the queue sets no dead-letter
 * routing key, so a reader who reaches for `#` here passes this check and still
 * routes nothing — `#` is a topic wildcard, and a direct exchange treats it as a
 * literal key. The guard structurally cannot catch that (it does not know which
 * key will arrive), so this message is the only place that can warn.
 *
 * @internal
 */
export function _internal_assertDeadLetterRoutable(
  queue: QueueDefinition,
  bindings: readonly BindingDefinition[],
): void {
  const verdict = _internal_resolveDeadLetterRoutability(queue, bindings);
  if (verdict !== "unroutable") return;

  // Non-null: "unroutable" is only reachable through rows 3 and 4, both of
  // which require a typed `deadLetter` config.
  const deadLetter = queue.deadLetter as NonNullable<QueueDefinition["deadLetter"]>;
  const exchange = deadLetter.exchange;
  const declared = _internal_declaredPatternsFor(exchange.name, bindings);
  const declaredText =
    declared.length > 0
      ? `Declared on "${exchange.name}": ${declared.map((p) => `"${p}"`).join(", ")}.`
      : `Nothing is bound to "${exchange.name}".`;
  const keyText =
    deadLetter.routingKey === undefined
      ? "its dead-lettered messages keep their original routing key"
      : `its dead-lettered messages are routed with "${deadLetter.routingKey}"`;
  // See the doc comment: on a direct exchange this check cannot tell a routing
  // binding from a decorative one, so the warning has to travel in the message.
  const directHint =
    exchange.type === "direct"
      ? ` Bind the routing key itself: "#" is a topic wildcard, and on a direct exchange it is a` +
        ` literal key that matches nothing.`
      : "";

  // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)
  throw new Error(
    `Queue "${queue.name}" dead-letters to exchange "${exchange.name}" (${exchange.type}), but ` +
      `nothing there can receive them: ${keyText}. ${declaredText} RabbitMQ discards a message ` +
      `routed to zero queues, so these would be lost exactly as silently as if the queue had no ` +
      `dead-letter exchange at all. Bind a queue to "${exchange.name}" that accepts them, or set ` +
      `\`externalConsumers: true\` on the deadLetter config if another service owns that queue.` +
      directHint,
  );
}
