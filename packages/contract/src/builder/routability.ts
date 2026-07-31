import type { BindingDefinition, ExchangeDefinition } from "../types.js";
import { _internal_matchesTopicPattern } from "./topic-match.js";

/**
 * Decides whether a publisher can reach at least one queue.
 *
 * Routability is a graph problem, not a single-hop lookup: an exchange can
 * forward to another exchange (`defineBridgedPublisher`), so the publisher's
 * message may reach a queue several hops away. A single-hop check would
 * falsely reject every bridged contract.
 *
 * @internal
 */

/** True when a binding declared on `exchange` accepts `routingKey`. */
function bindingAccepts(
  exchange: ExchangeDefinition,
  routingKey: string | undefined,
  bindingRoutingKey: string | undefined,
): boolean {
  switch (exchange.type) {
    case "fanout":
      // The broker ignores the routing key entirely: any binding routes.
      return true;
    case "headers":
      // Matching is on the binding's arguments, which cannot be decided
      // against a routing key. Treat any binding as potentially routable
      // rather than raising a false alarm.
      return true;
    case "direct":
      return routingKey !== undefined && routingKey === bindingRoutingKey;
    case "topic":
      return (
        routingKey !== undefined &&
        bindingRoutingKey !== undefined &&
        _internal_matchesTopicPattern(routingKey, bindingRoutingKey)
      );
  }
}

/**
 * The outcome of resolving a publisher against the binding graph.
 *
 * `reachedExchanges` is what makes a multi-hop failure diagnosable: it names
 * every exchange the routing key actually got to, in BFS order with the
 * publisher's own exchange first. When it holds more than one name the key
 * *did* match on the source and the loss happened further downstream — a
 * distinction a bare boolean cannot express, and one the error message needs
 * so it does not point the reader at a binding that visibly matches.
 *
 * @internal
 */
export type PublisherRoutability = {
  /** True when at least one queue is reachable. */
  readonly routable: boolean;
  /**
   * Exchanges the key reached, source first. Complete only when
   * `routable` is false — resolution stops at the first queue it finds.
   */
  readonly reachedExchanges: readonly string[];
};

/**
 * Resolve whether a message published to `exchange` with `routingKey` reaches
 * at least one queue, directly or through exchange-to-exchange forwards, and
 * report which exchanges it got to along the way.
 *
 * @internal
 */
export function _internal_resolvePublisherRoutability(
  exchange: ExchangeDefinition,
  routingKey: string | undefined,
  bindings: readonly BindingDefinition[],
): PublisherRoutability {
  // Cycle guard: exchange graphs may contain loops, and the routing key is
  // preserved across forwards, so the exchange name alone identifies a state.
  const visited = new Set<string>();
  const queue: ExchangeDefinition[] = [exchange];

  while (queue.length > 0) {
    const current = queue.shift() as ExchangeDefinition;
    if (visited.has(current.name)) {
      continue;
    }
    visited.add(current.name);

    for (const binding of bindings) {
      if (binding.type === "queue") {
        if (binding.exchange.name !== current.name) continue;
        const bindingKey = "routingKey" in binding ? binding.routingKey : undefined;
        if (bindingAccepts(current, routingKey, bindingKey)) {
          return { routable: true, reachedExchanges: [...visited] };
        }
        continue;
      }

      if (binding.source.name !== current.name) continue;
      const bindingKey = "routingKey" in binding ? binding.routingKey : undefined;
      if (bindingAccepts(current, routingKey, bindingKey)) {
        queue.push(binding.destination);
      }
    }
  }

  return { routable: false, reachedExchanges: [...visited] };
}

/**
 * True when a message published to `exchange` with `routingKey` reaches at
 * least one queue, directly or through exchange-to-exchange forwards.
 *
 * The boolean-only view of {@link _internal_resolvePublisherRoutability}, kept
 * for callers that only need the verdict.
 *
 * @internal
 */
export function _internal_isPublisherRoutable(
  exchange: ExchangeDefinition,
  routingKey: string | undefined,
  bindings: readonly BindingDefinition[],
): boolean {
  return _internal_resolvePublisherRoutability(exchange, routingKey, bindings).routable;
}

/**
 * The routing patterns declared on an exchange, in declaration order — used
 * to make the define-time error actionable by showing what *is* declared.
 *
 * @internal
 */
export function _internal_declaredPatternsFor(
  exchangeName: string,
  bindings: readonly BindingDefinition[],
): readonly string[] {
  const patterns: string[] = [];
  for (const binding of bindings) {
    const source = binding.type === "queue" ? binding.exchange : binding.source;
    if (source.name !== exchangeName) continue;
    if ("routingKey" in binding && typeof binding.routingKey === "string") {
      patterns.push(binding.routingKey);
    }
  }
  return patterns;
}
