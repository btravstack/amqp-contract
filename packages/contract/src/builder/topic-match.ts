/**
 * Runtime AMQP topic-pattern matching.
 *
 * Mirrors the compile-time `MatchesPattern` in `routing-types.ts`. The two
 * must agree on every input — `match-corpus.ts` is asserted against both,
 * and a divergence fails one of the two suites.
 *
 * AMQP semantics:
 * - a routing key is dot-separated words
 * - `*` matches exactly one word
 * - `#` matches zero or more words
 *
 * @internal
 */

/**
 * Backtracking match of `key[ki..]` against `pattern[pi..]`.
 *
 * `#` needs backtracking rather than a greedy consume: `order.#.v2` against
 * `order.a.b.v2` only matches if `#` gives back the trailing `v2`.
 */
function matchFrom(
  key: readonly string[],
  ki: number,
  pattern: readonly string[],
  pi: number,
): boolean {
  if (pi === pattern.length) {
    return ki === key.length;
  }

  const token = pattern[pi];

  if (token === "#") {
    // Try every number of words '#' could absorb, shortest first.
    for (let skip = 0; ki + skip <= key.length; skip += 1) {
      if (matchFrom(key, ki + skip, pattern, pi + 1)) {
        return true;
      }
    }
    return false;
  }

  if (ki === key.length) {
    return false;
  }

  if (token === "*" || token === key[ki]) {
    return matchFrom(key, ki + 1, pattern, pi + 1);
  }

  return false;
}

/**
 * True when `routingKey` is delivered by a binding declared with `pattern`.
 *
 * @param routingKey - Concrete routing key (no wildcards)
 * @param pattern - Binding pattern (may contain `*` and `#`)
 * @internal
 */
export function _internal_matchesTopicPattern(routingKey: string, pattern: string): boolean {
  const keyWords = routingKey === "" ? [] : routingKey.split(".");
  const patternWords = pattern === "" ? [] : pattern.split(".");
  return matchFrom(keyWords, 0, patternWords, 0);
}
