// ============================================================================
// Routing Key and Binding Pattern Validation Types
// ============================================================================

/**
 * Type-safe routing key that validates basic format.
 *
 * Validates that a routing key follows basic AMQP routing key rules:
 * - Must not contain wildcards (* or #)
 * - Must not be empty
 * - Should contain alphanumeric characters, dots, hyphens, and underscores
 *
 * Note: Full character-by-character validation is not performed to avoid TypeScript
 * recursion depth limits. Runtime validation is still recommended.
 *
 * @public
 * @template S - The routing key string to validate
 * @example
 * ```typescript
 * type Valid = RoutingKey<"order.created">; // "order.created"
 * type Invalid = RoutingKey<"order.*">; // never (contains wildcard)
 * type Invalid2 = RoutingKey<"">; // never (empty string)
 * ```
 */
export type RoutingKey<S extends string> = S extends ""
  ? never // Empty string not allowed
  : S extends `${string}*${string}` | `${string}#${string}`
    ? never // Wildcards not allowed in routing keys
    : S; // Accept the routing key as-is

/**
 * Type-safe binding pattern that validates basic format and wildcards.
 *
 * Validates that a binding pattern follows basic AMQP binding pattern rules:
 * - Can contain wildcards (* for one word, # for zero or more words)
 * - Must not be empty
 * - Should contain alphanumeric characters, dots, hyphens, underscores, and wildcards
 *
 * Note: Full character-by-character validation is not performed to avoid TypeScript
 * recursion depth limits. Runtime validation is still recommended.
 *
 * @public
 * @template S - The binding pattern string to validate
 * @example
 * ```typescript
 * type ValidPattern = BindingPattern<"order.*">; // "order.*"
 * type ValidHash = BindingPattern<"order.#">; // "order.#"
 * type ValidConcrete = BindingPattern<"order.created">; // "order.created"
 * type Invalid = BindingPattern<"">; // never (empty string)
 * ```
 */
export type BindingPattern<S extends string> = S extends "" ? never : S;

/**
 * True when every member of `S` is a string literal fully known at compile
 * time; false for `string`, for template-literal types with a `${…}` hole, for
 * a union containing either, and for the empty union.
 *
 * The matcher types below can only decide a match when both sides are fully
 * known. `string extends S` alone does not establish that: a partially literal
 * type such as `` `${string}.orders` `` is not `string`, so it passes that test
 * and then reaches a matcher that cannot decide it — which reports a pattern
 * that matches at runtime as an error. Deciding it here, once, is what keeps
 * the three matchers from drifting apart again.
 *
 * `Record<S, 1>` yields a concrete property for a literal key and a pattern
 * index signature for a template-literal key. `{}` is assignable to the latter
 * and not the former, which separates them in one step — no per-character
 * recursion, so no instantiation-depth risk on long routing keys.
 *
 * @internal
 */
export type IsStringLiteral<S extends string> = string extends S
  ? false
  : [S] extends [never]
    ? false
    : (S extends string ? ({} extends Record<S, 1> ? false : true) : never) extends true
      ? true
      : false;

/**
 * True when a pattern remainder consists only of `#` segments (`#`, `#.#`, …).
 * Such a remainder can match zero words, which matters once the routing key
 * has been fully consumed (e.g. pattern `order.created.#` vs key
 * `order.created` — `#` matches zero trailing words, so they match).
 * @internal
 */
type IsHashOnly<Pattern extends string> = Pattern extends "#"
  ? true
  : Pattern extends `#.${infer Rest}`
    ? IsHashOnly<Rest>
    : false;

/**
 * Helper type for pattern matching with # in the middle
 * Handles backtracking to match # with zero or more segments
 * @internal
 */
type MatchesAfterHash<Key extends string, PatternRest extends string> =
  MatchesPattern<Key, PatternRest> extends true
    ? true // # matches zero segments
    : Key extends `${string}.${infer KeyRest}`
      ? MatchesAfterHash<KeyRest, PatternRest> // # matches one or more segments
      : false;

/**
 * Check if a routing key matches a binding pattern
 * Implements AMQP topic exchange pattern matching:
 * - * matches exactly one word
 * - # matches zero or more words
 * @internal
 */
type MatchesPattern<
  Key extends string,
  Pattern extends string,
> = Pattern extends `${infer PatternPart}.${infer PatternRest}`
  ? PatternPart extends "#"
    ? MatchesAfterHash<Key, PatternRest> // # in the middle: backtrack over all possible segment lengths
    : Key extends `${infer KeyPart}.${infer KeyRest}`
      ? PatternPart extends "*"
        ? MatchesPattern<KeyRest, PatternRest> // * matches one segment
        : PatternPart extends KeyPart
          ? MatchesPattern<KeyRest, PatternRest> // Exact match
          : false
      : PatternPart extends "*" | Key
        ? IsHashOnly<PatternRest> // Last key word consumed; the rest matches only if it can match zero words
        : false
  : Pattern extends "#"
    ? true // # matches everything (including empty)
    : Pattern extends "*"
      ? Key extends `${string}.${string}`
        ? false // * matches exactly 1 segment, not multiple
        : true
      : Pattern extends Key
        ? true // Exact match
        : false;

/**
 * Validate that a routing key matches a binding pattern.
 *
 * This is a utility type for users who want compile-time validation that a
 * routing key matches a specific pattern. The library enforces the same
 * matching on `defineEventConsumer`'s topic routing-key overrides via
 * {@link MatchingBindingPattern} (which surfaces a readable error-message
 * string type instead of `never`).
 *
 * Returns the routing key if it's valid and matches the pattern, `never` otherwise.
 *
 * The check runs only when both the pattern and the key are fully known at
 * compile time. Plain `string`, template-literal types, and unions containing
 * either resolve to `Key` unchecked — the match cannot be decided, and
 * guessing would reject a key that routes at runtime.
 *
 * @example
 * ```typescript
 * type ValidKey = MatchingRoutingKey<"order.*", "order.created">; // "order.created"
 * type InvalidKey = MatchingRoutingKey<"order.*", "user.created">; // never
 * ```
 *
 * @template Pattern - The binding pattern (can contain * and # wildcards)
 * @template Key - The routing key to validate
 */
export type MatchingRoutingKey<Pattern extends string, Key extends string> =
  IsStringLiteral<Pattern> extends false
    ? Key // Undecidable at compile time — defer rather than guess
    : IsStringLiteral<Key> extends false
      ? Key
      : RoutingKey<Key> extends never
        ? never // Invalid routing key
        : BindingPattern<Pattern> extends never
          ? never // Invalid pattern
          : MatchesPattern<Key, Pattern> extends true
            ? Key
            : never;

/**
 * Binding pattern for a topic consumer, validated against the publisher's
 * concrete routing key.
 *
 * `defineEventConsumer` uses this on its topic overloads: a routing-key
 * override must be a pattern that can actually match the event publisher's
 * routing key, otherwise the binding compiles but silently receives nothing
 * at runtime. On a mismatch this resolves to a human-readable error-message
 * string type — so the compile error names both sides instead of collapsing
 * to a bare `never`:
 *
 * ```
 * Type '"user.*"' is not assignable to type
 *   "Error: binding pattern 'user.*' can never match the publisher routing key 'order.created'"
 * ```
 *
 * The check runs only when both sides are fully known at compile time. Plain
 * `string`, a template-literal type with a `${…}` hole (`` `${string}.created` ``),
 * and any union containing either are skipped: the match cannot be decided, and
 * guessing would reject a pattern that matches at runtime. Those contracts are
 * covered by the define-time routability check in `defineContract`, which runs
 * on concrete strings.
 *
 * @template Pattern - The consumer's binding pattern (can contain * and # wildcards)
 * @template PublisherKey - The publisher's concrete routing key
 */
export type MatchingBindingPattern<Pattern extends string, PublisherKey extends string> =
  IsStringLiteral<Pattern> extends false
    ? BindingPattern<Pattern>
    : IsStringLiteral<PublisherKey> extends false
      ? BindingPattern<Pattern>
      : [BindingPattern<Pattern>] extends [never]
        ? never // Empty pattern — same rejection as BindingPattern
        : MatchesPattern<PublisherKey, Pattern> extends true
          ? Pattern
          : `Error: binding pattern '${Pattern}' can never match the publisher routing key '${PublisherKey}'`;

/**
 * True when `Key` matches at least one pattern in the `Patterns` union.
 *
 * Distributes over the union rather than recursing across a list, which
 * keeps instantiation depth bounded by the longest single pattern instead
 * of by the number of bindings.
 * @internal
 */
type MatchesAnyPattern<Key extends string, Patterns extends string> = [Patterns] extends [never]
  ? false
  : true extends (Patterns extends string ? MatchesPattern<Key, Patterns> : never)
    ? true
    : false;

/**
 * A publisher routing key validated against the binding patterns declared on
 * its exchange.
 *
 * A message routed to zero queues is confirmed by RabbitMQ and then
 * discarded, so an unmatched routing key is silent total message loss. On no
 * match this resolves to a human-readable error-message string type, so the
 * compile error explains the problem instead of collapsing to `never` —
 * matching the {@link MatchingBindingPattern} convention.
 *
 * Skipped (resolves to `Key`) when either side is non-literal, or when no
 * patterns are declared: those cases cannot be decided at compile time and
 * are left to the define-time check in `defineContract`.
 *
 * Scope: single-hop queue bindings on topic and direct exchanges. Fanout,
 * headers, and exchange-to-exchange forwards are deliberately not modelled
 * here — deciding them needs graph traversal in the type system, which risks
 * recursion-depth failures and false compile errors on valid contracts. Those
 * cases fall through to the define-time check, which sees the whole graph.
 *
 * @template Key - The publisher's concrete routing key
 * @template Patterns - Union of binding patterns declared on the exchange
 * @example
 * ```typescript
 * type Ok = RoutableRoutingKey<"order.created", "order.#" | "user.#">; // "order.created"
 * type Bad = RoutableRoutingKey<"order.created", "user.#">;
 * // "Error: routing key 'order.created' matches none of the declared binding
 * //  patterns; the broker would confirm and discard every message"
 * ```
 */
export type RoutableRoutingKey<Key extends string, Patterns extends string> =
  IsStringLiteral<Key> extends false
    ? Key
    : IsStringLiteral<Patterns> extends false
      ? Key
      : MatchesAnyPattern<Key, Patterns> extends true
        ? Key
        : `Error: routing key '${Key}' matches none of the declared binding patterns; the broker would confirm and discard every message`;
