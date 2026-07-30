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
  RoutingKey<Key> extends never
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
 * Non-literal strings (plain `string` on either side) skip the check — the
 * match cannot be decided at compile time, so runtime behavior is preserved.
 *
 * @template Pattern - The consumer's binding pattern (can contain * and # wildcards)
 * @template PublisherKey - The publisher's concrete routing key
 */
export type MatchingBindingPattern<
  Pattern extends string,
  PublisherKey extends string,
> = string extends Pattern
  ? BindingPattern<Pattern>
  : string extends PublisherKey
    ? BindingPattern<Pattern>
    : [BindingPattern<Pattern>] extends [never]
      ? never // Empty pattern — same rejection as BindingPattern
      : MatchesPattern<PublisherKey, Pattern> extends true
        ? Pattern
        : `Error: binding pattern '${Pattern}' can never match the publisher routing key '${PublisherKey}'`;
