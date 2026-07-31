/**
 * Shared routing-key/pattern corpus.
 *
 * Asserted twice — once against the runtime matcher
 * (`topic-match.spec.ts`) and once against the type-level matcher
 * (`routability.test-d.ts`). That double assertion is what pins the
 * spec's invariant that the two implementations agree; if they ever
 * diverge, one of the two suites fails.
 *
 * @internal
 */
export const MATCH_CORPUS = [
  // Exact matches, no wildcards.
  { key: "order.created", pattern: "order.created", matches: true },
  { key: "order.created", pattern: "order.updated", matches: false },
  { key: "order", pattern: "order", matches: true },

  // '*' matches exactly one word.
  { key: "order.created", pattern: "order.*", matches: true },
  { key: "order.created.v2", pattern: "order.*", matches: false },
  { key: "order.created", pattern: "*.created", matches: true },
  { key: "order.created", pattern: "*.*", matches: true },
  { key: "order", pattern: "*", matches: true },
  { key: "order.created", pattern: "*", matches: false },

  // '#' matches zero or more words.
  { key: "order.created", pattern: "#", matches: true },
  { key: "order", pattern: "#", matches: true },
  { key: "order.created", pattern: "order.#", matches: true },
  { key: "order", pattern: "order.#", matches: true },
  { key: "order.created.v2", pattern: "order.#", matches: true },
  { key: "order.created", pattern: "#.created", matches: true },
  { key: "created", pattern: "#.created", matches: true },
  { key: "order.created.v2", pattern: "order.#.v2", matches: true },
  { key: "order.v2", pattern: "order.#.v2", matches: true },
  { key: "order.a.b.v2", pattern: "order.#.v2", matches: true },
  { key: "order.created", pattern: "order.#.v2", matches: false },

  // Mixed wildcards.
  { key: "order.created.v2", pattern: "order.*.#", matches: true },
  { key: "order.created", pattern: "order.*.#", matches: true },
  { key: "order", pattern: "order.*.#", matches: false },

  // Non-matches that must not accidentally pass.
  { key: "user.created", pattern: "order.#", matches: false },
  { key: "order.created", pattern: "order.created.v2", matches: false },
] as const satisfies readonly { key: string; pattern: string; matches: boolean }[];
