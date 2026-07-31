# H1 — Unroutable Publish Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make it impossible to ship a contract whose publisher routes to nowhere, and make a runtime-unroutable publish a modeled error instead of a silent `Ok`.

**Architecture:** A pure runtime topic matcher feeds a routability resolver that walks the contract's binding graph (queue bindings plus exchange-to-exchange forwards, with cycle detection). `defineContract` calls the resolver and throws for unroutable publishers (rung 2). A parallel type-level check rejects the single-hop case at compile time with a readable error-message type (rung 1). Rung 3 (broker-level `mandatory` + `basic.return`) is gated behind a spike because the correlation mechanism is unproven.

**Tech Stack:** TypeScript ESM, vitest (+ `expectTypeOf` for type tests), fast-check for property tests, testcontainers RabbitMQ for real-broker proofs.

**Source spec:** `docs/superpowers/specs/2026-08-01-robustness-hardening-design.md`

## Global Constraints

- No `any` — use `unknown` and narrow. Enforced by oxlint.
- Type aliases over interfaces (`type Foo = {}`).
- `.js` extensions required in all imports (ESM).
- Internal cross-module helpers use the `_internal_` prefix (no semver guarantee), matching `packages/contract/src/builder/validate.ts`.
- Define-time failures `throw` and need `// oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error` above them.
- Conventional commits (`feat`, `fix`, `test`, `docs`, `refactor`, `chore`). Enforced by commitlint.
- Dependencies go through `pnpm-workspace.yaml` catalog — never hardcode a version in a `package.json`.
- Public API changes need a changeset (`pnpm changeset`). The six publishable packages bump together.
- Run `pnpm typecheck` before declaring a task done; it is not in the pre-commit hook.
- This work lands **before** 3.0 stable ships. Breaking changes are expected and desired.

## Design decisions locked in during planning

Two refinements to the spec, both discovered by reading the type definitions:

1. **Routability is a graph, not a single hop.** `ExchangeBindingDefinition` lets an exchange forward to another exchange (`defineBridgedPublisher`). A publisher is routable if any path from its exchange reaches a queue binding. A single-hop check would falsely reject every bridged contract.

2. **Publish-only contracts are legitimate.** A service publishing to an exchange whose consumers deploy separately has zero bindings in its own contract. Throwing on those would break a valid, common pattern. Per the spec's fail-fast-with-explicit-opt-out policy, this requires an explicit marker on the publisher: `externalConsumers: true`. No heuristic — a heuristic would silently miss the typo case this whole feature exists to catch.

---

## File Structure

| File                                                         | Responsibility                                                       |
| ------------------------------------------------------------ | -------------------------------------------------------------------- |
| `packages/contract/src/builder/topic-match.ts` (create)      | Pure runtime AMQP topic-pattern matching. No deps.                   |
| `packages/contract/src/builder/topic-match.spec.ts` (create) | Unit + property tests for the matcher.                               |
| `packages/contract/src/builder/routability.ts` (create)      | Binding-graph traversal; decides if a publisher can reach a queue.   |
| `packages/contract/src/builder/routability.spec.ts` (create) | Unit tests for the resolver, including bridges and cycles.           |
| `packages/contract/src/builder/validate.ts` (modify)         | Add `_internal_assertPublisherRoutable`.                             |
| `packages/contract/src/builder/contract.ts` (modify)         | Call the assertion for every publisher after bindings are collected. |
| `packages/contract/src/builder/publisher.ts` (modify)        | Accept and carry `externalConsumers`.                                |
| `packages/contract/src/types.ts` (modify)                    | Add `externalConsumers?: boolean` to `PublisherDefinition`.          |
| `packages/contract/src/builder/routing-types.ts` (modify)    | Add the rung-1 type-level routability check.                         |
| `packages/contract/src/routability.test-d.ts` (create)       | Type-level tests + the shared corpus asserted at type level.         |
| `packages/contract/src/builder/match-corpus.ts` (create)     | Shared key/pattern corpus used by both runtime and type-level tests. |
| `tests/src/unroutable-publish.spec.ts` (create)              | Real-broker prove-the-loss / prove-the-guard tests.                  |

---

### Task 1: Runtime topic matcher

**Files:**

- Create: `packages/contract/src/builder/topic-match.ts`
- Create: `packages/contract/src/builder/match-corpus.ts`
- Test: `packages/contract/src/builder/topic-match.spec.ts`
- Modify: `pnpm-workspace.yaml` (add `fast-check` to catalog)
- Modify: `packages/contract/package.json` (add `fast-check` devDependency)

**Interfaces:**

- Produces: `_internal_matchesTopicPattern(routingKey: string, pattern: string): boolean`
- Produces: `MATCH_CORPUS: readonly { key: string; pattern: string; matches: boolean }[]`

- [ ] **Step 1: Add fast-check to the catalog**

In `pnpm-workspace.yaml`, add to the `catalog:` block (alphabetical position):

```yaml
fast-check: ^4.3.0
```

In `packages/contract/package.json` `devDependencies`, add (alphabetical):

```json
    "fast-check": "catalog:",
```

Then run:

```bash
pnpm install
```

- [ ] **Step 2: Write the shared corpus**

Create `packages/contract/src/builder/match-corpus.ts`:

```ts
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
```

- [ ] **Step 3: Write the failing test**

Create `packages/contract/src/builder/topic-match.spec.ts`:

```ts
import fc from "fast-check";
import { describe, expect, it } from "vitest";

import { MATCH_CORPUS } from "./match-corpus.js";
import { _internal_matchesTopicPattern } from "./topic-match.js";

describe("_internal_matchesTopicPattern", () => {
  describe("shared corpus", () => {
    for (const { key, pattern, matches } of MATCH_CORPUS) {
      it(`${matches ? "matches" : "does not match"} key "${key}" against pattern "${pattern}"`, () => {
        expect(_internal_matchesTopicPattern(key, pattern)).toBe(matches);
      });
    }
  });

  describe("properties", () => {
    // Words are non-empty, wildcard-free AMQP-ish tokens.
    const word = fc.stringMatching(/^[a-z0-9_-]{1,6}$/);
    const key = fc.array(word, { minLength: 1, maxLength: 4 }).map((w) => w.join("."));

    it("a wildcard-free pattern matches exactly its own key", () => {
      fc.assert(
        fc.property(key, (k) => {
          expect(_internal_matchesTopicPattern(k, k)).toBe(true);
        }),
      );
    });

    it("'#' alone matches every key", () => {
      fc.assert(
        fc.property(key, (k) => {
          expect(_internal_matchesTopicPattern(k, "#")).toBe(true);
        }),
      );
    });

    it("an all-'*' pattern matches iff the word counts are equal", () => {
      fc.assert(
        fc.property(key, fc.integer({ min: 1, max: 6 }), (k, starCount) => {
          const pattern = Array.from({ length: starCount }, () => "*").join(".");
          const expected = k.split(".").length === starCount;
          expect(_internal_matchesTopicPattern(k, pattern)).toBe(expected);
        }),
      );
    });

    it("appending '.#' to a matching pattern keeps it matching", () => {
      fc.assert(
        fc.property(key, (k) => {
          expect(_internal_matchesTopicPattern(k, `${k}.#`)).toBe(true);
        }),
      );
    });

    it("is deterministic", () => {
      fc.assert(
        fc.property(key, key, (k, p) => {
          expect(_internal_matchesTopicPattern(k, p)).toBe(_internal_matchesTopicPattern(k, p));
        }),
      );
    });
  });
});
```

- [ ] **Step 4: Run the test to verify it fails**

```bash
cd packages/contract && pnpm vitest run src/builder/topic-match.spec.ts
```

Expected: FAIL — cannot resolve `./topic-match.js`.

- [ ] **Step 5: Implement the matcher**

Create `packages/contract/src/builder/topic-match.ts`:

```ts
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
```

- [ ] **Step 6: Run the tests to verify they pass**

```bash
cd packages/contract && pnpm vitest run src/builder/topic-match.spec.ts
```

Expected: PASS, all corpus cases and all 5 properties.

- [ ] **Step 7: Typecheck and lint**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm typecheck && pnpm lint
```

Expected: both clean.

- [ ] **Step 8: Commit**

```bash
git add pnpm-workspace.yaml pnpm-lock.yaml packages/contract/package.json \
  packages/contract/src/builder/topic-match.ts \
  packages/contract/src/builder/match-corpus.ts \
  packages/contract/src/builder/topic-match.spec.ts
git commit -m "feat(contract): add a runtime AMQP topic-pattern matcher

Mirrors the compile-time MatchesPattern. The shared corpus is asserted
against both implementations so they cannot silently diverge."
```

---

### Task 2: Routability resolver

**Files:**

- Create: `packages/contract/src/builder/routability.ts`
- Test: `packages/contract/src/builder/routability.spec.ts`

**Interfaces:**

- Consumes: `_internal_matchesTopicPattern` from Task 1
- Produces: `_internal_isPublisherRoutable(exchange: ExchangeDefinition, routingKey: string | undefined, bindings: readonly BindingDefinition[]): boolean`
- Produces: `_internal_declaredPatternsFor(exchangeName: string, bindings: readonly BindingDefinition[]): readonly string[]`

- [ ] **Step 1: Write the failing test**

Create `packages/contract/src/builder/routability.spec.ts`:

```ts
import { describe, expect, it } from "vitest";

import type { BindingDefinition, ExchangeDefinition } from "../types.js";
import { defineExchange } from "./exchange.js";
import { defineQueue } from "./queue.js";
import { _internal_declaredPatternsFor, _internal_isPublisherRoutable } from "./routability.js";

const ordersTopic = defineExchange("orders", { type: "topic" });
const ordersDirect = defineExchange("orders-direct", { type: "direct" });
const broadcast = defineExchange("broadcast", { type: "fanout" });
const billing = defineExchange("billing", { type: "topic" });
const q = defineQueue("audit-log");

// Widened to ExchangeDefinition so topic exchanges with different name
// literals (`orders`, `billing`) share one helper.
function queueBinding(exchange: ExchangeDefinition, routingKey: string): BindingDefinition {
  return { type: "queue", queue: q, exchange, routingKey } as BindingDefinition;
}

describe("_internal_isPublisherRoutable", () => {
  it("is routable when a topic queue binding matches the key", () => {
    const bindings = [queueBinding(ordersTopic, "order.#")];
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", bindings)).toBe(true);
  });

  it("is NOT routable when no topic binding matches the key", () => {
    const bindings = [queueBinding(ordersTopic, "user.#")];
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", bindings)).toBe(false);
  });

  it("is NOT routable when there are no bindings at all", () => {
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", [])).toBe(false);
  });

  it("requires exact equality on a direct exchange", () => {
    const bindings = [
      { type: "queue", queue: q, exchange: ordersDirect, routingKey: "order.created" },
    ] as BindingDefinition[];
    expect(_internal_isPublisherRoutable(ordersDirect, "order.created", bindings)).toBe(true);
    expect(_internal_isPublisherRoutable(ordersDirect, "order.*", bindings)).toBe(false);
  });

  it("treats any binding on a fanout exchange as routable", () => {
    const bindings = [{ type: "queue", queue: q, exchange: broadcast }] as BindingDefinition[];
    expect(_internal_isPublisherRoutable(broadcast, undefined, bindings)).toBe(true);
  });

  it("is NOT routable on a fanout exchange with no bindings", () => {
    expect(_internal_isPublisherRoutable(broadcast, undefined, [])).toBe(false);
  });

  it("follows an exchange-to-exchange forward to a queue (bridged publisher)", () => {
    // orders --order.#--> billing --#--> queue
    const bindings = [
      { type: "exchange", source: ordersTopic, destination: billing, routingKey: "order.#" },
      { type: "queue", queue: q, exchange: billing, routingKey: "#" },
    ] as BindingDefinition[];
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", bindings)).toBe(true);
  });

  it("is NOT routable when the forward exists but the destination has no matching queue", () => {
    const bindings = [
      { type: "exchange", source: ordersTopic, destination: billing, routingKey: "order.#" },
      { type: "queue", queue: q, exchange: billing, routingKey: "user.#" },
    ] as BindingDefinition[];
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", bindings)).toBe(false);
  });

  it("terminates on a cyclic exchange graph", () => {
    // orders -> billing -> orders, with no queue anywhere.
    const bindings = [
      { type: "exchange", source: ordersTopic, destination: billing, routingKey: "#" },
      { type: "exchange", source: billing, destination: ordersTopic, routingKey: "#" },
    ] as BindingDefinition[];
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", bindings)).toBe(false);
  });
});

describe("_internal_declaredPatternsFor", () => {
  it("lists the patterns declared on an exchange, for the error message", () => {
    const bindings = [
      queueBinding(ordersTopic, "user.#"),
      queueBinding(ordersTopic, "audit.*"),
      queueBinding(billing, "order.#"),
    ];
    expect(_internal_declaredPatternsFor("orders", bindings)).toEqual(["user.#", "audit.*"]);
  });

  it("returns an empty list when nothing is declared on the exchange", () => {
    expect(_internal_declaredPatternsFor("orders", [])).toEqual([]);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/contract && pnpm vitest run src/builder/routability.spec.ts
```

Expected: FAIL — cannot resolve `./routability.js`.

- [ ] **Step 3: Implement the resolver**

Create `packages/contract/src/builder/routability.ts`:

```ts
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
 * True when a message published to `exchange` with `routingKey` reaches at
 * least one queue, directly or through exchange-to-exchange forwards.
 *
 * @internal
 */
export function _internal_isPublisherRoutable(
  exchange: ExchangeDefinition,
  routingKey: string | undefined,
  bindings: readonly BindingDefinition[],
): boolean {
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
          return true;
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

  return false;
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
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd packages/contract && pnpm vitest run src/builder/routability.spec.ts
```

Expected: PASS, 11 tests.

- [ ] **Step 5: Typecheck and lint**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract && pnpm typecheck && pnpm lint
```

- [ ] **Step 6: Commit**

```bash
git add packages/contract/src/builder/routability.ts \
  packages/contract/src/builder/routability.spec.ts
git commit -m "feat(contract): resolve publisher routability across the binding graph

Walks queue bindings and exchange-to-exchange forwards with a cycle guard,
so bridged publishers resolve correctly instead of being reported unroutable."
```

---

### Task 3: Rung 2 — `defineContract` throws for unroutable publishers

**Files:**

- Modify: `packages/contract/src/types.ts` (add `externalConsumers` to `PublisherDefinition`)
- Modify: `packages/contract/src/builder/publisher.ts` (accept and carry it)
- Modify: `packages/contract/src/builder/validate.ts` (add the assertion)
- Modify: `packages/contract/src/builder/contract.ts` (call it)
- Test: `packages/contract/src/builder/routability-define-time.spec.ts` (create)

**Interfaces:**

- Consumes: `_internal_isPublisherRoutable`, `_internal_declaredPatternsFor` from Task 2
- Produces: `_internal_assertPublisherRoutable(publisherName: string, exchange: ExchangeDefinition, routingKey: string | undefined, externalConsumers: boolean | undefined, bindings: readonly BindingDefinition[]): void`
- Produces: `PublisherDefinition` gains `externalConsumers?: boolean | undefined`

- [ ] **Step 1: Write the failing test**

Create `packages/contract/src/builder/routability-define-time.spec.ts`:

```ts
import { describe, expect, it } from "vitest";
import { z } from "zod";

import { defineConsumer } from "./consumer.js";
import { defineContract } from "./contract.js";
import { defineExchange } from "./exchange.js";
import { defineMessage } from "./message.js";
import { definePublisher } from "./publisher.js";
import { defineQueue } from "./queue.js";
import { defineQueueBinding } from "./binding.js";

const message = defineMessage(z.object({ orderId: z.string() }));
const orders = defineExchange("orders", { type: "topic" });
const auditQueue = defineQueue("audit-log");

describe("defineContract publisher routability", () => {
  it("throws when a publisher's routing key reaches no queue", () => {
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() =>
      defineContract({
        publishers: { orderCreated },
        bindings: {
          audit: defineQueueBinding(auditQueue, orders, { routingKey: "user.#" }),
        },
      }),
    ).toThrow(/orderCreated/);
  });

  it("names the routing key, the exchange, and the declared patterns", () => {
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() =>
      defineContract({
        publishers: { orderCreated },
        bindings: {
          audit: defineQueueBinding(auditQueue, orders, { routingKey: "user.#" }),
        },
      }),
    ).toThrow(/order\.created[\s\S]*orders[\s\S]*user\.#/);
  });

  it("accepts a publisher whose key matches a declared binding", () => {
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() =>
      defineContract({
        publishers: { orderCreated },
        bindings: {
          audit: defineQueueBinding(auditQueue, orders, { routingKey: "order.#" }),
        },
      }),
    ).not.toThrow();
  });

  it("accepts a publisher routable via a consumer-contributed binding", () => {
    // Consumers contribute bindings, so the check must run after they are
    // collected — not while publishers are being processed.
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() =>
      defineContract({
        publishers: { orderCreated },
        consumers: { audit: defineConsumer(auditQueue, message) },
        bindings: {
          audit: defineQueueBinding(auditQueue, orders, { routingKey: "order.*" }),
        },
      }),
    ).not.toThrow();
  });

  it("accepts an unroutable publisher explicitly marked externalConsumers", () => {
    const orderCreated = definePublisher(orders, message, {
      routingKey: "order.created",
      externalConsumers: true,
    });

    expect(() => defineContract({ publishers: { orderCreated } })).not.toThrow();
  });

  it("still throws for a publish-only contract that is NOT marked", () => {
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() => defineContract({ publishers: { orderCreated } })).toThrow(/externalConsumers/);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/contract && pnpm vitest run src/builder/routability-define-time.spec.ts
```

Expected: FAIL — the first test does not throw (contracts are currently accepted).

- [ ] **Step 3: Add `externalConsumers` to the publisher type**

In `packages/contract/src/types.ts`, inside the `PublisherDefinition` object half (the part before the `& (` union, alongside `message`), add:

```ts
  /**
   * Declares that this publisher's consumers live outside this contract —
   * a separate service or deployment owns the binding.
   *
   * Routability cannot be verified for such a publisher, so the define-time
   * check is skipped. This is deliberately explicit: a heuristic ("no
   * bindings at all means external") would silently miss the mistyped-key
   * case the check exists to catch.
   */
  externalConsumers?: boolean | undefined;
```

- [ ] **Step 4: Carry `externalConsumers` through `definePublisher`**

In `packages/contract/src/builder/publisher.ts`, add `"externalConsumers"` to the allowed-keys array passed to `_internal_assertKnownKeys`, and include it on the returned object:

```ts
    ...(options?.externalConsumers !== undefined
      ? { externalConsumers: options.externalConsumers }
      : {}),
```

- [ ] **Step 5: Add the assertion helper**

Append to `packages/contract/src/builder/validate.ts`:

```ts
/**
 * Throw when a publisher's routing key reaches no queue in this contract.
 *
 * RabbitMQ publisher confirms mean "the broker took responsibility", not "a
 * queue received it": a message routed to zero queues is confirmed and then
 * discarded. Without this check a mistyped binding pattern silently drops
 * every message while the publishing code observes success.
 *
 * Publishers whose consumers are owned by another service opt out with
 * `externalConsumers: true`.
 */
export function _internal_assertPublisherRoutable(
  publisherName: string,
  exchange: ExchangeDefinition,
  routingKey: string | undefined,
  externalConsumers: boolean | undefined,
  bindings: readonly BindingDefinition[],
): void {
  if (externalConsumers === true) return;
  if (_internal_isPublisherRoutable(exchange, routingKey, bindings)) return;

  const declared = _internal_declaredPatternsFor(exchange.name, bindings);
  const declaredText =
    declared.length > 0
      ? `Declared on "${exchange.name}": ${declared.map((p) => `"${p}"`).join(", ")}.`
      : `No bindings are declared on "${exchange.name}".`;

  // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)
  throw new Error(
    `Publisher "${publisherName}" is unroutable: routing key ` +
      `${routingKey === undefined ? "(none)" : `"${routingKey}"`} on exchange ` +
      `"${exchange.name}" (${exchange.type}) reaches no queue. ${declaredText} ` +
      `Messages published here would be confirmed by the broker and then discarded. ` +
      `Add a binding that matches, or set \`externalConsumers: true\` on the publisher ` +
      `if another service owns the binding.`,
  );
}
```

Add the imports at the top of `validate.ts`:

```ts
import type { BindingDefinition, ExchangeDefinition } from "../types.js";
import { _internal_declaredPatternsFor, _internal_isPublisherRoutable } from "./routability.js";
```

- [ ] **Step 6: Call it from `defineContract`**

In `packages/contract/src/builder/contract.ts`, the tail of `defineContract` currently reads:

```ts
result.exchanges = exchanges;
result.queues = queues;
result.bindings = bindings;

return result as ContractOutput<TContract>;
```

Insert the check between the assignments and the `return`, so it runs after every
`addResource` loop has populated the local `bindings` record:

```ts
result.exchanges = exchanges;
result.queues = queues;
result.bindings = bindings;

// Runs last: consumers and bridged publishers contribute bindings, so
// routability can only be decided once every binding is collected.
const declaredBindings = Object.values(bindings);
for (const [publisherName, publisher] of Object.entries(result.publishers ?? {})) {
  _internal_assertPublisherRoutable(
    publisherName,
    publisher.exchange,
    "routingKey" in publisher ? publisher.routingKey : undefined,
    publisher.externalConsumers,
    declaredBindings,
  );
}

return result as ContractOutput<TContract>;
```

Note `result.publishers` (set at line 233 from `processedPublishers`) rather than a local —
`defineContract` has no `publishers` local, and `result.publishers` is absent when the contract
declares none, hence the `?? {}`.

Import it:

```ts
import { _internal_assertPublisherRoutable } from "./validate.js";
```

- [ ] **Step 7: Run the new tests to verify they pass**

```bash
cd packages/contract && pnpm vitest run src/builder/routability-define-time.spec.ts
```

Expected: PASS, 6 tests.

- [ ] **Step 8: Run the whole repo test suite**

Existing fixtures across every package now go through this check, and some will legitimately need `externalConsumers: true` or a binding. Fix each one at the source — do not weaken the check.

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm build && pnpm typecheck && pnpm test
```

Expected: green. Where a fixture fails, decide deliberately: add the missing binding if the contract was genuinely wrong, or mark `externalConsumers: true` if the fixture models a publish-only service.

- [ ] **Step 9: Add a changeset**

```bash
pnpm changeset
```

Choose a **major** bump. Summary:

```
`defineContract` now throws when a publisher's routing key reaches no queue.
RabbitMQ confirms an unroutable message and then discards it, so a mistyped
binding pattern silently dropped every message while `publish()` returned
`Ok`. Publishers whose consumers live in another service opt out with
`externalConsumers: true`.
```

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(contract)!: throw at define time for unroutable publishers

A publisher whose routing key reaches no queue is a silent total message
loss at runtime, because publisher confirms acknowledge messages routed to
zero queues. Publish-only services opt out with externalConsumers: true."
```

---

### Task 4: Real-broker proof — the loss and the guard

**Files:**

- Create: `tests/src/unroutable-publish.spec.ts`

**Interfaces:**

- Consumes: the `externalConsumers` escape hatch from Task 3 (needed to construct the _unguarded_ scenario deliberately)

- [ ] **Step 1: Write the test**

Create `tests/src/unroutable-publish.spec.ts`:

```ts
import {
  defineContract,
  defineExchange,
  defineMessage,
  definePublisher,
} from "@amqp-contract/contract";
import { TypedAmqpClient } from "@amqp-contract/client";
import { it } from "@amqp-contract/testing";
import { describe, expect } from "vitest";
import { z } from "zod";

/**
 * H1, proven end to end against a real broker.
 *
 * Test 1 demonstrates the hazard is genuine: an unroutable publish is
 * confirmed by RabbitMQ and the message is gone. Test 2 demonstrates the
 * define-time guard catches the same contract before it can run.
 *
 * The first test is the reason the second exists; if anyone ever weakens
 * the guard, the pair reads as a complete argument for putting it back.
 */
describe("unroutable publish", () => {
  const message = defineMessage(z.object({ orderId: z.string() }));

  it("INVARIANT: an unroutable publish is confirmed by the broker and the message is lost", async ({
    amqpChannel,
    amqpConnectionUrl,
  }) => {
    const exchangeName = `orders-${Date.now()}`;
    await amqpChannel.assertExchange(exchangeName, "topic", { durable: false });

    // A queue bound with a pattern that cannot match the publisher's key.
    const queueName = `audit-${Date.now()}`;
    await amqpChannel.assertQueue(queueName, { durable: false });
    await amqpChannel.bindQueue(queueName, exchangeName, "user.#");

    const orders = defineExchange(exchangeName, { type: "topic", durable: false });
    const orderCreated = definePublisher(orders, message, {
      routingKey: "order.created",
      // Deliberately bypass the define-time guard so the raw broker
      // behavior is observable.
      externalConsumers: true,
    });
    const contract = defineContract({ publishers: { orderCreated } });

    const client = await TypedAmqpClient.create({
      contract,
      urls: [amqpConnectionUrl],
    }).get();

    const result = await client.publish("orderCreated", { orderId: "1" });

    // The broker confirms it — publish reports success...
    expect(result).toBeOk();

    // ...and the message reached no queue.
    const stats = await amqpChannel.checkQueue(queueName);
    expect(stats.messageCount).toBe(0);

    await client.close().get();
  });

  it("INVARIANT: the same contract is rejected at define time", () => {
    const orders = defineExchange("orders-guarded", { type: "topic", durable: false });
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() => defineContract({ publishers: { orderCreated } })).toThrow(/unroutable/i);
  });
});
```

- [ ] **Step 2: Run the integration suite**

Requires Docker.

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm --filter @amqp-contract/tests test:integration
```

Expected: PASS. If Docker is unavailable, say so explicitly — do not claim integration coverage that did not run.

- [ ] **Step 3: Record the invariant**

In `AGENTS.md`, append to the "Load-bearing invariants" list:

```markdown
19. **A publisher whose routing key reaches no queue is rejected at define time** (RabbitMQ confirms an unroutable message and discards it, so the runtime signal is indistinguishable from success) — `tests/src/unroutable-publish.spec.ts` (the paired prove-the-loss / prove-the-guard tests) + `packages/contract/src/builder/routability-define-time.spec.ts`.
```

- [ ] **Step 4: Commit**

```bash
git add tests/src/unroutable-publish.spec.ts AGENTS.md
git commit -m "test: prove unroutable publishes are lost, and that the guard catches them"
```

---

### Task 5: Rung 1 — type-level routability

**Files:**

- Modify: `packages/contract/src/builder/routing-types.ts`
- Create: `packages/contract/src/routability.test-d.ts`
- Modify: `packages/contract/src/builder/contract.ts` (apply the type to `defineContract`'s input)

**Interfaces:**

- Consumes: `MatchesPattern` (already in `routing-types.ts`), `MATCH_CORPUS` from Task 1
- Produces: `RoutableRoutingKey<Key, Patterns>` — resolves to `Key` when routable, otherwise a readable error-message string type

**Scope note:** the type-level check covers the **single-hop queue-binding case on topic and direct exchanges only**. Fanout, headers, and any exchange participating in an exchange-to-exchange forward are skipped and left to the Task 3 runtime check. This is deliberate: multi-hop graph traversal in the type system risks recursion-depth failures and, worse, false compile errors on valid bridged contracts. Never reject a valid contract at compile time.

- [ ] **Step 1: Write the failing type test**

Create `packages/contract/src/routability.test-d.ts`:

```ts
import { describe, expectTypeOf, it } from "vitest";

import type { RoutableRoutingKey } from "./builder/routing-types.js";

describe("RoutableRoutingKey", () => {
  it("resolves to the key when a pattern matches", () => {
    expectTypeOf<RoutableRoutingKey<"order.created", "order.#">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order.created", "order.*">>().toEqualTypeOf<"order.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.created">
    >().toEqualTypeOf<"order.created">();
  });

  it("resolves to the key when ANY pattern in the union matches", () => {
    expectTypeOf<
      RoutableRoutingKey<"order.created", "user.#" | "order.#">
    >().toEqualTypeOf<"order.created">();
  });

  it("resolves to a readable error when no pattern matches", () => {
    expectTypeOf<
      RoutableRoutingKey<"order.created", "user.#">
    >().toEqualTypeOf<"Error: routing key 'order.created' matches none of the declared binding patterns; the broker would confirm and discard every message">();
  });

  it("skips the check when the key is not a literal", () => {
    expectTypeOf<RoutableRoutingKey<string, "user.#">>().toEqualTypeOf<string>();
  });

  it("skips the check when the patterns are not literal", () => {
    expectTypeOf<RoutableRoutingKey<"order.created", string>>().toEqualTypeOf<"order.created">();
  });

  it("skips the check when there are no patterns", () => {
    expectTypeOf<RoutableRoutingKey<"order.created", never>>().toEqualTypeOf<"order.created">();
  });
});
```

- [ ] **Step 2: Run the type test to verify it fails**

```bash
cd packages/contract && pnpm vitest run --typecheck src/routability.test-d.ts
```

Expected: FAIL — `RoutableRoutingKey` is not exported.

- [ ] **Step 3: Implement the type**

Append to `packages/contract/src/builder/routing-types.ts`:

```ts
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
 * @template Key - The publisher's concrete routing key
 * @template Patterns - Union of binding patterns declared on the exchange
 */
export type RoutableRoutingKey<Key extends string, Patterns extends string> = string extends Key
  ? Key
  : string extends Patterns
    ? Key
    : [Patterns] extends [never]
      ? Key
      : MatchesAnyPattern<Key, Patterns> extends true
        ? Key
        : `Error: routing key '${Key}' matches none of the declared binding patterns; the broker would confirm and discard every message`;
```

- [ ] **Step 4: Run the type test to verify it passes**

```bash
cd packages/contract && pnpm vitest run --typecheck src/routability.test-d.ts
```

Expected: PASS, 6 tests.

- [ ] **Step 5: Assert the shared corpus at the type level**

Append to `packages/contract/src/routability.test-d.ts`:

```ts
/**
 * The corpus in `match-corpus.ts` is asserted against the runtime matcher in
 * `topic-match.spec.ts`. These assertions pin the same cases at the type
 * level, so the two implementations cannot diverge without a test failing.
 *
 * Kept as explicit lines rather than a loop: types cannot be generated from
 * a runtime array.
 */
describe("type-level matcher agrees with the runtime corpus", () => {
  it("matches the cases the runtime matcher matches", () => {
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.created">
    >().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order.created", "order.*">>().toEqualTypeOf<"order.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "*.created">
    >().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order.created", "*.*">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order", "*">>().toEqualTypeOf<"order">();
    expectTypeOf<RoutableRoutingKey<"order.created", "#">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order", "#">>().toEqualTypeOf<"order">();
    expectTypeOf<RoutableRoutingKey<"order.created", "order.#">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order", "order.#">>().toEqualTypeOf<"order">();
    expectTypeOf<
      RoutableRoutingKey<"order.created.v2", "order.#">
    >().toEqualTypeOf<"order.created.v2">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "#.created">
    >().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"created", "#.created">>().toEqualTypeOf<"created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created.v2", "order.#.v2">
    >().toEqualTypeOf<"order.created.v2">();
    expectTypeOf<RoutableRoutingKey<"order.v2", "order.#.v2">>().toEqualTypeOf<"order.v2">();
    expectTypeOf<
      RoutableRoutingKey<"order.a.b.v2", "order.#.v2">
    >().toEqualTypeOf<"order.a.b.v2">();
    expectTypeOf<
      RoutableRoutingKey<"order.created.v2", "order.*.#">
    >().toEqualTypeOf<"order.created.v2">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.*.#">
    >().toEqualTypeOf<"order.created">();
  });

  it("rejects the cases the runtime matcher rejects", () => {
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.updated">
    >().not.toEqualTypeOf<"order.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created.v2", "order.*">
    >().not.toEqualTypeOf<"order.created.v2">();
    expectTypeOf<RoutableRoutingKey<"order.created", "*">>().not.toEqualTypeOf<"order.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.#.v2">
    >().not.toEqualTypeOf<"order.created">();
    expectTypeOf<RoutableRoutingKey<"order", "order.*.#">>().not.toEqualTypeOf<"order">();
    expectTypeOf<
      RoutableRoutingKey<"user.created", "order.#">
    >().not.toEqualTypeOf<"user.created">();
    expectTypeOf<
      RoutableRoutingKey<"order.created", "order.created.v2">
    >().not.toEqualTypeOf<"order.created">();
  });
});
```

- [ ] **Step 6: Run and verify the corpus assertions pass**

```bash
cd packages/contract && pnpm vitest run --typecheck src/routability.test-d.ts
```

Expected: PASS. **If any case fails, the runtime and type-level matchers disagree — fix the mismatch rather than editing the expectation.**

- [ ] **Step 7: Measure `tsc` cost before wiring into `defineContract`**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
time pnpm typecheck
```

Record the wall-clock. Acceptance criterion 10 in the spec: a large regression means fall back to rung 2 only. Compare against the same command on `main` before this branch.

- [ ] **Step 8: Commit**

```bash
git add packages/contract/src/builder/routing-types.ts \
  packages/contract/src/routability.test-d.ts
git commit -m "feat(contract): add compile-time publisher routability checking

RoutableRoutingKey resolves to a readable error-message type when a routing
key matches none of the declared binding patterns. The shared corpus is now
asserted at both the runtime and type level so they cannot diverge."
```

**Note on wiring into `defineContract`'s signature:** applying `RoutableRoutingKey` to `defineContract`'s input requires inferring the binding-pattern union per exchange from the `bindings` record — a non-trivial mapped type over the input. Attempt it only after Step 7 confirms `tsc` cost is acceptable. If the inference proves unreliable or slow, stop here: the type is exported and usable directly, and rung 2 already provides full coverage. Document the decision in the commit message.

---

### Task 6: Rung 3 spike — `basic.return` correlation

**Files:**

- Create: `docs/superpowers/specs/2026-08-01-h1-rung3-spike-findings.md`
- Create (throwaway): `tests/src/spike-return-correlation.spec.ts` — deleted before the final commit

**Interfaces:**

- Produces: a findings document that unblocks a rung-3 implementation plan. **No production code.**

This task is research. Do not write library code. The deliverable is a decision.

- [ ] **Step 1: Establish whether `mandatory` returns are observable**

Write a throwaway integration test that publishes with `mandatory: true` to an exchange with no matching binding, and attaches a `return` listener to the underlying channel. Determine:

1. Does `amqp-connection-manager`'s `ChannelWrapper` surface `basic.return`? Check whether it re-emits `'return'`, and whether the raw `amqplib` channel is reachable via the `setup` callback.
2. Does the returned message carry enough to identify which publish it belongs to (`fields.exchange`, `fields.routingKey`, `properties.headers`, `properties.messageId`)?
3. What is the ordering relative to the publish promise resolving? Confirm empirically whether the return arrives before the confirm.

- [ ] **Step 2: Test the correlation candidate**

Stamp a unique header (for example `x-amqp-contract-publish-id`) on each publish, publish several unroutable messages concurrently, and verify every return can be matched back to its originating publish by that header.

- [ ] **Step 3: Test reconnect behavior**

Publish an unroutable message, force-close the connection via the management API (`DELETE /api/connections/{name}`, port available as `__TESTCONTAINERS_RABBITMQ_PORT_15672__`) and determine what happens to a pending return correlation across the reconnect. Establish whether a pending publish can be left unresolved.

- [ ] **Step 4: Write the findings document**

Create `docs/superpowers/specs/2026-08-01-h1-rung3-spike-findings.md` covering:

- Whether `basic.return` is reachable through `amqp-connection-manager`, and how
- The chosen correlation mechanism, with the evidence supporting it
- Ordering guarantees observed between return and confirm
- Reconnect behavior and how a pending correlation is resolved or abandoned
- Per-publish overhead of the correlation (extra header bytes, bookkeeping)
- **Recommendation: implement rung 3, or stop at rungs 1–2 and document the residual risk**

- [ ] **Step 5: Delete the throwaway test and commit the findings**

```bash
rm tests/src/spike-return-correlation.spec.ts
git add docs/superpowers/specs/2026-08-01-h1-rung3-spike-findings.md
git commit -m "docs: findings from the basic.return correlation spike"
```

---

## Out of scope for this plan

Covered by the follow-up plan for H2–H4 and mock removal:

- H2 — DLX-less queue throws unless `onPoison: "drop"`
- H3 — default `prefetch: 10`, `prefetch: "unbounded"` opt-out
- H4 — default `publishTimeoutMs: 30000`
- Migrating the 9 broker-mocking specs to management-API fault injection
- Ratcheting the `core` / `worker` / `client` coverage floors
- The upgrade-guide entries for every breaking change above

Rung 3 implementation is gated on the Task 6 spike and gets its own plan.
