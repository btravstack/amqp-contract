# Binding-Pattern Type Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the library rejecting binding patterns that match at runtime, by deciding "is this knowable at compile time?" in one shared type instead of three drifting expressions of it.

**Architecture:** Add one internal conditional type, `IsStringLiteral`, to `packages/contract/src/builder/routing-types.ts`. Replace the `string extends X` skip arms in `MatchingBindingPattern` and `RoutableRoutingKey` with it, and give `MatchingRoutingKey` the skip arms it never had. Nothing else changes: the matcher itself, the runtime matcher, and the define-time routability check are untouched.

**Tech Stack:** TypeScript 5.x conditional types, Vitest type-testing (`*.test-d.ts`, run via `typecheck.enabled` in `packages/contract/vitest.config.ts`), Changesets.

## Global Constraints

- No `any` — use `unknown` and narrow (enforced by oxlint).
- Type aliases over interfaces — `type Foo = {}`, not `interface Foo {}`.
- `.js` extensions required in all imports (ESM).
- The governing rule of this program: **a false negative is acceptable; rejecting a valid contract is not.** Every change here must move behavior in that direction, never the other.
- `IsStringLiteral` must NOT be added to `packages/contract/src/builder/index.ts` or `packages/contract/src/index.ts`. It is internal; exporting it from the package entry would make it public API and require a `minor` changeset.
- Do not weaken any existing assertion to make a change pass. If an existing `test-d` expectation now fails, that is a real regression — stop and report it, do not edit the expectation.
- Conventional commits required (`feat`, `fix`, `docs`, `chore`, `test`, `refactor`).
- Run `pnpm --filter @amqp-contract/contract typecheck` before claiming any task done; it is not in the pre-commit hook.

**Commands used throughout:**

- Type tests: `pnpm --filter @amqp-contract/contract test`
- Typecheck: `pnpm --filter @amqp-contract/contract typecheck`
- Integration-suite tests (Task 5 only): `pnpm --filter @amqp-contract/tests test`

---

## File Structure

| File                                                        | Responsibility                                                                                        | Task    |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ------- |
| `packages/contract/src/builder/routing-types.ts`            | All three matcher types plus the new `IsStringLiteral` guard                                          | 1, 2, 3 |
| `packages/contract/src/builder/is-string-literal.test-d.ts` | Direct type tests for the guard (new file)                                                            | 1       |
| `packages/contract/src/builder.test-d.ts`                   | Type tests for `MatchingBindingPattern`, `MatchingRoutingKey`, and `defineEventConsumer` reachability | 2, 3    |
| `packages/contract/src/routability.test-d.ts`               | Type tests for `RoutableRoutingKey`                                                                   | 3       |
| `.changeset/routable-routing-key-type.md`                   | Pending changeset whose limitation paragraph becomes false                                            | 4       |
| `.changeset/binding-pattern-literal-guard.md`               | New changeset for the fix (new file)                                                                  | 4       |
| `tests/src/docs/rule-paths.spec.ts`                         | Asserts every backticked path in the agent rule docs resolves (new file)                              | 5       |

---

### Task 1: The `IsStringLiteral` guard

Adds the shared decision procedure and proves it in isolation. No existing type changes behavior in this task — the guard is added and tested, nothing calls it yet. A reviewer can accept or reject this on its own.

**Files:**

- Modify: `packages/contract/src/builder/routing-types.ts` (add the type after `BindingPattern`, around line 52)
- Create: `packages/contract/src/builder/is-string-literal.test-d.ts`

**Interfaces:**

- Consumes: nothing.
- Produces: `export type IsStringLiteral<S extends string>` from `packages/contract/src/builder/routing-types.ts`, resolving to `true` when every member of `S` is a fully-known string literal and `false` otherwise. Tasks 2 and 3 apply it.

- [ ] **Step 1: Write the failing test**

Create `packages/contract/src/builder/is-string-literal.test-d.ts`:

```ts
import { describe, expectTypeOf, it } from "vitest";

import type { IsStringLiteral } from "./routing-types.js";

/**
 * The single decision procedure behind all three matcher types: can this
 * string be reasoned about at compile time?
 *
 * Answering it wrong in the "yes" direction is the expensive failure — the
 * matcher then runs on a type it cannot decide and reports a valid contract
 * as an error. Every case below that resolves to `false` is a case the
 * matchers must skip rather than guess at.
 */
describe("IsStringLiteral", () => {
  it("decides fully-known literals", () => {
    expectTypeOf<IsStringLiteral<"order.created">>().toEqualTypeOf<true>();
    expectTypeOf<IsStringLiteral<"">>().toEqualTypeOf<true>();
    expectTypeOf<IsStringLiteral<"order.*">>().toEqualTypeOf<true>();
    expectTypeOf<IsStringLiteral<"a" | "b">>().toEqualTypeOf<true>();
  });

  it("skips plain string", () => {
    expectTypeOf<IsStringLiteral<string>>().toEqualTypeOf<false>();
  });

  it("skips template literals, wherever the hole sits", () => {
    expectTypeOf<IsStringLiteral<`order.${string}`>>().toEqualTypeOf<false>();
    expectTypeOf<IsStringLiteral<`${string}.orders`>>().toEqualTypeOf<false>();
    expectTypeOf<IsStringLiteral<`a.${string}.b`>>().toEqualTypeOf<false>();
    expectTypeOf<IsStringLiteral<`${string}.orders.#`>>().toEqualTypeOf<false>();
    expectTypeOf<IsStringLiteral<`v${number}`>>().toEqualTypeOf<false>();
  });

  it("skips a union in which any member is not a literal", () => {
    // Without distribution this resolves to `true` and the templated member
    // reaches the matcher, which is the exact defect being fixed.
    expectTypeOf<IsStringLiteral<"a" | `b.${string}`>>().toEqualTypeOf<false>();
  });

  it("skips the empty union", () => {
    // `never` is vacuously assignable everywhere; without an explicit arm it
    // reports as a literal.
    expectTypeOf<IsStringLiteral<never>>().toEqualTypeOf<false>();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter @amqp-contract/contract test`

Expected: FAIL — `Module '"./routing-types.js"' has no exported member 'IsStringLiteral'`.

- [ ] **Step 3: Add the type**

In `packages/contract/src/builder/routing-types.ts`, immediately after the `BindingPattern` type (the line `export type BindingPattern<S extends string> = S extends "" ? never : S;`), insert:

```ts
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
```

Note on the inner conditional: `S extends string ? … : never` is what makes it distribute over a union, so each member is tested separately. The results are then collected and compared against `true` — a union of `true | false` fails that comparison, which is the conservative answer. Do not "simplify" the distribution away.

If oxlint rejects the bare `{}` type, replace `{} extends Record<S, 1>` with `Record<never, never> extends Record<S, 1>` — it is the same check — and note the substitution in the task report.

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --filter @amqp-contract/contract test`

Expected: PASS — all five `IsStringLiteral` cases green.

- [ ] **Step 5: Typecheck and lint**

Run: `pnpm --filter @amqp-contract/contract typecheck && pnpm lint`

Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add packages/contract/src/builder/routing-types.ts packages/contract/src/builder/is-string-literal.test-d.ts
git commit -m "feat: add the IsStringLiteral compile-time decidability guard"
```

---

### Task 2: Fix `MatchingBindingPattern` — the live defect

This is the only type wired into a signature, so this is the task that fixes a user's broken build. It ends with a test that goes through `defineEventConsumer` itself, not just the type.

**Files:**

- Modify: `packages/contract/src/builder/routing-types.ts` (the `MatchingBindingPattern` type and its doc comment)
- Modify: `packages/contract/src/builder.test-d.ts` (the `MatchingBindingPattern` describe block around line 159, and the `defineEventConsumer topic routing-key override enforcement` describe block around line 191)

**Interfaces:**

- Consumes: `IsStringLiteral<S extends string>` from `./routing-types.js` (same file — no import needed).
- Produces: no new exports. `MatchingBindingPattern<Pattern, PublisherKey>` keeps its signature and now resolves to `BindingPattern<Pattern>` whenever either side is non-literal.

- [ ] **Step 1: Write the failing tests**

In `packages/contract/src/builder.test-d.ts`, inside the existing `describe("MatchingBindingPattern (topic consumer override enforcement)", …)` block, add this test immediately after the existing test named `"skips the check for non-literal strings"`. Leave that existing test exactly as it is — do not edit or replace it.

```ts
test("skips the check for template-literal patterns", () => {
  // These all match at runtime. Deciding them at compile time is not
  // possible, so the check must defer to `defineContract` rather than
  // guess — guessing here rejected a valid contract.
  expectTypeOf<
    MatchingBindingPattern<`${string}.created`, "order.created">
  >().toEqualTypeOf<`${string}.created`>();
  expectTypeOf<
    MatchingBindingPattern<`order.${string}`, "order.created">
  >().toEqualTypeOf<`order.${string}`>();
  expectTypeOf<
    MatchingBindingPattern<`${string}.orders.#`, "acme.orders.created">
  >().toEqualTypeOf<`${string}.orders.#`>();
  expectTypeOf<MatchingBindingPattern<"order.#", `order.${string}`>>().toEqualTypeOf<"order.#">();
  // A union with one undecidable member is undecidable as a whole.
  expectTypeOf<MatchingBindingPattern<"order.*" | `x.${string}`, "order.created">>().toEqualTypeOf<
    "order.*" | `x.${string}`
  >();
});
```

Then, inside the existing `describe("defineEventConsumer topic routing-key override enforcement", …)` block, add this test after the existing `"accepts patterns that can match the publisher routing key"` test:

```ts
test("accepts a template-literal pattern through the public API", () => {
  // The defect this suite exists to prevent, reproduced end-to-end: a
  // tenant-prefixed pattern matches 'order.created' at runtime, and the
  // library used to fail the build with
  //   "binding pattern '${string}.created' can never match the publisher
  //    routing key 'order.created'".
  const tenantPattern = "acme.created" as `${string}.created`;
  defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: tenantPattern });

  const suffixPattern = "order.created" as `order.${string}`;
  defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: suffixPattern });

  defineEventConsumer(orderCreated, allOrdersQueue, {
    bridgeExchange,
    routingKey: tenantPattern,
  });
});
```

Leave every other test in both blocks exactly as it is. In particular the `"rejects patterns that can never match the publisher routing key"` test and its five `@ts-expect-error` comments must remain untouched — they are what proves this fix has not turned the guard into a blanket skip. If any `@ts-expect-error` there starts reporting "unused", that is a regression: stop and report it.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm --filter @amqp-contract/contract test`

Expected: FAIL. The `MatchingBindingPattern<...>` template-literal assertions resolve to `` `Error: binding pattern '…' can never match the publisher routing key '…'` `` instead of the pattern, and the `defineEventConsumer` calls report "No overload matches this call" with that same error string as the expected type.

- [ ] **Step 3: Apply the guard**

In `packages/contract/src/builder/routing-types.ts`, change the head of `MatchingBindingPattern` from:

```ts
> = string extends Pattern
  ? BindingPattern<Pattern>
  : string extends PublisherKey
    ? BindingPattern<Pattern>
    : [BindingPattern<Pattern>] extends [never]
```

to:

```ts
> = IsStringLiteral<Pattern> extends false
  ? BindingPattern<Pattern>
  : IsStringLiteral<PublisherKey> extends false
    ? BindingPattern<Pattern>
    : [BindingPattern<Pattern>] extends [never]
```

Everything below that line is unchanged — the empty-pattern rejection, the match, and the error-string branch all stay exactly as they are. `""` is a literal, so it still reaches the `[BindingPattern<Pattern>] extends [never]` arm and is still rejected.

- [ ] **Step 4: Fix the doc comment**

In the same file, in the JSDoc block above `MatchingBindingPattern`, replace this paragraph:

```
 * Non-literal strings (plain `string` on either side) skip the check — the
 * match cannot be decided at compile time, so runtime behavior is preserved.
```

with:

```
 * The check runs only when both sides are fully known at compile time. Plain
 * `string`, a template-literal type with a `${…}` hole (`` `${string}.created` ``),
 * and any union containing either are skipped: the match cannot be decided, and
 * guessing would reject a pattern that matches at runtime. Those contracts are
 * covered by the define-time routability check in `defineContract`, which runs
 * on concrete strings.
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pnpm --filter @amqp-contract/contract test`

Expected: PASS, including every pre-existing assertion in both describe blocks.

- [ ] **Step 6: Typecheck and lint**

Run: `pnpm --filter @amqp-contract/contract typecheck && pnpm lint`

Expected: both clean.

- [ ] **Step 7: Commit**

```bash
git add packages/contract/src/builder/routing-types.ts packages/contract/src/builder.test-d.ts
git commit -m "fix: stop rejecting template-literal binding patterns that match at runtime"
```

---

### Task 3: Apply the guard to `RoutableRoutingKey` and `MatchingRoutingKey`

Neither type is wired into a signature, so neither can break a build today — but both are exported for users to apply to their own helpers, where the same wrong answer surfaces as `never` or as an error string. Fixing them is what makes the rule live in one place.

**Files:**

- Modify: `packages/contract/src/builder/routing-types.ts` (the `RoutableRoutingKey` and `MatchingRoutingKey` types, and `MatchingRoutingKey`'s doc comment)
- Modify: `packages/contract/src/routability.test-d.ts` (the `RoutableRoutingKey` describe block)
- Modify: `packages/contract/src/builder.test-d.ts` (the `MatchingRoutingKey pattern matching` describe block around line 87)

**Interfaces:**

- Consumes: `IsStringLiteral<S extends string>` from `./routing-types.js` (same file — no import needed).
- Produces: no new exports. Both types keep their signatures.

- [ ] **Step 1: Write the failing tests**

In `packages/contract/src/routability.test-d.ts`, add this test inside the existing `describe("RoutableRoutingKey", …)` block:

```ts
it("skips the check when either side is not a compile-time literal", () => {
  expectTypeOf<
    RoutableRoutingKey<`order.${string}`, "order.#">
  >().toEqualTypeOf<`order.${string}`>();
  expectTypeOf<
    RoutableRoutingKey<"order.created", `order.${string}`>
  >().toEqualTypeOf<"order.created">();
  expectTypeOf<
    RoutableRoutingKey<"order.created", "order.#" | `x.${string}`>
  >().toEqualTypeOf<"order.created">();
});
```

In `packages/contract/src/builder.test-d.ts`, add this test inside the existing `describe("MatchingRoutingKey pattern matching", …)` block:

```ts
test("skips the check when either side is not a compile-time literal", () => {
  // Previously asymmetric: a plain-`string` pattern collapsed to `never`
  // while a plain-`string` key did not. Both now skip.
  expectTypeOf<MatchingRoutingKey<string, "order.created">>().toEqualTypeOf<"order.created">();
  expectTypeOf<MatchingRoutingKey<"order.#", string>>().toEqualTypeOf<string>();
  expectTypeOf<
    MatchingRoutingKey<`order.${string}`, "order.created">
  >().toEqualTypeOf<"order.created">();
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm --filter @amqp-contract/contract test`

Expected: FAIL — the `RoutableRoutingKey` cases resolve to the `` `Error: routing key '…' matches none of the declared binding patterns; …` `` string, and the `MatchingRoutingKey<string, "order.created">` and ``MatchingRoutingKey<`order.${string}`, "order.created">`` cases resolve to `never`.

Note: `MatchingRoutingKey<"order.#", string>` may already pass before the change — the type is asymmetric today. Do not treat that single passing assertion as a reason to skip the task.

- [ ] **Step 3: Apply the guard to `RoutableRoutingKey`**

In `packages/contract/src/builder/routing-types.ts`, change the head of `RoutableRoutingKey` from:

```ts
export type RoutableRoutingKey<Key extends string, Patterns extends string> = string extends Key
  ? Key
  : string extends Patterns
    ? Key
    : [Patterns] extends [never]
```

to:

```ts
export type RoutableRoutingKey<Key extends string, Patterns extends string> =
  IsStringLiteral<Key> extends false
    ? Key
    : IsStringLiteral<Patterns> extends false
      ? Key
      : [Patterns] extends [never]
```

Then delete the now-dead `[Patterns] extends [never] ? Key :` arm and its comment, so the type reads `… : IsStringLiteral<Patterns> extends false ? Key : MatchesAnyPattern<Key, Patterns> extends true ? Key : …`.

That arm is unreachable once the guard is in place: `IsStringLiteral<never>` is `false`, so the empty union already returns `Key` one arm earlier. Behavior is identical — the existing assertion `RoutableRoutingKey<"order.created", never>` resolving to `"order.created"` must still pass, and it is what proves the deletion is safe. Leaving a dead arm in place to "document intent" is how a reader later concludes the guard does not cover `never`.

Its doc comment already says "non-literal" and needs no change.

- [ ] **Step 4: Apply the guard to `MatchingRoutingKey`**

In the same file, change `MatchingRoutingKey` from:

```ts
export type MatchingRoutingKey<Pattern extends string, Key extends string> =
  RoutingKey<Key> extends never
    ? never // Invalid routing key
    : BindingPattern<Pattern> extends never
      ? never // Invalid pattern
      : MatchesPattern<Key, Pattern> extends true
        ? Key
        : never;
```

to:

```ts
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
```

- [ ] **Step 5: Document the new behavior on `MatchingRoutingKey`**

In its JSDoc block, immediately after the line `* Returns the routing key if it's valid and matches the pattern, `never` otherwise.`, add:

```
 *
 * The check runs only when both the pattern and the key are fully known at
 * compile time. Plain `string`, template-literal types, and unions containing
 * either resolve to `Key` unchecked — the match cannot be decided, and
 * guessing would reject a key that routes at runtime.
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `pnpm --filter @amqp-contract/contract test`

Expected: PASS. Every pre-existing assertion in both files must still pass — in particular `routability.test-d.ts`'s mismatch cases (`RoutableRoutingKey<"user.created", "order.#">` and friends) and `builder.test-d.ts`'s `MatchingRoutingKey<"order.*", "user.created">` collapsing to `never`. Those prove the guard has not become a blanket skip.

- [ ] **Step 7: Typecheck and lint**

Run: `pnpm --filter @amqp-contract/contract typecheck && pnpm lint`

Expected: both clean.

- [ ] **Step 8: Commit**

```bash
git add packages/contract/src/builder/routing-types.ts packages/contract/src/routability.test-d.ts packages/contract/src/builder.test-d.ts
git commit -m "fix: skip the routing-key match when either side is not a compile-time literal"
```

---

### Task 4: Changesets

The pending `RoutableRoutingKey` changeset documents the template-literal limitation as intended behavior. It is unreleased, so it must be corrected rather than shipped alongside a release that removes the limitation.

**Files:**

- Modify: `.changeset/routable-routing-key-type.md`
- Create: `.changeset/binding-pattern-literal-guard.md`

**Interfaces:**

- Consumes: the behavior changes from Tasks 2 and 3.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Correct the pending changeset**

In `.changeset/routable-routing-key-type.md`, delete this paragraph entirely:

```
Matching is over literal segments only: a pattern built from a template literal
(`` `order.${string}` ``) is not recognised as matching, so a key it would
accept at runtime still resolves to the error string. Use concrete literal
patterns, or leave the key unconstrained and rely on the define-time check.
```

and replace the sentence

```
Non-literal inputs and an empty pattern union skip the
check and resolve to `Key`.
```

with

```
The check runs only when both the key and the patterns are fully known at
compile time. Plain `string`, template-literal types such as
`` `order.${string}` ``, unions containing either, and an empty pattern union
all skip the check and resolve to `Key` — an undecidable case defers to the
define-time check rather than being guessed at.
```

Leave the final paragraph (about `defineContract` deliberately not being constrained) unchanged — it is still accurate.

- [ ] **Step 2: Add the changeset for the fix**

Create `.changeset/binding-pattern-literal-guard.md`:

```markdown
---
"@amqp-contract/contract": patch
---

Fixed `defineEventConsumer` rejecting a routing-key override typed as a template
literal. A pattern such as `` `${string}.created` `` matches `order.created` at
runtime, but `MatchingBindingPattern` treated any type that was not plain
`string` as decidable, could not decide it, and failed the build with
"binding pattern '${string}.created' can never match the publisher routing key
'order.created'". Tenant- and environment-prefixed routing keys are the common
way to hit this.

The three matcher types — `MatchingBindingPattern`, `MatchingRoutingKey`, and
`RoutableRoutingKey` — now share one test for whether a string is fully known at
compile time, and skip the match when it is not. `MatchingRoutingKey` also loses
an asymmetry where a plain-`string` pattern collapsed to `never` while a
plain-`string` key did not. Undecidable cases defer to the define-time
routability check in `defineContract`, which runs on concrete strings; patterns
that genuinely cannot match are still rejected exactly as before.
```

- [ ] **Step 3: Verify the changeset is picked up**

Run: `git add -A && git commit -m "docs: changesets for the binding-pattern literal guard"`

Then run: `pnpm changeset status --since=origin/main`

Expected: it reports `@amqp-contract/contract` (and the rest of the fixed group) with pending changes, and exits 0. `changeset status` diffs committed state, so the commit must happen first — that is why the commit is part of this step rather than a separate one at the end.

---

### Task 5: Assert every backticked path in the agent rule docs resolves

Separable from the rest of this plan — it touches no library code. The snippet-execution branch broke nine `AGENTS.md` path references while adding a tenth, under a line instructing the reader to extend the mapping, and nothing caught it for three tasks.

Scope note: the design spec named `AGENTS.md`. This task covers `AGENTS.md` **and** `.agents/rules/*.md`, because it is the same loop over the same class of reference and the rule files carry more paths than the index does. Say so in the task report.

**Files:**

- Create: `tests/src/docs/rule-paths.spec.ts`

**Interfaces:**

- Consumes: nothing from earlier tasks.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Write the test**

Create `tests/src/docs/rule-paths.spec.ts`:

```ts
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

/**
 * A path in the agent rules that no longer resolves is worse than no
 * reference: it sends a reader looking for a guarantee that has moved.
 *
 * This is not hypothetical. The snippet-execution branch relocated eleven spec
 * files and left nine `AGENTS.md` invariant references pointing at nothing —
 * while the same edit added a tenth invariant, directly under a line telling
 * the reader to extend the mapping. Three task reviews did not notice.
 */

const repoRoot = join(import.meta.dirname, "..", "..", "..");

/** Backticked tokens that look like a repo path: contain a slash and end in a known extension. */
const PATH_LIKE = /^[\w@./-]+\.(?:ts|tsx|js|mjs|cjs|md|json|ya?ml)(?::\d+)?$/;

function pathsIn(markdown: string): readonly string[] {
  return [...markdown.matchAll(/`([^`\n]+)`/g)]
    .map((match) => match[1] ?? "")
    .filter((token) => token.includes("/") && !token.includes("*"))
    .filter((token) => PATH_LIKE.test(token))
    .map((token) => token.replace(/:\d+$/, ""));
}

const sources: readonly string[] = [
  "AGENTS.md",
  ...readdirSync(join(repoRoot, ".agents", "rules"))
    .filter((name) => name.endsWith(".md"))
    .map((name) => join(".agents", "rules", name)),
];

describe("agent rule docs", () => {
  // `CLAUDE.md` is a symlink to `AGENTS.md`; listing only `AGENTS.md` avoids
  // asserting the same corpus twice.
  it("has rule files to check", () => {
    expect(sources.length).toBeGreaterThan(5);
  });

  for (const source of sources) {
    const paths = pathsIn(readFileSync(join(repoRoot, source), "utf8"));

    // Per-file rather than a total count: if the extraction regex ever breaks,
    // every file yields zero and this fails immediately, with no number to
    // keep bumped as the docs change. A total floor would stay green while one
    // file silently stopped contributing.
    it(`${source}: extracts path references`, () => {
      expect(paths.length).toBeGreaterThan(0);
    });

    for (const path of paths) {
      it(`${source}: ${path} resolves`, () => {
        expect(
          existsSync(join(repoRoot, path)),
          `${source} references ${path}, which does not exist`,
        ).toBe(true);
      });
    }
  }
});
```

- [ ] **Step 2: Run it**

Run: `pnpm --filter @amqp-contract/tests test`

Expected: the `unit` project runs the new file. Every extracted path should resolve — the six dead `AGENTS.md` paths were fixed on the snippet branch. If any path fails, that is a real dead reference: fix the doc, do not loosen the regex.

If the `has rule files to check` or an `extracts path references` case fails, the extraction is broken — fix the regex, do not delete the assertion.

- [ ] **Step 3: Prove the test can fail**

Temporarily append this line to `AGENTS.md`:

```markdown
See `packages/contract/src/builder/does-not-exist.ts` for details.
```

Run: `pnpm --filter @amqp-contract/tests test`

Expected: FAIL, with `AGENTS.md references packages/contract/src/builder/does-not-exist.ts, which does not exist`.

Then remove the line and re-run. Expected: PASS. Confirm with `git status --short` that `AGENTS.md` is unmodified before committing — a leaked scratch edit to the rules file is worse than the bug being fixed.

- [ ] **Step 4: Commit**

```bash
git add tests/src/docs/rule-paths.spec.ts
git commit -m "test: assert every backticked path in the agent rule docs resolves"
```

---

## Final verification

- [ ] **Step 1: Full build and test**

Run from the repo root:

```bash
pnpm build && pnpm typecheck && pnpm test --concurrency=1 && pnpm lint && pnpm knip && pnpm exec oxfmt --check .
```

Expected: all green. `pnpm knip` matters here: `IsStringLiteral` is a new export consumed only by its own module and a `*.test-d.ts` file, which is exactly the shape knip flags as unused. If it does, do not delete the export — report it, and the fix is a knip config entry, not removing the guard's direct tests. `--concurrency=1` matters: several projects start their own testcontainer and a parallel run fails on Docker contention, which is a pre-existing environment issue and not a signal about this branch.

- [ ] **Step 2: Confirm the public API surface did not change**

Run: `grep -rn "IsStringLiteral" packages/contract/src/index.ts packages/contract/src/builder/index.ts packages/contract/src/builder.ts`

Expected: no matches. `IsStringLiteral` is internal; if it appears in an entry point, remove the export — a public type addition would need a `minor` changeset, not the `patch` written in Task 4.

- [ ] **Step 3: Confirm nothing weakened**

Run: `git diff main -- packages/contract/src/builder.test-d.ts packages/contract/src/routability.test-d.ts | grep '^-' | grep -v '^---'`

Expected: no output at all. Every change to these two files is an addition. If any line was removed, an assertion was weakened — stop and report it. In particular no `@ts-expect-error` and no `toEqualTypeOf<never>()` may appear.
