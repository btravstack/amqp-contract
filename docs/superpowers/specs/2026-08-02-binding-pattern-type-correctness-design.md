# Binding-Pattern Type Correctness — Design

**Date:** 2026-08-02
**Status:** approved

## Problem

`MatchingBindingPattern` guards the routing-key override on
`defineEventConsumer`'s topic overloads
(`packages/contract/src/builder/event.ts:448` and `:581`). On a pattern that
cannot match the publisher's routing key it resolves to a readable error-message
string type, so the binding fails to compile instead of silently receiving
nothing at runtime.

It rejects patterns that **do** match at runtime.

The type skips its check when either side is non-literal — but it tests for that
with `string extends Pattern`, which is true only for plain `string`. A
_partially_ literal type such as `` `${string}.orders` `` is not plain `string`,
so the check runs, `MatchesPattern` cannot decide it, and the type resolves to
the error string. Reproduced through the public API:

```
Type '`${string}.orders`' is not assignable to type
  '`Error: binding pattern '${string}.orders' can never match
    the publisher routing key 'acme.orders'`'
```

`` `${string}.orders` `` matches `acme.orders` at runtime. The contract is valid
and the library rejects it. This violates the governing rule of the robustness
program: **a false negative is acceptable; rejecting a valid contract is not.**
Template-literal routing keys are the natural encoding for a tenant or
environment prefix, so this is reachable in ordinary use, not a curiosity.

The type's own doc comment states the intended behavior correctly —
"Non-literal strings (plain `string` on either side) skip the check" — while
naming only the case the code actually handles. The comment and the defect are
the same sentence.

## Scope of the defect

Three exported types perform this match. All three share the hole, and each
mitigates it differently:

| Type                     | Applied to                   | Plain `string`                                        | Template literal      |
| ------------------------ | ---------------------------- | ----------------------------------------------------- | --------------------- |
| `MatchingBindingPattern` | `defineEventConsumer` (live) | skipped                                               | **rejected**          |
| `RoutableRoutingKey`     | nothing — opt-in utility     | skipped                                               | rejected (documented) |
| `MatchingRoutingKey`     | nothing — opt-in utility     | rejected on the pattern side, skipped on the key side | rejected              |

Only `MatchingBindingPattern` is wired into a signature, so only it can break a
user's build today. The other two are exported utilities a user may apply to
their own helpers, where the same wrong answer appears as `never`.

`RoutableRoutingKey`'s limitation is documented in its pending changeset
(`.changeset/routable-routing-key-type.md`) and was a deliberate trade at the
time. That changeset is unreleased, so the text can be corrected rather than
shipped.

This is the program's second recurring pattern — _a rule expressed in more than
one place drifts_ — with three expressions and three different drifts. The fix
consolidates rather than patching each site.

## Solution

One internal type answers "can this be decided at compile time?", and all three
matchers call it.

```ts
type IsStringLiteral<S extends string> = string extends S
  ? false
  : [S] extends [never]
    ? false
    : (S extends string ? ({} extends Record<S, 1> ? false : true) : never) extends true
      ? true
      : false;
```

`Record<S, 1>` produces a concrete property for a literal key and a _pattern
index signature_ for a template-literal key. `{}` is assignable to the latter
and not the former, which distinguishes them in one step — no per-character
recursion, so no instantiation-depth risk.

The conditional distributes over `S`, so every member of a union must itself be
a literal. A mixed union such as `` "a" | `b.${string}` `` resolves to `false`
and skips. Without distribution it resolves to `true` and the templated member
produces the original false positive.

`[S] extends [never]` guards the empty union: `never` is vacuously assignable
everywhere, and without the guard it would report as literal.

### Application

In `MatchingBindingPattern` and `RoutableRoutingKey`, replace the two
`string extends X ? …` skip arms with `IsStringLiteral<X> extends false ? …`.
The rest of each type is unchanged — in particular
`[BindingPattern<Pattern>] extends [never]` still rejects the empty pattern,
since `""` is a literal and reaches that arm.

`MatchingRoutingKey` has no skip arm at all; it gains the same two, returning
`Key` when either side is undecidable. This also fixes its asymmetry, where a
plain-`string` pattern collapsed to `never` but a plain-`string` key did not.

### Consequence

An undecidable pattern becomes a false negative: nothing is rejected at compile
time, and the case falls through to the define-time routability check in
`defineContract`, which runs on concrete strings with the whole binding graph in
view. That check is unaffected by this change.

## Verification performed during design

`IsStringLiteral` was checked against 12 cases: `"order.created"`, `""`,
`"order.*"` and literal unions decide; plain `string`,
`` `order.${string}` ``, `` `${string}.orders` ``, `` `${string}.orders.#` ``,
`` `a.${string}.b` ``, `` `v${number}` ``, mixed unions and `never` skip.

The full fix was applied to a scratch copy of `routing-types.ts` and checked
against 18 assertions: the three confirmed false positives are accepted, a
fourth templated case that was already correct stays correct, and all ten
existing `test-d` expectations still hold — including that genuine mismatches
still produce their exact error strings, which is what proves the guard has not
degraded into a blanket skip.

These are design-time probes, not deliverables. The implementation must land
them as committed `test-d` cases.

## Documentation and release

- Three doc comments say "plain `string`" where they mean "non-literal". That
  phrasing is the defect written down; each becomes an accurate statement that
  any type not fully known at compile time — including a template literal —
  skips the check and defers to the define-time check.
- `.changeset/routable-routing-key-type.md` contains a paragraph presenting the
  template-literal limitation as intended behavior. It is unreleased; the
  paragraph is removed and the surrounding text corrected.
- A new patch changeset covers the fix, naming the user-visible effect: a
  template-literal binding pattern no longer fails to compile.

## Testing

Type-level only. The runtime matcher (`builder/topic-match.ts`) always receives
concrete strings, so it has no analogue of this case; `MATCH_CORPUS` and the
invariant that the two implementations agree are untouched.

New `test-d` cases in `builder.test-d.ts` and `routability.test-d.ts`:

- the skip path on both sides of all three types, covering plain `string`,
  template literals with the hole in leading, trailing and middle position, and
  a mixed union;
- the existing mismatch assertions retained, so a regression to a blanket skip
  fails rather than passing quietly;
- `MatchingRoutingKey`'s new skip arms, including the asymmetry that is being
  removed.

## Out of scope

- **Rejecting a wildcard in a direct-exchange binding key at define time.**
  Considered and rejected: binding a direct exchange with key `order.*` and
  publishing the literal key `order.*` is legal AMQP and works. The guard would
  reject a valid contract, which the governing rule forbids. The existing
  routability check already catches the case where it actually loses messages.
- **Duplicate delivery (H5).** Exactly-once delivery is not achievable over
  AMQP; the ack can always be lost after the work is done. Exactly-once _effect_
  requires an idempotent consumer keyed on application state, which needs a
  transaction boundary shared with the user's database. That is out of this
  library's scope. The eventual answer is documentation: state that delivery is
  at-least-once, enumerate when redelivery happens, and show the
  idempotency-key pattern as the consumer's responsibility.

## Separable addition

A test asserting that every backticked path in `AGENTS.md` resolves on disk.
The snippet-execution branch broke nine such paths while adding a tenth, under a
line instructing the reader to extend the mapping, and nothing caught it for
three tasks. Unrelated to binding patterns and independently droppable.
