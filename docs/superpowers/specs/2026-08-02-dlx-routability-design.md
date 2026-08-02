# Define-time DLX routability check

**Date:** 2026-08-02
**Status:** Approved design, ready for implementation planning

## Context

`@amqp-contract/*` carries production financial workloads. The H2 work (merged) made
`defineContract` reject a consumed queue that has no way to handle poison messages. Its final
review found a Critical defect that the guard could not catch:

> A dead-letter exchange with no queue bound to it satisfies `defineContract`, passes review,
> and loses every rejected message — while the worker logs `Sending message to DLQ` at `info`,
> giving the operator positive confirmation that nothing is wrong. That is strictly worse than
> the pre-guard state, where a `warn` fired.

The root cause is stated in `2026-08-01-robustness-hardening-design.md`:

> **Every guard in this library checks that something was _declared_, not that it _routes_.**

Publishers got a routability check in H1 (`_internal_assertPublisherRoutable`). Dead-lettering
has identical semantics — RabbitMQ silently drops a message routed to zero queues, so the
runtime signal is indistinguishable from success — and has no equivalent guard.

That pattern has now produced three separate defects: H1's `alternate-exchange` false positive,
the H2 Critical, and — most tellingly — the text written to fix the H2 Critical, which told
readers to bind `#` on a direct exchange. Measured against a real broker: topic + `#` receives
1 message, **direct + `#` receives 0**.

Documentation can only mitigate this. Snippet execution, which this project now does, proves a
snippet _compiles and constructs_ — not that it _routes_, because `defineContract` accepts an
unroutable binding by design.

### The sequencing constraint

3.0 stable has not shipped (`latest: 2.4.0`, `beta: 3.0.0-beta.4`, 21 changesets pending). This
guard rejects contracts that construct today, so it is breaking: **free inside the beta window,
and a 4.0 plus a client migration afterwards.**

## Non-goals

- Verifying anything the broker owns. A policy-applied DLX, or an exchange named only as a
  string, is outside what a contract can see.
- Pattern-subset reasoning over routing keys (see the decision below).
- Changing `deadLetter`'s runtime behavior, or the `setup.ts` / `asyncapi` precedence
  inconsistency tracked separately.
- Re-opening H5 (duplicate delivery), which remains unaddressed by design.

## Mechanism

A second check in `defineContract`, beside the existing publisher-routability and poison-loss
checks. For each queue carrying a `deadLetter` config:

The rows are **evaluated in order, first match wins** — they are not independent conditions.
(Without an explicit order a fanout DLX with no `routingKey` would match two rows; both happen to
agree, but the implementation must not depend on that coincidence.)

| #   | Case                                                                           | Decision                                                                                          |
| --- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| 1   | DLX supplied only through the `arguments` passthrough (no `deadLetter` config) | Skip — undecidable.                                                                               |
| 2   | `externalConsumers: true`                                                      | Skip — the author declared the DLQ is owned outside this contract.                                |
| 3   | `routingKey` set, **or** the DLX is `fanout`/`headers`                         | Decidable. Run `_internal_isPublisherRoutable(deadLetter.exchange, routingKey, bindings)`.        |
| 4   | `routingKey` unset (a `direct` or `topic` DLX)                                 | Accept if the DLX declares `alternate-exchange`, else iff at least one binding is declared on it. |

Row 4 exists because the shared resolver's `bindingAccepts` returns `false` for a `direct` or
`topic` exchange when the routing key is `undefined` — correct for publishers, where a missing key
really is unroutable, but wrong here, where the key exists at runtime and simply is not known at
define time. Rows 3 and 4 together keep the resolver's semantics intact rather than loosening them
for both callers.

**No new resolver and no new matcher.** `_internal_isPublisherRoutable(exchange, routingKey,
bindings)` already asks exactly this question, including multi-hop exchange-to-exchange
traversal, cycle detection, and per-exchange-type semantics. This work adds a `deadLetter`-shaped
wrapper that decides which row applies and calls it.

### Why the `routingKey`-unset row is deliberately weak

`packages/core/src/setup.ts:95` sets `x-dead-letter-routing-key` only when `deadLetter.routingKey`
is given. Otherwise RabbitMQ **preserves the message's original routing key**, so the key
arriving at the DLX is whatever the message came in with — any key matching a binding _into_ the
source queue.

Verifying that case properly means proving every key that can reach the source queue also
matches a binding out of the DLX: pattern-vs-pattern subset reasoning. It is genuinely hard, and
getting it wrong rejects a valid contract — the one outcome this project's governing rule
forbids:

> A false negative is acceptable; rejecting a valid contract is not.

"At least one binding" catches the defect actually observed in production documentation — a DLX
with **nothing** bound. A DLX bound only to non-matching patterns still passes: a known, accepted
false negative.

Binding count alone is **not** free of false positives, though, and the review of this branch
found the one case it gets wrong: an exchange declaring `alternate-exchange` has no unroutable
keys at all, so a DLX with that argument and zero bindings dead-letters correctly on a real
broker and was still rejected. That is the same false positive H1 had, reintroduced by a row that
decides routability without the resolver H1 was fixed in. Row 4 therefore short-circuits on
`_internal_exchangeHasAlternateExchange` (`packages/contract/src/builder/routability.ts`) — the
resolver's own predicate, promoted to a shared export so the two checks cannot disagree about
what an alternate exchange means — before it counts bindings. The remaining inaccuracy is the
false negative above, and only that.

### Why the `arguments` form is skipped

`arguments: { "x-dead-letter-exchange": "orders-dlx" }` names an exchange as a **bare string**.
There is no `ExchangeDefinition`, and the contract need not declare that exchange at all, so its
bindings are unknowable. Undecidable resolves to "routable", matching the escape valve H1 already
uses for non-literal routing keys.

This is consistent with `_internal_queueHasDeadLetterExchange`, which accepts the `arguments`
form for the poison-loss guard: both say the queue _dead-letters_, neither claims it _routes_.

### The escape hatch

`DeadLetterConfig` gains `externalConsumers?: boolean`:

```ts
deadLetter: { exchange: ordersDlx, externalConsumers: true }
```

Reusing publishers' existing vocabulary rather than inventing a second word for the identical
concept — "the consuming side lives outside this contract". A reader who has met
`externalConsumers` on a publisher already knows what it means here.

**Expect it to be needed more often than feels comfortable.** H2's `onPoison` opt-out was needed
in 10 of 10 fixtures in one package, far above the design's assumption, because topology owned
outside the contract is ordinary rather than exotic. That is a signal about adoption cost, not a
reason to weaken the guard.

## Error message

Follows the H1 shape: name the queue, the dead-letter exchange, the bindings that _are_ declared
on it, and both remedies — bind a queue to the DLX, or set `externalConsumers: true` if another
service owns it.

**It must not tell the author to add a `deadLetter` config.** They have one; that is the whole
point of the finding. A remedy that does not apply is the confidently-wrong-pointer failure this
project has already hit three times, and it is the specific way this message could do harm.

## Blast radius

This rejects contracts that construct today.

- Every fixture and documentation snippet declaring a bare DLX now fails to construct. That
  **includes the nine documentation files** already tracked as follow-up work in
  `2026-08-01-robustness-hardening-design.md` — the guard converts that manual worklist into a
  build error, which is the outcome worth having.
- The sweep must run every command that executes a contract, not only `pnpm test`. H1 established
  that a sweep driven by a single test command misses whole classes of call site: `.test-d.ts`
  files (typechecked, never executed), example packages (no `test` script), and the integration
  vitest project (root `pnpm test` runs `--project unit` only).
- Fixture policy follows H2's precedent: default to binding a real DLQ; use `externalConsumers:
true` only where the fixture genuinely models externally-owned topology. Fixtures are
  exemplars — people copy them.

## Testing

- **Unit tests, one per row of the mechanism table**, plus a decidable-but-unroutable case
  (a `routingKey` that matches no binding on the DLX) and the multi-hop case the shared resolver
  already supports.
- **Paired real-broker proof**, the pattern this project has settled into: one test showing the
  loss is genuine — publish, reject, and assert the message reaches neither the DLQ nor anywhere
  else — and one showing the guard rejects that same contract. The first test is why the second
  exists.
- **Mutation-verify the guard**: removing the check must fail the new tests. Every guard added in
  this project that was not mutation-verified turned out to have a dead arm.
- The routing-key matching itself needs no new tests: `_internal_matchesTopicPattern` and its
  shared corpus already cover it on both the runtime and type-level sides.

## Acceptance criteria

1. A contract whose queue dead-letters to an exchange with no declared binding fails to
   construct, naming the queue, the exchange, and both remedies.
2. The same contract with `externalConsumers: true` constructs.
3. A DLX with a bound DLQ constructs, with `routingKey` set and unset.
4. A `routingKey` that matches no binding on a topic DLX fails to construct; a `#` binding passes.
5. A DLX supplied only through `arguments` constructs, unchanged.
6. A real-broker test demonstrates a message dead-lettered to an unbound exchange is genuinely
   gone, and the guard rejects that contract.
7. Removing the guard fails the new unit tests.
8. Every command that executes a contract is green: root `pnpm test`, all four integration
   projects run serially, and the example packages.

## Carried forward from the implementation

Shipped on branch `audit/dlx-routability`. The guard works: the final review independently
confirmed it cannot reject a valid contract, and that all 48 swept bindings genuinely route.

### The one structural gap left open

**No CI guard on documentation snippets.** This branch's sweep introduced three broken snippets
(`core/README.md` missing an import, `bridge-domains.md` — a page with no import lines at all —
gaining a `defineQueueBinding` call, and `adding-request-reply.md` gaining one without saying
where it came from). All three were found only by an extraction harness that used **each
snippet's own imports**; an earlier harness that injected a superset preamble found none of them,
because it supplied imports the reader would not have.

That harness is still throwaway and uncommitted. It is the only structural guard against a class
of defect that has now consumed four reviews across three branches, and it stays unguarded the
moment this work merges. Committing a snippet-execution test that uses only each snippet's own
import block would close it — the extractor works, it just is not in the repo.

Note what snippet execution does and does not prove: a snippet **compiles and constructs**. It
does not prove the topology **routes**, because `defineContract` accepts a binding that reaches
nowhere by design in the undecidable rows. The two checks are complementary.

### Deferred

- **`packages/contract/src/builder.test-d.ts` fixture 1** takes `externalConsumers: true` with a
  rationale the re-review disproved: all eight sibling tests sharing that queue use
  `toHaveProperty`, tolerate extra queues, and the assertion the comment cites is on an unrelated
  queue. A real DLQ and binding would have worked with no rework. The comment ships misleading.
  Two of three type-test fixtures took the opt-out, so the exemplar value lands in one.
- **The `Declared on "X": …` and `routed with "<key>"` error-message arms** are reachable (row 3)
  but untested.
- **The `#` hint fires for row 3 as well as row 4**, where the advice is generic rather than
  targeted. Not wrong, just imprecise.
- **The guard is define-time only.** A hand-rolled `ContractDefinition` literal bypasses it
  entirely, as several core and worker specs do.

### What the direct-`#` trap cost

Worth recording, because it recurred: `#` is a **topic** wildcard. On a direct exchange it is a
literal key matching nothing. Measured on a real broker and now pinned by
`tests/src/dlx-routability.spec.ts`: topic + `#` receives 1, **direct + `#` receives 0**.

It appeared three times on this branch — in the original sweep (four sites needed literal keys),
in two ttl-backoff fixtures where the retry pipeline rewrites the key to the queue name so the
publisher's key never arrives, and latent in `.agents/rules/contract-patterns.md`, which was
correct only because the queue happened to be quorum. The guard structurally cannot catch it:
in the no-`routingKey` row, **any** binding satisfies the check. The error message now carries the
warning when the DLX is direct, because that is the only place it can live.

---

## Migration impact

Breaking by design, and it lands before 3.0 stable.

A client whose contract declares a dead-letter exchange with nothing bound to it will fail to
build. That is the point: their rejected messages are currently being discarded silently. The
upgrade guide must state plainly that this is not a new restriction but the discovery of an
existing data-loss path, give the fix (declare the DLQ and bind it — the shape at
`docs/how-to/define-a-contract.md:141-156`), and name `externalConsumers: true` for topology
owned elsewhere.

The RabbitMQ 406 constraint documented for H2 applies here too: a client adding a DLQ binding to
live topology should read the existing "If the queue already exists in production" section rather
than being told to edit and redeploy.
