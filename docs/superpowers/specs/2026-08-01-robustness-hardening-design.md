# Robustness hardening for 3.0 stable

**Date:** 2026-08-01
**Status:** Approved design, ready for implementation planning

## Context

`@amqp-contract/*` carries production financial workloads for external clients. This spec
defines a hardening pass targeting four properties: strong typing, a strong API, patterns
that make end-user best practices automatic, and high-quality tests that do not mock the
broker.

The trigger is proactive, not incident-driven. Breadth over depth: close the systemic gaps
in priority order.

### The sequencing constraint

3.0 stable has **not** shipped. npm shows `latest: 2.4.0`, `beta: 3.0.0-beta.4`, with ~20
changesets pending.

The four properties have different clocks:

- **Typing, API shape, enforced patterns** — breaking. Free inside the beta window; after
  3.0.0 the same improvement costs a 4.0 and a migration for every client running real money.
- **Test coverage and mock removal** — additive. Costs the same today or in six months.

**Decision: hold 3.0 stable until the API hardening below lands.** Test hardening follows
continuously and does not gate the release.

## Non-goals

- Refactoring for its own sake. Every change traces to a hazard in the catalog.
- Throughput or latency optimization. Where safety and throughput conflict, safety wins and
  the trade is documented.
- Idempotency/deduplication machinery (H5) beyond documentation — deferred.
- Toxiproxy or network-level fault injection — deferred (see Testing).

## The safety ladder

The organizing principle. For any unsafe configuration, the question is _when it is caught_:

1. **Impossible** — the type system rejects it
2. **Fails fast** — throws at `defineContract` / `create`, before a message moves
3. **Safe by default** — the right thing happens unless explicitly opted out
4. **Warned** — logged at runtime, after the damage

Every hazard below currently sits on rung 3 or 4, or is entirely unguarded. Each one moves as
far up as it economically goes.

**Enforcement policy: fail fast with no implicit escape hatch.** An unsafe contract throws.
Opting out requires the author to write it explicitly (`onPoison: "drop"`,
`prefetch: "unbounded"`). A client upgrading must consciously acknowledge each hazard; the
migration guide carries that burden.

---

## Hazard catalog

### H1 — Unroutable publish reports success

**Severity: critical. Currently unguarded at every rung.**

`defineContract` accepts a publisher whose routing key matches no binding in the contract.
Verified empirically: a contract with `publishers: { orderCreated }` on a topic exchange with
routing key `order.created` and zero bindings is accepted without complaint.

At runtime `client.publish(...)` resolves to `Ok(void)`, because RabbitMQ publisher confirms
mean _the broker took responsibility_, not _a queue received it_. A message routed to zero
queues is confirmed and discarded.

Client-visible consequence: a typo in a binding pattern, or a consumer deployed without its
topology set up, and every message silently vanishes while the publishing code observes
success. No error, no log, no metric.

This library is uniquely able to fix this at the top of the ladder because the contract knows
the entire topology at define time — a generic AMQP client cannot. The type-level machinery
already exists: `MatchesPattern` / `MatchingBindingPattern` in
`packages/contract/src/builder/routing-types.ts`.

#### Rung 1 — type-level routability

`defineContract` checks each publisher's routing key against the union of binding patterns on
the same exchange, reusing `MatchesPattern`.

On no match, resolve to a template-literal error-message type naming the publisher, its
routing key, and the exchange — following the established `MatchingBindingPattern` convention
(`routing-types.ts:162`) so the compile error is readable rather than a bare `never`.

Escape valve, matching existing behavior: if either side is non-literal `string`, skip the
check. It cannot be decided at compile time.

Exchange type changes the rule and must be handled explicitly:

| Exchange type | Routability rule                                          |
| ------------- | --------------------------------------------------------- |
| `direct`      | exact string equality between routing key and binding key |
| `topic`       | AMQP pattern match (`*`, `#`) via `MatchesPattern`        |
| `fanout`      | routing key ignored by broker — routable iff ≥1 binding   |
| `headers`     | matches on arguments, not keys — key check does not apply |

**Risk:** this is a cross-product over two records. TypeScript recursion depth and compile
time are real concerns. Mitigation: check each publisher against a _union_ of that exchange's
patterns so union distribution does the work rather than nested recursion. `tsc` wall-clock on
the example contracts is an acceptance criterion (see below).

#### Rung 2 — define-time throw

The type check cannot see JS callers, dynamically-built contracts, or non-literal routing
keys. `defineContract` therefore also computes routability at runtime and throws, naming the
publisher, its routing key, the exchange, and the patterns that _are_ declared on it.

This requires a runtime topic matcher — new, small, pure, total.

**Derived invariant:** the runtime matcher and the type-level matcher must agree on every
input. Pinned by a property-based test (see Testing).

#### Rung 3 — runtime unroutable detection

`mandatory: true` on publishes plus a `basic.return` listener on the channel. An unroutable
message is returned by the broker before its confirm; each publish must be correlated with any
return issued against it.

`publish()` gains `UnroutableMessageError` in its error channel — a **modeled error**, not a
defect, because it is expected and actionable by the caller. This is a breaking signature
change and is a primary reason the release is held.

**This mechanism requires a spike before implementation** — see Open Questions.

### H2 — DLX-less queue drops messages

**Current rung: 4 (warning).** `packages/worker/src/retry.ts:360` logs
`"Queue does not have DLX configured - message will be lost on nack"` — emitted at the instant
the message is already being lost, and only when a logger is wired.

**Target rung: 2.** `defineQueue` throws unless the queue has a dead-letter exchange or the
author explicitly writes `onPoison: "drop"`.

### H3 — Unbounded prefetch

**Current: no rung.** `packages/core/src/amqp-client.ts:455` only calls `basic.qos` when
`prefetch` is explicitly set. Unset means AMQP's default: unlimited. The broker pushes the
entire ready backlog into a single worker — unbounded memory, and a crash redelivers all of it
at once.

**Target rung: 3.** Default `prefetch: 10`. Conservative and predictable: bounds in-flight
memory to 10 messages per consumer and keeps the redelivery burst small. Throughput-bound
users raise it explicitly. `prefetch: "unbounded"` is the explicit opt-out.

### H4 — No `publishTimeoutMs` default

**Current: no rung.** The option exists (`packages/client/src/client.ts:133`) but defaults to
undefined, so publishes issued during a broker outage buffer without bound and their promises
never settle.

**Target rung: 3.** Default `publishTimeoutMs: 30000` — long enough that a brief reconnect
does not fail healthy publishes, short enough that an outage surfaces as an error rather than
an unbounded buffer.

### H5 — No idempotency support

At-least-once delivery is inherent and correct, but the library offers clients no help
deduplicating. **Deferred to documentation only** for this pass. Not an enforcement change.

---

## Testing strategy

### What "no mocks" means here

Two distinct things get conflated:

- **Mocking the broker** — `vi.mock("amqp-connection-manager")`, present in 9 spec files.
  Harmful: it proves the code matches _our beliefs about RabbitMQ_, which is exactly the
  assumption that breaks in production. These are the target.
- **A test double for a user-supplied callback** — a handler that throws, a telemetry provider
  that throws. Not mocking a dependency; it is the test's input. Legitimate, retained.

The 9 broker-mocking specs are:

```
packages/core/src/channel-epoch.spec.ts
packages/core/src/publish-buffer-full.spec.ts
packages/core/src/connection-manager.spec.ts
packages/core/src/amqp-client-error-events.spec.ts
packages/worker/src/publish-timeout.spec.ts
packages/worker/src/rpc-reply-failure.spec.ts
packages/client/src/unknown-names.spec.ts
packages/client/src/publish-timeout.spec.ts
packages/client/src/rpc-timeout.spec.ts
```

These cover the highest-risk paths in the library — reconnect/epoch, buffer-full,
connection-pool leasing, RPC timeout, retry-publish failure.

### Prove the loss, then prove the guard

Each hazard gets a pair of real-broker tests:

1. A test demonstrating the hazard is **genuine** — publish to a deliberately unbound routing
   key and assert the message is actually gone from the broker.
2. A test demonstrating the guard **catches** it.

The first test documents why the guard exists and fails loudly if anyone weakens it. It also
makes the hazard catalog credible to a client as evidence rather than as a claim.

### Fault injection: management API

`packages/testing/src/global-setup.ts` already exposes port 15672 and provides it to tests
(`__TESTCONTAINERS_RABBITMQ_PORT_15672__`), and each test already gets an isolated vhost.
The RabbitMQ management HTTP API is therefore reachable today with **no new infrastructure**.

Capabilities used:

- `DELETE /api/connections/{name}` — force-close a connection server-side, producing a genuine
  reconnect rather than a synthesized `'error'` emit. This is what de-mocks the channel-epoch
  and error-event specs.
- Deleting queues/bindings mid-flight — consumer-cancel and topology-drift paths.

**Accepted limit:** write-buffer-full is internal `ChannelWrapper` state. Producing it
naturally requires publishing faster than the socket drains, which is inherently
timing-dependent. `publish-buffer-full.spec.ts` may remain broker-mocked. Network-level
throttling (Toxiproxy) would close this and is explicitly deferred; revisit only if the
remaining mocked tests are found to hide real bugs.

### Property-based tests

The runtime topic matcher and the type-level matcher must agree on every input. `fast-check`
over generated routing-key/pattern pairs pins this. Scope is deliberately narrow — the matcher
is pure and total, which is where property testing pays without straining CI.

### Coverage floors

Ratchet after the work lands, not before. Current state for reference:

| Package  | Stmts | Branch | Funcs | Lines | Floors (s/b/f/l) |
| -------- | ----- | ------ | ----- | ----- | ---------------- |
| core     | 54.17 | 39.52  | 47.50 | 55.52 | 20/10/12/20      |
| client   | 61.11 | 54.32  | 60.93 | 62.72 | 14/15/18/14      |
| worker   | 76.06 | 67.35  | 79.16 | 75.96 | 30/33/31/30      |
| contract | 94.88 | 91.56  | 93.18 | 95.16 | 90/88/90/90      |
| asyncapi | 98.22 | 80.51  | 100   | 98.17 | 95/78/95/95      |

`core` is both the weakest and the most safety-critical, and its floors are the loosest in the
repo: a **10%** branch floor against 39.52% actual means roughly three quarters of its covered
branches could regress without CI noticing. `worker` and `client` floors likewise sit at less
than half their actual coverage.

Ratchet all three to just under actual once this work lands, following the convention already
applied to `contract` — the floors exist to catch regressions, and at their current values they
cannot.

---

## Carried forward from the H2–H4 implementation

H2–H4 shipped on branch `audit/h2-h4-safe-defaults`. The final review's Critical was not a
defect in any guard — it was that our own documentation taught users to satisfy the H2 guard in
a way that does not protect them.

### The pattern worth naming

**Every guard in this library checks that something was _declared_, not that it _routes_.**

A dead-letter exchange with no queue bound to it satisfies `defineContract`, passes review, and
loses every message — while the worker logs `Sending message to DLQ` at `info`, giving the
operator positive confirmation that nothing is wrong. That is strictly worse than the
pre-guard state, where a `warn` fired.

This shape has now appeared three times: H1's `alternate-exchange` false positive, the H2
Critical, and — most tellingly — inside the text written to fix the H2 Critical, where the
broker-policy remediation instructed readers to give the DLX "the same type as the exchange the
policy targets" while still binding `#`. A direct exchange bound with `#` matches nothing.
Measured against a real broker: topic + `#` receives 1, **direct + `#` receives 0**.

Publishers got a routability check in H1 (`_internal_assertPublisherRoutable`). Dead-lettering
has identical semantics and has none.

### Highest-value follow-up: a define-time DLX routability check

Mirror `_internal_assertPublisherRoutable`: when `queue.deadLetter` is set, require a declared
binding from that exchange, with an explicit escape hatch for a genuinely external DLX. This is
the only thing that would have caught either instance of the defect above — snippet execution
proves a snippet _compiles and constructs_, not that it _routes_, because `defineContract`
accepts an unroutable binding by design.

It rejects contracts that currently construct, so it is breaking and needs its own plan,
changeset, and fixture sweep. **It is free inside the beta window and costs a 4.0 afterwards.**

### Documentation still teaching a bare DLX (9 files)

Fixed on the branch: `docs/tutorial/getting-started.md`, `docs/how-to/define-a-contract.md`,
`docs/how-to/upgrade.md`, `docs/how-to/troubleshoot.md`, `docs/index.md`, root `README.md`.

Still outstanding, in rough value order:

- `packages/contract/README.md`, `packages/core/README.md`, `packages/worker/README.md` —
  npm landing pages, same class as the root README
- `docs/how-to/retry-failed-messages.md` (5 sites) — deserves care; read by people whose
  messages are already failing
- `docs/explanation/core-concepts.md`, `docs/tutorial/adding-request-reply.md`,
  `docs/how-to/use-request-reply.md`, `docs/how-to/bridge-domains.md`,
  `docs/examples/command-pattern.md`

### Other deferred items

- **`setup.ts` / `asyncapi` disagree on DLX precedence.** `setup.ts:94` lets `deadLetter` win
  over `arguments`; `asyncapi/src/index.ts:371` lets `arguments` win. Pre-existing, newly
  exposed. `setup.ts` is right — `asyncapi` describes what `setupAmqpTopology` declares, so it
  is downstream, not a second opinion. Fix is `{ ...queue.arguments, ...derivedArgs }`.
- **`deadLetter` + `onPoison: "drop"` together** is silently accepted; the contradiction is
  never surfaced.
- **`publishTimeoutMs` is unvalidated**, unlike its sibling `connectTimeoutMs`.
- **Explicit `prefetch: 0`** is still legal and means unlimited — arguably it should now be
  rejected in favour of `"unbounded"`.
- **No automated gate on documentation snippets.** Two shipped snippets on this branch did not
  compile; verification was manual throughout.
- **A pre-existing parallel-test flake** affects both gates: multiple testcontainers competing
  for one Docker daemon fail several projects at default concurrency. Proved to predate this
  branch by stash-and-rebuild. Everything passes serialized. Recommend `--concurrency=1` on the
  test tasks before CI meets a constrained runner.

---

## Carried forward from the H1 implementation

H1 shipped on branch `audit/h1-unroutable-publish`. These items were triaged during that work
and deliberately deferred — the final whole-branch review confirmed none of them blocks merge.
Recorded here because the working ledger is scratch and does not survive.

**Feed into the H2–H4 / mock-removal plan:**

- **`@amqp-contract/tests` has no `@unthrown/vitest` dependency and no `setupFiles`**, so unthrown
  matchers are unavailable across the whole integration suite; every spec hand-rolls `isOk()`.
  Small shared-test-config change.
- **`docs/**/*.md` and the JSDoc `@example` blocks in ~5 source files are unswept.** Smaller than
  first feared — the main guides (`docs/index.md`, `getting-started.md`,
  `how-to/define-a-contract.md`) route correctly via `defineEventConsumer`/`defineCommandConsumer`.
  Neither markdown nor JSDoc is typechecked, so nothing breaks CI.
- **`packages/client/src/client-cleanup.spec.ts` → "releases the pooled connection when
  waitForConnect times out" is flaky** under full-monorepo parallel load. Passes 3/3 in isolation;
  the full suite passes on retry. Pre-existing — the H1 branch changed no runtime source in
  `client` or `core`. Worth de-flaking: a flaky test erodes trust in the CI signal.
- **Coverage floors** for `core` / `worker` / `client` (see the table above) still sit far below
  actual and cannot catch a regression.

**Independent bug, pre-existing and live:**

- **`MatchingBindingPattern` (`packages/contract/src/builder/routing-types.ts:165`) has a
  template-literal hole.** Its non-literal escape valve tests `string extends Pattern`, which does
  not catch template-literal types — so a pattern typed `` `order.${string}` `` resolves to the
  error-message type and produces a **false compile error on a valid binding**. Unlike the same
  hole in `RoutableRoutingKey` (inert, since nothing wires it), `MatchingBindingPattern` **is**
  wired into `defineEventConsumer`, so this one is live today. Not introduced by H1; worth its own
  fix outside this design.

**Minor, low value, fix opportunistically:**

- `_internal_isPublisherRoutable` has no production callers — kept as a stable boolean view with
  its own tests.
- `routability.ts` marks its BFS `visited` set on dequeue rather than enqueue, so a node can be
  queued twice; bounded and correctness-neutral, but the inline comment overstates the guard.
- The runtime and type-level match corpora have no length guard, so they can drift (one missing
  case was already found this way). `expect(MATCH_CORPUS).toHaveLength(N)` would pin it.
- Type-level rejection assertions use the weak `.not.toEqualTypeOf` form; the dangerous direction
  is still caught, only error-message wording is unguarded.
- `definePublisher` options do not go through `_internal_assertKnownKeys`, unlike its sibling
  builders, so a typo'd option name yields the unroutable error rather than "unknown option".
  Fail-safe direction only.

**Operational notes for the rung-3 implementation plan:**

- A 40,000-message `mandatory`-unroutable burst killed the broker container outright. Keep
  integration bursts at or below ~12k.
- The management API takes 0.5–5s to register a connection; `DELETE /api/connections/{name}`
  before that silently no-ops. Poll `/api/connections` until non-empty first.
- The `_messageRejected` requeue branch proved **unreachable** from integration (0 of 2,947
  rejections, via both connection-kill and channel-level 404). The duplicate-return hazard is
  therefore untested rather than disproven, and needs a unit test faking the republish.

---

## Open questions

### H1 rung 3 correlation mechanism — resolved by the spike

The spike ran and its findings are committed at
`docs/superpowers/specs/2026-08-01-h1-rung3-spike-findings.md`. Outcome: `basic.return` is
reachable via the raw `ConfirmChannel` handed to the `setup` callback (not via `ChannelWrapper`,
which never emits it), and header-based correlation held across 45,331 confirmed publishes with a
forced reconnect — 0 false negatives, 0 false positives. **Recommendation: implement rung 3**,
justified primarily by the coverage gap (`externalConsumers: true` publishers have no check at any
rung and fail totally and silently), not by the loopback measurements alone.

### Original framing, retained for context

A returned (unroutable) message arrives before its confirm, so publishes must be correlated
with returns. Normally this uses the confirm sequence number, but `amqp-connection-manager`
abstracts that away.

Likely approach: stamp a unique header on each publish and match returns by it. This must be
**proven against a real broker before being specced**, including behavior when a return arrives
during a reconnect. Resolve this before writing the H1 rung 3 implementation plan; H1 rungs 1
and 2 are unblocked and can proceed in parallel.

---

## Acceptance criteria

1. A contract with a publisher whose routing key matches no binding fails to compile, with an
   error naming the publisher, its routing key, and the exchange.
2. The same contract built dynamically (defeating the type check) throws from
   `defineContract`, naming the same three things plus the declared patterns.
3. Publishing to a routing key that is unroutable _at the broker_ resolves to
   `Err(UnroutableMessageError)`, not `Ok`.
4. `defineQueue` without a DLX and without `onPoison: "drop"` throws.
5. A consumer with no explicit prefetch is observably limited to 10 unacked messages against a
   real broker.
6. A publish issued while the broker is unreachable settles within ~30s rather than hanging.
7. Every hazard has both a prove-the-loss and a prove-the-guard test against a real broker.
8. The runtime and type-level matchers agree across the property-test corpus.
9. `channel-epoch.spec.ts` and `amqp-client-error-events.spec.ts` no longer mock
   `amqp-connection-manager`; they force real reconnects via the management API.
10. `tsc` wall-clock on the example contracts does not regress materially from the rung-1 type
    machinery. Measure before and after; treat a large regression as a design failure requiring
    a fallback to rung 2 only.

## Migration impact

Every item below breaks existing client contracts by design — that is the point, and it is why
this lands inside the beta window rather than after.

- Publishers with no matching binding: compile error, then define-time throw.
- Queues with no DLX: define-time throw until `onPoison: "drop"` is added.
- Consumers relying on unlimited prefetch: silently limited to 10 unless
  `prefetch: "unbounded"` is set. **This is a behavior change, not a compile error** — it must
  be prominent in the upgrade guide, since it is the one item a client can miss.
- `publish()` error channel gains `UnroutableMessageError`: exhaustive `errCases` matchers
  fail to compile until the case is handled.

The upgrade guide must present each as: what breaks, why it was unsafe, and the exact edit.
