# Documentation snippet-execution guard

**Date:** 2026-08-02
**Status:** Approved design, ready for implementation planning

## Context

Three branches of guard work (`audit/h1-unroutable-publish`, `audit/h2-h4-safe-defaults`,
`audit/dlx-routability`) each added a define-time check that rejects a contract which would lose
messages. Each time, the sweep that followed found our own documentation teaching the shape the
new guard forbids — and twice the _fix_ for that documentation introduced the same defect again.

The most consequential instance: the H2 final review found `docs/tutorial/getting-started.md`
teaching a dead-letter exchange with nothing bound to it. A client following it would have paid
the migration cost, believed they were protected, and lost the same messages — now with the
worker logging `Sending message to DLQ` at `info`, confirming all was well.

Every one of those defects was found by a throwaway extraction harness written during the branch
and deleted afterwards. **Nothing in CI has ever executed a documentation snippet.**

### The harness detail that matters

An early version of that harness injected a shared import preamble before each snippet. It found
**zero** defects. The version that used **only each snippet's own imports** immediately found
three, including `packages/core/README.md` calling `defineQueueBinding` without importing it.

A harness that supplies imports the reader does not have proves less than it appears to. That is
the single most important constraint on this design.

## Non-goals

- **Type-checking snippets.** See the decision below.
- Testing generated API documentation (`docs/api/**`) — TypeDoc output, and signature fragments
  rather than programs.
- Testing planning documents (`docs/superpowers/**`) — deliberately illustrative.
- Verifying that a snippet's topology _routes_ on a broker. `defineContract` accepts an
  unroutable binding by design in its undecidable rows; execution proves construction, not
  delivery. The two checks are complementary and this one is the cheap half.

## The corpus

A census of hand-written markdown (excluding generated API docs, planning docs, and build
artifacts) found **244 fenced TypeScript blocks across 43 files**, of which **101 carry an import
line** and **28 call `defineContract`**.

**Only the 28 are in scope.** Every guard this project has built — unroutable publisher,
poison-loss, DLX routability — fires at `defineContract`. A snippet that never calls it cannot
trip them. All three broken snippets found on the previous branch are in that 28, which is the
evidence for scoping here rather than wider.

Testing all 244 would mean annotating roughly 150 deliberate fragments (continuations,
pseudo-code, signature illustrations) as exclusions. The work would become exclusion-marking, and
a wall of skip markers trains readers to add one reflexively.

## Mechanism

### Placement — and the structural change it requires

The `tests` package already depends on all five workspace packages and is the natural home. But
it currently has **only** a `test:integration` script, so root `pnpm test` skips it entirely.

Give it the same unit/integration split `client` and `worker` already use:

- `src/**/*.spec.ts` → the `unit` project
- `src/**/__tests__/*.spec.ts` → the `integration` project, with `globalSetup` and the broker

Move the existing specs into `__tests__/`, add a `test` script, and the snippet test lands in
`unit`. **It needs no Docker and runs in the main gate** — which is the point. A guard that only
runs under `test:integration` would demand a broker it does not use and be absent from the gate
most changes go through.

All nine current specs in `tests/src/` move. Eight depend on the broker through the
`@amqp-contract/testing/extension` fixture; the ninth, `rabbitmq-config.spec.ts`, has no fixture
import and reads as a unit test at a glance, but calls `inject("__TESTCONTAINERS_RABBITMQ_IP__")`
and so depends on `globalSetup` having started the container. It belongs in `__tests__/` with the
rest — worth naming, because "no fixture import" is the obvious wrong heuristic for sorting them.

This is the one structural change the design requires.

### The test

1. **Discover.** Walk `docs/`, `README.md`, `packages/*/README.md` and `.agents/`, excluding
   `docs/api/`, `docs/superpowers/`, `node_modules`, and build output. Parse fenced `ts` /
   `typescript` blocks with a line-state machine, not a regex — a regex over nested and
   language-tagged fences miscounts, which it did during this design's own exploration. Keep
   blocks whose body contains `defineContract(`.

2. **Execute each in isolation.** Write the block **verbatim** to a temp `.ts` file inside the
   repository, so workspace module resolution works, and dynamic-import it.

   **Verbatim is load-bearing.** No preamble, no injected imports, no shared scaffold. A snippet
   that does not carry its own imports must fail. This is the constraint the whole design exists
   to satisfy.

3. **Assert construction.** The import completing without throwing _is_ the assertion. Every
   guard fires at `defineContract`, so an unroutable publisher, a consumed queue with no
   dead-letter route, and a DLX with nothing bound all surface here as a thrown error.

4. **Fail actionably.** Report the source file, the line number of the opening fence, and the
   error message. A failure must read as
   `docs/how-to/route-dead-letters.md:41 — Queue "orders" dead-letters to exchange "orders-dlx"…`,
   not a stack trace in a temp path. A guard whose failures are hard to locate gets muted.

### Why execution and not type-checking

Execution catches the entire defect class this project has actually shipped: a missing import (a
`ReferenceError` at import time) and a guard violation (a throw from `defineContract`). Both of
the real historical defects are in that class.

Type-checking each snippet needs a separate `tsc` program per block or a synthesized composite,
and catches a different class — a snippet using a removed option that still runs. That class has
not shipped once across three branches.

Shipping the guard that closes the real gap beats delaying it for a larger one. Adding a
type-check pass later is straightforward if a type-level defect ever reaches a release.

## Scope of the sweep

The four blocks that currently show no imports —
`docs/explanation/core-concepts.md`, `docs/explanation/why-amqp-contract.md`, and parts of
`docs/how-to/route-dead-letters.md` and `.agents/rules/contract-patterns.md` — **get real import
blocks**. There is deliberately no skip mechanism: nothing can rot unnoticed, and nobody can opt
out reflexively.

**Expect more than four failures on first run.** Twenty-four of the 28 have never been executed by
anything committed. `route-dead-letters.md` and `contract-patterns.md` have partial import
coverage, and the nine files carrying a bare dead-letter exchange were fixed only where a previous
sweep reached them.

Each failure is fixed at the source. A snippet that cannot be made to construct is a snippet that
should not be in the documentation as executable TypeScript.

## Acceptance criteria

1. `pnpm test` from the repository root executes the snippet suite — no Docker required.
2. All 28 `defineContract` blocks construct, with no injected imports and no skip markers.
3. Deleting an import line from any snippet fails the suite, naming that file and fence line.
4. Introducing an unbound dead-letter exchange into any snippet fails the suite.
5. The existing broker-dependent specs in `tests/src/` still run under `test:integration` and
   still pass.
6. A new markdown file with a `defineContract` snippet is picked up automatically — discovery is
   by walk, never by a hand-maintained list.

## Carried forward from the implementation

Shipped on branch `audit/snippet-execution-guard`. **16 of 29 snippets failed on first run** —
more than half the corpus — which is the strongest evidence the guard was worth committing.

### The strongest remaining follow-up

**The same rot class exists for file paths, and has no guard.** This branch moved
`tests/src/*.spec.ts` into `__tests__/` and broke nine invariant-to-test mappings in
`AGENTS.md` — while adding invariant 23 to that very file, under a line that says "extend the
mapping when you add one". `CLAUDE.md` is a symlink, so it inherited all of them. An agent
verifying an invariant would have got file-not-found and could reasonably have concluded the
guard did not exist.

A test asserting that every backticked path in `AGENTS.md` resolves would have caught it at
commit time. That is precisely the argument this branch makes for snippets, applied to a
different artifact, and it is cheaper than what was built here.

### Known limits of what shipped

- **The corpus floor is a hand-maintained number.** It is set to the exact corpus size (31), so
  removing coverage fails loudly — but it drifts slack upward as pages are added and nobody
  raises it. Deriving it, or failing on any decrease, would be tighter.
- **`AGENTS.md` and `CLAUDE.md` are not in the guard's `ROOTS`.** A contract snippet added to
  either would be unguarded. None exists today.
- **`discover.ts` walks all of `packages/` before filtering**, recursing into each package's
  `node_modules` and `dist`. Wasted I/O only, measured ~450ms.

### A trap the guard structurally cannot catch

Of the dead-letter bindings in this branch's swept fences, **14 route only because
`defineExchange` defaults to `topic`**. Two pages — `docs/how-to/define-a-contract.md` and
`docs/tutorial/adding-request-reply.md` — place a `direct` main exchange on the line adjacent to
a bare DLX. A reader "harmonizing" those two lines creates total silent loss, and nothing in the
repository fails: `dead-letter-routability.ts` documents accepting any binding in that row as a
deliberate false negative.

Worth its own follow-up: reject a `direct` exchange bound with a wildcard-containing key at
define time. `#` is a topic wildcard and matches nothing on a direct exchange — measured, topic
receives 1 and direct receives 0 — and that trap has now produced defects on three separate
branches, including inside text written to fix it.

---

## What this does and does not guarantee

It guarantees a documented contract **compiles far enough to run and constructs successfully with
the imports a reader would copy**.

It does not guarantee the topology **routes**. `defineContract` accepts bindings it cannot decide
— the `arguments` passthrough, an `alternate-exchange`, a routing key unknowable at define time.
A snippet can construct and still deliver nothing, which is exactly how `#` on a direct exchange
survived three reviews.

That residual belongs to the guards, not to this test. Worth stating plainly so nobody reads a
green suite as proof the examples work.
