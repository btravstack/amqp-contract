# Delivery Guarantees Documentation — Design

**Date:** 2026-08-03
**Status:** approved

## Problem

The library delivers at-least-once. The documentation says so in places, but
never as a property of the system, and the framing it does use invites a reader
to conclude the property does not apply to them.

Three specific gaps, each verified against the current tree.

### 1. A failed publish may still have been delivered — undocumented

`AmqpClient` creates its channel with `confirm: true`
(`packages/core/src/amqp-client.ts:304`) and applies a default `publishTimeout`
of 30 s (`DEFAULT_PUBLISH_TIMEOUT_MS`, `packages/core/src/amqp-client.ts:92`).
A timeout means the client stopped waiting for the confirm — not that the broker
failed to receive the message. The reference text says only that the promise
"settles with a failure".

Nothing in the documentation tells a reader that a publish failure is
_ambiguous_. The obvious reaction to a failed `publish()` is to publish again,
and that is the publisher-side duplicate source. It is currently invisible.

### 2. Duplicates are framed as a consequence of enabling retries

`docs/explanation/the-retry-model.md:56` — "Retrying is a choice with real
consequences (duplicate side effects, queue growth), so it is not on by
default." And `:94` — "the assumption most often left unexamined _when retries
are switched on_."

Retry mode defaults to `none`, so a reader who has not configured retries can
reasonably read both sentences as not applying to them. They do apply: a worker
crash, a connection or channel loss before the ack, and a drain timeout on
`close()` all redeliver with no retry configuration at all.

Those facts are present, but only as asides on pages about something else —
`docs/how-to/consume-messages.md:205` (drain timeout, the one place the phrase
"at-least-once semantics" appears outside the retry page),
`docs/how-to/upgrade.md:22` (crash redelivers the unacked backlog), and
`docs/how-to/troubleshoot.md:536`. A reader must assemble the guarantee from
three unrelated pages.

### 3. There is no message identity to deduplicate on

`docs/explanation/the-retry-model.md:94` recommends "a deduplication table keyed
on message ID". No such id exists unless the user creates one: `messageId` does
not appear anywhere in `packages/core/src` or `packages/client/src`, so the
library never sets it.

A user can set it — `AmqpPublishOptions` is amqplib's `Options.Publish`
(`packages/core/src/amqp-client.ts:178`), which carries `messageId` — but
nothing says to. And the advice is incomplete in a way that matters: an id
regenerated on each publish attempt makes consumer-side deduplication useless
against exactly the publisher-side duplicates in gap 1. The id has to be stable
across the _sender's own_ retries to be worth anything.

## Solution

One new explanation page, `docs/explanation/delivery-guarantees.md`, that states
the guarantee once and completely, plus targeted edits where the existing
framing misleads.

Scope is documentation. No new API, no deduplication machinery, no automatic
`messageId` generation — consistent with the ruling that idempotency is an
application pattern rather than this library's job.

### The new page

Three sections:

**Delivery is at-least-once, always.** Independent of retry configuration.
Every redelivery source enumerated in one list: worker crash mid-handler;
connection or channel loss before the ack is written; drain timeout on
`worker.close()`; `immediate-requeue` retries; `ttl-backoff` republishes. The
first three need no retry configuration, which is the point the current
documentation does not make.

**A failed publish is ambiguous.** What a `publishTimeout` expiry does and does
not tell you, and the consequence: a publish error is not proof of
non-delivery, so republishing can duplicate. State the honest options — accept
the possible duplicate and make the consumer idempotent, or carry an id stable
across the sender's retries so the consumer can recognise the repeat.

**Idempotency is the consumer's job.** The identity problem stated plainly: the
library sets no `messageId`, so deduplication needs an id the sender supplies —
either via `publishOptions.messageId` or a business key in the payload — and
that id must survive the sender's own retries. Name the ordinary approaches
(idempotency key at the downstream provider, upsert instead of insert,
deduplication table) without prescribing one.

### Edits to existing pages

- `docs/explanation/the-retry-model.md:56` and `:94` — remove the framing that
  ties duplicates to enabling retries, and link to the new page. The section
  heading "Retries are not exactly-once" stays; its content narrows to what is
  retry-specific, with the general property deferred to the new page.
- `docs/how-to/consume-messages.md:205`, `docs/how-to/upgrade.md:22`,
  `docs/how-to/troubleshoot.md:536` — keep the local statements, link to the new
  page rather than each half-stating the guarantee.
- `docs/.vitepress/config.ts` — register the page in the Explanation section of
  `GUIDE_SIDEBAR` (around :110-114). An unregistered page is unreachable.

## Constraints

- **Any `defineContract` snippet on the new page is executed in CI.** The
  snippet guard walks `docs` (`tests/src/snippets/discover.ts`, `ROOTS`), and
  `docs/explanation` is not excluded. A snippet must construct using only the
  imports it shows.
- **The snippet corpus count is pinned exactly** at 31 in
  `tests/src/snippets/snippet-execution.spec.ts`. Adding any fenced snippet the
  parser recognises requires bumping that number in the same commit; the
  assertion is deliberately exact so this cannot pass unnoticed.
- **Every backticked repo path is asserted to resolve** by
  `tests/src/docs/rule-paths.spec.ts` — but only for `AGENTS.md` and
  `.agents/rules/*.md`, not for `docs/`. Paths cited on the new page are not
  covered by that test; check them by hand.
- Prose must state what the code does and no more. Where a guarantee has a
  boundary, name the boundary rather than rounding it off.

## Verification

No behavior changes, so the deliverable is checked by review rather than by new
tests:

- `pnpm --filter @amqp-contract/tests test` must stay green — it executes the
  documentation snippets and would catch a broken example or a corpus-count
  drift.
- Every factual claim on the new page must cite the code it describes, and each
  citation must be checked against the current tree during implementation. The
  three gaps above were each verified this way; the same standard applies to the
  page that fixes them.
- The VitePress build must succeed with the new page registered and every
  internal link resolving.

## Out of scope

- Automatic `messageId` generation, a deduplication store, or any publish-side
  idempotency helper. Exactly-once delivery is not achievable over AMQP, and
  exactly-once _effect_ requires a transaction boundary shared with the user's
  database.
- Whether `publish` should make message identity more discoverable in the API.
  Raised and deliberately deferred: this pass documents the current shape.
