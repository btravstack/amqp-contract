# Delivery Guarantees Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** State the library's at-least-once delivery guarantee once, completely, and in one place — including the two facts currently absent: that redelivery does not require retry configuration, and that a failed publish may still have been delivered.

**Architecture:** One new explanation page, registered in the VitePress sidebar, plus targeted edits to four existing pages whose current framing either ties duplicates to retry configuration or half-states the guarantee in passing.

**Tech Stack:** Markdown, VitePress (Diátaxis structure: tutorial / how-to / reference / explanation).

## Global Constraints

- Documentation only. No source change, no API change, no new dependency.
- Every factual claim must be true of the current tree, and defaults must be stated with the value the code actually uses. Where a guarantee has a boundary, name the boundary rather than rounding it off. This branch exists because the existing docs rounded one off.
- **Do not add a `defineContract` snippet to any page in this plan.** `tests/src/snippets/extract.ts` executes every `ts / `typescript fence that calls `defineContract`, and `tests/src/snippets/snippet-execution.spec.ts` pins the corpus at exactly `31`. A `defineContract` example would have to construct standalone from only the imports it shows AND require bumping that number in the same commit. The examples in this plan deliberately show `publish` and handler code instead, which the parser skips.
- Do not edit generated output: `docs/api/**` and `docs/.vitepress/dist/**` are generated (`dist` is gitignored). `docs/guide/**` pages are 3.0 redirect stubs — leave them alone.
- Conventional commits required (`docs` for all tasks here).
- Internal links use VitePress root-relative form without the `.md` extension: `/explanation/delivery-guarantees`.

**Commands used throughout:**

- Docs-affecting test suite: `pnpm --filter @amqp-contract/tests test`
- Docs build (catches a dead internal link or an unregistered page): `pnpm --filter @amqp-contract/docs build`. Its `prebuild` step runs `tsx scripts/copy-docs.ts` first, so the build may touch generated files under `docs/api/**` — that is expected and must not be committed.

---

## File Structure

| File                                      | Responsibility                                               | Task |
| ----------------------------------------- | ------------------------------------------------------------ | ---- |
| `docs/explanation/delivery-guarantees.md` | The complete statement of the guarantee (new)                | 1    |
| `docs/.vitepress/config.ts`               | Sidebar registration — an unregistered page is unreachable   | 1    |
| `docs/explanation/the-retry-model.md`     | Stop framing duplicates as a consequence of enabling retries | 2    |
| `docs/how-to/consume-messages.md`         | Link the drain-timeout aside to the full statement           | 3    |
| `docs/how-to/upgrade.md`                  | Link the crash-redelivery aside to the full statement        | 3    |
| `docs/how-to/troubleshoot.md`             | Link the redelivery-burst aside to the full statement        | 3    |

---

### Task 1: The delivery-guarantees page

**Files:**

- Create: `docs/explanation/delivery-guarantees.md`
- Modify: `docs/.vitepress/config.ts` (the Explanation section of `GUIDE_SIDEBAR`, around lines 108-115)

**Interfaces:**

- Consumes: nothing.
- Produces: the route `/explanation/delivery-guarantees`, which Tasks 2 and 3 link to.

- [ ] **Step 1: Verify every factual claim the page makes**

Before writing, confirm each of these against the tree, and record the file:line you checked in your report. If any disagrees with the value below, STOP and report it rather than writing the claim.

- `packages/core/src/amqp-client.ts` — the channel is created with `confirm: true`, so `publish` waits for a publisher confirm.
- `packages/core/src/amqp-client.ts` — `DEFAULT_PUBLISH_TIMEOUT_MS` is `30_000`.
- The worker's default consumer prefetch is `10` (`DEFAULT_PREFETCH`).
- The worker's default drain timeout is `30_000` ms (`DEFAULT_DRAIN_TIMEOUT_MS`).
- `packages/core/src/amqp-client.ts` — `AmqpPublishOptions` is amqplib's `Options.Publish`, so `messageId` is a valid publish option.
- `messageId` appears nowhere in `packages/core/src` or `packages/client/src` — the library never sets one.
- `packages/client/src/client.ts` — `publish(publisherName, message, options?)` returns `AsyncResult<void, MessageValidationError>`.
- `packages/worker/src/types.ts` — a handler's second argument is amqplib's `ConsumeMessage`, so `rawMessage.properties.messageId` is valid.

- [ ] **Step 2: Create the page**

Create `docs/explanation/delivery-guarantees.md` with exactly the content between the two markers below. Do not include the marker lines themselves.

**>>> FILE CONTENT BEGINS ON THE NEXT LINE >>>**

---

title: Delivery guarantees - amqp-contract
description: Why delivery is at-least-once regardless of retry configuration, why a failed publish is ambiguous, and where idempotency has to live.
---

# Delivery guarantees

amqp-contract delivers **at-least-once**. A message the broker accepts will reach a consumer one or more times. No configuration makes that exactly once, because no such configuration exists to make.

This page is the whole statement in one place: when a message can arrive twice, what a failed publish does and does not tell you, and what those two facts leave you responsible for.

## At-least-once is not a retry setting

The retry mode defaults to `none`, and it is tempting to read that as having opted out of duplicates. It is not: `none` governs what happens to a `RetryableError` raised by your handler, and nothing else.

A message can arrive a second time with no retry configuration at all.

- **The worker crashes mid-handler.** Everything it had not acked returns to the queue. At the default prefetch of 10, that is up to ten messages for each consumer that was running.
- **The connection or channel drops before the ack is written.** Your handler may have finished the work. The broker never heard so, and redelivers.
- **`close()` reaches its drain timeout.** `worker.close()` waits for in-flight handlers, bounded by `drainTimeoutMs` (30 000 ms by default). On timeout the channel closes anyway, and whatever was still un-acked goes back.

Two more appear once you do configure retries:

- **`immediate-requeue`** returns the message to its own queue for another attempt.
- **`ttl-backoff`** republishes it through a wait queue.

The first three are properties of running a consumer against a broker; you did not choose them and you cannot switch them off. The last two are choices. All five produce the same thing at your handler: a message it has seen before.

## A failed publish is ambiguous

`publish()` waits for a publisher confirm, bounded by `publishTimeoutMs` (30 000 ms by default). When that deadline passes, the call settles as a failure.

That failure means the client stopped waiting. It does not mean the broker failed to receive the message.

So a publish error is not proof of non-delivery, and the natural response — send it again — can produce a duplicate. Nothing closes this gap from the publisher's side: an acknowledgement can always be lost after the work it acknowledges is done, and no protocol removes that.

Two honest responses:

- **Republish, and let the consumer cope.** The simplest, and correct as long as the consumer is idempotent.
- **Republish with the same identifier.** If the message carries an id that stays the same across your own retries, the consumer can recognise the repeat. An id regenerated per attempt is worse than none — every retry looks like new work.

## Idempotency lives in your handler

The library does not deduplicate for you, and it attaches no identity you could deduplicate on: it never sets AMQP's `messageId`. If you want one, you set it.

Publish options are amqplib's, so `messageId` is available:

```typescript
await client
  .publish("orderCreated", { orderId: "ORD-123" }, { messageId: idempotencyKey })
  .getOrThrow();
```

A handler reads it from the raw message:

```typescript
processOrder: ({ payload }, rawMessage) => {
  const id = rawMessage.properties.messageId;
  return OkAsync(undefined);
},
```

Or ignore AMQP's field and carry a business key in the payload — which has the advantage of surviving anything that rebuilds the message on the way through.

Whichever you choose, the property that matters is that the id is **stable across the sender's own retries** and unique per logical operation. An id that changes when the sender retries deduplicates nothing.

What you do with it is ordinary application work, and the right answer depends on what the handler touches:

- An **idempotency key** at the downstream provider. Most payment APIs accept one and do the deduplication for you, which is the strongest option when it is available.
- An **upsert** instead of an insert, so a repeat converges instead of conflicting.
- A **deduplication table** keyed on the id, written in the same transaction as the effect. Two separate transactions reintroduce the gap you are trying to close: a crash between them leaves the effect applied and the id unrecorded.

## Where next

- [The retry model](/explanation/the-retry-model) — what retrying does, and why validation failures never do.
- [Consume messages](/how-to/consume-messages) — prefetch, draining, and reaching the raw message.
- [Route dead letters](/how-to/route-dead-letters) — where a message goes when retrying is over.

**<<< FILE CONTENT ENDS ON THE PREVIOUS LINE <<<**

- [ ] **Step 3: Register the page in the sidebar**

In `docs/.vitepress/config.ts`, in the `GUIDE_SIDEBAR` array's Explanation section, add an entry for the new page after "The retry model" and before "Comparison":

```ts
      { text: "Delivery guarantees", link: "/explanation/delivery-guarantees" },
```

The resulting Explanation items, in order: Why amqp-contract? · Core concepts · Errors as values · The retry model · Delivery guarantees · Comparison.

- [ ] **Step 4: Build the docs**

Run the docs build. Expected: success, with no warning about a dead link and no unresolved route. If the build reports a dead link, fix the link — do not remove the "Where next" section to make it pass.

- [ ] **Step 5: Confirm the snippet corpus is unchanged**

Run: `pnpm --filter @amqp-contract/tests test`

Expected: PASS, including `documentation snippets > finds snippets to check`, which asserts exactly 31. The two examples on the new page do not call `defineContract`, so the parser skips them and the count must not move. If that assertion fails, an example was written in a way the parser picks up — report it rather than editing the number.

- [ ] **Step 6: Commit**

```bash
git add docs/explanation/delivery-guarantees.md docs/.vitepress/config.ts
git commit -m "docs: state the at-least-once delivery guarantee in one place"
```

---

### Task 2: Stop framing duplicates as a retry consequence

The retry page currently presents duplicate side effects as something you take on by enabling retries. A reader on the default `none` mode can reasonably conclude the warning is not about them.

**Files:**

- Modify: `docs/explanation/the-retry-model.md` (the paragraph at :56, and the "Retries are not exactly-once" section at :90-94)

**Interfaces:**

- Consumes: the route `/explanation/delivery-guarantees` created in Task 1.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Reframe the default-mode paragraph**

Replace this paragraph (currently at :56):

The default mode is `none`, which means a `RetryableError` behaves like a `NonRetryableError` until you opt into a retry policy. This is intentional. Retrying is a choice with real consequences (duplicate side effects, queue growth), so it is not on by default.

with:

The default mode is `none`, which means a `RetryableError` behaves like a `NonRetryableError` until you opt into a retry policy. This is intentional. Retrying is a choice with real consequences — more duplicate deliveries, and queue growth — so it is not on by default. It does not follow that `none` means no duplicates: a crash, a dropped connection, or a drain timeout redelivers whatever was un-acked, whatever the retry mode. [Delivery guarantees](/explanation/delivery-guarantees) has the full list.

- [ ] **Step 2: Narrow the exactly-once section to what is retry-specific**

Replace this paragraph (currently at :94, the second paragraph under "## Retries are not exactly-once"):

The retry model guarantees delivery attempts, not idempotency. Making the _work_ idempotent — an idempotency key at the payment provider, an upsert instead of an insert, a deduplication table keyed on message ID — remains yours. This is inherent to at-least-once messaging rather than specific to this library, but it is the assumption most often left unexamined when retries are switched on.

with:

The retry model guarantees delivery attempts, not idempotency. Making the _work_ idempotent remains yours, and switching retries on raises how often it matters rather than introducing the problem — delivery is at-least-once either way. [Delivery guarantees](/explanation/delivery-guarantees) covers where duplicates come from, why a failed publish is ambiguous, and the identifier you need before a deduplication table is worth anything.

Leave the first paragraph of that section (the card-charging example at :92) and the dead-letter-exchange paragraph at :96 exactly as they are.

- [ ] **Step 3: Add the page to "Where next"**

In the "## Where next" list at the end of the file, add as the first entry:

- [Delivery guarantees](/explanation/delivery-guarantees) — why at-least-once holds regardless of retry configuration, and where idempotency has to live.

- [ ] **Step 4: Verify**

Run the docs build and `pnpm --filter @amqp-contract/tests test`. Expected: both pass, snippet count still 31.

Re-read the whole "Retries are not exactly-once" section as it now stands and confirm it does not contradict the new page or repeat it at length — the section should say what is true of retries and defer the general property.

- [ ] **Step 5: Commit**

```bash
git add docs/explanation/the-retry-model.md
git commit -m "docs: separate the at-least-once property from the retry choice"
```

---

### Task 3: Point the scattered asides at the full statement

Three how-to pages each state part of the guarantee in passing. They stay — each is useful where it sits — but each should point at the complete statement instead of being the only place a reader learns the property.

**Files:**

- Modify: `docs/how-to/consume-messages.md` (the drain-timeout paragraph, around :205)
- Modify: `docs/how-to/upgrade.md` (the prefetch rationale, around :22)
- Modify: `docs/how-to/troubleshoot.md` (the prefetch rationale, around :535)

**Interfaces:**

- Consumes: the route `/explanation/delivery-guarantees` created in Task 1.
- Produces: nothing.

- [ ] **Step 1: `docs/how-to/consume-messages.md`**

In the paragraph beginning "The drain is bounded by `drainTimeoutMs`", change the parenthetical `(at-least-once semantics)` to a link:

([at-least-once semantics](/explanation/delivery-guarantees))

Change nothing else in that paragraph.

- [ ] **Step 2: `docs/how-to/upgrade.md`**

In the paragraph beginning "**Why it was unsafe:**", append this sentence to the end of the paragraph:

Redelivery on a crash is not specific to prefetch — see [Delivery guarantees](/explanation/delivery-guarantees).

- [ ] **Step 3: `docs/how-to/troubleshoot.md`**

In the paragraph containing "a large redelivery burst if the worker crashes", append this sentence after that sentence's closing parenthesis:

See [Delivery guarantees](/explanation/delivery-guarantees) for when redelivery happens.

- [ ] **Step 4: Verify**

Run the docs build and `pnpm --filter @amqp-contract/tests test`. Expected: both pass; no dead links; snippet count still 31.

Then run `grep -rn "delivery-guarantees" docs --include="*.md"` and confirm every link uses the exact route `/explanation/delivery-guarantees` — no `.md` suffix, no relative form.

- [ ] **Step 5: Commit**

```bash
git add docs/how-to/consume-messages.md docs/how-to/upgrade.md docs/how-to/troubleshoot.md
git commit -m "docs: link the redelivery asides to the full delivery guarantee"
```

---

## Final verification

- [ ] **Step 1: Full gate**

```bash
pnpm build && pnpm typecheck && pnpm test --concurrency=1 && pnpm lint && pnpm exec oxfmt --check .
```

Expected: all green. `--concurrency=1` is required — several packages start their own testcontainer and a parallel run fails on Docker contention, which is an environment artifact rather than a signal about this branch.

- [ ] **Step 2: No source file was touched**

Run: `git diff main --stat -- packages tests`

Expected: no output. This branch changes documentation only; a source or test change means something exceeded the plan.

- [ ] **Step 3: Every default stated on the new page still matches the code**

Re-check the four numbers the page commits to — prefetch 10, drain timeout 30 000 ms, publish timeout 30 000 ms, retry mode default `none` — against the constants in the tree. A page that states a default is a page that goes stale when the default moves; confirm it is correct on the day it ships.
