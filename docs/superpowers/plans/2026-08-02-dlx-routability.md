# Define-time DLX Routability Check Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make it impossible to construct a contract whose dead-letter exchange routes nowhere, closing the silent-loss path that the H2 guard could not see.

**Architecture:** A pure verdict function decides one of four outcomes per queue, reusing the existing publisher-routability resolver rather than adding a second one. A thin assert wraps it and `defineContract` calls that assert beside its two existing checks. No new resolver, no new matcher.

**Tech Stack:** TypeScript ESM, vitest, testcontainers RabbitMQ (`rabbitmq:4.2.1-management-alpine`), `unthrown` for error handling.

**Source spec:** `docs/superpowers/specs/2026-08-02-dlx-routability-design.md`

## Global Constraints

- No `any` — use `unknown` and narrow. Enforced by oxlint.
- Type aliases over interfaces (`type Foo = {}`, never `interface`).
- `.js` extensions required in all relative imports (ESM).
- Internal cross-module helpers use the `_internal_` prefix (no semver guarantee).
- Define-time throws need `// oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)` immediately above them.
- `unthrown`, not neverthrow: `.get()` when `E = never`, `.getOrThrow()` only on fallible results.
- Catalog dependencies only — never hardcode a version in a `package.json`.
- Public API changes need a changeset (`pnpm changeset`); the six publishable packages are a `fixed` group, so one entry covers all.
- Conventional commits; breaking changes use `!`. Enforced by commitlint.
- **The governing rule, which overrides any urge to strengthen a check:** a false negative is acceptable; **rejecting a valid contract is not.** A missed unroutable DLX is caught later in review or production; a false alarm breaks a paying client's build.
- **This work lands before 3.0 stable ships.** Breaking changes are expected and desired.

## The decision table

Evaluated **in order, first match wins**. Not independent conditions — a fanout DLX with no
`routingKey` matches rows 3 and 4 both, and while they happen to agree, the implementation must
not depend on that.

| #   | Case                                                                   | Verdict                                                                    |
| --- | ---------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| 1   | No `deadLetter` config (including a DLX supplied only via `arguments`) | `"skipped-undecidable"`                                                    |
| 2   | `deadLetter.externalConsumers === true`                                | `"skipped-external"`                                                       |
| 3   | `deadLetter.routingKey` set, **or** the DLX is `fanout` / `headers`    | resolver verdict → `"routable"` / `"unroutable"`                           |
| 4   | `routingKey` unset on a `direct` / `topic` DLX                         | `"routable"` iff ≥1 binding declared on that exchange, else `"unroutable"` |

Row 4 exists because the shared resolver's `bindingAccepts` returns `false` for direct/topic when
the routing key is `undefined` — correct for publishers, where a missing key really is
unroutable, but wrong here, where the key exists at runtime and is simply unknowable at define
time. Rows 3 and 4 together leave the resolver's semantics untouched for its original caller.

---

## File Structure

| File                                                                          | Responsibility                                              |
| ----------------------------------------------------------------------------- | ----------------------------------------------------------- |
| `packages/contract/src/builder/dead-letter-routability.ts` (create)           | The verdict function and its assert. Owns the error text.   |
| `packages/contract/src/builder/dead-letter-routability.spec.ts` (create)      | Unit tests, one per row plus the decidable-unroutable case. |
| `packages/contract/src/types.ts` (modify)                                     | `DeadLetterConfig.externalConsumers`.                       |
| `packages/contract/src/builder/queue.ts` (modify)                             | Allow `externalConsumers` in the `deadLetter` option bag.   |
| `packages/contract/src/builder/contract.ts` (modify)                          | Call the assert for every queue.                            |
| `tests/src/dlx-routability.spec.ts` (create)                                  | Paired real-broker proof.                                   |
| `docs/how-to/troubleshoot.md`, `docs/how-to/upgrade.md`, `AGENTS.md` (modify) | Error guidance, migration entry, invariant 22.              |

---

### Task 1: The verdict function

**Files:**

- Create: `packages/contract/src/builder/dead-letter-routability.ts`
- Test: `packages/contract/src/builder/dead-letter-routability.spec.ts`
- Modify: `packages/contract/src/types.ts` (`DeadLetterConfig`)
- Modify: `packages/contract/src/builder/queue.ts:134-137` (the `deadLetter` allow-list)

**Interfaces:**

- Consumes: `_internal_resolvePublisherRoutability(exchange, routingKey, bindings)` and `_internal_declaredPatternsFor(exchangeName, bindings)` from `./routability.js`
- Produces: `_internal_resolveDeadLetterRoutability(queue: QueueDefinition, bindings: readonly BindingDefinition[]): DeadLetterVerdict`
- Produces: `DeadLetterVerdict = "skipped-undecidable" | "skipped-external" | "routable" | "unroutable"`
- Produces: `DeadLetterConfig.externalConsumers?: boolean | undefined`

**Why:** RabbitMQ drops a dead-lettered message routed to zero queues exactly as silently as it drops an unroutable publish. The H2 guard checks that a DLX was _declared_; nothing checks that anything is bound to it. This task builds the decision; Task 2 enforces it.

This task deliberately does **not** wire the check into `defineContract` — the repo stays green
throughout, and the verdict function is reviewable on its own.

- [ ] **Step 1: Add `externalConsumers` to the config type**

In `packages/contract/src/types.ts`, add to `DeadLetterConfig` (after `routingKey`):

```ts
  /**
   * Declares that the queue bound to this dead-letter exchange lives outside
   * this contract — another service, or infrastructure-as-code, owns it.
   *
   * `defineContract` otherwise requires a declared binding from the exchange,
   * because a dead-letter exchange with nothing bound to it discards every
   * message routed to it, exactly as silently as an unroutable publish.
   *
   * Named to match `PublisherDefinition.externalConsumers`: the concept is
   * identical — the consuming side is not this contract's to declare.
   */
  externalConsumers?: boolean | undefined;
```

- [ ] **Step 2: Allow the new key on `defineQueue`**

In `packages/contract/src/builder/queue.ts`, the `deadLetter` allow-list at line 134 currently
reads `["exchange", "routingKey"]`. Add the third key:

```ts
_internal_assertKnownKeys("queue deadLetter config of", name, options.deadLetter, [
  "exchange",
  "routingKey",
  "externalConsumers",
]);
```

Without this, passing the option throws `Unknown option "externalConsumers"`.

- [ ] **Step 3: Write the failing test**

Create `packages/contract/src/builder/dead-letter-routability.spec.ts`:

```ts
import { describe, expect, it } from "vitest";

import type { BindingDefinition } from "../types.js";
import { defineExchange } from "./exchange.js";
import { defineQueue } from "./queue.js";
import { _internal_resolveDeadLetterRoutability } from "./dead-letter-routability.js";

const topicDlx = defineExchange("orders-dlx", { type: "topic" });
const directDlx = defineExchange("orders-dlx-direct", { type: "direct" });
const fanoutDlx = defineExchange("orders-dlx-fanout", { type: "fanout" });
const dlq = defineQueue("orders-dlq", { onPoison: "drop" });

/** Widened so topic and direct exchanges share one helper. */
function bindingTo(exchange: typeof topicDlx, routingKey: string): BindingDefinition {
  return { type: "queue", queue: dlq, exchange, routingKey } as BindingDefinition;
}

describe("_internal_resolveDeadLetterRoutability", () => {
  it("row 1: skips a queue with no deadLetter config", () => {
    const queue = defineQueue("order-processing", { onPoison: "drop" });
    expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("skipped-undecidable");
  });

  it("row 1: skips a DLX supplied only through the arguments passthrough", () => {
    // A bare exchange NAME — there is no ExchangeDefinition to look bindings up on.
    const queue = defineQueue("order-processing", {
      arguments: { "x-dead-letter-exchange": "orders-dlx" },
    });
    expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("skipped-undecidable");
  });

  it("row 2: skips when externalConsumers is declared", () => {
    const queue = defineQueue("order-processing", {
      deadLetter: { exchange: topicDlx, externalConsumers: true },
    });
    expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("skipped-external");
  });

  it("row 3: a matching routingKey on a topic DLX is routable", () => {
    const queue = defineQueue("order-processing", {
      deadLetter: { exchange: topicDlx, routingKey: "order.failed" },
    });
    const bindings = [bindingTo(topicDlx, "order.#")];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("routable");
  });

  it("row 3: a routingKey matching no binding is unroutable", () => {
    const queue = defineQueue("order-processing", {
      deadLetter: { exchange: topicDlx, routingKey: "order.failed" },
    });
    const bindings = [bindingTo(topicDlx, "user.#")];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("unroutable");
  });

  it("row 3: a direct DLX bound with '#' is unroutable — '#' is a topic wildcard", () => {
    // Measured against a real broker: topic + '#' receives 1, direct + '#' receives 0.
    const queue = defineQueue("order-processing", {
      deadLetter: { exchange: directDlx, routingKey: "order.failed" },
    });
    const bindings = [bindingTo(directDlx, "#")];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("unroutable");
  });

  it("row 3: a fanout DLX with any binding is routable, key or no key", () => {
    const queue = defineQueue("order-processing", { deadLetter: { exchange: fanoutDlx } });
    const bindings = [{ type: "queue", queue: dlq, exchange: fanoutDlx }] as BindingDefinition[];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("routable");
  });

  it("row 3: a fanout DLX with no bindings at all is unroutable", () => {
    const queue = defineQueue("order-processing", { deadLetter: { exchange: fanoutDlx } });
    expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("unroutable");
  });

  it("row 4: an unset routingKey accepts any declared binding", () => {
    // The key reaching the DLX is the message's original, unknowable here — so
    // presence of a binding is all this row claims. A deliberate false negative.
    const queue = defineQueue("order-processing", { deadLetter: { exchange: topicDlx } });
    const bindings = [bindingTo(topicDlx, "user.#")];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("routable");
  });

  it("row 4: an unset routingKey with nothing bound is unroutable", () => {
    // This is the defect the whole guard exists for.
    const queue = defineQueue("order-processing", { deadLetter: { exchange: topicDlx } });
    expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("unroutable");
  });

  it("follows an exchange-to-exchange forward out of the DLX", () => {
    const archive = defineExchange("archive", { type: "topic" });
    const queue = defineQueue("order-processing", {
      deadLetter: { exchange: topicDlx, routingKey: "order.failed" },
    });
    const bindings = [
      { type: "exchange", source: topicDlx, destination: archive, routingKey: "#" },
      { type: "queue", queue: dlq, exchange: archive, routingKey: "#" },
    ] as BindingDefinition[];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("routable");
  });
});
```

- [ ] **Step 4: Run the test to verify it fails**

```bash
cd packages/contract && pnpm vitest run src/builder/dead-letter-routability.spec.ts
```

Expected: FAIL — cannot resolve `./dead-letter-routability.js`.

- [ ] **Step 5: Implement the verdict function**

Create `packages/contract/src/builder/dead-letter-routability.ts`:

```ts
import type { BindingDefinition, QueueDefinition } from "../types.js";
import {
  _internal_declaredPatternsFor,
  _internal_resolvePublisherRoutability,
} from "./routability.js";

/**
 * Can a queue's dead-lettered messages reach a queue?
 *
 * RabbitMQ drops a dead-lettered message routed to zero queues exactly as
 * silently as it drops an unroutable publish — the runtime signal is
 * indistinguishable from success, and the worker logs a reassuring
 * "Sending message to DLQ" while every message vanishes. The H2 guard checks
 * that a dead-letter exchange was *declared*; this one checks that something
 * is bound to it.
 *
 * Reuses {@link _internal_resolvePublisherRoutability} rather than adding a
 * second resolver: multi-hop forwards, cycle detection and per-exchange-type
 * semantics are the same problem on a different edge of the graph.
 *
 * The governing rule is never to reject a valid contract, so every case that
 * cannot be decided resolves to "skipped".
 *
 * @internal
 */
export type DeadLetterVerdict =
  "skipped-undecidable" | "skipped-external" | "routable" | "unroutable";

/**
 * Decide the verdict for one queue. Rows are evaluated in order, first match
 * wins — see the decision table in the design spec.
 *
 * @internal
 */
export function _internal_resolveDeadLetterRoutability(
  queue: QueueDefinition,
  bindings: readonly BindingDefinition[],
): DeadLetterVerdict {
  const deadLetter = queue.deadLetter;

  // Row 1. No typed config: either the queue does not dead-letter at all (H2's
  // guard covers that), or the DLX came through the `arguments` passthrough as
  // a bare exchange NAME. There is no ExchangeDefinition to look bindings up
  // on, and the contract need not declare that exchange at all, so routability
  // is genuinely unknowable.
  if (deadLetter === undefined) {
    return "skipped-undecidable";
  }

  // Row 2.
  if (deadLetter.externalConsumers === true) {
    return "skipped-external";
  }

  const exchange = deadLetter.exchange;
  const ignoresRoutingKey = exchange.type === "fanout" || exchange.type === "headers";

  // Row 3. The key is known, or the exchange ignores it — the shared resolver
  // decides, exactly as it does for publishers.
  if (deadLetter.routingKey !== undefined || ignoresRoutingKey) {
    return _internal_resolvePublisherRoutability(exchange, deadLetter.routingKey, bindings).routable
      ? "routable"
      : "unroutable";
  }

  // Row 4. A direct or topic DLX with no routing key: RabbitMQ preserves the
  // message's ORIGINAL key, which is not knowable at define time. Proving this
  // case properly means showing every key that can reach the source queue also
  // matches a binding out of the DLX — pattern-subset reasoning, and getting it
  // wrong rejects a valid contract. "At least one binding" catches the defect
  // actually observed (a DLX with nothing bound) at zero false-positive risk,
  // and accepts a DLX bound only to non-matching patterns. A known, deliberate
  // false negative.
  return _internal_declaredPatternsFor(exchange.name, bindings).length > 0
    ? "routable"
    : "unroutable";
}
```

- [ ] **Step 6: Run the tests to verify they pass**

```bash
cd packages/contract && pnpm vitest run src/builder/dead-letter-routability.spec.ts
```

Expected: PASS, 11 tests.

- [ ] **Step 7: Verify the repo is still green**

Nothing is wired up yet, so nothing should break.

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm build && pnpm typecheck && pnpm test --concurrency=1 && pnpm lint
```

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(contract): decide whether a dead-letter exchange routes anywhere

RabbitMQ drops a dead-lettered message routed to zero queues as silently as
an unroutable publish. The H2 guard checks a DLX was declared; this decides
whether anything is bound to it, reusing the publisher resolver rather than
adding a second one. Not yet enforced."
```

---

### Task 2: Enforce it, and sweep the repo

**Files:**

- Modify: `packages/contract/src/builder/dead-letter-routability.ts` (add the assert)
- Modify: `packages/contract/src/builder/dead-letter-routability.spec.ts` (assert tests)
- Modify: `packages/contract/src/builder/contract.ts:314-321` (call it)
- Modify: fixtures and documentation across the repo

**Interfaces:**

- Consumes: `_internal_resolveDeadLetterRoutability` and `DeadLetterVerdict` from Task 1
- Produces: `_internal_assertDeadLetterRoutable(queue: QueueDefinition, bindings: readonly BindingDefinition[]): void`

**Why:** This is where the guard becomes real, and where every contract declaring a bare DLX
starts failing — including the **nine documentation files** already tracked as follow-up work in
`docs/superpowers/specs/2026-08-01-robustness-hardening-design.md`. The guard turns that manual
worklist into a build error, which is the outcome worth having.

- [ ] **Step 1: Write the failing test**

Append to `packages/contract/src/builder/dead-letter-routability.spec.ts`:

```ts
import { z } from "zod";

import { defineQueueBinding } from "./binding.js";
import { defineConsumer } from "./consumer.js";
import { defineContract } from "./contract.js";
import { defineMessage } from "./message.js";
import { definePublisher } from "./publisher.js";
import { defineRpc } from "./rpc.js";

describe("defineContract dead-letter routability", () => {
  const message = defineMessage(z.object({ orderId: z.string() }));
  const orders = defineExchange("orders", { type: "topic" });

  /** A routable publisher and binding, so only the DLX check can fail. */
  function contractWith(queue: ReturnType<typeof defineQueue>, extra: object = {}) {
    return {
      publishers: {
        orderCreated: definePublisher(orders, message, { routingKey: "order.created" }),
      },
      consumers: { processOrder: defineConsumer(queue, message) },
      bindings: {
        processOrder: defineQueueBinding(queue, orders, { routingKey: "order.created" }),
      },
      ...extra,
    };
  }

  it("throws when the dead-letter exchange has nothing bound to it", () => {
    const dlx = defineExchange("orders-dlx-bare", { type: "topic" });
    const queue = defineQueue("order-processing-bare", { deadLetter: { exchange: dlx } });

    expect(() => defineContract(contractWith(queue))).toThrow(/orders-dlx-bare/);
    expect(() => defineContract(contractWith(queue))).toThrow(/order-processing-bare/);
  });

  it("does NOT tell the author to add a deadLetter config — they have one", () => {
    // The remedy that does not apply is the failure mode this project has hit
    // three times: a confidently wrong pointer sends the reader after the wrong
    // thing entirely.
    const dlx = defineExchange("orders-dlx-advice", { type: "topic" });
    const queue = defineQueue("order-processing-advice", { deadLetter: { exchange: dlx } });

    expect(() => defineContract(contractWith(queue))).not.toThrow(/add .*deadLetter/i);
    expect(() => defineContract(contractWith(queue))).toThrow(/externalConsumers/);
  });

  it("accepts a dead-letter exchange with a bound dead-letter queue", () => {
    const dlx = defineExchange("orders-dlx-bound", { type: "topic" });
    const boundDlq = defineQueue("orders-dlq-bound", { onPoison: "drop" });
    const queue = defineQueue("order-processing-bound", { deadLetter: { exchange: dlx } });

    expect(() =>
      defineContract(
        contractWith(queue, {
          queues: { boundDlq },
          bindings: {
            processOrder: defineQueueBinding(queue, orders, { routingKey: "order.created" }),
            dlq: defineQueueBinding(boundDlq, dlx, { routingKey: "#" }),
          },
        }),
      ),
    ).not.toThrow();
  });

  it("accepts an unbound dead-letter exchange marked externalConsumers", () => {
    const dlx = defineExchange("orders-dlx-external", { type: "topic" });
    const queue = defineQueue("order-processing-external", {
      deadLetter: { exchange: dlx, externalConsumers: true },
    });

    expect(() => defineContract(contractWith(queue))).not.toThrow();
  });

  it("checks an rpc's queue too, not only consumers", () => {
    const dlx = defineExchange("rpc-dlx-bare", { type: "topic" });
    const rpcQueue = defineQueue("rpc-processing-bare", { deadLetter: { exchange: dlx } });

    expect(() =>
      defineContract({
        rpcs: {
          calculate: defineRpc(rpcQueue, { request: message, response: message }),
        },
      }),
    ).toThrow(/rpc-processing-bare/);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/contract && pnpm vitest run src/builder/dead-letter-routability.spec.ts
```

Expected: FAIL — the first test does not throw.

- [ ] **Step 3: Write the assert**

Append to `packages/contract/src/builder/dead-letter-routability.ts`:

```ts
/**
 * Throw when a queue's dead-lettered messages would reach no queue.
 *
 * The remedy deliberately never says "add a `deadLetter` config" — the author
 * has one; that is the premise of the finding. A remedy that does not apply is
 * worse than none, because it sends the reader after the wrong thing while
 * looking authoritative.
 *
 * @internal
 */
export function _internal_assertDeadLetterRoutable(
  queue: QueueDefinition,
  bindings: readonly BindingDefinition[],
): void {
  const verdict = _internal_resolveDeadLetterRoutability(queue, bindings);
  if (verdict !== "unroutable") return;

  // Non-null: "unroutable" is only reachable through rows 3 and 4, both of
  // which require a typed `deadLetter` config.
  const deadLetter = queue.deadLetter as NonNullable<QueueDefinition["deadLetter"]>;
  const exchange = deadLetter.exchange;
  const declared = _internal_declaredPatternsFor(exchange.name, bindings);
  const declaredText =
    declared.length > 0
      ? `Declared on "${exchange.name}": ${declared.map((p) => `"${p}"`).join(", ")}.`
      : `Nothing is bound to "${exchange.name}".`;
  const keyText =
    deadLetter.routingKey === undefined
      ? "its dead-lettered messages keep their original routing key"
      : `its dead-lettered messages are routed with "${deadLetter.routingKey}"`;

  // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)
  throw new Error(
    `Queue "${queue.name}" dead-letters to exchange "${exchange.name}" (${exchange.type}), but ` +
      `nothing there can receive them: ${keyText}. ${declaredText} RabbitMQ discards a message ` +
      `routed to zero queues, so these would be lost exactly as silently as if the queue had no ` +
      `dead-letter exchange at all. Bind a queue to "${exchange.name}" that accepts them, or set ` +
      `\`externalConsumers: true\` on the deadLetter config if another service owns that queue.`,
  );
}
```

- [ ] **Step 4: Call it from `defineContract`**

In `packages/contract/src/builder/contract.ts`, the tail currently reads:

```ts
for (const [consumerName, consumer] of Object.entries(result.consumers ?? {})) {
  _internal_assertNoSilentPoisonLoss(consumer.queue, consumerName);
}
for (const [rpcName, rpc] of Object.entries(result.rpcs ?? {})) {
  _internal_assertNoSilentPoisonLoss(rpc.queue, rpcName);
}

return result as ContractOutput<TContract>;
```

Add the routability check over **every declared queue**, not just consumed ones — an unbound DLX
loses messages regardless of who consumes the source queue:

```ts
for (const [consumerName, consumer] of Object.entries(result.consumers ?? {})) {
  _internal_assertNoSilentPoisonLoss(consumer.queue, consumerName);
}
for (const [rpcName, rpc] of Object.entries(result.rpcs ?? {})) {
  _internal_assertNoSilentPoisonLoss(rpc.queue, rpcName);
}

// Every declared queue, not only consumed ones: a queue whose DLX routes
// nowhere loses its dead-lettered messages whoever consumes it, and
// `queues` already holds every queue the contract knows about.
for (const queue of Object.values(queues)) {
  _internal_assertDeadLetterRoutable(queue, declaredBindings);
}

return result as ContractOutput<TContract>;
```

`declaredBindings` is the `Object.values(bindings)` local the publisher-routability loop already
builds; reuse it rather than recomputing. Import the assert:

```ts
import { _internal_assertDeadLetterRoutable } from "./dead-letter-routability.js";
```

- [ ] **Step 5: Run the new tests to verify they pass**

```bash
cd packages/contract && pnpm vitest run src/builder/dead-letter-routability.spec.ts
```

Expected: PASS, 16 tests.

- [ ] **Step 6: Mutation-verify the guard**

Comment out the new loop in `contract.ts`, re-run the spec, and confirm the five
`defineContract` tests fail. Restore it. **Report the exact failure output.** Every guard added
in this project that was not mutation-verified turned out to have a dead arm.

- [ ] **Step 7: Sweep the repo**

Run **every command that executes a contract**. A sweep driven by one test command has missed
whole classes of call site three times in this project: `.test-d.ts` files (typechecked, never
executed), example packages (no `test` script), and the integration vitest project (root
`pnpm test` runs `--project unit` only).

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm build && pnpm typecheck && pnpm test --concurrency=1 && pnpm lint
pnpm --filter @amqp-contract/core test:integration
pnpm --filter @amqp-contract/client test:integration
pnpm --filter @amqp-contract/worker test:integration
pnpm --filter @amqp-contract/tests test:integration
```

Run the integration projects **one at a time** — there is a known pre-existing flake when several
testcontainers share one Docker daemon. Do not try to fix that flake.

Decision rule per failure, and record every one in your report as a table (file, fix, one-line
reason):

- **Bind a dead-letter queue to the DLX** — the default choice. The shape is at
  `docs/how-to/define-a-contract.md:141-156`: declare the DLQ, then
  `defineQueueBinding(dlq, dlx, { routingKey: "#" })` on a topic DLX.
- **Set `externalConsumers: true`** only where the fixture genuinely models a DLQ owned outside
  the contract.

Fixtures are exemplars — people copy them — so prefer binding a real DLQ wherever the fixture
reads like a production contract.

**Watch for the trap this project already hit:** `#` is a _topic_ wildcard. On a `direct` DLX it
is a literal key matching nothing. Measured against a real broker: topic + `#` receives 1,
direct + `#` receives 0. If a DLX is direct, bind the actual routing key.

- [ ] **Step 8: Sweep the nine documentation files**

These were already tracked as follow-up work and now fail to construct:

`packages/contract/README.md`, `packages/core/README.md`, `packages/worker/README.md`,
`docs/explanation/core-concepts.md`, `docs/tutorial/adding-request-reply.md`,
`docs/how-to/use-request-reply.md`, `docs/how-to/retry-failed-messages.md` (5 sites),
`docs/how-to/bridge-domains.md`, `docs/examples/command-pattern.md`.

Each declares a DLX with nothing bound. Add the DLQ and the binding, matching
`docs/how-to/define-a-contract.md:141-156`. **Verify every snippet you touch actually
constructs** — extract it, typecheck it, and run it. Two snippets shipped on a previous branch
did not compile, and snippet execution is the only thing that caught them.

- [ ] **Step 9: Add a changeset**

```bash
pnpm changeset
```

Choose **major**. Summary:

```
`defineContract` now throws when a queue's dead-letter exchange has nothing bound
to it. RabbitMQ discards a message routed to zero queues, so such a queue lost
every rejected message exactly as silently as one with no dead-letter exchange at
all — while the worker logged a reassuring "Sending message to DLQ". Bind a queue
to the exchange, or set `externalConsumers: true` on the deadLetter config if
another service owns it. A dead-letter exchange supplied through the raw
`arguments` passthrough names an exchange this contract cannot inspect and is not
checked.
```

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(contract)!: reject a dead-letter exchange that routes nowhere

A DLX with nothing bound discards every message routed to it, as silently as
having no DLX at all, while the worker logs a reassuring hand-off. Closes the
Critical the H2 review found, and the class behind it: guards here checked
that something was declared, never that it routes."
```

---

### Task 3: Real-broker proof and migration guide

**Files:**

- Create: `tests/src/dlx-routability.spec.ts`
- Modify: `AGENTS.md` (invariant 22)
- Modify: `docs/how-to/upgrade.md`, `docs/how-to/troubleshoot.md`

**Interfaces:**

- Consumes: the guard from Task 2; `externalConsumers` from Task 1

**Why:** This project's settled pattern is a pair — one test showing the loss is genuine, one
showing the guard catches it. The first is why the second exists, and it fails loudly if anyone
ever weakens the guard.

- [ ] **Step 1: Write the paired proof**

Create `tests/src/dlx-routability.spec.ts`:

```ts
import {
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  definePublisher,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { it } from "@amqp-contract/testing/extension";
import { describe, expect } from "vitest";
import { z } from "zod";

/**
 * The dead-letter half of the routability guard, proven end to end.
 *
 * Test 1 shows the hazard is genuine: a message dead-lettered to an exchange
 * with nothing bound is discarded by the broker, with no error anywhere. Test 2
 * shows the guard rejects that contract before it can run.
 */
describe("dead-letter routability", () => {
  const message = defineMessage(z.object({ orderId: z.string() }));

  it("INVARIANT: a message dead-lettered to an unbound exchange is discarded by the broker", async ({
    amqpChannel,
  }) => {
    const dlxName = `dlx-unbound-${Date.now()}`;
    const queueName = `src-${Date.now()}`;
    const witnessName = `witness-${Date.now()}`;

    await amqpChannel.assertExchange(dlxName, "topic", { durable: false });
    await amqpChannel.assertQueue(queueName, {
      durable: false,
      deadLetterExchange: dlxName,
    });
    // A queue on the same broker bound to nothing of ours, so a "0 messages"
    // result cannot be explained by the broker having dropped everything.
    await amqpChannel.assertQueue(witnessName, { durable: false });

    amqpChannel.sendToQueue(queueName, Buffer.from(JSON.stringify({ orderId: "1" })));
    amqpChannel.sendToQueue(witnessName, Buffer.from(JSON.stringify({ orderId: "w" })));

    const delivery = await amqpChannel.get(queueName, { noAck: false });
    expect(delivery).not.toBe(false);
    if (delivery === false) throw new Error("no delivery");

    // Reject it: the broker dead-letters to an exchange bound to nothing.
    amqpChannel.nack(delivery, false, false);
    await new Promise((resolve) => setTimeout(resolve, 300));

    // The witness proves the broker is alive and delivering.
    const witness = await amqpChannel.checkQueue(witnessName);
    expect(witness.messageCount).toBe(1);

    // The dead-lettered message reached nothing, and nothing reported it.
    const source = await amqpChannel.checkQueue(queueName);
    expect(source.messageCount).toBe(0);

    await amqpChannel.deleteQueue(queueName);
    await amqpChannel.deleteQueue(witnessName);
    await amqpChannel.deleteExchange(dlxName);
  }, 20_000);

  it("INVARIANT: the same contract is rejected at define time", () => {
    const orders = defineExchange("orders-dlxr", { type: "topic", durable: false });
    const dlx = defineExchange("orders-dlx-dlxr", { type: "topic", durable: false });
    const queue = defineQueue("order-processing-dlxr", {
      type: "classic",
      durable: false,
      deadLetter: { exchange: dlx },
    });

    expect(() =>
      defineContract({
        publishers: {
          orderCreated: definePublisher(orders, message, { routingKey: "order.created" }),
        },
        consumers: { processOrder: defineConsumer(queue, message) },
        bindings: {
          processOrder: defineQueueBinding(queue, orders, { routingKey: "order.created" }),
        },
      }),
    ).toThrow(/dead-letters to exchange/);
  });
});
```

The witness queue matters: without it, "the message is gone" is also consistent with a broken
broker or a bad queue name, and the test would pass for the wrong reason.

- [ ] **Step 2: Run the integration suite**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm --filter @amqp-contract/tests test:integration
```

Requires Docker. If it is unavailable, say so explicitly — do not claim integration coverage that
did not run. If test 1 does **not** show the message being lost, that is a major finding: report
it rather than adjusting the test, because it would mean the hazard is not what we believe.

- [ ] **Step 3: Record the invariant**

Append to the "Load-bearing invariants" list in `AGENTS.md`:

```markdown
22. **A queue whose dead-letter exchange routes nowhere is rejected at define time** (RabbitMQ discards a message routed to zero queues, so an unbound DLX loses every rejected message exactly as silently as having no DLX — while the worker logs a reassuring hand-off) — `tests/src/dlx-routability.spec.ts` (the paired broker-proof / guard tests) + `packages/contract/src/builder/dead-letter-routability.spec.ts`.
```

- [ ] **Step 4: Write the migration entry**

Add to `docs/how-to/upgrade.md`, in the voice of the existing 3.0 entries:

- **What breaks:** a contract whose queue declares a `deadLetter` exchange with nothing bound to
  it now fails to construct.
- **Why:** this is not a new restriction — it is the discovery of an existing data-loss path.
  Those messages are already being discarded silently.
- **The fix:** declare the dead-letter queue and bind it, shape at
  `docs/how-to/define-a-contract.md:141-156`. On a **direct** DLX bind the real routing key —
  `#` is a topic wildcard and matches nothing on a direct exchange.
- **The opt-out:** `deadLetter: { exchange, externalConsumers: true }` when another service or
  IaC owns the dead-letter queue.
- **Not checked:** a DLX supplied through the raw `arguments` passthrough, which names an
  exchange this contract cannot inspect.
- Cross-link the existing "If the queue already exists in production" section in
  `troubleshoot.md` rather than restating the RabbitMQ 406 routes — adding a binding to live
  topology has the same constraint.

Add a matching entry to `docs/how-to/troubleshoot.md` explaining the error text and both
remedies.

- [ ] **Step 5: Final verification**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm build && pnpm typecheck && pnpm test --concurrency=1 && pnpm lint && npx oxfmt --check .
pnpm --filter @amqp-contract/core test:integration
pnpm --filter @amqp-contract/client test:integration
pnpm --filter @amqp-contract/worker test:integration
pnpm --filter @amqp-contract/tests test:integration
```

All green, integration projects run one at a time.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test: prove an unbound dead-letter exchange discards messages

Adds invariant 22 and the 3.0 migration entry. The witness queue is what
makes the loss proof non-vacuous: without it, 'the message is gone' is also
consistent with a broken broker."
```

---

## Out of scope

Tracked in `docs/superpowers/specs/2026-08-01-robustness-hardening-design.md`:

- **`MatchingBindingPattern`'s template-literal hole** — a live, pre-existing false compile error
  on valid bindings via `defineEventConsumer`. Non-breaking, independent.
- **`setup.ts` / `asyncapi` DLX precedence inconsistency** — `setup.ts` is right; asyncapi should
  follow.
- **The parallel-test flake**, `publishTimeoutMs` validation, explicit `prefetch: 0`, the
  coverage-floor ratchet, and an automated gate on documentation snippets.
- **H5, duplicate delivery.** Untouched by design and still the largest unaddressed risk: clients
  get no help deduplicating, which in a payments path means double-processing.
