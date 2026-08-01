# H2–H4 — Safe Defaults and Poison-Loss Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the three remaining hazards from the robustness catalog — unbounded prefetch, unbounded publish buffering, and silent poison-message loss on DLX-less queues — by making the safe choice the default and requiring an explicit, written opt-out.

**Architecture:** H3 and H4 are single-decision-point defaults resolved in `@amqp-contract/core`, so every caller (client, worker, direct core user) inherits them without duplication. H2 is a define-time check in `@amqp-contract/contract`, following the same shape as the H1 routability guard already on `main`: computed in `defineContract` where the full picture is available, throwing with an actionable message, opted out by an explicit queue option.

**Tech Stack:** TypeScript ESM, vitest, testcontainers RabbitMQ (`rabbitmq:4.2.1-management-alpine`), `unthrown` for error handling.

**Source spec:** `docs/superpowers/specs/2026-08-01-robustness-hardening-design.md` (hazards H2, H3, H4)

## Global Constraints

- No `any` — use `unknown` and narrow. Enforced by oxlint.
- Type aliases over interfaces (`type Foo = {}`, never `interface`).
- `.js` extensions required in all relative imports (ESM).
- Internal cross-module helpers use the `_internal_` prefix (no semver guarantee).
- Define-time throws need `// oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)` immediately above them.
- `unthrown`, not neverthrow: `.get()` when `E = never`, `.getOrThrow()` only on fallible results, `.match({ ok, errCases, defect })`.
- Catalog dependencies only — never hardcode a version in a `package.json`.
- Public API changes need a changeset (`pnpm changeset`); the six publishable packages are a `fixed` group, so one entry covers all.
- Conventional commits; breaking changes use `!`. Enforced by commitlint.
- Run `pnpm typecheck` before declaring a task done — it is not in the pre-commit hook.
- **This work lands before 3.0 stable ships.** Breaking changes are expected and desired.

## Design decision locked in during planning

**The H2 check moves from `defineQueue` to `defineContract`, scoped to consumed queues only.**

The spec said `defineQueue` throws when a queue has no dead-letter exchange. Reading the code shows that would reject valid contracts, which the governing rule forbids:

1. **A dead-letter queue has no DLX of its own** — that would be infinite regress. Every user with a DLQ would trip the check on the DLQ itself.
2. **Only a _consumed_ queue can poison-loop.** A queue declared for inspection, or one another service consumes, is never nacked by this worker and cannot lose a message this way.
3. **`defineQueue` is standalone** and cannot know whether the queue is consumed. Only `defineContract` sees `consumers` and `rpcs`.

So: `defineContract` throws when a queue that appears in `consumers` or `rpcs` has neither a `deadLetter` config nor `onPoison: "drop"`. The `onPoison` option still lives on `defineQueue`, where the author reasons about the queue.

This mirrors H1 exactly — the same "compute where the full picture is available" correction, for the same reason.

---

## File Structure

| File                                                         | Responsibility                                                         |
| ------------------------------------------------------------ | ---------------------------------------------------------------------- |
| `packages/core/src/amqp-client.ts` (modify)                  | Resolve the prefetch and publish-timeout defaults; own both constants. |
| `packages/core/src/amqp-client.spec.ts` (create)             | Unit tests for both resolutions.                                       |
| `packages/core/src/index.ts` (modify)                        | Export the two new constants.                                          |
| `packages/contract/src/builder/queue.ts` (modify)            | Accept and carry `onPoison`.                                           |
| `packages/contract/src/types.ts` (modify)                    | Add `onPoison` to the queue definition.                                |
| `packages/contract/src/builder/poison-loss.ts` (create)      | Decide whether a consumed queue can silently drop; own the error text. |
| `packages/contract/src/builder/poison-loss.spec.ts` (create) | Unit tests for the decision and message.                               |
| `packages/contract/src/builder/contract.ts` (modify)         | Call the check for every consumed queue.                               |
| `tests/src/safe-defaults.spec.ts` (create)                   | Real-broker proofs for H2 and H3.                                      |
| `docs/how-to/troubleshoot.md` (modify)                       | Entries for the new throw and the prefetch change.                     |
| `docs/how-to/upgrade.md` (modify)                            | Migration entries for all three.                                       |
| `AGENTS.md` (modify)                                         | Invariants 20 and 21.                                                  |

---

### Task 1: H3 — default prefetch

**Files:**

- Modify: `packages/core/src/amqp-client.ts` (the `AmqpConsumeOptions` type ~line 145, and `consume()` ~line 444)
- Modify: `packages/core/src/index.ts` (export the constant)
- Test: `packages/core/src/amqp-client.spec.ts` (create)
- Modify: `docs/how-to/troubleshoot.md`

**Interfaces:**

- Produces: `DEFAULT_PREFETCH = 10` exported from `@amqp-contract/core`
- Produces: `AmqpConsumeOptions.prefetch?: number | "unbounded"` (was `number`)

**Why:** `consume()` only calls `basic.qos` when `prefetch` is set, so the default today is AMQP's unlimited — the broker pushes the entire ready backlog into one consumer. Unbounded memory, and a crash redelivers all of it at once.

`0` already means unlimited in AMQP, but `prefetch: 0` reads like "none" — the opposite of what it does. `"unbounded"` is the explicit opt-out, and core maps it to `0` at the boundary.

- [ ] **Step 1: Write the failing test**

Create `packages/core/src/amqp-client.spec.ts`:

```ts
import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AmqpClient, DEFAULT_PREFETCH } from "./amqp-client.js";
import { ConnectionManagerSingleton } from "./connection-manager.js";

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
  consume: ReturnType<typeof vi.fn>;
};
const fakes = vi.hoisted(() => ({ wrapper: undefined as unknown }));
const wrapper = (): FakeWrapper => fakes.wrapper as FakeWrapper;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
  w.consume = vi.fn(() => Promise.resolve({ consumerTag: "tag-1" }));
  fakes.wrapper = w;
  return {
    default: {
      connect: vi.fn(() => ({
        createChannel: vi.fn(() => w),
        close: vi.fn(() => Promise.resolve()),
      })),
    },
  };
});

const contract = {} as ContractDefinition;

describe("AmqpClient.consume prefetch default", () => {
  beforeEach(async () => {
    wrapper().consume.mockClear();
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  it(`defaults an unset prefetch to ${String(DEFAULT_PREFETCH)} rather than AMQP's unlimited`, async () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    await client.consume("orders", () => {});

    expect(wrapper().consume).toHaveBeenCalledWith(
      "orders",
      expect.any(Function),
      expect.objectContaining({ prefetch: DEFAULT_PREFETCH }),
    );

    void client.close();
  });

  it("honors an explicit numeric prefetch", async () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    await client.consume("orders", () => {}, { prefetch: 42 });

    expect(wrapper().consume).toHaveBeenCalledWith(
      "orders",
      expect.any(Function),
      expect.objectContaining({ prefetch: 42 }),
    );

    void client.close();
  });

  it('maps the "unbounded" opt-out to AMQP 0 (unlimited)', async () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    await client.consume("orders", () => {}, { prefetch: "unbounded" });

    expect(wrapper().consume).toHaveBeenCalledWith(
      "orders",
      expect.any(Function),
      expect.objectContaining({ prefetch: 0 }),
    );

    void client.close();
  });

  it("still rejects an out-of-range prefetch as a defect", async () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    const result = await client.consume("orders", () => {}, { prefetch: 70_000 });

    expect(result).toBeDefect();

    void client.close();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/core && pnpm vitest run --project unit src/amqp-client.spec.ts
```

Expected: FAIL — `DEFAULT_PREFETCH` is not exported.

- [ ] **Step 3: Widen the option type and add the constant**

In `packages/core/src/amqp-client.ts`, replace the `prefetch` field of `AmqpConsumeOptions`:

```ts
export type AmqpConsumeOptions = Omit<Options.Consume, "prefetch"> & {
  /**
   * Per-consumer prefetch count, applied before `channel.consume(...)`.
   *
   * Defaults to {@link DEFAULT_PREFETCH}. Pass `"unbounded"` to opt out and
   * let the broker push the entire ready backlog — AMQP's original default,
   * and a memory hazard on any queue that can build a backlog.
   *
   * `"unbounded"` rather than `0` because AMQP's `0` means *unlimited*, which
   * reads at a call site as its opposite.
   */
  prefetch?: number | "unbounded";
};
```

Add the constant next to `DEFAULT_CONNECT_TIMEOUT_MS`:

```ts
/**
 * Default per-consumer prefetch.
 *
 * Bounds in-flight messages per consumer, which bounds both memory and the
 * redelivery burst when a worker crashes. Throughput-bound consumers raise it
 * explicitly; `"unbounded"` restores AMQP's unlimited behavior.
 */
export const DEFAULT_PREFETCH = 10;
```

- [ ] **Step 4: Resolve the default in `consume()`**

In `consume()`, replace the `const prefetch = options?.prefetch;` line and the block that follows, keeping the existing range validation:

```ts
    // Resolve before validating so the range check sees the real value that
    // will reach the broker. `"unbounded"` maps to AMQP's 0 (unlimited).
    const requested = options?.prefetch;
    const prefetch = requested === undefined ? DEFAULT_PREFETCH : requested === "unbounded" ? 0 : requested;

    if (!Number.isInteger(prefetch) || prefetch < 0 || prefetch > 65_535) {
      // A misconfigured prefetch is a programming fault, not a modeled
      // failure — surface it through the defect channel.
      return fromSafeThrowable((): string => {
        // oxlint-disable-next-line unthrown/no-throw -- deliberate defect-channel routing inside the fromSafeThrowable thunk
        throw new TechnicalError(
          `Invalid prefetch: expected a non-negative integer ≤ 65535 or "unbounded", got ${String(requested)}`,
        );
      })().toAsync();
    }

    return fromPromise(
      this.channelWrapper.consume(queue, callback, { ...options, prefetch }),
      (error: unknown, defect) =>
```

Leave the rest of the `fromPromise` call unchanged.

- [ ] **Step 5: Export the constant**

In `packages/core/src/index.ts`, add `DEFAULT_PREFETCH` to the existing export list from `./amqp-client.js`.

- [ ] **Step 6: Run the tests to verify they pass**

```bash
cd packages/core && pnpm vitest run --project unit src/amqp-client.spec.ts
```

Expected: PASS, 4 tests.

- [ ] **Step 7: Verify the whole repo still builds and passes**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm build && pnpm typecheck && pnpm test && pnpm lint
```

The worker's `ConsumerOptions` is `Pick<AmqpConsumeOptions, "prefetch" | ...>`, so the widened type flows through automatically. If a call site breaks on `number | "unbounded"`, fix the call site — do not narrow the type back.

- [ ] **Step 8: Document it**

Add to `docs/how-to/troubleshoot.md`, matching the surrounding voice and heading style:

````markdown
### My worker suddenly processes fewer messages at once

Consumers now prefetch **10** messages by default (previously unlimited — the
broker pushed the entire ready backlog to a single consumer, which is unbounded
memory and a large redelivery burst if the worker crashes).

Raise it if you are throughput-bound and your handlers are cheap:

```typescript
const worker = await TypedAmqpWorker.create({
  contract,
  urls: ["amqp://localhost"],
  handlers,
  prefetch: 100,
}).get();
```
````

Or restore the old behavior explicitly:

```typescript
prefetch: "unbounded";
```

`"unbounded"` rather than `0` — AMQP's `0` means _unlimited_, which reads at a
call site as its opposite.

````

- [ ] **Step 9: Add a changeset**

```bash
pnpm changeset
````

Choose **major**. Summary:

```
Consumers now prefetch 10 messages by default instead of AMQP's unlimited,
bounding in-flight memory and the redelivery burst on a worker crash. Set
`prefetch` to a number to tune it, or `"unbounded"` to restore the previous
behavior.
```

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(core)!: default consumer prefetch to 10

Unset prefetch meant AMQP's unlimited: the broker pushed the entire ready
backlog into one consumer, unbounded in memory and redelivering all of it
on a crash. \"unbounded\" is the explicit opt-out — AMQP's 0 means unlimited,
which reads as its opposite at a call site."
```

---

### Task 2: H4 — default publish timeout

**Files:**

- Modify: `packages/core/src/amqp-client.ts` (constant, and the `createChannel` options ~line 249)
- Modify: `packages/core/src/index.ts`
- Test: `packages/core/src/publish-timeout-default.spec.ts` (create)
- Modify: `docs/how-to/troubleshoot.md`

**Interfaces:**

- Consumes: nothing from Task 1
- Produces: `DEFAULT_PUBLISH_TIMEOUT_MS = 30_000` exported from `@amqp-contract/core`
- Produces: `AmqpClientOptions.publishTimeoutMs?: number | null | undefined`

**Correction to this task, decided before execution.** An earlier draft put the
disable opt-out on the passthrough `channelOptions` bag as
`{ publishTimeout: null }`. That does not typecheck — amqp-connection-manager
declares `publishTimeout?: number` (`ChannelWrapper.d.ts:28`). The opt-out
therefore lives on a **library-owned** option instead:
`AmqpClientOptions.publishTimeoutMs?: number | null`, resolved in the
`AmqpClient` constructor. This is type-safe, keeps the single decision point,
and mirrors `connectTimeoutMs`'s existing `null`-means-disabled convention in
that same file. Steps below reflect this; ignore any `channelOptions` phrasing
elsewhere in the plan.

**Why:** `publishTimeoutMs` exists on both `TypedAmqpClient.create` and `TypedAmqpWorker.create`, but defaults to undefined — so during a broker outage publishes buffer without bound and their promises never settle. Defaulting it in core's channel creation covers client, worker, and direct core users from one place.

`null` disables the timeout, matching `connectTimeoutMs`'s existing convention in this file.

- [ ] **Step 1: Write the failing test**

Create `packages/core/src/publish-timeout-default.spec.ts`:

```ts
import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AmqpClient, DEFAULT_PUBLISH_TIMEOUT_MS } from "./amqp-client.js";
import { ConnectionManagerSingleton } from "./connection-manager.js";

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
};
const fakes = vi.hoisted(() => ({ createChannel: undefined as unknown }));
const createChannel = (): ReturnType<typeof vi.fn> =>
  fakes.createChannel as ReturnType<typeof vi.fn>;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
  const cc = vi.fn(() => w);
  fakes.createChannel = cc;
  return {
    default: {
      connect: vi.fn(() => ({ createChannel: cc, close: vi.fn(() => Promise.resolve()) })),
    },
  };
});

const contract = {} as ContractDefinition;

describe("publishTimeout default", () => {
  beforeEach(async () => {
    createChannel().mockClear();
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  it(`defaults publishTimeout to ${String(DEFAULT_PUBLISH_TIMEOUT_MS)}ms so a buffered publish always settles`, () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    expect(createChannel()).toHaveBeenCalledWith(
      expect.objectContaining({ publishTimeout: DEFAULT_PUBLISH_TIMEOUT_MS }),
    );

    void client.close();
  });

  it("honors an explicit publishTimeoutMs", () => {
    const client = new AmqpClient(contract, {
      urls: ["amqp://localhost"],
      publishTimeoutMs: 5_000,
    });

    expect(createChannel()).toHaveBeenCalledWith(
      expect.objectContaining({ publishTimeout: 5_000 }),
    );

    void client.close();
  });

  it("omits publishTimeout entirely when explicitly disabled with null", () => {
    const client = new AmqpClient(contract, {
      urls: ["amqp://localhost"],
      publishTimeoutMs: null,
    });

    const opts = createChannel().mock.calls[0]?.[0] as Record<string, unknown>;
    expect(opts).not.toHaveProperty("publishTimeout");

    void client.close();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/core && pnpm vitest run --project unit src/publish-timeout-default.spec.ts
```

Expected: FAIL — `DEFAULT_PUBLISH_TIMEOUT_MS` is not exported.

- [ ] **Step 3: Add the constant**

In `packages/core/src/amqp-client.ts`, next to `DEFAULT_PREFETCH`:

```ts
/**
 * Default `publishTimeout` for the channel, in milliseconds.
 *
 * Without a bound, publishes issued while the broker is unreachable buffer
 * indefinitely and their promises never settle — a caller awaiting one waits
 * forever. 30s is long enough that a brief reconnect does not fail healthy
 * publishes, short enough that a real outage surfaces as an error.
 *
 * Pass `channelOptions: { publishTimeout: null }` to disable, matching the
 * `connectTimeoutMs` convention.
 */
export const DEFAULT_PUBLISH_TIMEOUT_MS = 30_000;
```

- [ ] **Step 4: Add the option and resolve it at channel creation**

Add to `AmqpClientOptions`, next to `connectTimeoutMs`:

```ts
/**
 * Maximum time in ms a publish may sit buffered waiting for the broker
 * before its promise settles with a failure. Defaults to
 * {@link DEFAULT_PUBLISH_TIMEOUT_MS}. Pass `null` to disable, restoring
 * unbounded buffering — a publish issued during an outage then never
 * settles. Same convention as `connectTimeoutMs`.
 */
publishTimeoutMs?: number | null | undefined;
```

In the constructor, immediately before `this.channelWrapper = this.connection.createChannel(channelOpts);`, insert:

```ts
// Resolved here, at the single point where every caller's channel is
// created, so client, worker, and direct core users all inherit it.
// `null` is the explicit "no timeout" opt-out (see connectTimeoutMs).
if (options.publishTimeoutMs !== null) {
  channelOpts.publishTimeout = options.publishTimeoutMs ?? DEFAULT_PUBLISH_TIMEOUT_MS;
}
```

`channelOpts.publishTimeout` is typed `number | undefined` upstream, so `null`
never reaches it — that branch simply leaves the field unset.

`TypedAmqpClient` and `TypedAmqpWorker` already accept their own
`publishTimeoutMs` and forward it as `channelOptions.publishTimeout`
(`packages/client/src/client.ts:227`, `packages/worker/src/worker.ts:525`).
Change both to pass it through as `publishTimeoutMs` instead, so the default
resolves in exactly one place, and widen both of their option types to
`number | null | undefined`.

- [ ] **Step 5: Export the constant**

Add `DEFAULT_PUBLISH_TIMEOUT_MS` to the `./amqp-client.js` export list in `packages/core/src/index.ts`.

- [ ] **Step 6: Run the tests to verify they pass**

```bash
cd packages/core && pnpm vitest run --project unit src/publish-timeout-default.spec.ts
```

Expected: PASS, 3 tests.

- [ ] **Step 7: Verify the repo**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm build && pnpm typecheck && pnpm test && pnpm lint
```

Two existing specs pin `publishTimeoutMs` threading — `packages/client/src/publish-timeout.spec.ts` and `packages/worker/src/publish-timeout.spec.ts`. They assert an explicitly-passed value, which still holds. If either asserts the _absence_ of `publishTimeout` when unset, update that assertion to the new default and say so in your report.

- [ ] **Step 8: Document it**

Add to `docs/how-to/troubleshoot.md`:

````markdown
### A publish hangs forever during a broker outage

It no longer does. Channels now set a **30s** `publishTimeout` by default, so a
publish issued while the broker is unreachable settles with a failure instead of
buffering indefinitely with a promise that never resolves.

Tune it per client or worker:

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  publishTimeoutMs: 10_000,
}).get();
```
````

Or disable it entirely, restoring the previous unbounded buffering:

```typescript
channelOptions: {
  publishTimeout: null;
}
```

````

- [ ] **Step 9: Add a changeset**

```bash
pnpm changeset
````

Choose **major**. Summary:

```
Channels now set a 30s `publishTimeout` by default. Publishes issued during a
broker outage previously buffered without bound and their promises never
settled. Set `publishTimeoutMs` to tune it, or `channelOptions.publishTimeout:
null` to disable.
```

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(core)!: default the channel publishTimeout to 30s

Publishes issued while the broker is unreachable buffered without bound and
their promises never settled. Resolved at channel creation so client, worker,
and direct core users inherit it; null disables, matching connectTimeoutMs."
```

---

### Task 3: H2 — reject silent poison loss on consumed queues

**Files:**

- Modify: `packages/contract/src/types.ts` (queue definition + options)
- Modify: `packages/contract/src/builder/queue.ts` (allow-list ~line 121, carry the option)
- Create: `packages/contract/src/builder/poison-loss.ts`
- Test: `packages/contract/src/builder/poison-loss.spec.ts`
- Modify: `packages/contract/src/builder/contract.ts` (call the check)
- Modify: `docs/how-to/troubleshoot.md`

**Interfaces:**

- Produces: `QueueDefinition.onPoison?: "drop" | undefined`
- Produces: `_internal_assertNoSilentPoisonLoss(queue: QueueDefinition, consumedBy: string): void`

**Why:** `packages/worker/src/retry.ts:360` logs `"Queue does not have DLX configured - message will be lost on nack"` — at the instant the message is already gone, and only if a logger happens to be wired. A queue whose poison messages vanish should not be constructible without the author saying so.

**Read the design decision at the top of this plan before starting.** The check runs in `defineContract` over **consumed** queues only (those reachable from `consumers` or `rpcs`), not in `defineQueue`. A dead-letter queue has no DLX of its own and is usually not consumed; rejecting it would be a false positive.

- [ ] **Step 1: Write the failing test**

Create `packages/contract/src/builder/poison-loss.spec.ts`:

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
const dlx = defineExchange("orders-dlx", { type: "topic" });

/** A routable publisher, so the H1 check never fires in these tests. */
function contractWith(queue: ReturnType<typeof defineQueue>) {
  return {
    publishers: { orderCreated: definePublisher(orders, message, { routingKey: "order.created" }) },
    consumers: { processOrder: defineConsumer(queue, message) },
    bindings: { processOrder: defineQueueBinding(queue, orders, { routingKey: "order.created" }) },
  };
}

describe("silent poison-loss guard", () => {
  it("throws for a consumed queue with neither a DLX nor an explicit onPoison", () => {
    const queue = defineQueue("order-processing");

    expect(() => defineContract(contractWith(queue))).toThrow(/order-processing/);
    expect(() => defineContract(contractWith(queue))).toThrow(/onPoison/);
  });

  it("names the consumer that makes the queue poisonable", () => {
    const queue = defineQueue("order-processing");

    expect(() => defineContract(contractWith(queue))).toThrow(/processOrder/);
  });

  it("accepts a consumed queue with a dead-letter exchange", () => {
    const queue = defineQueue("order-processing", { deadLetter: { exchange: dlx } });

    expect(() => defineContract(contractWith(queue))).not.toThrow();
  });

  it('accepts a consumed queue that explicitly opts in to dropping with onPoison: "drop"', () => {
    const queue = defineQueue("order-processing", { onPoison: "drop" });

    expect(() => defineContract(contractWith(queue))).not.toThrow();
  });

  it("does NOT require a DLX on a declared-but-unconsumed queue (the dead-letter queue case)", () => {
    // A DLQ has no DLX of its own — that would be infinite regress — and is
    // typically inspected rather than consumed. It must not trip the check.
    const dlq = defineQueue("orders-dlq");
    const processing = defineQueue("order-processing", { deadLetter: { exchange: dlx } });

    expect(() =>
      defineContract({
        ...contractWith(processing),
        queues: { dlq },
        bindings: {
          processOrder: defineQueueBinding(processing, orders, { routingKey: "order.created" }),
          dlq: defineQueueBinding(dlq, dlx, { routingKey: "#" }),
        },
      }),
    ).not.toThrow();
  });

  it("DOES require it once that same dead-letter queue is consumed", () => {
    // A DLQ processor can itself poison-loop, so the check applies again.
    const dlq = defineQueue("orders-dlq");
    const processing = defineQueue("order-processing", { deadLetter: { exchange: dlx } });

    expect(() =>
      defineContract({
        ...contractWith(processing),
        consumers: {
          processOrder: defineConsumer(processing, message),
          inspectDlq: defineConsumer(dlq, message),
        },
        bindings: {
          processOrder: defineQueueBinding(processing, orders, { routingKey: "order.created" }),
          dlq: defineQueueBinding(dlq, dlx, { routingKey: "#" }),
        },
      }),
    ).toThrow(/orders-dlq/);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd packages/contract && pnpm vitest run src/builder/poison-loss.spec.ts
```

Expected: FAIL — the first test does not throw.

- [ ] **Step 3: Add `onPoison` to the queue type**

In `packages/contract/src/types.ts`, add to the queue definition object alongside `deadLetter`:

```ts
  /**
   * Declares that poison messages on this queue are deliberately dropped.
   *
   * A consumed queue with no dead-letter exchange loses every message it
   * rejects — `nack(requeue: false)` discards it and nothing observes the
   * loss. `defineContract` therefore refuses such a queue unless the author
   * writes this, which makes the loss a stated decision rather than an
   * accident.
   *
   * Only meaningful on a queue that is consumed; a declared-but-unconsumed
   * queue is never nacked and is not checked.
   */
  onPoison?: "drop" | undefined;
```

Add the same field to the queue _options_ type used by `defineQueue`.

- [ ] **Step 4: Carry it through `defineQueue`**

In `packages/contract/src/builder/queue.ts`, add `"onPoison"` to the `_internal_assertKnownKeys` allow-list at line 121 (after `"deadLetter"`), and include it on the returned object next to the existing `deadLetter` spread:

```ts
    ...(opts.onPoison !== undefined && { onPoison: opts.onPoison }),
```

- [ ] **Step 5: Write the check**

Create `packages/contract/src/builder/poison-loss.ts`:

```ts
import type { QueueDefinition } from "../types.js";

/**
 * Reject a consumed queue that would silently drop its poison messages.
 *
 * A queue with no dead-letter exchange loses every message its consumer
 * rejects: `nack(requeue: false)` discards it, and nothing observes the loss.
 * The worker logs a warning at the moment it happens — too late, and only if a
 * logger is wired — so the decision moves to define time.
 *
 * Scoped to consumed queues deliberately. A dead-letter queue has no DLX of its
 * own (that would be infinite regress) and is usually inspected rather than
 * consumed; requiring one there would reject a correct contract.
 *
 * @internal
 */
export function _internal_assertNoSilentPoisonLoss(
  queue: QueueDefinition,
  consumedBy: string,
): void {
  if (queue.deadLetter !== undefined) return;
  if (queue.onPoison === "drop") return;

  // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error (see module doc)
  throw new Error(
    `Queue "${queue.name}" is consumed by "${consumedBy}" but has no dead-letter exchange, ` +
      `so every message its handler rejects is discarded with no record. Add ` +
      `\`deadLetter: { exchange: … }\` to keep failed messages for inspection, or set ` +
      `\`onPoison: "drop"\` on the queue if losing them is deliberate.`,
  );
}
```

`defineContract` re-keys queues by their broker name, so the contract key and
`queue.name` are always the same string — the message names it once.

- [ ] **Step 6: Call it from `defineContract`**

In `packages/contract/src/builder/contract.ts`, immediately after the publisher-routability loop added by the H1 work (just before `return result as ContractOutput<TContract>;`):

```ts
// Only consumed queues can poison-loop: a declared-but-unconsumed queue is
// never nacked by this contract's workers. Runs after the consumer and rpc
// sections have populated `result`.
for (const [consumerName, consumer] of Object.entries(result.consumers ?? {})) {
  _internal_assertNoSilentPoisonLoss(consumer.queue, consumerName);
}
for (const [rpcName, rpc] of Object.entries(result.rpcs ?? {})) {
  _internal_assertNoSilentPoisonLoss(rpc.queue, rpcName);
}
```

Import it:

```ts
import { _internal_assertNoSilentPoisonLoss } from "./poison-loss.js";
```

- [ ] **Step 7: Run the new tests to verify they pass**

```bash
cd packages/contract && pnpm vitest run src/builder/poison-loss.spec.ts
```

Expected: PASS, 6 tests.

- [ ] **Step 8: Sweep the repo and drive everything green**

This throws for every existing consumed queue without a DLX — expect broad fixture breakage across packages, docs examples, and the example apps.

**Run every command that executes a contract, not just the unit suite.** The H1 work established that a sweep driven by one test command misses whole classes of call site:

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm build && pnpm typecheck && pnpm test && pnpm lint
pnpm --filter @amqp-contract/core test:integration
pnpm --filter @amqp-contract/client test:integration
pnpm --filter @amqp-contract/worker test:integration
pnpm --filter @amqp-contract/tests test:integration
```

Decision rule per fixture, and record each in your report as a table (file, fix applied, one-line reason):

- **Add a `deadLetter` exchange** when the fixture models a queue whose failures should be inspectable — the default choice, and the one that matches what a production contract should look like.
- **Set `onPoison: "drop"`** only where the test is deliberately about drop behavior, or where adding a DLX would change what the test asserts.

Do not weaken or skip the check.

- [ ] **Step 9: Document it**

Add to `docs/how-to/troubleshoot.md`:

````markdown
### `defineContract` says my queue has no dead-letter exchange

A consumed queue with no dead-letter exchange discards every message its handler
rejects — `nack(requeue: false)` drops it and nothing records that it existed.
The worker used to warn as it happened, which is both too late and invisible
unless a logger was wired.

Keep failed messages for inspection:

```typescript
const ordersDlx = defineExchange("orders-dlx");

const orderQueue = defineQueue("order-processing", {
  deadLetter: { exchange: ordersDlx },
});
```
````

Or state that dropping them is intentional:

```typescript
const metricsQueue = defineQueue("metrics-ingest", { onPoison: "drop" });
```

Only _consumed_ queues are checked. A dead-letter queue you declare but do not
consume needs neither — it has no dead-letter exchange of its own by design.

````

- [ ] **Step 10: Add a changeset**

```bash
pnpm changeset
````

Choose **major**. Summary:

```
`defineContract` now throws when a consumed queue has no dead-letter exchange,
because such a queue silently discards every message its handler rejects. Add
`deadLetter: { exchange: … }`, or `onPoison: "drop"` if the loss is deliberate.
Declared-but-unconsumed queues (including dead-letter queues themselves) are not
checked.
```

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "feat(contract)!: reject consumed queues that silently drop poison messages

A consumed queue with no DLX discards every rejected message with no record;
the worker's runtime warning fired too late and only with a logger wired.
Checked in defineContract, where consumers are known, so declared-but-unconsumed
queues (dead-letter queues included) are correctly exempt."
```

---

### Task 4: Real-broker proofs and migration guide

**Files:**

- Create: `tests/src/safe-defaults.spec.ts`
- Modify: `AGENTS.md`
- Modify: `docs/how-to/upgrade.md`

**Interfaces:**

- Consumes: `DEFAULT_PREFETCH` from Task 1; `onPoison` from Task 3

**Why:** H1 established the pattern that each hazard gets a paired proof — one test showing the loss is genuine, one showing the guard catches it. That pair is what makes the hazard catalog evidence rather than assertion.

H4 is deliberately excluded: proving it needs a broker outage mid-publish, which is timing-dependent and belongs with the deferred Toxiproxy work. Its unit tests in Task 2 pin the threading, which is the part that can regress.

- [ ] **Step 1: Write the H3 proof**

Create `tests/src/safe-defaults.spec.ts`:

```ts
import { TypedAmqpClient } from "@amqp-contract/client";
import {
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  definePublisher,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { DEFAULT_PREFETCH } from "@amqp-contract/core";
import { it } from "@amqp-contract/testing/extension";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { fromPromise } from "unthrown";
import { describe, expect } from "vitest";
import { z } from "zod";

/**
 * H3, proven against a real broker.
 *
 * Test 1 shows the hazard is genuine: with the opt-out, one consumer takes the
 * entire backlog unacked. Test 2 shows the default bounds it.
 */
describe("default prefetch", () => {
  /**
   * Drives a real TypedAmqpWorker so this proves OUR default is applied, not
   * merely that RabbitMQ's basic.qos works. Asserting through `amqpChannel`
   * directly would pass even if the default were never wired.
   *
   * The handler blocks until released, so every delivered message stays
   * unacked and the in-flight count is exactly what prefetch allows.
   */
  const message = defineMessage(z.object({ i: z.number() }));

  async function inFlightUnderPrefetch(
    amqpConnectionUrl: string,
    prefetch: number | "unbounded",
  ): Promise<number> {
    const exchange = defineExchange(`prefetch-x-${prefetch}`, { type: "topic", durable: false });
    const dlx = defineExchange(`prefetch-dlx-${prefetch}`, { type: "topic", durable: false });
    const queue = defineQueue(`prefetch-q-${prefetch}`, {
      type: "classic",
      durable: false,
      deadLetter: { exchange: dlx },
    });
    const contract = defineContract({
      publishers: { emit: definePublisher(exchange, message, { routingKey: "p.one" }) },
      consumers: { onOne: defineConsumer(queue, message) },
      bindings: { onOne: defineQueueBinding(queue, exchange, { routingKey: "p.one" }) },
    });

    let inFlight = 0;
    let peak = 0;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });

    const client = await TypedAmqpClient.create({ contract, urls: [amqpConnectionUrl] }).get();
    const worker = await TypedAmqpWorker.create({
      contract,
      urls: [amqpConnectionUrl],
      prefetch,
      handlers: {
        onOne: () =>
          fromPromise(
            (async () => {
              inFlight += 1;
              peak = Math.max(peak, inFlight);
              await gate;
              inFlight -= 1;
            })(),
            (cause, defect) => defect(cause),
          ),
      },
    }).get();

    try {
      for (let i = 0; i < 30; i += 1) {
        await client.publish("emit", { i });
      }
      // Let the broker push as many as prefetch permits, then read the peak.
      await new Promise((resolve) => setTimeout(resolve, 750));
      return peak;
    } finally {
      release();
      await worker.close().get();
      await client.close().get();
    }
  }

  it("INVARIANT: an unbounded consumer takes far more than the default in flight", async ({
    amqpConnectionUrl,
  }) => {
    const peak = await inFlightUnderPrefetch(amqpConnectionUrl, "unbounded");
    expect(peak).toBeGreaterThan(DEFAULT_PREFETCH);
  }, 20_000);

  it("INVARIANT: the default bounds in-flight handlers to DEFAULT_PREFETCH", async ({
    amqpConnectionUrl,
  }) => {
    // No explicit prefetch anywhere: this is the library's default doing it.
    const peak = await inFlightUnderPrefetch(amqpConnectionUrl, DEFAULT_PREFETCH);
    expect(peak).toBe(DEFAULT_PREFETCH);
  }, 20_000);
});

describe("poison-loss guard", () => {
  const message = defineMessage(z.object({ orderId: z.string() }));

  it("INVARIANT: a consumed DLX-less queue is rejected at define time", () => {
    const orders = defineExchange("orders-poison", { type: "topic" });
    const queue = defineQueue("order-processing-poison");

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
    ).toThrow(/dead-letter exchange/);
  });
});
```

**Note on the H3 tests:** they drive a real `TypedAmqpWorker` deliberately — asserting through `amqpChannel.prefetch(...)` directly would only prove RabbitMQ's `basic.qos` works, and would pass even if our default were never applied. The handler blocks on a gate so delivered messages stay unacked and the peak in-flight count is exactly what prefetch permitted. If the 750ms settle proves flaky in CI, replace it with a polling wait matching the style of the existing specs in `tests/src/` — do not simply lengthen the sleep.

- [ ] **Step 2: Run the integration suite**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm --filter @amqp-contract/tests test:integration
```

Expected: PASS. Requires Docker. If Docker is unavailable, say so explicitly — do not claim integration coverage that did not run.

- [ ] **Step 3: Record the invariants**

Append to the "Load-bearing invariants" list in `AGENTS.md`:

```markdown
20. **A consumer's unacked deliveries are bounded by the default prefetch** (unset prefetch used to mean AMQP unlimited — the whole ready backlog in one consumer's memory, all of it redelivered on a crash) — `tests/src/safe-defaults.spec.ts` ("default prefetch" describe: the paired unbounded-vs-default tests).
21. **A consumed queue that would silently discard its poison messages is rejected at define time** (no DLX means `nack(requeue: false)` drops the message with no record; declared-but-unconsumed queues, including dead-letter queues, are correctly exempt) — `packages/contract/src/builder/poison-loss.spec.ts` + `tests/src/safe-defaults.spec.ts`.
```

- [ ] **Step 4: Write the upgrade-guide entries**

Add a section to `docs/how-to/upgrade.md` covering all three, in the voice of the existing 2.4→3.0 entries. Each entry states what breaks, why it was unsafe, and the exact edit:

- **Consumed queues need a dead-letter exchange.** `defineContract` throws otherwise. Add `deadLetter: { exchange: … }`, or `onPoison: "drop"` if dropping is deliberate. Unconsumed queues are exempt.
- **Consumers prefetch 10 by default.** This is a _behavior_ change, not a compile error — nothing fails to build, throughput characteristics change. Anyone relying on unlimited prefetch must set `prefetch: "unbounded"` explicitly. **Call this out prominently: it is the one item a client can miss entirely.**
- **Channels set a 30s publish timeout.** Publishes during an outage now fail instead of hanging. Set `publishTimeoutMs`, or `channelOptions.publishTimeout: null` to disable.

- [ ] **Step 5: Final verification**

```bash
cd /Users/btravers/Projects/btravstack/amqp-contract
pnpm build && pnpm typecheck && pnpm test && pnpm lint
npx oxfmt --check .
pnpm --filter @amqp-contract/core test:integration
pnpm --filter @amqp-contract/client test:integration
pnpm --filter @amqp-contract/worker test:integration
pnpm --filter @amqp-contract/tests test:integration
```

All green.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test: prove the prefetch bound and the poison-loss guard against a real broker

Adds invariants 20 and 21 and the 3.0 upgrade entries for all three safe
defaults. The prefetch change is a behavior change rather than a compile
error, so the upgrade guide leads with it."
```

---

## Out of scope

Tracked in `docs/superpowers/specs/2026-08-01-robustness-hardening-design.md`:

- **Rung 3** (runtime unroutable detection). Breaking — widens `publish()`'s error channel — so it must land before 3.0 stable or wait for 4.0. The spike is done and recommends implementing; it needs its own plan.
- **H5** (idempotency/deduplication) — documentation only, deferred.
- **`MatchingBindingPattern` template-literal hole** — a live, pre-existing false compile error on valid bindings via `defineEventConsumer`. Non-breaking to fix, independent of this work.
- **Mock removal** for the 9 broker-mocking specs, via management-API fault injection.
- **Coverage-floor ratchet** — `core` still has a 10% branch floor against ~40% actual.
- **De-flaking `packages/client/src/client-cleanup.spec.ts`**, which fails intermittently under full-monorepo parallel load.
