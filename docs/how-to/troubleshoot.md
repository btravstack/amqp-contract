---
title: Troubleshoot - amqp-contract
description: Diagnose connection failures, type errors, validation failures, messages that never arrive, and topology conflicts.
---

# Troubleshoot

Organised by what you observe. If your symptom is "it works but it is slow", go to [tune performance](/how-to/tune-performance) instead.

## Connection problems

### `ECONNREFUSED`

Nothing is listening. Check the broker is running and the port is mapped:

```bash
docker ps | grep rabbitmq
docker start rabbitmq   # if it exists but is stopped
```

From inside a container, `localhost` is that container. Use the service name on a Docker network (`amqp://rabbitmq:5672`), or `host.docker.internal` on Docker Desktop.

### `ACCESS_REFUSED`

Credentials or virtual host. `guest` only works from localhost — RabbitMQ refuses it over a network connection, which is why a setup that works locally fails as soon as it is containerised. Create a real user:

```bash
docker exec rabbitmq rabbitmqctl add_user app secret
docker exec rabbitmq rabbitmqctl set_permissions -p / app ".*" ".*" ".*"
```

A vhost in the URL must exist and be URL-encoded: `amqp://user:pass@host:5672/my-vhost`, and `/` as a vhost is `%2F`.

### `create()` throws with a `TechnicalError`

Expected — connection failures are defects, and `.get()` rethrows the cause. To log rather than crash:

```typescript
const client = await TypedAmqpClient.create({ contract, urls })
  .tapDefect((cause) => logger.error({ cause }, "connect failed"))
  .get();
```

To wait indefinitely instead of timing out after 30s, pass `connectTimeoutMs: null`. That is the only way to disable it — an invalid value (`NaN`, zero, negative, `Infinity`) is itself a defect from `create()` rather than silently turning the timeout off.

### Connections drop repeatedly

Usually missed heartbeats caused by a blocked event loop, not a network fault. See [tune performance](/how-to/tune-performance#heartbeats).

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

Or disable it entirely, restoring the previous unbounded buffering:

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  publishTimeoutMs: null,
}).get();
```

## Type errors

### `Property 'X' does not exist on type …` in a handler

The payload type comes from the contract, so this means the contract disagrees with your expectation. Check the message schema, and check whether the consumer is bound to the publisher you think it is.

For a wildcard consumer receiving several message types, the payload is a union — narrow it before accessing anything type-specific:

```typescript
notifyOrder: ({ input: { payload } }) => {
  if ("items" in payload) {
    console.log(payload.items); // the full order
  } else {
    console.log(payload.status); // a status update
  }
  return OkAsync(undefined);
},
```

### `This expression is not callable` on a `declareHandler` result

`declareHandler` returns a handler _entry_ — either the function or a `[function, options]` tuple — so it cannot be invoked directly. For a callable standalone handler, type it with `WorkerInferConsumerHandler` instead. See [consume messages](/how-to/consume-messages#move-handlers-into-their-own-modules).

### `'this' context of type … is not assignable`

This is `.get()`'s type gate: the result still has a non-empty modeled error channel, so you must handle the error first. Either address it with `.match` / `.recoverErrCases`, or use `.getOrThrow()` if throwing is acceptable here.

### `Expected 2 arguments, but got 1` on `fromPromise`

The `qualify` mapper is required — it is how a rejection becomes a classified error. Pass one, or use a prebuilt factory:

```typescript
fromPromise(work(), qualifyRetryable("work failed"));
```

### Missing handler, or `Object literal may only specify known properties`

Every consumer and RPC in the contract needs exactly one handler, and nothing else may appear in the object. A stray key usually means a rename in the contract that the handlers object did not follow.

### `Cannot find module './contract'`

ESM requires the extension: `./contract.js`, even in TypeScript source. And `tsconfig.json` needs `"module": "NodeNext"` with `"moduleResolution": "NodeNext"`.

## Validation failures

### `MessageValidationError` on publish

The payload does not satisfy the publisher's schema. `error.issues` carries the per-field detail:

```typescript
errCases: (matcher) =>
  matcher.with(P.tag("@amqp-contract/MessageValidationError"), (error) =>
    console.error(JSON.stringify(error.issues, null, 2)),
  ),
```

A common surprise is a schema that is stricter than the type — `z.string().email()` and `z.number().positive()` both accept any `string` / `number` at compile time.

### Messages dead-letter immediately with a validation error in the logs

The consumer rejected them. Validation failures bypass retries entirely, so they arrive in the dead-letter queue on the first attempt.

Nearly always a contract skew: the publisher is running a newer contract than the consumer, or a producer outside your control is sending something else. Compare the two sides' contract versions first.

Note the dead-lettered message carries no `x-last-error` — the reason is only in the worker's log. See [retry failed messages](/how-to/retry-failed-messages#inspect-retry-state).

### Headers fail validation but the publish succeeded

Expected: headers are validated on the **consumer** side only. A publish cannot catch a headers mismatch. See [publish messages](/how-to/publish-messages#send-headers).

## Messages do not arrive

Work down this list in order.

**Is the consumer running?** A queue with no consumer accumulates messages. `rabbitmqctl list_queues name messages consumers`.

**Is the queue bound?** In the management UI, open the queue and check its bindings. No binding means the exchange has nowhere to route to, and RabbitMQ drops unroutable messages silently. If the binding is missing from the contract itself, `defineContract` says so — see below.

**Do the routing keys match?** `order.*` matches `order.created` but _not_ `order.created.urgent` — `*` is exactly one segment, `#` is zero or more. This is the single most common cause.

**Is the exchange type what you think?** A `fanout` exchange ignores routing keys entirely; a `direct` exchange requires an exact match and does not do wildcards.

**Are two consumers sharing a queue?** Then they compete, and each message goes to one of them. For broadcast, give each consumer its own queue.

**Did you publish before the consumer existed?** In tests especially — `initConsumer` must be awaited before publishing, or the message is routed nowhere. See [test with RabbitMQ](/how-to/test-with-rabbitmq#assert-on-raw-messages).

### `Publisher is unroutable` at define time

`defineContract` throws `Publisher "orderCreated" is unroutable` when a publisher's routing key reaches no queue anywhere in the contract's binding graph — directly, or through exchange-to-exchange forwards. The error names the key, the exchange it stopped on, and the patterns actually declared there.

This is a define-time error precisely because it cannot be caught at runtime. A publisher confirm means "the broker took responsibility", not "a queue received it": RabbitMQ confirms a message that matched no binding and then discards it, so `publish()` returns `Ok` while every message is lost. Nothing downstream ever reports the loss.

Two remedies, and they are not interchangeable:

**Add a binding that matches.** Correct when this contract owns the consumer. Usually the routing key and the binding pattern have drifted apart — `order.*` does not match `order.created.urgent`. Fix whichever one is wrong.

**Set `externalConsumers: true` on the publisher.** Correct when the binding genuinely lives elsewhere: another service owns the queue, or you publish into a shared exchange whose topology you do not declare. It is an assertion that the loss is somebody else's contract to guarantee, so use it only when that is true — it disables the check for that publisher permanently.

```typescript
const orderCreated = definePublisher(orders, orderMessage, {
  routingKey: "order.created",
  externalConsumers: true,
});
```

Accepted by `definePublisher`, `defineEventPublisher` and `defineCommandPublisher` alike.

An exchange declaring an `alternate-exchange` argument never triggers this — the broker routes its unmatched messages there instead of discarding them, so no key on it is unroutable.

The check reads the bindings passed to `defineContract`. Mutating `contract.bindings` after it returns does not re-run it; declare every binding in the call.

### `defineContract` says my queue has no dead-letter exchange

A consumed queue with no dead-letter exchange discards every message its handler
rejects — `nack(requeue: false)` drops it and nothing records that it existed.
The worker used to warn as it happened, which is both too late and invisible
unless a logger was wired.

On a queue that does not exist on the broker yet, keep failed messages for
inspection. Declare the dead-letter queue and its binding as well. These are two
separate rules: the `deadLetter` pointer is required on _consumed_ queues, while
the "something must be bound to that exchange" rule applies to **every queue the
contract declares** — the broker silently drops a dead letter that matches no
binding, whoever consumes the source queue:

```typescript
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { z } from "zod";

const ordersExchange = defineExchange("orders");
const ordersDlx = defineExchange("orders-dlx");
const orderDlq = defineQueue("order-processing-dlq");

const orderQueue = defineQueue("order-processing", {
  deadLetter: { exchange: ordersDlx },
});

const orderCreated = defineEventPublisher(ordersExchange, defineMessage(z.object({})), {
  routingKey: "order.created",
});

export const contract = defineContract({
  publishers: { orderCreated },
  consumers: { processOrder: defineEventConsumer(orderCreated, orderQueue) },
  queues: { orderDlq },
  bindings: { orderDlq: defineQueueBinding(orderDlq, ordersDlx, { routingKey: "#" }) },
});
```

A DLX with nothing bound to it is not a fix: the message is as lost as it was
before, and the worker now logs `Sending message to DLQ` at `info` while it
happens.

Or state that dropping them is intentional:

```typescript
const metricsQueue = defineQueue("metrics-ingest", { onPoison: "drop" });
```

Only _consumed_ queues are checked. A dead-letter queue you declare but do not
consume needs neither — it has no dead-letter exchange of its own by design. If
you do consume your dead-letter queue, it needs `onPoison: "drop"`: a DLQ cannot
dead-letter to itself, so a message its handler also rejects has nowhere to go.

#### If the queue already exists in production

**You cannot add `deadLetter` to a live queue.** It becomes the
`x-dead-letter-exchange` argument, which is part of the queue's identity, so the
worker's redeclaration fails with
[`PRECONDITION_FAILED - inequivalent arg`](#precondition-failed-inequivalent-arg)
— a 406 at startup rather than a define-time error. Three routes out:

| Route                                                                                         | What it costs                                                                                                                                                                                                                                              |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Declare a **new** queue carrying the DLX and migrate consumers to it                          | Drain the old queue first; two queues exist during the cutover                                                                                                                                                                                             |
| Apply dead-lettering as a **broker policy** (`rabbitmqctl set_policy … dead-letter-exchange`) | Works on existing queues, since policies are not part of queue identity — but the contract cannot see the policy, so it still needs `onPoison: "drop"` to pass this check, and the worker's logs will describe a discard that is not happening (see below) |
| `onPoison: "drop"`                                                                            | You accept the loss. Honest for a metrics firehose; a lie anywhere else                                                                                                                                                                                    |

If you take the broker-policy route, expect the worker to log a discard on every
rejected message. There are **two** wordings, from two code paths — grep for the
shared tail, `queue is declared onPoison: "drop" and has no DLX`, not for either
sentence on its own:

| Log line                                                                       | Emitted when                                                                                                            |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `Discarding poison message: queue is declared onPoison: "drop" and has no DLX` | The payload failed decompression or schema validation — it never reached your handler                                   |
| `Discarding message: queue is declared onPoison: "drop" and has no DLX`        | The handler rejected the message and the retry pipeline sent it to the DLQ — the common one on a retry-configured queue |

**Nothing is lost _provided_ the policy's target exchange exists and has a queue
bound to it.** Both log lines report what the _contract_ declares, and the
contract cannot see a broker policy — so on this route neither the log line nor
`defineContract` can tell you whether the dead-letter path actually terminates
somewhere. If the policy names an exchange that does not exist, or one with no
binding, RabbitMQ drops every dead-lettered message and the `info` line is the
only trace.

The library declares no DLX topology on this route: `x-dead-letter-exchange` is
derived from `deadLetter`, which you did not set. Declare the exchange, the
dead-letter queue and the binding yourself, as
[standalone topology](/how-to/define-a-contract#declare-standalone-topology),
naming the same exchange the policy targets:

```typescript
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { z } from "zod";

const ordersExchange = defineExchange("orders");
const ordersDlx = defineExchange("orders-dlx");
const orderDlq = defineQueue("order-processing-dlq");

// The policy supplies the dead-lettering, so the queue argument stays unset.
const orderQueue = defineQueue("order-processing", { onPoison: "drop" });

const orderCreated = defineEventPublisher(ordersExchange, defineMessage(z.object({})), {
  routingKey: "order.created",
});

export const contract = defineContract({
  publishers: { orderCreated },
  consumers: { processOrder: defineEventConsumer(orderCreated, orderQueue) },
  exchanges: { ordersDlx },
  queues: { orderDlq },
  bindings: { orderDlq: defineQueueBinding(orderDlq, ordersDlx, { routingKey: "#" }) },
});
```

`exchanges` is listed explicitly here because a standalone binding does not
extract the exchange it references — only `deadLetter` does, and there is no
`deadLetter` on this route.

::: warning Match the existing DLX before you copy this

The snippet declares `orders-dlx` as a **topic** exchange (the default) and binds
with `#`, which on a topic exchange matches every routing key. Both halves have
to match what is already on the broker, and changing one without the other is
how a DLQ ends up bound to nothing:

- **Exchange type.** Declaring `orders-dlx` with a type different from the
  existing one fails with `PRECONDITION_FAILED - inequivalent arg` at startup.
  Pass `{ type: "direct" }` (or whatever the policy targets) to match.
- **Routing key, and this one is silent.** `#` is a _topic_ wildcard. On a
  **direct** exchange it is an ordinary routing key that matches the literal
  string `#` — so switching the type and leaving `#` in place declares a DLQ
  that receives nothing, dead-letters into the void, and reports no error
  anywhere. On a direct DLX, bind the actual dead-letter routing key: the
  policy's `dead-letter-routing-key` if it sets one, otherwise every routing key
  the main queue can receive, since RabbitMQ preserves the original key when the
  policy does not override it. On a **fanout** DLX the key is ignored and any
  value binds.
- **Queue type and durability.** `defineQueue("order-processing-dlq")` is quorum
  and durable by default. If that DLQ already exists as a classic or
  non-durable queue — likely on the very brownfield broker this section is
  about — the redeclaration 406s too. Pass `{ type: "classic" }` (with
  `durable: false` if that is how it exists) to match it.

Check the broker before declaring: the management UI's Exchanges and Queues tabs
show the type, durability and bindings you have to reproduce.
:::

The main queue's arguments stay unchanged, so its redeclaration is still
equivalent and the 406 does not return. With that in place, treat both log lines as
expected noise on policy-migrated queues and keep your dead-letter alerting on
the DLQ's depth rather than on these logs.

The upgrade guide covers the migration in full.

### `defineContract` says nothing is bound to my dead-letter exchange

```
Queue "order-processing" dead-letters to exchange "orders-dlx" (topic), but nothing
there can receive them: its dead-lettered messages keep their original routing key.
Nothing is bound to "orders-dlx". RabbitMQ discards a message routed to zero queues,
so these would be lost exactly as silently as if the queue had no dead-letter
exchange at all. Bind a queue to "orders-dlx" that accepts them, or set
`externalConsumers: true` on the deadLetter config if another service owns that queue.
```

A dead letter is an ordinary publish, and RabbitMQ discards a publish that
matches no binding. So a `deadLetter` pointer at an exchange with nothing bound
loses precisely the messages you added it to keep — and the worker logs
`Sending message to DLQ` at `info` while it happens, so the loss reads as a
successful hand-off. This check does not create that loss; it surfaces one that
was already running.

This is a **separate rule** from [the `deadLetter` pointer
itself](#definecontract-says-my-queue-has-no-dead-letter-exchange). The pointer
is required on _consumed_ queues; this rule applies to **every** queue the
contract declares, because a dead letter is dropped whoever consumes the source.

Two remedies, and they are not interchangeable.

**Bind a queue to the exchange.** Correct when this contract owns the DLQ. The
DLQ is not consumed, so it needs no DLX of its own; declare it as
[standalone topology](/how-to/define-a-contract#declare-standalone-topology):

```typescript
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { z } from "zod";

const orders = defineExchange("orders");
const ordersDlx = defineExchange("orders-dlx");
const orderMessage = defineMessage(z.object({ orderId: z.string() }));
const orderCreated = defineEventPublisher(orders, orderMessage, {
  routingKey: "order.created",
});

const orderQueue = defineQueue("order-processing", {
  deadLetter: { exchange: ordersDlx },
});
const orderDlq = defineQueue("order-processing-dlq");

export const contract = defineContract({
  publishers: { orderCreated },
  consumers: { processOrder: defineEventConsumer(orderCreated, orderQueue) },
  queues: { orderDlq },
  bindings: { orderDlq: defineQueueBinding(orderDlq, ordersDlx, { routingKey: "#" }) },
});
```

**Set `externalConsumers: true` on the `deadLetter` config.** Correct when the
dead-letter queue genuinely lives elsewhere — another service owns it, or your
IaC declares it. It asserts that the binding exists and that guaranteeing it is
somebody else's job, so use it only when that is true; it disables the check for
that queue permanently:

```typescript
import { defineExchange, defineQueue } from "@amqp-contract/contract";

const inventoryDlx = defineExchange("inventory-dlx");
const inventoryCommands = defineQueue("inventory-commands", {
  deadLetter: { exchange: inventoryDlx, externalConsumers: true },
});
```

A dead-letter exchange declaring an `alternate-exchange` argument never triggers this, exactly as for a publisher's exchange — the broker routes what matches no binding there instead of discarding it, so no dead letter is lost whatever key it arrives under.

::: warning `#` on a direct DLX binds nothing

`#` is a _topic_ wildcard. A direct exchange has no wildcards and treats it as
the literal routing key `#`, so a dead letter arriving under any other key
matches nothing. Measured against RabbitMQ 4.2 in
`tests/src/__tests__/dlx-routability.spec.ts`: the same `#` binding receives the dead
letter on a topic DLX and receives nothing on a direct one.

The check cannot catch this. When the queue sets no `deadLetter.routingKey`, the
key a dead letter arrives under is the message's _original_ key, which is not
knowable at define time — so the check accepts any binding on the exchange, and
a `#` binding on a direct DLX passes it while routing nothing. That is why the
error text carries the warning rather than the check carrying a rule.

On a direct DLX, bind the key the message will actually carry: the queue's
`deadLetter.routingKey` if it sets one, otherwise every key the source queue can
receive. On a **fanout** or **headers** DLX the key is ignored and any binding
routes.

```typescript
import { defineExchange, defineQueue, defineQueueBinding } from "@amqp-contract/contract";

const paymentsDlx = defineExchange("payments-dlx", { type: "direct" });
const paymentsDlq = defineQueue("payments-dlq");

const paymentsQueue = defineQueue("payments", {
  deadLetter: { exchange: paymentsDlx, routingKey: "payments.dead" },
});

const paymentsDlqBinding = defineQueueBinding(paymentsDlq, paymentsDlx, {
  routingKey: "payments.dead",
});
```

:::

**A DLX supplied through the raw `arguments` passthrough is not checked.** It
names an exchange as a bare string rather than an `ExchangeDefinition`, and the
contract need not declare that exchange at all, so its bindings are unknowable.
The queue is skipped — no error, and no protection either:

```typescript
import { defineQueue } from "@amqp-contract/contract";

// Skipped by the check — verify this exchange's bindings on the broker yourself.
const legacyQueue = defineQueue("legacy-processing", {
  arguments: { "x-dead-letter-exchange": "legacy-dlx" },
});
```

**If the queue already exists in production,** adding a binding to live topology
carries the same constraints as adding `deadLetter` to a live queue — see
[if the queue already exists in production](#if-the-queue-already-exists-in-production),
and especially the warning there about matching the existing exchange type,
routing key, queue type and durability before you declare. A binding is cheap to
add to a live exchange; declaring the _exchange_ with a type that differs from
the live one is what 406s.

## Topology conflicts

### `PRECONDITION_FAILED - inequivalent arg`

The queue or exchange already exists with different properties. RabbitMQ will not redeclare it, and this is deliberate — silently mutating a live queue would be worse.

It usually follows a contract change: switching a queue from classic to quorum, changing durability, or adding a `deadLetter`. None of those can be applied to an existing queue.

Delete the old one and let the contract recreate it:

```bash
docker exec rabbitmq rabbitmqctl delete_queue order-processing
```

In production, that means draining it first — or declaring a new queue under a new name and migrating consumers across. Note that a queue's `arguments` (including dead-letter configuration and TTL) are part of its identity.

### `NOT_FOUND - no exchange`

A publish targeted an exchange that was never declared. Since the worker declares the contract's topology at startup, this usually means the _client_ started first and the worker has never run. Start the worker once to establish topology.

## Worker problems

### Worker creation fails immediately

Handler validation runs before any connection is acquired, so a missing or misnamed handler fails fast. Read the error — it names the key.

### Messages are acknowledged but nothing happened

A handler returning `OkAsync(undefined)` says "done". If it returns before its async work finishes — a bare `.then()`, or work not lifted into the result chain — the message is acknowledged early and the work is lost on restart. Everything must be in the returned chain.

### A handler threw and the message went to the dead-letter queue

The worker's safety net caught it. Return `ErrAsync(...)` instead so you can classify the failure and let it retry. See [consume messages](/how-to/consume-messages#know-how-a-return-value-routes-the-message).

### Messages vanish on failure

The queue was declared with `onPoison: "drop"` and no `deadLetter`, so `nack(requeue=false)` discards them. Any other consumed queue would have failed at define time — see [`defineContract` says my queue has no dead-letter exchange](#definecontract-says-my-queue-has-no-dead-letter-exchange). To start keeping them, replace `onPoison: "drop"` with a `deadLetter` exchange: see [route dead letters](/how-to/route-dead-letters).

### My worker suddenly processes fewer messages at once

Consumers now prefetch **10** messages by default (previously unlimited — the
broker pushed the entire ready backlog to a single consumer, which is unbounded
memory and a large redelivery burst if the worker crashes). See [Delivery guarantees](/explanation/delivery-guarantees) for when redelivery happens.

Prefetch is a _consumer_ option, so it goes in `defaultConsumerOptions` (or the
per-handler tuple), not at the top level of the worker options. Raise it if you
are throughput-bound and your handlers are cheap:

```typescript
const worker = await TypedAmqpWorker.create({
  contract,
  urls: ["amqp://localhost"],
  handlers,
  defaultConsumerOptions: { prefetch: 100 },
}).get();
```

Or restore the old behavior explicitly:

```typescript
defaultConsumerOptions: { prefetch: "unbounded" },
```

`"unbounded"` rather than `0` — AMQP's `0` means _unlimited_, which reads at a
call site as its opposite.

## RPC problems

### Every call times out

Check, in order: the worker is running and has a handler for that RPC; both sides share the same contract; and the handler's return value satisfies the response schema.

That last one is easy to miss. A reply failing its schema is dropped rather than sent malformed, so the caller sees a timeout even though the handler ran successfully. A timeout against an evidently healthy server almost always means a response-schema mismatch.

### `RpcCancelledError`

The client closed while a call was in flight. Normally shutdown ordering — close the client after in-flight work completes.

### An error reply resolves to a defect

The reply carried an error code the local contract does not declare — a version skew between caller and callee. Redeploy both from the same contract.

## Still stuck

Look at the broker directly, since it is the one component that knows the truth:

```bash
docker exec rabbitmq rabbitmqctl list_queues name messages consumers
docker exec rabbitmq rabbitmqctl list_bindings
docker exec rabbitmq rabbitmqctl list_connections
```

Then attach a [logger](/how-to/add-logging) if you have not — the worker's log lines cover validation, retry and dead-letter decisions, and are usually where the answer is.

Failing that, [open an issue](https://github.com/btravstack/amqp-contract/issues) with your contract definition and the failing snippet.
