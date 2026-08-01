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

## Type errors

### `Property 'X' does not exist on type …` in a handler

The payload type comes from the contract, so this means the contract disagrees with your expectation. Check the message schema, and check whether the consumer is bound to the publisher you think it is.

For a wildcard consumer receiving several message types, the payload is a union — narrow it before accessing anything type-specific:

```typescript
notifyOrder: ({ payload }) => {
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

The queue has no `deadLetter` configured, so `nack(requeue=false)` discards them. The worker logs `Queue does not have DLX configured - message will be lost on nack`. See [route dead letters](/how-to/route-dead-letters).

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
