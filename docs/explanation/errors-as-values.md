---
title: Errors as values - amqp-contract
description: Why nothing in the public API throws, what the third defect channel is for, and how the model maps onto a broker's ack/retry/dead-letter decision.
---

# Errors as values

Nothing in amqp-contract's public API throws. Every fallible operation returns a result you inspect. This page explains why, and what the three channels are actually for.

It assumes you have seen the shape already — [getting started](/tutorial/getting-started) uses it. For the catalogue of concrete error types, see the [error model reference](/reference/error-model).

## The decision a broker forces

An HTTP handler that throws has an obvious fallback: return 500 and move on. The request is over either way.

A message consumer has no such luxury. When processing fails, the message still exists, and something must happen to it:

- **acknowledge** it — treat it as done, and it is gone forever;
- **retry** it — put it back, and risk looping forever;
- **dead-letter** it — set it aside for a human, and risk nobody looking.

Choosing wrongly loses data or wedges a queue. So the interesting question is not "did this fail?" but "_how_ did it fail?" — and an exception is a bad way to carry that answer. It has type `unknown`. It can be thrown from any depth. Nothing forces a caller to handle it, or even to know it exists.

Returning the outcome makes the answer part of the signature:

```typescript
type Handler = (msg) => AsyncResult<void, HandlerError>;
```

`HandlerError` is `RetryableError | NonRetryableError`. The handler's job is to classify, the worker's job is to route, and the type system connects the two. A handler cannot fail in a way the worker does not know how to handle, because the only failures it can express are the ones the worker understands.

## Three channels, not two

Most result types have two channels: success and failure. amqp-contract uses [unthrown](https://github.com/btravstack/unthrown), which has three. The third exists because "failure" conflates two genuinely different things.

Consider two ways `publish` can fail:

1. The payload does not match the schema.
2. The TCP connection to the broker died mid-write.

The first is an **anticipated outcome**. You can predict it, you can branch on it, and there is something sensible to do — reject the request, log the invalid field, fix the caller. It belongs in the type signature so callers are made to handle it.

The second is a **defect**. You did not ask for it, there is no meaningful branch, and it is not specific to this call — the connection being gone affects everything. Putting it in the error channel would force every caller to write an arm for a case they cannot act on, which is how `catch (e) { /* ignore */ }` gets written.

So `publish` has this signature:

```typescript
publish(...): AsyncResult<void, MessageValidationError>
```

`MessageValidationError` is the entire modeled error channel. Transport failures arrive as defects, carrying a `TechnicalError` as their cause.

Handling all three is one expression:

```typescript
result.match({
  ok: () => reply(202),
  errCases: (matcher) =>
    matcher.with(P.tag("@amqp-contract/MessageValidationError"), (e) => reply(400, e.message)),
  defect: (cause) => {
    logger.error({ cause }, "broker unreachable");
    reply(503);
  },
});
```

That maps cleanly onto the distinction an HTTP boundary already makes: modeled errors are the caller's fault (4xx), defects are ours (5xx).

## Why `errCases` and not a single `err` callback

The middle branch is not one function taking the error. It receives a matcher and requires an arm for every member of the union:

```typescript
errCases: (matcher) =>
  matcher
    .with(P.tag("@amqp-contract/RpcTimeoutError"), () => …)
    .with(P.tag("@amqp-contract/RpcCancelledError"), () => …)
    .with(P.tag("@amqp-contract/RpcError"), (e) => …),
```

The payoff is what happens when the union grows. Add a declared error to an RPC and every `match` over its result stops compiling until the new case is handled. A single `err: (e) => …` callback would keep compiling and silently take the wrong branch — which is exactly the failure mode of `catch`, reintroduced.

You do not have to enumerate when you genuinely do not care: `.with(P._, handler)` is a catch-all. The difference is that ignoring the distinction becomes a thing you wrote down on purpose.

## The type-gated escape hatch

`.get()` extracts the success value, and it only compiles when the modeled error channel is empty (`E = never`). This is why you can write:

```typescript
const client = await TypedAmqpClient.create({ contract, urls }).get();
```

`create` returns `AsyncResult<TypedAmqpClient, never>` — an empty error channel, because everything that can go wrong while connecting is infrastructure, and infrastructure failures are defects. There is nothing to handle, so `.get()` is allowed.

On a still-fallible result it will not compile:

```typescript
await client.publish("orderCreated", order).get(); // compile error
```

That is the gate doing its job — `publish` has a modeled error, so you must address it. When throwing is genuinely acceptable (a script, a test, an example) `.getOrThrow()` says so explicitly.

Note that `E = never` empties the _modeled_ channel only. `.get()` still rethrows a defect's cause — a failed `create()` throws the underlying `TechnicalError`. `Result<T, never>` does not mean "cannot throw"; it means "has no errors you were supposed to handle".

## Why handlers are not `async`

Handlers return `AsyncResult`, not `Promise`. It is a real constraint and it is the most common thing newcomers push back on, so it is worth defending.

An `async` handler can reject, and a rejection is an exception again — type `unknown`, unclassified, arriving from anywhere. The worker would be back to guessing whether to retry.

Instead, promises are lifted explicitly, and lifting them requires saying what a rejection _means_:

```typescript
fromPromise(chargeCard(payload), (cause) =>
  cause instanceof CardDeclined
    ? new NonRetryableError("declined", cause)
    : new RetryableError("gateway unavailable", cause),
);
```

The second argument is mandatory. There is no way to bring a promise into the pipeline without deciding how its failure should be routed — which is precisely the decision that gets skipped when it is optional. For the common cases, `qualifyRetryable(message)` and `qualifyNonRetryable(message)` are prebuilt mappers.

The worker does still wrap handler invocation in `try`/`catch`, so a handler that throws by accident cannot leave a message neither acked nor nacked — it is logged and dead-lettered. That is a safety net for bugs, not an alternative interface. A thrown error has already lost the classification that would have let it be retried.

## What this does not solve

Errors as values make failure _visible and typed_. They do not make it correct.

You can still classify wrongly — mark a permanent failure retryable and it will cycle to the dead-letter queue slowly instead of quickly. You can still write `.with(P._, () => {})` and swallow everything. You can still return `OkAsync(undefined)` from a handler that did not do the work, and the message will be acknowledged and lost.

What the model guarantees is narrower: no failure disappears without someone writing code that discards it, and no new failure mode gets added to an operation without every caller being made to look at it. That is a floor, not a ceiling.

## Where next

- [Error model](/reference/error-model) — every error type, where it surfaces, and the RPC wire format.
- [The retry model](/explanation/the-retry-model) — what the worker does with the classification you return.
- [Consume messages](/how-to/consume-messages) — handler recipes.
