---
title: FAQ - amqp-contract
description: Short answers to common questions, each pointing at the page that covers it properly.
---

# FAQ

Short answers. Each links to the page that treats the question properly.

## Why doesn't `client.publish()` throw?

Every fallible operation returns a result instead. Awaiting one gives you a `Result` you inspect — it does not throw on failure. This is what lets the compiler enumerate what can go wrong.

→ [Errors as values](/explanation/errors-as-values)

## What is the `defect` branch for?

Failures nobody anticipated — a dropped connection, a channel error. They are not in the type signature because there is no meaningful way to branch on them. Modeled errors (like a validation failure) go in `errCases`; everything else arrives as a defect with a `TechnicalError` cause.

→ [Error model](/reference/error-model)

## Why can't I use `async` in a handler?

Because a rejected promise is an untyped exception again, and the worker would have to guess whether to retry. Lifting with `fromPromise(promise, qualify)` forces you to say what a failure _means_, which is the decision the worker needs.

→ [Errors as values](/explanation/errors-as-values#why-handlers-are-not-async)

## Which schema libraries can I use?

Anything implementing Standard Schema — Zod, Valibot, ArkType. Use Zod unless you have a specific reason not to.

→ [Schema libraries](/reference/schema-libraries)

## Do I need to install `amqplib`?

No. It ships as a dependency of the amqp-contract packages. You do need `unthrown`, because it appears in the types you write.

→ [Getting started](/tutorial/getting-started#step-2-create-the-project)

## Why are queues quorum by default?

Quorum queues replicate through Raft and survive broker failure. The cost is some write latency, which is almost always worth paying. Classic queues are opt-in for the features quorum does not support: `exclusive`, `autoDelete`, and priority queues.

→ [Topology options](/reference/topology-options#definequeue)

## Can I change the routing key per publish?

No. It comes from the publisher definition. Define a publisher per routing key — that keeps every key a service can emit visible in the contract.

→ [Publish messages](/how-to/publish-messages#send-a-message-on-a-different-routing-key)

## Does it support request/reply?

Yes. `defineRpc` declares a request schema, a response schema and optionally a set of typed error codes.

→ [Use request/reply](/how-to/use-request-reply)

## Does it work with NestJS or other frameworks?

Yes — it is a plain library with no framework coupling. Create the client and worker wherever your framework builds singletons, and use `createContext` for per-message dependency injection.

→ [Add middleware](/how-to/add-middleware#inject-dependencies-per-message)

## Does it support Kafka, NATS or SQS?

No. AMQP 0.9.1 only. The generated AsyncAPI document is the only part of a contract that travels to another transport.

→ [Comparison](/explanation/comparison)

## How do non-TypeScript services join in?

Generate an AsyncAPI document from the contract and use AsyncAPI's code generators. They get types from the same source of truth, though not the runtime validation.

→ [Generate AsyncAPI](/how-to/generate-asyncapi#use-the-document)

## How do I test my contracts?

`@amqp-contract/testing` runs your tests against a real RabbitMQ container with an isolated virtual host per test.

→ [Test with RabbitMQ](/how-to/test-with-rabbitmq)

## Why did my message go straight to the dead-letter queue without retrying?

Either the handler returned a `NonRetryableError`, or the message failed schema validation. Validation failures bypass retries entirely, because a payload that does not match the schema will not start matching it on a later attempt.

→ [The retry model](/explanation/the-retry-model#why-validation-failures-never-retry)

## Why is there no failure reason on my dead-lettered message?

Diagnostic headers are only stamped on paths that republish the message. A direct nack — the common case — leaves the message exactly as delivered, and the reason is in the worker's log instead.

→ [Retry failed messages](/how-to/retry-failed-messages#inspect-retry-state)

## My RPC times out but the server looks healthy. Why?

Most often the handler's return value fails the response schema. The worker refuses to publish a malformed reply, so the caller sees a timeout rather than a wrong answer. Check that both sides share the same contract version.

→ [Use request/reply](/how-to/use-request-reply#understand-what-makes-a-call-fail)

## Does a shared connection mean one connection for my whole cluster?

No — per process. The cache is a per-process singleton, so each process, worker thread or Lambda instance opens its own.

→ [Share connections](/how-to/share-connections#know-the-limits)

## Are retries exactly-once?

No. Retries are delivery attempts. A handler that charges a card and then fails will charge again on retry. Making the work idempotent is yours.

→ [The retry model](/explanation/the-retry-model#retries-are-not-exactly-once)
