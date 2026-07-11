---
title: FAQ - amqp-contract
description: Frequently asked questions about amqp-contract, type-safe AMQP/RabbitMQ messaging for TypeScript.
---

# FAQ

## Why doesn't `client.publish()` throw on failure?

Every fallible operation returns a `Result` / `AsyncResult` (from [`unthrown`](https://github.com/btravstack/unthrown)) instead of throwing — `await client.publish(...)` gives you a `Result` that you `.match()` or chain. (`unthrown` 4 gates `.get()` to infallible results, so to throw on failure you unwrap with `.getOrElse((e) => { throw e })` — see [Getting the value out](/guide/error-model#getting-the-value-out).) This makes failure handling explicit and type-checked rather than an invisible `try`/`catch` obligation. The [Error Model guide](/guide/error-model) explains the full `Ok` / `Err` / `Defect` model.

## Which schema libraries can I use?

Any library implementing [Standard Schema v1](https://standardschema.dev/) — [Zod](https://zod.dev/), [Valibot](https://valibot.dev/), and [ArkType](https://arktype.io/) are all tested. See [Schema Libraries](/guide/schema-libraries).

## Do I need to install `amqplib` myself?

No. `amqplib` (and `amqp-connection-manager`) ship as regular dependencies of the amqp-contract packages. Installing them yourself only risks version drift. You do need to install [`unthrown`](https://github.com/btravstack/unthrown) and your schema library directly, since both appear in the types you write against.

## Why are queues quorum by default?

Quorum queues are RabbitMQ's recommended replicated queue type for data safety. amqp-contract defaults to them and only uses classic queues for features quorum queues don't support (`exclusive`, `autoDelete`, `maxPriority`, non-durable). See [Defining Contracts](/guide/defining-contracts).

## Does it work with NestJS (or other frameworks)?

amqp-contract is framework-agnostic — it runs anywhere Node.js does, and you can wire the client/worker into any framework's lifecycle. There is no dedicated NestJS module today; if that would unblock you, [open an issue](https://github.com/btravstack/amqp-contract/issues).

## Does it support Kafka, NATS, or SQS?

No — amqp-contract targets AMQP 0-9-1 (RabbitMQ) specifically, which is what allows it to model exchanges, queues, bindings, and routing keys in the type system.

## Does it support request/reply (RPC)?

Yes — define an RPC in the contract and both the caller and the responder are typed, with timeout and cancellation handling. See [Client Usage](/guide/client-usage) and [Worker Usage](/guide/worker-usage).

## How do I test my contracts?

`@amqp-contract/testing` provides Vitest fixtures that spin up a real RabbitMQ via [testcontainers](https://testcontainers.com/) with per-test vhost isolation. See the [Testing guide](/guide/testing).
