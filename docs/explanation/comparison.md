---
title: Comparison - amqp-contract
description: How amqp-contract relates to other RabbitMQ libraries for Node.js, spec-first AsyncAPI tooling, tRPC and oRPC, BullMQ, SQS/SNS and Kafka — what each is for, and when to prefer it.
---

# Comparison

Most of the tools people weigh against amqp-contract are not really alternatives to it — they solve adjacent problems, and several are things you would sensibly use at the same time. This page tries to say what each is actually for.

## amqplib

[amqplib](https://github.com/amqp-node/amqplib) is the Node.js AMQP client. It is not an alternative in the usual sense: amqp-contract is built on it, and every message you send goes through it.

The difference is the layer. amqplib gives you the protocol — channels, exchanges, `Buffer`s. amqp-contract adds a typed contract over that, and takes over topology declaration, serialization and validation.

```typescript
// amqplib
channel.publish("orders", "order.created", Buffer.from(JSON.stringify(order)));

// amqp-contract
await client.publish("orderCreated", order);
```

The second line is checked against a schema at compile time and validated at runtime; the first will accept anything you can serialize.

**Prefer amqplib directly** when you need protocol-level control the abstraction does not expose, when you are writing a throwaway script and a contract is overhead, or when you are building your own abstraction and want the primitives. It is also the smaller dependency, which matters for a library.

**Prefer amqp-contract** once more than one service or more than one person is involved, because that is when the cost of an unenforced schema starts compounding.

## Other RabbitMQ libraries for Node.js

These are the libraries a team shortlists when RabbitMQ is already decided. All of them sit at roughly the same layer as amqp-contract, and several are better choices in the right setting.

| Library                                                                                         | What it is                                                                                                                                                                                                                                                                                          | Choose it when                                                                                                                                                |
| ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [amqp-connection-manager](https://github.com/jwalton/node-amqp-connection-manager)              | amqplib plus automatic reconnection, round-robin failover across brokers, setup functions re-run on every reconnect, and buffering of publishes while disconnected. amqp-contract is built on it.                                                                                                   | You want amqplib with reconnection and nothing else — no contract, no validation, your own topology code.                                                     |
| [Rascal](https://github.com/onebeyond/rascal)                                                   | A config-driven client: vhosts, exchanges, queues, bindings, publications and subscriptions are declared in configuration and asserted at startup, with redelivery handling (republish with attempt limits, forwarding, dead-lettering), publication confirms and timeouts, and message encryption. | Operations owns the topology as configuration, you need its redelivery strategies or multi-vhost setup, and compile-time message types are not a requirement. |
| [@golevelup/nestjs-rabbitmq](https://github.com/golevelup/nestjs/tree/master/packages/rabbitmq) | NestJS decorators (`@RabbitSubscribe`, `@RabbitRPC`) for pub/sub and RPC over direct reply-to, built on amqp-connection-manager, with Nest guards, interceptors and pipes applied to handlers.                                                                                                      | Your services are NestJS and you want handlers wired through Nest's dependency injection and decorators.                                                      |
| [NestJS microservices RabbitMQ transport](https://docs.nestjs.com/microservices/rabbitmq)       | Nest's built-in transport: `@MessagePattern` (request/reply) and `@EventPattern` handlers over a queue per service.                                                                                                                                                                                 | Both ends are Nest microservices and you want Nest's messaging model rather than your own AMQP topology.                                                      |
| [Moleculer](https://moleculer.services/docs/0.14/networking.html) AMQP transporter              | A transport for the Moleculer microservices framework: it carries Moleculer's own protocol over RabbitMQ. You program against Moleculer services, actions and events, not exchanges and queues.                                                                                                     | You are adopting Moleculer as the framework, and RabbitMQ is just the wire.                                                                                   |
| [rabbitmq-client](https://github.com/cody-greene/node-rabbitmq-client)                          | A zero-dependency AMQP 0.9.1 client written from scratch in TypeScript, with automatic reconnection and re-subscription, higher-level `Consumer` / `Publisher` objects that declare their queues and exchanges, publisher confirms and an RPC client.                                               | You want a modern, typed low-level client with recovery built in, and will define message shapes yourself.                                                    |
| [@cloudamqp/amqp-client](https://github.com/cloudamqp/amqp-client.js)                           | A zero-dependency TypeScript AMQP 0.9.1 client for Node.js **and browsers** (over WebSocket), with RPC over direct reply-to and publisher confirms.                                                                                                                                                 | You need to speak AMQP from a browser, or want the smallest possible dependency footprint.                                                                    |

The difference in kind is what the contract does. Several of these can validate a payload on the consumer side (a Nest `ValidationPipe`, for example), and some type the message body. None of them derives the publisher's types, the consumer's types, runtime validation on both ends, the broker topology and an AsyncAPI document from one shared definition — which is the whole of what amqp-contract is for. If a single service owns both ends and the topology is trivial, that buys you little, and the table above is the better answer.

## Spec-first AsyncAPI code generation

The other way to get a shared contract is to write it as an [AsyncAPI](https://www.asyncapi.com/) document first and generate code from it. [Modelina](https://github.com/asyncapi/modelina) generates typed data models (TypeScript among many languages) from a spec, and the [AsyncAPI generator](https://github.com/asyncapi/generator) renders documentation and code from templates. What the ecosystem does not currently offer is a maintained generator for a full Node.js AMQP client — the Node.js template is archived — so you generate the types and hand-write the messaging code around them.

amqp-contract is code-first in the opposite direction: the TypeScript contract is the source of truth, and the AsyncAPI document is [generated from it](/how-to/generate-asyncapi).

**Prefer spec-first** when the spec itself is the product — services in several languages sharing one contract, or a governance process that reviews AsyncAPI documents before any code exists.

**Prefer amqp-contract** when the services are TypeScript: the contract is code you import, so a change that breaks a consumer fails to compile instead of drifting from a generated copy.

## tRPC and oRPC

[tRPC](https://trpc.io/) and [oRPC](https://orpc.dev/) inspired this library's approach, and the family resemblance is intentional: define a contract once, get end-to-end types.

They target a different transport and a different shape of problem. tRPC is synchronous request/response over HTTP, typically browser to server. The caller waits, and if the server is down the call fails now.

AMQP messaging is asynchronous and brokered. The publisher does not wait, does not know who consumes, and the broker holds the message durably if no consumer is available. Work queues, fan-out to several independent subscribers, and load distribution across worker replicas all fall out of that; none of them are things tRPC is trying to do.

These compose rather than compete. A common arrangement is tRPC between the browser and an API service, and amqp-contract between that service and the workers behind it.

The one genuine overlap is [request/reply](/how-to/use-request-reply), where amqp-contract does synchronous-feeling calls over AMQP. Reach for it when the callee is already an AMQP service — not as a general replacement for HTTP, which is simpler when both ends can speak it.

## GraphQL subscriptions

[GraphQL subscriptions](https://www.apollographql.com/docs/apollo-server/data/subscriptions/) push updates to clients over a WebSocket. The audience is external and usually a UI; delivery is best-effort and tied to connection lifetime — a disconnected client misses what it missed.

AMQP is for internal service-to-service traffic, with durable queues that hold messages for consumers that are not currently running.

They are frequently used together, with the subscription layer being one more consumer of the internal event stream.

## BullMQ

[BullMQ](https://docs.bullmq.io/) is a Redis-backed job queue, and it is the closest real alternative here, because a task queue is genuinely a thing both can do.

BullMQ is the better fit when you want a _job_ abstraction: scheduled and repeatable jobs, progress reporting, priorities, a job lifecycle you query, and a ready-made dashboard. If you are already running Redis and not RabbitMQ, it is also considerably less infrastructure.

amqp-contract is the better fit when you want _messaging_: several independent consumers of the same event, topic routing with wildcards, exchange-to-exchange composition across domains, and AMQP's delivery guarantees. It also gives you schema validation and generated AsyncAPI documentation, which BullMQ does not attempt.

Rule of thumb: if you find yourself describing the work as "jobs", BullMQ probably fits better. If you describe it as "events" or "commands between services", this does.

## SQS and SNS

[SQS](https://aws.amazon.com/sqs/) and [SNS](https://aws.amazon.com/sns/) are managed, which is a decisive advantage — no broker to run, patch or scale.

The trade is routing expressiveness and portability. AMQP's topic exchanges, header exchanges and exchange-to-exchange bindings have no direct equivalent, and you are on AWS.

amqp-contract does not support them. It speaks AMQP 0.9.1 and nothing else. If a managed queue is a hard requirement, this is the wrong library, and the [AsyncAPI document](/how-to/generate-asyncapi) is the only part of a contract that would carry across.

## Kafka

[Kafka](https://kafka.apache.org/) is a distributed log, not a message queue, and the difference matters more than the surface similarity suggests.

Kafka retains messages after consumption, so consumers hold offsets and can replay history. That makes it the right tool for event sourcing, stream processing, and any pipeline where several consumers read the same stream at their own pace, repeatedly.

RabbitMQ deletes a message once acknowledged. In exchange it gives you per-message acknowledgement, flexible routing decided by the broker, and dead-lettering — which is what you want for task distribution and command dispatch, and which Kafka makes awkward.

Choose by whether you need to _replay_. If the ability to re-read last month's events matters, Kafka. If messages are work to be done once, RabbitMQ.

## Summary

| If you need                                                       | Use                                                  |
| ----------------------------------------------------------------- | ---------------------------------------------------- |
| Protocol-level AMQP control, or a one-off script                  | [amqplib](https://github.com/amqp-node/amqplib)      |
| Reconnection on top of amqplib, nothing more                      | amqp-connection-manager                              |
| Topology as operations-owned configuration                        | Rascal                                               |
| RabbitMQ handlers wired through NestJS                            | @golevelup/nestjs-rabbitmq / Nest RMQ transport      |
| A polyglot contract, spec written first                           | AsyncAPI + code generation                           |
| Typed browser-to-server calls                                     | [tRPC](https://trpc.io/) / [oRPC](https://orpc.dev/) |
| Real-time push to UI clients                                      | GraphQL subscriptions                                |
| Scheduled/repeatable jobs, a job dashboard, Redis already running | [BullMQ](https://docs.bullmq.io/)                    |
| A managed queue, on AWS                                           | SQS / SNS                                            |
| Replayable event history, stream processing                       | [Kafka](https://kafka.apache.org/)                   |
| Typed events and commands between TypeScript services on RabbitMQ | amqp-contract                                        |

The honest summary of the last row: amqp-contract is a narrow tool. It requires TypeScript, requires RabbitMQ, and requires that both ends can share a contract package. Within those constraints it removes a class of bug that is otherwise very hard to remove. Outside them, one of the rows above is a better answer.

## Where next

- [Why amqp-contract?](/explanation/why-amqp-contract) — the case in more depth, including the costs.
- [Getting started](/tutorial/getting-started) — try it in fifteen minutes.
