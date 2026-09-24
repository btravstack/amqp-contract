---
title: Evolve a contract - amqp-contract
description: Change message schemas when publishers and consumers deploy independently — additive changes, tolerant readers, versioned messages, two-phase removals and reviewing changes through the AsyncAPI diff.
---

# Evolve a contract

A shared contract package makes both sides agree at compile time. It cannot make them deploy at the same moment: during a rollout, and for as long as some service still pins the old contract version, old and new code exchange messages. This guide keeps those messages valid in both directions.

What goes wrong if you get it wrong is specific. The consumer validates every message against **its** version of the schema, and a message that fails validation is dead-lettered on first delivery — never retried, never handled. An incompatible change does not crash anything; it quietly moves traffic into the dead-letter queue.

## Make additive changes

Adding an **optional** field is safe in either deploy order:

```typescript
const orderCreatedV1 = z.object({ orderId: z.string(), amount: z.number() });

// Next contract version
const orderCreated = z.object({
  orderId: z.string(),
  amount: z.number(),
  currency: z.string().optional(), // new, optional
});
```

- An **old consumer** receives `currency` and ignores it — `z.object` strips keys it does not declare.
- A **new consumer** receives messages from an old publisher without `currency`, which the schema allows.

Changes that look additive but are not:

- **A new required field.** Old publishers do not send it, so new consumers reject their messages.
- **A new enum value.** Old consumers declared the old set, so they reject the new value.
- **A narrowed type** (a longer minimum length, a stricter format). Messages already in the queue, or from old publishers, may fail it.

For a field that must become required, add it as optional, deploy every publisher so it is always sent, then make it required.

## Keep readers tolerant

Additive changes only work if consumers ignore what they do not know. Default object schemas do: Zod's `z.object` and Valibot's `v.object` strip undeclared keys, and ArkType ignores them. Strict variants — `z.strictObject`, `v.strictObject`, ArkType's `"+": "reject"` — reject any message carrying a field the reader has not heard of, which turns every additive change on the publisher into dead letters on the consumer. Avoid them on consumed messages.

## Remove a field in two phases

1. **Make it optional in the contract**, and change consumers to stop reading it. Deploy every consumer.
2. **Stop sending it.** Remove it from the schema and from publishers. Deploy.

Doing both at once breaks every consumer still running the old contract, which treats the missing field as invalid. A rename is the same procedure: add the new field (optional), move readers to it, then remove the old one.

## Version a message you cannot change compatibly

When a message has to change shape outright, publish the new shape as a new message on its own routing key, and run both until every consumer has moved:

```typescript
const orderCreatedV1 = defineEventPublisher(ordersExchange, orderMessageV1, {
  routingKey: "order.created",
});
const orderCreatedV2 = defineEventPublisher(ordersExchange, orderMessageV2, {
  routingKey: "order.created.v2",
});
```

1. Add the `v2` publisher, and publish both versions.
2. Move each consumer to the `v2` event — `defineEventConsumer(orderCreatedV2, queue)` — one service at a time.
3. When no consumer is bound to `order.created` any more, stop publishing `v1` and remove it.

Choose the new key with your bindings in mind: a consumer bound to `order.created.#` would receive both versions.

`defineContract` rejects a publisher whose routing key reaches no queue in the contract. While a consumer owned by another service is the only one bound to the new key, declare it with `externalConsumers: true` — see [define a contract](/how-to/define-a-contract#publish-to-a-consumer-you-do-not-own).

## Do not change a queue in place

Queue and exchange properties — queue type, dead-letter settings, arguments — are fixed once the broker has declared them. A contract that re-declares an existing queue with different properties is refused by RabbitMQ, and the worker fails to start. To change one, declare a queue under a new name, move consumers to it, and delete the old one once it is empty.

## Review changes through the AsyncAPI diff

Commit the generated AsyncAPI document and regenerate it in CI, failing the build when it changed without being committed:

```yaml
- run: pnpm generate:asyncapi:json
- run: git diff --exit-code asyncapi.json
```

This detects **any** change to the contract, not only breaking ones — it does not classify them. Its value is that every contract change arrives in a pull request as a readable diff of the message schemas, where a reviewer can check it against the rules above: new required fields, new enum values, removed fields and changed queues are the lines to question. See [generate AsyncAPI](/how-to/generate-asyncapi).

## Where next

- [Define a contract](/how-to/define-a-contract#share-a-contract-between-services) — sharing one contract package between services.
- [Route dead letters](/how-to/route-dead-letters) — inspecting and replaying messages that failed validation.
- [Schema libraries](/reference/schema-libraries) — how Zod, Valibot and ArkType behave as message schemas.
