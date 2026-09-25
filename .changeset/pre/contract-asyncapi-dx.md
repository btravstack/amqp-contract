---
"@amqp-contract/contract": major
"@amqp-contract/asyncapi": major
---

**Contract**

- `defineQueueBinding` / `defineExchangeBinding` now throw when a binding on a
  **direct** exchange uses a `*` or `#` segment. A direct exchange matches keys
  literally, so such a binding receives nothing — on a direct dead-letter
  exchange it passed the DLX routability guard while every rejected message was
  discarded. Bind the exact key, or make the exchange `topic`.
- Quorum queues with `retry: { mode: "immediate-requeue" }` now get
  `x-delivery-limit: maxRetries + 1` in their queue arguments. RabbitMQ 4.x
  defaults the quorum delivery limit to 20 and dead-letters past it, so with
  `maxRetries >= 20` the broker used to dead-letter before the worker's retry
  budget was spent. An explicit `x-delivery-limit` is kept; one below
  `maxRetries + 1` is rejected at define time. **Migration:** an existing quorum
  queue declared without the argument fails redeclaration with
  `PRECONDITION_FAILED - inequivalent arg 'x-delivery-limit'` — recreate the
  queue, or pin `arguments: { "x-delivery-limit": … }` to match what the broker
  holds.
- Queue option errors name the queue and the remedy, e.g.
  `Queue "orders": exclusive is not supported on quorum queues (the default type). Set type: "classic" on this queue.`
  `maxPriority` must now be an integer. The `maxPriority`-on-quorum error and the
  type docs no longer claim quorum queues cannot prioritise: they honor the
  per-message `priority` natively on RabbitMQ 4.0+ with no queue argument
  (`x-max-priority` is classic-only).
- `defineEventPublisher`, `defineEventConsumer`, `defineCommandConsumer` and
  `defineCommandPublisher` are single generic signatures instead of overload sets,
  so a mistake is reported against the options (`Property 'routingKey' is
missing`, `'routingKey' does not exist in type …`) instead of "DirectExchangeDefinition
  is not assignable to FanoutExchangeDefinition | HeadersExchangeDefinition".
  Every valid call keeps its inferred type. Explicit type arguments, if you passed
  any, follow the new parameter order.
- New `defineDeadLetterQueue(dlx, name, options?)` returns `{ queue, binding }`:
  it binds `#` on a topic dead-letter exchange, no key on fanout/headers, and
  requires the exact key on direct.
- `extractConsumer`, `isBridgedPublisherConfig`, `isCommandConsumerConfig`,
  `isEventConsumerResult`, `isEventPublisherConfig`,
  `deriveTtlBackoffInfrastructure`, `ttlBackoffBaseDelay` and
  `ttlBackoffWaitQueueName` are available from `@amqp-contract/contract/internal`;
  the root exports are deprecated aliases.

**AsyncAPI**

- New `vhost` generator option (default `"/"`, previously hardcoded).
- Schemas implementing Standard JSON Schema (`~standard.jsonSchema` — Zod 4,
  ArkType) are converted natively (draft-07) and need no converter; they take
  precedence over `schemaConverters`, which remain the fallback (e.g. Valibot).
  Generated payloads can differ slightly (Zod adds a `pattern` for `datetime()`,
  ArkType drops its `$schema` marker).
- `@orpc/openapi` is no longer a dependency. `schemaConverters` is typed with
  the package's own structural `SchemaConverter`, which oRPC converters satisfy.
