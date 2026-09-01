---
"@amqp-contract/contract": major
"@amqp-contract/core": major
"@amqp-contract/client": major
"@amqp-contract/worker": major
"@amqp-contract/testing": major
"@amqp-contract/asyncapi": major
---

Pre-3.0 audit: routing-key safety and naming/export hygiene, taken while the major window is open.

- **`defineEventConsumer` topic routing-key overrides are now checked against the publisher's routing key at compile time.** A pattern that can never match — `defineEventConsumer(orderCreatedEvent, queue, { routingKey: "user.*" })` against a publisher on `order.created` — used to compile and silently receive nothing at runtime; it is now a type error whose message names both sides (`"Error: binding pattern 'user.*' can never match the publisher routing key 'order.created'"`). The new `MatchingBindingPattern<Pattern, PublisherKey>` type is exported; pattern matching also learned that a trailing `#` matches zero words (`order.created.#` now accepts `order.created`).
- **Direct/topic publishers and queue bindings require a non-empty routing key at define time.** `definePublisher` silently defaulted a missing routing key to `""` and `defineQueueBinding` left it `undefined` — both silently misroute. `definePublisher`, `defineQueueBinding`, `defineEventPublisher`, and `defineCommandConsumer` (via its binding) now throw an actionable error at define time for JavaScript callers and casts. Fanout/headers exchanges are exempt (they ignore routing keys).
- **`@amqp-contract/core`'s option types renamed**: `ConsumerOptions` → `AmqpConsumeOptions`, `PublishOptions` → `AmqpPublishOptions`. They collided with the user-facing `ConsumerOptions` (worker) and `PublishOptions` (client), which keep their names and shapes.
- **`_internal_*` helpers moved off `@amqp-contract/core`'s root** to a new `@amqp-contract/core/internal` subpath: `_internal_getConnectionCount`, `_internal_resetConnections`, `_internal_resetTelemetryCache`. They are test-lifecycle helpers with no semver guarantee and no longer clutter the public root.
- **`defineEventPublisher`'s `arguments` option renamed to `bindingArguments`.** It never was a publish argument — it is forwarded as the default AMQP binding arguments for that event's consumers' queue bindings (a consumer's own `arguments` option overrides it). The name now says so; `EventPublisherConfig`/`EventPublisherConfigBase` carry `bindingArguments` accordingly.
- **Builder-result brands are now a non-exported `unique symbol`** instead of `__brand: "EventConsumerResult"`-style string fields on `EventPublisherConfig`, `EventConsumerResult`, `CommandConsumerConfig`, and `BridgedPublisherConfig` (and their `*Base` types). The brand no longer shows up in IDE hovers and cannot be forged by user code; nominal separation and the `is*` type guards behave as before. Code that constructed these config objects by hand (rather than via `defineEvent*` / `defineCommand*`) no longer compiles — use the builders.
