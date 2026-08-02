---
"@amqp-contract/contract": major
---

`defineContract` now throws when a queue's dead-letter exchange has nothing bound
to it. RabbitMQ discards a message routed to zero queues, so such a queue lost
every rejected message exactly as silently as one with no dead-letter exchange at
all — while the worker logged a reassuring "Sending message to DLQ". Bind a queue
to the exchange, or set `externalConsumers: true` on the deadLetter config if
another service owns it. A dead-letter exchange supplied through the raw
`arguments` passthrough names an exchange this contract cannot inspect and is not
checked.

`DeadLetterConfig.externalConsumers?: boolean` is the new opt-out, accepted by
`defineQueue` and mirroring `PublisherDefinition.externalConsumers`. On a
`direct` dead-letter exchange bind the actual routing key: `#` is a topic
wildcard and matches nothing there.
