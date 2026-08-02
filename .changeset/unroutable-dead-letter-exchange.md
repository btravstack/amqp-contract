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
`defineQueue` and mirroring `PublisherDefinition.externalConsumers`.

Bind the key that will actually arrive. On a `direct` dead-letter exchange `#` is
a literal that matches nothing — the error message says so, because when the
queue sets no `deadLetter.routingKey` the check accepts any binding and cannot
catch it. On a queue with `retry: { mode: "ttl-backoff" }` a retried message
re-enters through the wait queue carrying the queue name as its routing key, so
the publisher's key is not what reaches the dead-letter exchange either. Setting
an explicit `deadLetter.routingKey` sidesteps both.
