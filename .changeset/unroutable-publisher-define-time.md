---
"@amqp-contract/contract": major
---

`defineContract` now throws when a publisher's routing key reaches no queue.
RabbitMQ confirms an unroutable message and then discards it, so a mistyped
binding pattern silently dropped every message while `publish()` returned
`Ok`. Publishers whose consumers live in another service opt out with
`externalConsumers: true`.
