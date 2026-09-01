---
"@amqp-contract/contract": major
---

`defineContract` now throws when a publisher's routing key reaches no queue.
RabbitMQ confirms an unroutable message and then discards it, so a mistyped
binding pattern silently dropped every message while `publish()` returned
`Ok`. Publishers whose consumers live in another service opt out with
`externalConsumers: true`, accepted by `definePublisher`,
`defineEventPublisher`, and `defineCommandPublisher` alike. An exchange
declaring an `alternate-exchange` argument is always routable — the broker
catches its unmatched keys.

The check runs on the bindings passed to `defineContract`: mutating
`contract.bindings` afterwards no longer makes a publisher routable, since the
verdict was already reached. Declare every binding in the `defineContract` call.
