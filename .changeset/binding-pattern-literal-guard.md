---
"@amqp-contract/contract": patch
---

Fixed `defineEventConsumer` rejecting a routing-key override typed as a template
literal. A pattern such as `` `${string}.created` `` matches `order.created` at
runtime, but `MatchingBindingPattern` treated any type that was not plain
`string` as decidable, could not decide it, and failed the build with
"binding pattern '${string}.created' can never match the publisher routing key
'order.created'". Tenant- and environment-prefixed routing keys are the common
way to hit this.

The three matcher types — `MatchingBindingPattern`, `MatchingRoutingKey`, and
`RoutableRoutingKey` — now share one test for whether a string is fully known at
compile time, and skip the match when it is not. `MatchingRoutingKey` also loses
an asymmetry where a plain-`string` pattern collapsed to `never` while a
plain-`string` key did not.

A pattern fully known at compile time and unable to match is still rejected
exactly as before. The guard gives up a narrower class in exchange: a pattern
with a hole and a literal tail, such as `` `user.${string}` `` against
`order.created`, could in principle be shown unmatchable and is now accepted.
That trade is deliberate — rejecting a valid contract is the costlier error.

The define-time routability check in `defineContract` covers the publisher
side — it fails a contract whose publisher reaches no queue. It does not cover
`MatchingBindingPattern` or `MatchingRoutingKey`'s undecidable cases: a
consumer binding that receives nothing while a sibling binding keeps the
publisher routable gets no compile-time and no define-time signal.
