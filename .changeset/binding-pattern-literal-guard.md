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
plain-`string` key did not. Undecidable cases defer to the define-time
routability check in `defineContract`, which runs on concrete strings; patterns
that genuinely cannot match are still rejected exactly as before.
