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

Rejection is unchanged only when both the pattern and the publisher routing
key are fully known at compile time. When either side is not — a hole
anywhere in the type, a union containing one, or a type carrying extra
structure such as a brand — the check is skipped, even when the known side
alone already proves no match is possible. ``MatchingBindingPattern<"user.x",
`${string}.created`>`` is one such case: `"user.x"` can never equal any
string ending in `.created`, and this used to fail the build; it is accepted
now, unchecked, because the key side is not fully known. The same happens
when the pattern side is the undecidable one — a pattern with a hole and a
literal tail, such as `` `${string}.updated` ``, or a pattern narrowed by an
intersection such as `"user.*" & {__b: "x"}`, is accepted against a fully
known key for the same reason. That trade is deliberate — rejecting a valid
contract is the costlier error.

The define-time routability check in `defineContract` covers the publisher
side — it fails a contract whose publisher reaches no queue. It does not cover
`MatchingBindingPattern` or `MatchingRoutingKey`'s undecidable cases: a
consumer binding that receives nothing while a sibling binding keeps the
publisher routable gets no compile-time and no define-time signal.
