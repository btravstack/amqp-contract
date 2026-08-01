---
"@amqp-contract/core": major
"@amqp-contract/worker": major
---

Consumers now prefetch 10 messages by default instead of AMQP's unlimited,
bounding in-flight memory and the redelivery burst on a worker crash. Set
`prefetch` to a number to tune it, or `"unbounded"` to restore the previous
behavior.
