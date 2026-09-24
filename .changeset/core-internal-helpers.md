---
"@amqp-contract/core": major
---

Implementation helpers moved off the `@amqp-contract/core` root to
`@amqp-contract/core/internal` (no semver guarantee): `safeJsonParse`,
`technicalDefect`, `setupAmqpTopology`, `startPublishSpan`,
`startConsumeSpan`, `endSpanSuccess`, `endSpanError`, `recordPublishMetric`,
`recordConsumeMetric`, `recordLateRpcReply`, `recordRpcCallMetric` and the
`ConnectionLease` type. Nothing was removed — import them from
`@amqp-contract/core/internal` instead. `TopologyMode` stays public.
