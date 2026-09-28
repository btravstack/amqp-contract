---
"@amqp-contract/worker": patch
---

A retry publish that times out or loses its channel (`PublishError` `timeout`
/ `channel-closed`) now requeues the original delivery (`nack(requeue: true)`)
instead of flowing on as a defect that dead-lettered it — or dropped it on an
`onPoison: "drop"` queue. The retry headers are unchanged, so the retry budget
is intact. The log line is now `Publish for retry failed; requeueing the
original for redelivery`.

A retry copy the broker **nacks** (e.g. a wait queue at `x-max-length` with
`x-overflow: reject-publish`) still dead-letters the original: the broker would
refuse every copy, and a requeued classic-queue original is never counted, so
requeueing would re-run the handler in an unbounded loop. The log line is
`Publish for retry was nacked by the broker; dead-lettering the original`.
