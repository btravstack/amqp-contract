---
"@amqp-contract/worker": patch
---

A retry publish the broker refuses (`PublishError`: timeout, nack, channel
closed) now requeues the original delivery (`nack(requeue: true)`) instead of
flowing on as a defect that dead-lettered it — or dropped it on an
`onPoison: "drop"` queue. The retry headers are unchanged, so the retry budget
is intact. The log line is now `Publish for retry failed; requeueing the
original for redelivery`.
