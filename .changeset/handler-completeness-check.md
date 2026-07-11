---
"@amqp-contract/worker": patch
---

Validate handler completeness at startup (reverse check). `TypedAmqpWorker.create` now fails fast with `Err(TechnicalError)` — before any connection is acquired — when a contract `consumers`/`rpcs` entry has no handler, and `defineHandlers` throws with the list of missing names. Previously the type system was the only guard; a JavaScript caller or a cast could pass an incomplete handlers object, and the failure surfaced later as an opaque `TypeError` inside the consume loop.
