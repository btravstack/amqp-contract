---
"@amqp-contract/worker": patch
---

The `x-last-error` header stamped on a retry copy is truncated to 1024
characters. A handler error carrying a stack or payload dump could exceed the
broker's `frame_max` — a connection error on every retry attempt, a poison
loop.
